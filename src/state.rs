//! Shared filesystem state: FsState, PathStateMap, and shared predicates.
//!
//! `FsState` is the global context shared by FUSE callbacks, replication
//! workers, and the scrub thread.  It's stored in a `OnceLock<Arc<FsState>>`.
//!
//! `PathStateMap` is a pure data structure (no I/O, no locking) that tracks
//! per-file metadata (write reference count, dirty flag) used to coordinate
//! between FUSE ops, checksumming, and replication.  It is shared between the
//! real implementation and the stateright model.

use std::collections::BTreeMap;
use std::path::PathBuf;
use std::sync::atomic::AtomicBool;
use std::sync::{Arc, OnceLock, RwLock};

use crate::helpers;
use crate::repl_log::ReplLog;

// ---------------------------------------------------------------------------
// Global state
// ---------------------------------------------------------------------------

static STATE: OnceLock<Arc<FsState>> = OnceLock::new();

pub fn set_global_state(state: Arc<FsState>) {
    STATE.set(state).unwrap_or_else(|_| panic!("global state already initialized"));
}

pub fn get_state() -> &'static Arc<FsState> {
    STATE.get().expect("global state not initialized")
}

// ---------------------------------------------------------------------------
// Per-path info
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct PathInfo {
    /// Number of open file handles with write access.
    pub write_ref: u32,
    /// File has been written to since last checksum.
    pub dirty: bool,
}

impl Default for PathInfo {
    fn default() -> Self {
        Self {
            write_ref: 0,
            dirty: false,
        }
    }
}

// ---------------------------------------------------------------------------
// PathStateMap — pure path-state tracking (no I/O, no synchronization)
// ---------------------------------------------------------------------------

/// Pure path-state tracking logic shared between the real implementation and
/// the stateright model.  Contains no I/O or locking.
#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub struct PathStateMap {
    map: BTreeMap<String, PathInfo>,
}

impl Default for PathStateMap {
    fn default() -> Self {
        Self::new()
    }
}

impl PathStateMap {
    pub fn new() -> Self {
        Self {
            map: BTreeMap::new(),
        }
    }

    pub fn inc_write_ref(&mut self, path: &str) {
        let info = self.map.entry(path.to_string()).or_default();
        info.write_ref += 1;
    }

    pub fn dec_write_ref(&mut self, path: &str) {
        if let Some(info) = self.map.get_mut(path) {
            info.write_ref = info.write_ref.saturating_sub(1);
        }
    }

    pub fn mark_dirty(&mut self, path: &str) {
        let info = self.map.entry(path.to_string()).or_default();
        info.dirty = true;
    }

    pub fn clear_dirty(&mut self, path: &str) {
        if let Some(info) = self.map.get_mut(path) {
            info.dirty = false;
        }
    }

    /// Returns true if the file currently has open write handles or is dirty.
    pub fn is_busy(&self, path: &str) -> bool {
        if let Some(info) = self.map.get(path) {
            info.write_ref > 0 || info.dirty
        } else {
            false
        }
    }

    pub fn get(&self, path: &str) -> Option<&PathInfo> {
        self.map.get(path)
    }

    pub fn get_write_ref(&self, path: &str) -> u32 {
        self.map.get(path).map(|i| i.write_ref).unwrap_or(0)
    }

    pub fn remove(&mut self, path: &str) {
        self.map.remove(path);
    }

    /// Returns true if the file has no open write handles and is dirty,
    /// i.e. it should be checksummed and enqueued for replication.
    pub fn should_checksum(&self, path: &str) -> bool {
        if let Some(info) = self.map.get(path) {
            info.write_ref == 0 && info.dirty
        } else {
            false
        }
    }

    /// Transfer path state from one path to another (used in rename).
    pub fn transfer(&mut self, from: &str, to: &str) {
        if let Some(info) = self.map.remove(from) {
            self.map.insert(to.to_string(), info);
        }
    }

    /// Check if all tracked paths are idle (no writers, not dirty).
    pub fn all_idle(&self) -> bool {
        self.map
            .values()
            .all(|info| info.write_ref == 0 && !info.dirty)
    }

    /// Iterate over all (path, PathInfo) entries.
    pub fn iter(&self) -> impl Iterator<Item = (&String, &PathInfo)> {
        self.map.iter()
    }

    /// Iterate over all PathInfo values.
    pub fn values(&self) -> impl Iterator<Item = &PathInfo> {
        self.map.values()
    }

    /// Clear all entries (used for crash recovery in model).
    pub fn clear(&mut self) {
        self.map.clear();
    }
}

// ---------------------------------------------------------------------------
// Shared predicates
// ---------------------------------------------------------------------------

/// Predicate: should a replica delete be skipped because the backing file
/// was re-created and checksummed since the delete was enqueued?
///
/// With multiple worker threads, a delete entry claimed before a subsequent
/// re-create+put can execute *after* the put, wiping out the replica copy.
/// This guard prevents that race.
pub fn should_skip_delete(file_exists: bool, sum_exists: bool) -> bool {
    file_exists && sum_exists
}

/// Predicate: is a file eligible for healing from the replica?
///
/// A file can only be healed if there is no pending put (which would
/// overwrite the replica with a newer version) and the file is not
/// currently being written to (busy).
pub fn can_heal(has_pending_put: bool, is_busy: bool) -> bool {
    !has_pending_put && !is_busy
}

// ---------------------------------------------------------------------------
// FsState
// ---------------------------------------------------------------------------

pub struct FsState {
    pub backing_dir: PathBuf,
    pub replica_dir: PathBuf,
    pub repl_log: ReplLog,
    pub path_state: RwLock<PathStateMap>,
    pub shutting_down: AtomicBool,
    pub scrub_interval_secs: u64,
}

impl FsState {
    pub fn new(
        backing_dir: PathBuf,
        replica_dir: PathBuf,
        scrub_interval_secs: u64,
    ) -> std::io::Result<Self> {
        let repl_log = ReplLog::new(&backing_dir)?;
        Ok(Self {
            backing_dir,
            replica_dir,
            repl_log,
            path_state: RwLock::new(PathStateMap::new()),
            shutting_down: AtomicBool::new(false),
            scrub_interval_secs,
        })
    }

    /// Resolve a relative path against the backing directory.
    pub fn backing_path(&self, rel: &str) -> PathBuf {
        self.backing_dir.join(rel)
    }

    /// Resolve a relative path against the replica files directory.
    pub fn replica_file_path(&self, rel: &str) -> PathBuf {
        self.replica_dir.join("files").join(rel)
    }

    // -----------------------------------------------------------------------
    // Write-ref tracking (delegates to PathStateMap under RwLock)
    // -----------------------------------------------------------------------

    pub fn inc_write_ref(&self, rel: &str) {
        self.path_state.write().unwrap().inc_write_ref(rel);
    }

    pub fn dec_write_ref(&self, rel: &str) {
        self.path_state.write().unwrap().dec_write_ref(rel);
    }

    pub fn mark_dirty(&self, rel: &str) {
        self.path_state.write().unwrap().mark_dirty(rel);
    }

    pub fn get_path_info(&self, rel: &str) -> Option<PathInfo> {
        self.path_state.read().unwrap().get(rel).cloned()
    }

    pub fn clear_dirty(&self, rel: &str) {
        self.path_state.write().unwrap().clear_dirty(rel);
    }

    pub fn remove_path_state(&self, rel: &str) {
        self.path_state.write().unwrap().remove(rel);
    }

    /// Returns true if the file currently has open write handles or is dirty.
    pub fn is_busy(&self, rel: &str) -> bool {
        self.path_state.read().unwrap().is_busy(rel)
    }

    /// Returns true if the file should be checksummed (write_ref==0 && dirty).
    pub fn should_checksum(&self, rel: &str) -> bool {
        self.path_state.read().unwrap().should_checksum(rel)
    }

    /// Transfer path state from one path to another (used in rename).
    pub fn transfer_path_state(&self, from: &str, to: &str) {
        self.path_state.write().unwrap().transfer(from, to);
    }
}

// ---------------------------------------------------------------------------
// Checksum-and-enqueue helper
// ---------------------------------------------------------------------------

/// Compute BLAKE3 checksum for `rel` path, write the `.sum` sidecar, and
/// enqueue a `put` in the replication log. Clears the dirty flag on success.
pub fn checksum_and_enqueue(state: &FsState, rel: &str) {
    let abs = state.backing_path(rel);
    if !abs.is_file() {
        return;
    }

    match helpers::compute_blake3(&abs) {
        Ok(hex) => {
            let sum_path = helpers::sum_path_for(&abs);
            if let Err(e) = helpers::write_sum_file(&sum_path, &hex) {
                log::error!("Failed to write .sum for {}: {}", rel, e);
                return;
            }
            state.clear_dirty(rel);
            state.repl_log.enqueue_put(rel);
            log::debug!("Checksummed and enqueued put: {}", rel);
        }
        Err(e) => {
            log::error!("Failed to compute checksum for {}: {}", rel, e);
        }
    }
}

/// Checksum-and-enqueue only if the file is not currently being written to.
/// Returns true if checksumming was performed.
pub fn checksum_if_idle(state: &FsState, rel: &str) -> bool {
    if state.should_checksum(rel) {
        checksum_and_enqueue(state, rel);
        true
    } else {
        false
    }
}
