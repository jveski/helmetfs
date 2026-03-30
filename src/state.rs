//! Shared filesystem state: FsState and PathStateMap.
//!
//! `FsState` is the global context shared by FUSE callbacks, replication
//! workers, and the scrub thread.  It's stored in a `OnceLock<Arc<FsState>>`.
//!
//! `PathStateMap` tracks per-file metadata (write reference count, dirty flag)
//! used to coordinate between FUSE ops, checksumming, and replication.

use std::collections::HashMap;
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

#[derive(Debug, Clone)]
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
// FsState
// ---------------------------------------------------------------------------

pub struct FsState {
    pub backing_dir: PathBuf,
    pub replica_dir: PathBuf,
    pub repl_log: ReplLog,
    pub path_state: RwLock<HashMap<String, PathInfo>>,
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
            path_state: RwLock::new(HashMap::new()),
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
    // Write-ref tracking
    // -----------------------------------------------------------------------

    pub fn inc_write_ref(&self, rel: &str) {
        let mut map = self.path_state.write().unwrap();
        let info = map.entry(rel.to_string()).or_default();
        info.write_ref += 1;
    }

    pub fn dec_write_ref(&self, rel: &str) {
        let mut map = self.path_state.write().unwrap();
        if let Some(info) = map.get_mut(rel) {
            info.write_ref = info.write_ref.saturating_sub(1);
        }
    }

    pub fn mark_dirty(&self, rel: &str) {
        let mut map = self.path_state.write().unwrap();
        let info = map.entry(rel.to_string()).or_default();
        info.dirty = true;
    }

    pub fn get_path_info(&self, rel: &str) -> Option<PathInfo> {
        let map = self.path_state.read().unwrap();
        map.get(rel).cloned()
    }

    pub fn clear_dirty(&self, rel: &str) {
        let mut map = self.path_state.write().unwrap();
        if let Some(info) = map.get_mut(rel) {
            info.dirty = false;
        }
    }

    pub fn remove_path_state(&self, rel: &str) {
        let mut map = self.path_state.write().unwrap();
        map.remove(rel);
    }

    /// Returns true if the file currently has open write handles or is dirty.
    pub fn is_busy(&self, rel: &str) -> bool {
        let map = self.path_state.read().unwrap();
        if let Some(info) = map.get(rel) {
            info.write_ref > 0 || info.dirty
        } else {
            false
        }
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
    let should_checksum = {
        let map = state.path_state.read().unwrap();
        match map.get(rel) {
            Some(info) => info.write_ref == 0 && info.dirty,
            None => false,
        }
    };
    if should_checksum {
        checksum_and_enqueue(state, rel);
        true
    } else {
        false
    }
}
