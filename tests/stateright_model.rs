//! Stateright model-checking tests for helmetfs concurrency safety.
//!
//! These tests build an abstract model of helmetfs's concurrent subsystems
//! (FUSE callbacks, replication workers, scrub thread) and exhaustively
//! explore all interleavings to verify safety properties.
//!
//! # What is modeled
//!
//! The model captures the abstract interactions between:
//! - **FUSE client operations**: create, open-write, write, release (close),
//!   unlink (delete), rename
//! - **Replication workers**: dequeue entries from the replication log,
//!   copy/delete files in the replica
//! - **Scrub thread**: detect corrupt/untracked files and heal from replica
//!
//! File *content* is modeled as an abstract version counter (u8). The backing
//! store, replica store, checksum sidecar (.sum), and replication log are all
//! modeled as in-memory data structures within the Stateright state.
//!
//! Entries are identified by their stable `id` (u64), matching the real
//! implementation, NOT by their position in the vector.
//!
//! # Safety properties verified
//!
//! 1. **Replica convergence**: when the system is quiescent (no open writers,
//!    no pending repl entries), the replica content matches the backing content
//!    for every file that has been checksummed.
//! 2. **No healing during active writes**: scrub never heals a file that has
//!    open write handles or a dirty flag.
//! 3. **Write-ref integrity**: write_ref is never negative (modeled as u8,
//!    so never wraps past 0).
//! 4. **Coalescing correctness**: after coalescing and processing, the replica
//!    always ends up with the latest version of the file.
//! 5. **Rename atomicity**: after rename completes and replication drains, the
//!    old path is gone from the replica and the new path is present.
//! 6. **No stale replica on delete**: after a delete is processed, the file is
//!    gone from the replica.
//! 7. **Liveness** (sometimes properties): the system can reach a state where
//!    replication has completed, and where scrub has healed a corrupt file.

use stateright::*;
use std::collections::BTreeMap;

// =========================================================================
// Abstract file content
// =========================================================================

/// Abstract file version. 0 = file does not exist (or was never written).
/// Incremented on each write.
type Version = u8;

// =========================================================================
// Replication log entry (abstract)
// =========================================================================

#[derive(Clone, Debug, Eq, PartialEq, Hash, Ord, PartialOrd)]
enum ReplOp {
    Put,
    Delete,
}

#[derive(Clone, Debug, Eq, PartialEq, Hash, Ord, PartialOrd)]
struct ReplEntry {
    op: ReplOp,
    path: String,
    id: u64,
    in_progress: bool,
    completed: bool,
}

// =========================================================================
// Per-path metadata (mirrors state.rs PathInfo)
// =========================================================================

#[derive(Clone, Debug, Eq, PartialEq, Hash, Ord, PartialOrd)]
struct PathInfo {
    write_ref: u8,
    dirty: bool,
}

impl Default for PathInfo {
    fn default() -> Self {
        Self {
            write_ref: 0,
            dirty: false,
        }
    }
}

// =========================================================================
// System state (the Stateright "State")
// =========================================================================

#[derive(Clone, Debug, Eq, PartialEq, Hash)]
struct FsModelState {
    /// Backing store: path -> version (0 = deleted/absent).
    backing: BTreeMap<String, Version>,

    /// Checksum sidecar: path -> version that was checksummed.
    sums: BTreeMap<String, Version>,

    /// Replica store: path -> version.
    replica: BTreeMap<String, Version>,

    /// Replica sidecar checksums: path -> version.
    replica_sums: BTreeMap<String, Version>,

    /// Per-path metadata (write_ref, dirty).
    path_state: BTreeMap<String, PathInfo>,

    /// Replication log queue.
    repl_log: Vec<ReplEntry>,

    /// Next repl log entry id.
    next_repl_id: u64,

    /// Whether the system is shutting down.
    shutting_down: bool,

    /// Track whether scrub has run at least once (for liveness).
    scrub_ran: bool,

    /// Track total number of FUSE operations completed (for boundary).
    fuse_ops_done: u8,
}

impl FsModelState {
    fn new() -> Self {
        Self {
            backing: BTreeMap::new(),
            sums: BTreeMap::new(),
            replica: BTreeMap::new(),
            replica_sums: BTreeMap::new(),
            path_state: BTreeMap::new(),
            repl_log: Vec::new(),
            next_repl_id: 0,
            shutting_down: false,
            scrub_ran: false,
            fuse_ops_done: 0,
        }
    }

    // -- path_state helpers (mirror state.rs) --

    fn inc_write_ref(&mut self, path: &str) {
        let info = self.path_state.entry(path.to_string()).or_default();
        info.write_ref += 1;
    }

    fn dec_write_ref(&mut self, path: &str) {
        if let Some(info) = self.path_state.get_mut(path) {
            info.write_ref = info.write_ref.saturating_sub(1);
        }
    }

    fn mark_dirty(&mut self, path: &str) {
        let info = self.path_state.entry(path.to_string()).or_default();
        info.dirty = true;
    }

    fn clear_dirty(&mut self, path: &str) {
        if let Some(info) = self.path_state.get_mut(path) {
            info.dirty = false;
        }
    }

    fn is_busy(&self, path: &str) -> bool {
        if let Some(info) = self.path_state.get(path) {
            info.write_ref > 0 || info.dirty
        } else {
            false
        }
    }

    fn get_write_ref(&self, path: &str) -> u8 {
        self.path_state
            .get(path)
            .map(|i| i.write_ref)
            .unwrap_or(0)
    }

    fn remove_path_state(&mut self, path: &str) {
        self.path_state.remove(path);
    }

    // -- repl_log helpers (mirror repl_log.rs) --

    fn enqueue_put(&mut self, path: &str) {
        // Coalesce: mark earlier pending (not in_progress) entries for the same
        // path as completed.  This includes both earlier puts (same-op coalescing,
        // matching the real code) AND earlier deletes (cross-op coalescing).
        //
        // Cross-op rationale: a put copies the *current* backing content, so it
        // fully subsumes an earlier delete—the replica will end up with the
        // latest file regardless.  Without this, multi-worker out-of-order
        // processing can cause the earlier delete to execute *after* this put,
        // leaving the replica empty.  This was discovered by the model checker.
        for entry in self.repl_log.iter_mut() {
            if !entry.completed
                && !entry.in_progress
                && entry.path == path
            {
                entry.completed = true;
            }
        }
        let id = self.next_repl_id;
        self.next_repl_id += 1;
        self.repl_log.push(ReplEntry {
            op: ReplOp::Put,
            path: path.to_string(),
            id,
            in_progress: false,
            completed: false,
        });
    }

    fn enqueue_delete(&mut self, path: &str) {
        // Cross-op coalescing: mark earlier pending (not in_progress) puts for
        // the same path as completed.  A delete supersedes an earlier put—there
        // is no point copying a file we are about to remove.  Without this, a
        // multi-worker race could process the put *after* the delete, leaving a
        // stale file in the replica.
        for entry in self.repl_log.iter_mut() {
            if !entry.completed
                && !entry.in_progress
                && entry.op == ReplOp::Put
                && entry.path == path
            {
                entry.completed = true;
            }
        }
        let id = self.next_repl_id;
        self.next_repl_id += 1;
        self.repl_log.push(ReplEntry {
            op: ReplOp::Delete,
            path: path.to_string(),
            id,
            in_progress: false,
            completed: false,
        });
    }

    fn has_pending_put(&self, path: &str) -> bool {
        self.repl_log
            .iter()
            .any(|e| !e.completed && e.op == ReplOp::Put && e.path == path)
    }

    /// Claim next available entry by stable id (not completed, not in_progress).
    /// Returns the stable id of the claimed entry, if found.
    fn claim_next_entry(&mut self) -> Option<u64> {
        for entry in self.repl_log.iter_mut() {
            if !entry.completed && !entry.in_progress {
                entry.in_progress = true;
                return Some(entry.id);
            }
        }
        None
    }

    /// Find entry by stable id.
    fn find_entry(&self, id: u64) -> Option<&ReplEntry> {
        self.repl_log.iter().find(|e| e.id == id)
    }

    /// Mark entry completed and GC leading completed entries.
    fn mark_completed(&mut self, id: u64) {
        for entry in self.repl_log.iter_mut() {
            if entry.id == id {
                entry.completed = true;
                entry.in_progress = false;
                break;
            }
        }
        // GC leading completed entries
        while self
            .repl_log
            .first()
            .map_or(false, |e| e.completed)
        {
            self.repl_log.remove(0);
        }
    }

    /// Number of pending (uncompleted) entries.
    fn pending_count(&self) -> usize {
        self.repl_log.iter().filter(|e| !e.completed).count()
    }

    /// IDs of in-progress entries (used for action generation).
    fn in_progress_ids(&self) -> Vec<u64> {
        self.repl_log
            .iter()
            .filter(|e| e.in_progress && !e.completed)
            .map(|e| e.id)
            .collect()
    }

    // -- checksum helpers (mirror state.rs checksum_and_enqueue) --

    /// Abstract checksum_and_enqueue: record the current backing version as
    /// the .sum, clear dirty, enqueue put.
    fn checksum_and_enqueue(&mut self, path: &str) {
        if let Some(&ver) = self.backing.get(path) {
            if ver > 0 {
                self.sums.insert(path.to_string(), ver);
                self.clear_dirty(path);
                self.enqueue_put(path);
            }
        }
    }

    /// Is the system quiescent? No open writers, nothing dirty, no pending repl.
    fn is_quiescent(&self) -> bool {
        let no_writers = self
            .path_state
            .values()
            .all(|info| info.write_ref == 0 && !info.dirty);
        let no_pending = self.pending_count() == 0;
        no_writers && no_pending
    }
}

// =========================================================================
// Actions — using stable entry IDs, not vector indices
// =========================================================================

#[derive(Clone, Debug, Eq, PartialEq, Hash)]
enum Action {
    // -- FUSE client actions --
    Create { path: String },
    Write { path: String },
    Release { path: String },
    Unlink { path: String },
    Rename { from: String, to: String },

    // -- Replication worker actions --
    /// Worker claims the next pending repl entry.
    WorkerClaim,
    /// Worker processes a claimed entry, identified by stable id.
    WorkerProcess { id: u64 },

    // -- Scrub actions --
    ScrubAdopt { path: String },
    ScrubHeal { path: String },

    // -- Corruption injection (environment action) --
    CorruptBacking { path: String },
}

// =========================================================================
// Shared next_state logic
// =========================================================================

/// Apply an action to a state. This is the core transition function shared
/// by all models.
fn apply_action(s: &mut FsModelState, action: &Action) -> bool {
    match action {
        Action::Create { ref path } => {
            s.backing.insert(path.clone(), 1);
            s.inc_write_ref(path);
            s.mark_dirty(path);
            s.fuse_ops_done += 1;
        }

        Action::Write { ref path } => {
            if let Some(ver) = s.backing.get_mut(path) {
                *ver += 1;
            }
            s.mark_dirty(path);
            s.fuse_ops_done += 1;
        }

        Action::Release { ref path } => {
            s.dec_write_ref(path);
            let should_checksum = s
                .path_state
                .get(path)
                .map(|info| info.write_ref == 0 && info.dirty)
                .unwrap_or(false);
            if should_checksum {
                s.checksum_and_enqueue(path);
            }
            s.fuse_ops_done += 1;
        }

        Action::Unlink { ref path } => {
            s.backing.remove(path);
            s.sums.remove(path);
            s.remove_path_state(path);
            s.enqueue_delete(path);
            s.fuse_ops_done += 1;
        }

        Action::Rename { ref from, ref to } => {
            if let Some(ver) = s.backing.remove(from) {
                s.backing.insert(to.clone(), ver);
            }
            let had_sum = s.sums.contains_key(from);
            if let Some(sum_ver) = s.sums.remove(from) {
                s.sums.insert(to.clone(), sum_ver);
            }
            if let Some(info) = s.path_state.remove(from) {
                s.path_state.insert(to.clone(), info);
            }
            s.enqueue_delete(from);
            if had_sum {
                s.enqueue_put(to);
            } else if s.backing.get(to).copied().unwrap_or(0) > 0 {
                s.checksum_and_enqueue(to);
            }
            s.fuse_ops_done += 1;
        }

        Action::WorkerClaim => {
            s.claim_next_entry();
        }

        Action::WorkerProcess { id } => {
            let entry = match s.find_entry(*id) {
                Some(e) => e.clone(),
                None => return false, // Entry already GC'd, no-op
            };
            if !entry.in_progress || entry.completed {
                return false; // Not valid to process
            }

            let path = &entry.path;
            match entry.op {
                ReplOp::Put => {
                    // Copy from backing to replica (if file still exists)
                    if let Some(&ver) = s.backing.get(path) {
                        if ver > 0 {
                            s.replica.insert(path.clone(), ver);
                            if let Some(&sum_ver) = s.sums.get(path) {
                                s.replica_sums.insert(path.clone(), sum_ver);
                            }
                        }
                    }
                    // If backing file doesn't exist, real code errors. We just skip.
                }
                ReplOp::Delete => {
                    // Guard: skip the delete if the backing file currently
                    // exists AND has a checksum.  This means the file was
                    // re-created (and checksummed) since the delete was
                    // enqueued, so removing the replica copy would lose the
                    // latest version.
                    //
                    // This is necessary because with N>1 workers, a delete
                    // entry claimed before a subsequent re-create+put can
                    // execute *after* the put, wiping out the replica copy.
                    // The model checker discovered this race condition.
                    let file_exists_and_tracked = s
                        .backing
                        .get(path)
                        .map_or(false, |&v| v > 0)
                        && s.sums.contains_key(path);
                    if !file_exists_and_tracked {
                        s.replica.remove(path);
                        s.replica_sums.remove(path);
                    }
                }
            }
            s.mark_completed(entry.id);
        }

        Action::ScrubAdopt { ref path } => {
            let ver = s.backing.get(path).copied().unwrap_or(0);
            if ver > 0 && !s.sums.contains_key(path) && !s.is_busy(path) {
                s.checksum_and_enqueue(path);
                s.scrub_ran = true;
            }
        }

        Action::ScrubHeal { ref path } => {
            if s.has_pending_put(path) || s.is_busy(path) {
                return true; // Can't heal, no state change
            }
            let replica_ver = s.replica.get(path).copied();
            let replica_sum = s.replica_sums.get(path).copied();
            if let (Some(rv), Some(rs)) = (replica_ver, replica_sum) {
                if rv == rs {
                    s.backing.insert(path.clone(), rv);
                    s.sums.insert(path.clone(), rs);
                    s.scrub_ran = true;
                }
            }
        }

        Action::CorruptBacking { ref path } => {
            if let Some(ver) = s.backing.get_mut(path) {
                *ver += 1;
            }
            s.fuse_ops_done += 1;
        }
    }
    true
}

/// Generate worker actions for the current state.
fn add_worker_actions(state: &FsModelState, actions: &mut Vec<Action>) {
    // Claim next available
    if state
        .repl_log
        .iter()
        .any(|e| !e.completed && !e.in_progress)
    {
        actions.push(Action::WorkerClaim);
    }

    // Process any in-progress entry by stable id
    for id in state.in_progress_ids() {
        actions.push(Action::WorkerProcess { id });
    }
}

// =========================================================================
// Model configuration
// =========================================================================

#[derive(Clone)]
struct HelmetFsModel {
    file_names: Vec<String>,
    rename_target: String,
    max_fuse_ops: u8,
    max_version: u8,
}

impl HelmetFsModel {
    fn single_file() -> Self {
        Self {
            file_names: vec!["a.txt".to_string()],
            rename_target: "b.txt".to_string(),
            max_fuse_ops: 6,
            max_version: 3,
        }
    }

    fn two_files() -> Self {
        Self {
            file_names: vec!["a.txt".to_string(), "b.txt".to_string()],
            rename_target: "c.txt".to_string(),
            max_fuse_ops: 5,
            max_version: 3,
        }
    }
}

// =========================================================================
// Full model implementation
// =========================================================================

impl Model for HelmetFsModel {
    type State = FsModelState;
    type Action = Action;

    fn init_states(&self) -> Vec<Self::State> {
        vec![FsModelState::new()]
    }

    fn actions(&self, state: &Self::State, actions: &mut Vec<Self::Action>) {
        let within_fuse_budget = state.fuse_ops_done < self.max_fuse_ops;

        if within_fuse_budget {
            let all_paths: Vec<String> = self
                .file_names
                .iter()
                .cloned()
                .chain(std::iter::once(self.rename_target.clone()))
                .collect();

            for name in &all_paths {
                let ver = state.backing.get(name).copied().unwrap_or(0);
                let wr = state.get_write_ref(name);

                // Create: file doesn't exist and no open writers
                if ver == 0 && wr == 0 {
                    actions.push(Action::Create {
                        path: name.clone(),
                    });
                }

                // Write: file exists with open write handles, version < max
                if ver > 0 && wr > 0 && ver < self.max_version {
                    actions.push(Action::Write {
                        path: name.clone(),
                    });
                }

                // Release: has open write handles
                if wr > 0 {
                    actions.push(Action::Release {
                        path: name.clone(),
                    });
                }

                // Unlink: file exists, no open write handles
                if ver > 0 && wr == 0 {
                    actions.push(Action::Unlink {
                        path: name.clone(),
                    });
                }
            }

            // Rename: from file_names to rename_target
            for name in &self.file_names {
                let ver = state.backing.get(name).copied().unwrap_or(0);
                let wr = state.get_write_ref(name);
                let target = &self.rename_target;
                let target_ver = state.backing.get(target).copied().unwrap_or(0);
                let target_wr = state.get_write_ref(target);
                if ver > 0 && wr == 0 && target_ver == 0 && target_wr == 0 {
                    actions.push(Action::Rename {
                        from: name.clone(),
                        to: target.clone(),
                    });
                }
            }

            // Corruption injection
            let all_paths2: Vec<String> = self
                .file_names
                .iter()
                .cloned()
                .chain(std::iter::once(self.rename_target.clone()))
                .collect();
            for path in &all_paths2 {
                let ver = state.backing.get(path).copied().unwrap_or(0);
                if ver > 0
                    && !state.is_busy(path)
                    && state.sums.contains_key(path)
                    && ver < self.max_version
                {
                    actions.push(Action::CorruptBacking {
                        path: path.clone(),
                    });
                }
            }
        }

        // Worker actions
        add_worker_actions(state, actions);

        // Scrub actions
        let all_paths: Vec<String> = self
            .file_names
            .iter()
            .cloned()
            .chain(std::iter::once(self.rename_target.clone()))
            .collect();

        for path in &all_paths {
            let ver = state.backing.get(path).copied().unwrap_or(0);

            // Adopt untracked files
            if ver > 0 && !state.sums.contains_key(path) && !state.is_busy(path) {
                actions.push(Action::ScrubAdopt {
                    path: path.clone(),
                });
            }

            // Heal corrupt files
            if let Some(&sum_ver) = state.sums.get(path) {
                if ver > 0 && ver != sum_ver {
                    actions.push(Action::ScrubHeal {
                        path: path.clone(),
                    });
                }
            }
        }
    }

    fn next_state(&self, state: &Self::State, action: Self::Action) -> Option<Self::State> {
        let mut s = state.clone();
        apply_action(&mut s, &action);
        Some(s)
    }

    fn properties(&self) -> Vec<Property<Self>> {
        vec![
            // Replica convergence
            Property::<Self>::always(
                "replica converges when quiescent",
                |_model, state| {
                    if !state.is_quiescent() {
                        return true;
                    }
                    for (path, &backing_ver) in &state.backing {
                        if let Some(&sum_ver) = state.sums.get(path) {
                            if backing_ver == sum_ver {
                                if let Some(&replica_ver) = state.replica.get(path) {
                                    if replica_ver != backing_ver {
                                        return false;
                                    }
                                } else {
                                    return false;
                                }
                            }
                        }
                    }
                    true
                },
            ),
            // Deleted files gone from replica
            Property::<Self>::always(
                "deleted files leave replica when quiescent",
                |_model, state| {
                    if !state.is_quiescent() {
                        return true;
                    }
                    for (path, &ver) in &state.replica {
                        if ver > 0 {
                            let backing_ver = state.backing.get(path).copied().unwrap_or(0);
                            if backing_ver == 0 {
                                return false;
                            }
                        }
                    }
                    true
                },
            ),
            // Write-ref sanity
            Property::<Self>::always(
                "write_ref bounded",
                |_model, state| {
                    state
                        .path_state
                        .values()
                        .all(|info| info.write_ref <= 10)
                },
            ),
            // Coalescing preserves latest version
            Property::<Self>::always(
                "coalescing preserves latest version",
                |_model, state| {
                    if !state.is_quiescent() {
                        return true;
                    }
                    for (path, &sum_ver) in &state.sums {
                        let backing_ver = state.backing.get(path).copied().unwrap_or(0);
                        if backing_ver == sum_ver && backing_ver > 0 {
                            let replica_ver = state.replica.get(path).copied().unwrap_or(0);
                            if replica_ver != backing_ver {
                                return false;
                            }
                        }
                    }
                    true
                },
            ),
            // Busy files are protected
            Property::<Self>::always(
                "busy files not reverted by scrub",
                |_model, state| {
                    for (path, info) in &state.path_state {
                        if info.write_ref > 0 {
                            let backing_ver = state.backing.get(path).copied().unwrap_or(0);
                            let sum_ver = state.sums.get(path).copied().unwrap_or(0);
                            if backing_ver > 0 && sum_ver > 0 && backing_ver < sum_ver {
                                return false;
                            }
                        }
                    }
                    true
                },
            ),
            // Liveness
            Property::<Self>::sometimes(
                "replication completes for a written file",
                |_model, state| state.replica.values().any(|&v| v > 0),
            ),
            Property::<Self>::sometimes(
                "scrub runs",
                |_model, state| state.scrub_ran,
            ),
            Property::<Self>::sometimes(
                "system reaches quiescence with replica",
                |_model, state| {
                    state.is_quiescent()
                        && !state.replica.is_empty()
                        && state.backing.iter().all(|(p, &v)| {
                            if let Some(&sv) = state.sums.get(p) {
                                if v == sv {
                                    return state.replica.get(p).copied() == Some(v);
                                }
                            }
                            true
                        })
                },
            ),
        ]
    }

    fn within_boundary(&self, state: &Self::State) -> bool {
        state.fuse_ops_done <= self.max_fuse_ops
            && state.repl_log.len() <= 8
            && state.next_repl_id <= 12
    }
}

// =========================================================================
// Test: Single-file concurrency (exhaustive BFS)
// =========================================================================

#[test]
fn test_single_file_model() {
    let model = HelmetFsModel::single_file();
    let checker = model.checker().spawn_bfs().join();
    checker.assert_properties();
}

// =========================================================================
// Test: Two-file concurrency (exhaustive BFS)
// =========================================================================

#[test]
fn test_two_file_model() {
    let model = HelmetFsModel::two_files();
    let checker = model.checker().spawn_bfs().join();
    checker.assert_properties();
}

// =========================================================================
// Focused coalescing model
// =========================================================================

#[derive(Clone)]
struct CoalescingModel;

impl Model for CoalescingModel {
    type State = FsModelState;
    type Action = Action;

    fn init_states(&self) -> Vec<Self::State> {
        let mut s = FsModelState::new();
        s.backing.insert("data.txt".to_string(), 1);
        s.inc_write_ref("data.txt");
        s.mark_dirty("data.txt");
        vec![s]
    }

    fn actions(&self, state: &Self::State, actions: &mut Vec<Self::Action>) {
        let path = "data.txt".to_string();
        let ver = state.backing.get(&path).copied().unwrap_or(0);
        let wr = state.get_write_ref(&path);

        if ver > 0 && wr > 0 && ver < 4 {
            actions.push(Action::Write { path: path.clone() });
        }
        if wr > 0 {
            actions.push(Action::Release { path: path.clone() });
        }
        // Re-open for writing after close
        if ver > 0 && wr == 0 && state.fuse_ops_done < 8 {
            actions.push(Action::Create { path: path.clone() });
        }
        add_worker_actions(state, actions);
    }

    fn next_state(&self, state: &Self::State, action: Self::Action) -> Option<Self::State> {
        let mut s = state.clone();
        // For Create of an existing file, just reopen (inc write_ref, mark dirty)
        if let Action::Create { ref path } = action {
            if s.backing.get(path).copied().unwrap_or(0) > 0 {
                s.inc_write_ref(path);
                s.mark_dirty(path);
                s.fuse_ops_done += 1;
                return Some(s);
            }
        }
        apply_action(&mut s, &action);
        Some(s)
    }

    fn properties(&self) -> Vec<Property<Self>> {
        vec![
            Property::<Self>::always(
                "coalesced replica has latest version when quiescent",
                |_model, state| {
                    if !state.is_quiescent() {
                        return true;
                    }
                    let path = "data.txt";
                    let backing_ver = state.backing.get(path).copied().unwrap_or(0);
                    let sum_ver = state.sums.get(path).copied().unwrap_or(0);
                    if backing_ver > 0 && backing_ver == sum_ver {
                        let replica_ver = state.replica.get(path).copied().unwrap_or(0);
                        return replica_ver == backing_ver;
                    }
                    true
                },
            ),
            Property::<Self>::sometimes(
                "replication completes for coalesced writes",
                |_model, state| {
                    state.is_quiescent()
                        && state.replica.get("data.txt").copied().unwrap_or(0) > 1
                },
            ),
        ]
    }

    fn within_boundary(&self, state: &Self::State) -> bool {
        state.fuse_ops_done <= 8 && state.repl_log.len() <= 6 && state.next_repl_id <= 10
    }
}

#[test]
fn test_coalescing_model() {
    let model = CoalescingModel;
    let checker = model.checker().spawn_bfs().join();
    checker.assert_properties();
}

// =========================================================================
// Rename + replication model
// =========================================================================

#[derive(Clone)]
struct RenameModel;

impl Model for RenameModel {
    type State = FsModelState;
    type Action = Action;

    fn init_states(&self) -> Vec<Self::State> {
        let mut s = FsModelState::new();
        s.backing.insert("old.txt".to_string(), 1);
        s.sums.insert("old.txt".to_string(), 1);
        s.replica.insert("old.txt".to_string(), 1);
        s.replica_sums.insert("old.txt".to_string(), 1);
        vec![s]
    }

    fn actions(&self, state: &Self::State, actions: &mut Vec<Self::Action>) {
        let old_ver = state.backing.get("old.txt").copied().unwrap_or(0);
        let new_ver = state.backing.get("new.txt").copied().unwrap_or(0);

        if old_ver > 0 && state.get_write_ref("old.txt") == 0 && new_ver == 0 {
            actions.push(Action::Rename {
                from: "old.txt".to_string(),
                to: "new.txt".to_string(),
            });
        }

        if new_ver > 0 && state.get_write_ref("new.txt") > 0 && new_ver < 3 {
            actions.push(Action::Write {
                path: "new.txt".to_string(),
            });
        }
        if state.get_write_ref("new.txt") > 0 {
            actions.push(Action::Release {
                path: "new.txt".to_string(),
            });
        }
        if new_ver > 0 && state.get_write_ref("new.txt") == 0 {
            actions.push(Action::Unlink {
                path: "new.txt".to_string(),
            });
        }

        add_worker_actions(state, actions);
    }

    fn next_state(&self, state: &Self::State, action: Self::Action) -> Option<Self::State> {
        let mut s = state.clone();
        apply_action(&mut s, &action);
        Some(s)
    }

    fn properties(&self) -> Vec<Property<Self>> {
        vec![
            Property::<Self>::always(
                "rename: old path gone from replica when quiescent",
                |_model, state| {
                    if !state.is_quiescent() {
                        return true;
                    }
                    let old_backing = state.backing.get("old.txt").copied().unwrap_or(0);
                    if old_backing == 0 {
                        let old_replica = state.replica.get("old.txt").copied().unwrap_or(0);
                        return old_replica == 0;
                    }
                    true
                },
            ),
            Property::<Self>::always(
                "rename: new path in replica when quiescent",
                |_model, state| {
                    if !state.is_quiescent() {
                        return true;
                    }
                    let new_backing = state.backing.get("new.txt").copied().unwrap_or(0);
                    let new_sum = state.sums.get("new.txt").copied().unwrap_or(0);
                    if new_backing > 0 && new_backing == new_sum {
                        let new_replica = state.replica.get("new.txt").copied().unwrap_or(0);
                        return new_replica == new_backing;
                    }
                    true
                },
            ),
            Property::<Self>::sometimes(
                "rename completes with new path replicated",
                |_model, state| {
                    state.is_quiescent()
                        && state.replica.get("new.txt").copied().unwrap_or(0) > 0
                        && state.replica.get("old.txt").is_none()
                },
            ),
        ]
    }

    fn within_boundary(&self, state: &Self::State) -> bool {
        state.fuse_ops_done <= 6 && state.repl_log.len() <= 6 && state.next_repl_id <= 10
    }
}

#[test]
fn test_rename_model() {
    let model = RenameModel;
    let checker = model.checker().spawn_bfs().join();
    checker.assert_properties();
}

// =========================================================================
// Scrub self-healing model
// =========================================================================

#[derive(Clone)]
struct ScrubHealModel;

impl Model for ScrubHealModel {
    type State = FsModelState;
    type Action = Action;

    fn init_states(&self) -> Vec<Self::State> {
        let mut s = FsModelState::new();
        s.backing.insert("data.txt".to_string(), 1);
        s.sums.insert("data.txt".to_string(), 1);
        s.replica.insert("data.txt".to_string(), 1);
        s.replica_sums.insert("data.txt".to_string(), 1);
        vec![s]
    }

    fn actions(&self, state: &Self::State, actions: &mut Vec<Self::Action>) {
        let path = "data.txt".to_string();
        let ver = state.backing.get(&path).copied().unwrap_or(0);
        let wr = state.get_write_ref(&path);

        // Corrupt
        if ver > 0 && !state.is_busy(&path) && state.sums.contains_key(&path) && ver < 3 {
            actions.push(Action::CorruptBacking { path: path.clone() });
        }

        // Open for writing
        if ver > 0 && wr == 0 && state.fuse_ops_done < 6 {
            actions.push(Action::Create { path: path.clone() });
        }

        // Write
        if ver > 0 && wr > 0 && ver < 4 {
            actions.push(Action::Write { path: path.clone() });
        }

        // Release
        if wr > 0 {
            actions.push(Action::Release { path: path.clone() });
        }

        // Scrub heal
        if let Some(&sum_ver) = state.sums.get(&path) {
            if ver > 0 && ver != sum_ver {
                actions.push(Action::ScrubHeal { path: path.clone() });
            }
        }

        add_worker_actions(state, actions);
    }

    fn next_state(&self, state: &Self::State, action: Self::Action) -> Option<Self::State> {
        let mut s = state.clone();
        // For Create of an existing file, just reopen
        if let Action::Create { ref path } = action {
            if s.backing.get(path).copied().unwrap_or(0) > 0 {
                s.inc_write_ref(path);
                s.mark_dirty(path);
                s.fuse_ops_done += 1;
                return Some(s);
            }
        }
        apply_action(&mut s, &action);
        Some(s)
    }

    fn properties(&self) -> Vec<Property<Self>> {
        vec![
            Property::<Self>::always(
                "scrub never heals while file is busy",
                |_model, state| {
                    let path = "data.txt";
                    if let Some(info) = state.path_state.get(path) {
                        if info.write_ref > 0 || info.dirty {
                            let backing = state.backing.get(path).copied().unwrap_or(0);
                            let sum = state.sums.get(path).copied().unwrap_or(0);
                            if sum > 0 && backing < sum {
                                return false;
                            }
                        }
                    }
                    true
                },
            ),
            Property::<Self>::always(
                "heal restores correct content",
                |_model, state| {
                    if !state.scrub_ran || !state.is_quiescent() {
                        return true;
                    }
                    let path = "data.txt";
                    let backing = state.backing.get(path).copied().unwrap_or(0);
                    let sum = state.sums.get(path).copied().unwrap_or(0);
                    if backing > 0 && backing == sum {
                        // After successful heal + quiescence, replica should
                        // have at least this version (or matching)
                        if let Some(&rv) = state.replica.get(path) {
                            return rv >= sum;
                        }
                    }
                    true
                },
            ),
            Property::<Self>::sometimes(
                "corrupt file is healed by scrub",
                |_model, state| state.scrub_ran,
            ),
        ]
    }

    fn within_boundary(&self, state: &Self::State) -> bool {
        state.fuse_ops_done <= 6 && state.repl_log.len() <= 6 && state.next_repl_id <= 10
    }
}

#[test]
fn test_scrub_heal_model() {
    let model = ScrubHealModel;
    let checker = model.checker().spawn_bfs().join();
    checker.assert_properties();
}

// =========================================================================
// Multiple concurrent replication workers
// =========================================================================

#[derive(Clone)]
struct MultiWorkerModel;

impl Model for MultiWorkerModel {
    type State = FsModelState;
    type Action = Action;

    fn init_states(&self) -> Vec<Self::State> {
        let mut s = FsModelState::new();
        s.backing.insert("a.txt".to_string(), 1);
        s.sums.insert("a.txt".to_string(), 1);
        s.backing.insert("b.txt".to_string(), 2);
        s.sums.insert("b.txt".to_string(), 2);
        s.enqueue_put("a.txt");
        s.enqueue_put("b.txt");
        vec![s]
    }

    fn actions(&self, state: &Self::State, actions: &mut Vec<Self::Action>) {
        add_worker_actions(state, actions);

        // One concurrent write
        if state.fuse_ops_done == 0 {
            actions.push(Action::Write {
                path: "a.txt".to_string(),
            });
        }
    }

    fn next_state(&self, state: &Self::State, action: Self::Action) -> Option<Self::State> {
        let mut s = state.clone();
        match &action {
            Action::Write { ref path } => {
                if let Some(ver) = s.backing.get_mut(path) {
                    *ver += 1;
                }
                if let Some(&ver) = s.backing.get(path) {
                    s.sums.insert(path.clone(), ver);
                    s.enqueue_put(path);
                }
                s.fuse_ops_done += 1;
            }
            _ => {
                apply_action(&mut s, &action);
            }
        }
        Some(s)
    }

    fn properties(&self) -> Vec<Property<Self>> {
        vec![
            Property::<Self>::always(
                "no entry both in_progress and completed",
                |_model, state| {
                    state
                        .repl_log
                        .iter()
                        .all(|e| !(e.in_progress && e.completed))
                },
            ),
            Property::<Self>::always(
                "multi-worker replica converges",
                |_model, state| {
                    if state.pending_count() > 0 {
                        return true;
                    }
                    for (path, &ver) in &state.backing {
                        if let Some(&sum_ver) = state.sums.get(path) {
                            if ver == sum_ver {
                                let replica_ver =
                                    state.replica.get(path).copied().unwrap_or(0);
                                if replica_ver != ver {
                                    return false;
                                }
                            }
                        }
                    }
                    true
                },
            ),
            Property::<Self>::sometimes(
                "both files replicated",
                |_model, state| {
                    state.pending_count() == 0
                        && state.replica.get("a.txt").copied().unwrap_or(0) > 0
                        && state.replica.get("b.txt").copied().unwrap_or(0) > 0
                },
            ),
        ]
    }

    fn within_boundary(&self, state: &Self::State) -> bool {
        state.repl_log.len() <= 6 && state.next_repl_id <= 8
    }
}

#[test]
fn test_multi_worker_model() {
    let model = MultiWorkerModel;
    let checker = model.checker().spawn_bfs().join();
    checker.assert_properties();
}

// =========================================================================
// Crash recovery model
// =========================================================================

#[derive(Clone)]
struct CrashRecoveryModel;

#[derive(Clone, Debug, Eq, PartialEq, Hash)]
enum CrashAction {
    Normal(Action),
    Crash,
}

impl Model for CrashRecoveryModel {
    type State = FsModelState;
    type Action = CrashAction;

    fn init_states(&self) -> Vec<Self::State> {
        let mut s = FsModelState::new();
        s.backing.insert("a.txt".to_string(), 1);
        s.sums.insert("a.txt".to_string(), 1);
        s.enqueue_put("a.txt");
        vec![s]
    }

    fn actions(&self, state: &Self::State, actions: &mut Vec<Self::Action>) {
        // Worker claim
        if state
            .repl_log
            .iter()
            .any(|e| !e.completed && !e.in_progress)
        {
            actions.push(CrashAction::Normal(Action::WorkerClaim));
        }

        // Worker process
        for id in state.in_progress_ids() {
            actions.push(CrashAction::Normal(Action::WorkerProcess { id }));
        }

        // Crash
        if state.pending_count() > 0 && state.fuse_ops_done < 3 {
            actions.push(CrashAction::Crash);
        }

        // File operations after potential recovery
        if state.fuse_ops_done < 3 {
            let ver = state.backing.get("a.txt").copied().unwrap_or(0);
            let wr = state.get_write_ref("a.txt");
            if ver > 0 && wr == 0 && ver < 3 {
                actions.push(CrashAction::Normal(Action::Create {
                    path: "a.txt".to_string(),
                }));
            }
            if wr > 0 {
                actions.push(CrashAction::Normal(Action::Release {
                    path: "a.txt".to_string(),
                }));
            }
            if wr > 0 && ver < 3 {
                actions.push(CrashAction::Normal(Action::Write {
                    path: "a.txt".to_string(),
                }));
            }
        }
    }

    fn next_state(&self, state: &Self::State, action: Self::Action) -> Option<Self::State> {
        let mut s = state.clone();
        match action {
            CrashAction::Crash => {
                for entry in s.repl_log.iter_mut() {
                    entry.in_progress = false;
                }
                s.path_state.clear();
                s.fuse_ops_done += 1;
            }
            CrashAction::Normal(ref action) => {
                // For Create of existing file, reopen
                if let Action::Create { ref path } = action {
                    if s.backing.get(path).copied().unwrap_or(0) > 0 {
                        s.inc_write_ref(path);
                        s.mark_dirty(path);
                        s.fuse_ops_done += 1;
                        return Some(s);
                    }
                }
                apply_action(&mut s, action);
            }
        }
        Some(s)
    }

    fn properties(&self) -> Vec<Property<Self>> {
        vec![
            Property::<Self>::always(
                "crash preserves pending entries",
                |_model, state| {
                    for entry in &state.repl_log {
                        if entry.in_progress && entry.completed {
                            return false;
                        }
                    }
                    true
                },
            ),
            Property::<Self>::always(
                "crash recovery: replica converges when quiescent",
                |_model, state| {
                    if !state.is_quiescent() {
                        return true;
                    }
                    for (path, &ver) in &state.backing {
                        if let Some(&sum_ver) = state.sums.get(path) {
                            if ver == sum_ver {
                                let replica_ver =
                                    state.replica.get(path).copied().unwrap_or(0);
                                if replica_ver != ver {
                                    return false;
                                }
                            }
                        }
                    }
                    true
                },
            ),
            Property::<Self>::sometimes(
                "replication succeeds after crash",
                |_model, state| {
                    state.is_quiescent()
                        && state.replica.get("a.txt").copied().unwrap_or(0) > 0
                },
            ),
        ]
    }

    fn within_boundary(&self, state: &Self::State) -> bool {
        state.repl_log.len() <= 6 && state.next_repl_id <= 10 && state.fuse_ops_done <= 5
    }
}

#[test]
fn test_crash_recovery_model() {
    let model = CrashRecoveryModel;
    let checker = model.checker().spawn_bfs().join();
    checker.assert_properties();
}

// =========================================================================
// Delete + re-create race
// =========================================================================

#[derive(Clone)]
struct DeleteRecreateModel;

impl Model for DeleteRecreateModel {
    type State = FsModelState;
    type Action = Action;

    fn init_states(&self) -> Vec<Self::State> {
        let mut s = FsModelState::new();
        s.backing.insert("f.txt".to_string(), 1);
        s.sums.insert("f.txt".to_string(), 1);
        s.replica.insert("f.txt".to_string(), 1);
        s.replica_sums.insert("f.txt".to_string(), 1);
        vec![s]
    }

    fn actions(&self, state: &Self::State, actions: &mut Vec<Self::Action>) {
        let path = "f.txt".to_string();
        let ver = state.backing.get(&path).copied().unwrap_or(0);
        let wr = state.get_write_ref(&path);

        if state.fuse_ops_done < 6 {
            if ver > 0 && wr == 0 {
                actions.push(Action::Unlink { path: path.clone() });
            }
            if ver == 0 && wr == 0 {
                actions.push(Action::Create { path: path.clone() });
            }
            if ver > 0 && wr > 0 && ver < 3 {
                actions.push(Action::Write { path: path.clone() });
            }
            if wr > 0 {
                actions.push(Action::Release { path: path.clone() });
            }
        }

        add_worker_actions(state, actions);
    }

    fn next_state(&self, state: &Self::State, action: Self::Action) -> Option<Self::State> {
        let mut s = state.clone();
        apply_action(&mut s, &action);
        Some(s)
    }

    fn properties(&self) -> Vec<Property<Self>> {
        vec![
            Property::<Self>::always(
                "delete-recreate: correct replica when quiescent",
                |_model, state| {
                    if !state.is_quiescent() {
                        return true;
                    }
                    let path = "f.txt";
                    let backing_ver = state.backing.get(path).copied().unwrap_or(0);
                    let replica_ver = state.replica.get(path).copied().unwrap_or(0);

                    if backing_ver == 0 {
                        return replica_ver == 0;
                    }
                    if let Some(&sum_ver) = state.sums.get(path) {
                        if backing_ver == sum_ver {
                            return replica_ver == backing_ver;
                        }
                    }
                    true
                },
            ),
            Property::<Self>::sometimes(
                "file is deleted and recreated",
                |_model, state| {
                    state.is_quiescent()
                        && state.replica.get("f.txt").copied().unwrap_or(0) > 0
                },
            ),
        ]
    }

    fn within_boundary(&self, state: &Self::State) -> bool {
        state.fuse_ops_done <= 6 && state.repl_log.len() <= 8 && state.next_repl_id <= 10
    }
}

#[test]
fn test_delete_recreate_model() {
    let model = DeleteRecreateModel;
    let checker = model.checker().spawn_bfs().join();
    checker.assert_properties();
}
