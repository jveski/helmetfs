//! Replication log: in-memory queue with disk persistence.
//!
//! Each entry is either `put <path>` or `delete <path>`.
//! Put entries support coalescing: when a new put is enqueued for a path that
//! already has a pending (not-yet-started) put, the earlier entry is marked
//! completed so the replication worker skips it.  Deletes are never coalesced.
//!
//! On-disk format (`<backing>/.helmetfs/repl.log`):
//!   put <path>\n
//!   delete <path>\n

use std::collections::VecDeque;
use std::fs;
use std::io::{self, BufRead, Write};
use std::path::{Path, PathBuf};
use std::sync::{Condvar, Mutex};

// ---------------------------------------------------------------------------
// Entry types
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum ReplOp {
    Put,
    Delete,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct ReplEntry {
    pub op: ReplOp,
    pub path: String,
    /// Stable identifier that never changes once assigned.
    pub id: u64,
    /// Set to true when a worker has claimed this entry via wait_next/try_next.
    pub in_progress: bool,
    /// Set to true when the entry has been processed (or coalesced away).
    pub completed: bool,
}

// ---------------------------------------------------------------------------
// ReplQueue — pure replication queue logic (no I/O, no synchronization)
// ---------------------------------------------------------------------------

/// Pure replication queue logic shared between the real implementation and
/// the stateright model.  Contains no I/O, locking, or persistence.
#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub struct ReplQueue {
    pub entries: VecDeque<ReplEntry>,
    /// Monotonically increasing counter for assigning stable entry IDs.
    pub next_id: u64,
}

impl Default for ReplQueue {
    fn default() -> Self {
        Self::new()
    }
}

impl ReplQueue {
    pub fn new() -> Self {
        Self {
            entries: VecDeque::new(),
            next_id: 0,
        }
    }

    /// Enqueue a `put` entry. Coalesces with any existing uncompleted entry for
    /// the same path (marks earlier ones completed).
    ///
    /// This coalesces both earlier puts (same-op) AND earlier deletes (cross-op).
    /// A put copies the current backing content, so it fully subsumes an
    /// earlier delete—the replica will end up with the latest file regardless.
    /// Without cross-op coalescing, multi-worker out-of-order processing can
    /// cause an earlier delete to execute after this put, leaving the replica
    /// empty.
    pub fn enqueue_put(&mut self, path: &str) {
        for entry in self.entries.iter_mut() {
            if !entry.completed && !entry.in_progress && entry.path == path {
                entry.completed = true;
            }
        }
        let id = self.next_id;
        self.next_id += 1;
        self.entries.push_back(ReplEntry {
            op: ReplOp::Put,
            path: path.to_string(),
            id,
            in_progress: false,
            completed: false,
        });
    }

    /// Enqueue a `delete` entry. Cross-op coalescing: mark earlier pending
    /// (not in_progress) puts for the same path as completed—there is no point
    /// copying a file we are about to remove.
    pub fn enqueue_delete(&mut self, path: &str) {
        for entry in self.entries.iter_mut() {
            if !entry.completed && !entry.in_progress && entry.op == ReplOp::Put && entry.path == path {
                entry.completed = true;
            }
        }
        let id = self.next_id;
        self.next_id += 1;
        self.entries.push_back(ReplEntry {
            op: ReplOp::Delete,
            path: path.to_string(),
            id,
            in_progress: false,
            completed: false,
        });
    }

    /// Check if there is a pending (uncompleted) `put` for the given path.
    /// Entries that are in_progress are also considered pending (being processed).
    pub fn has_pending_put(&self, path: &str) -> bool {
        self.entries
            .iter()
            .any(|e| !e.completed && e.op == ReplOp::Put && e.path == path)
    }

    /// Claim next available entry (not completed, not in_progress).
    /// Marks it as in_progress and returns its stable id and a clone.
    pub fn claim_next(&mut self) -> Option<(u64, ReplEntry)> {
        for entry in self.entries.iter_mut() {
            if !entry.completed && !entry.in_progress {
                entry.in_progress = true;
                return Some((entry.id, entry.clone()));
            }
        }
        None
    }

    /// Claim next available entry, returning only its stable id.
    pub fn claim_next_id(&mut self) -> Option<u64> {
        for entry in self.entries.iter_mut() {
            if !entry.completed && !entry.in_progress {
                entry.in_progress = true;
                return Some(entry.id);
            }
        }
        None
    }

    /// Find entry by stable id.
    pub fn find_entry(&self, id: u64) -> Option<&ReplEntry> {
        self.entries.iter().find(|e| e.id == id)
    }

    /// Mark entry with the given stable `id` as completed and garbage-collect
    /// leading completed entries.
    pub fn mark_completed(&mut self, id: u64) {
        for entry in self.entries.iter_mut() {
            if entry.id == id {
                entry.completed = true;
                entry.in_progress = false;
                break;
            }
        }
        // GC: remove completed entries from the front
        while self
            .entries
            .front()
            .map_or(false, |e| e.completed)
        {
            self.entries.pop_front();
        }
    }

    /// Number of pending (uncompleted) entries.
    pub fn pending_count(&self) -> usize {
        self.entries.iter().filter(|e| !e.completed).count()
    }

    /// IDs of in-progress entries (used for action generation in model).
    pub fn in_progress_ids(&self) -> Vec<u64> {
        self.entries
            .iter()
            .filter(|e| e.in_progress && !e.completed)
            .map(|e| e.id)
            .collect()
    }

    /// Whether there are claimable entries (not completed, not in_progress).
    pub fn has_claimable(&self) -> bool {
        self.entries.iter().any(|e| !e.completed && !e.in_progress)
    }

    /// Reset all in_progress flags (used for crash recovery).
    pub fn reset_in_progress(&mut self) {
        for entry in self.entries.iter_mut() {
            entry.in_progress = false;
        }
    }
}

// ---------------------------------------------------------------------------
// ReplLog — thread-safe wrapper with persistence
// ---------------------------------------------------------------------------

pub struct ReplLog {
    inner: Mutex<ReplQueue>,
    cond: Condvar,
    log_path: PathBuf,
}

impl ReplLog {
    /// Create a new replication log. If `log_path` exists on disk, its entries
    /// are loaded (providing crash-recovery).
    pub fn new(backing_dir: &Path) -> io::Result<Self> {
        let helmetfs_dir = backing_dir.join(".helmetfs");
        fs::create_dir_all(&helmetfs_dir)?;
        let log_path = helmetfs_dir.join("repl.log");

        let mut queue = ReplQueue::new();
        if log_path.exists() {
            let file = fs::File::open(&log_path)?;
            for line in io::BufReader::new(file).lines() {
                let line = line?;
                let line = line.trim();
                if line.is_empty() {
                    continue;
                }
                if let Some(path) = line.strip_prefix("put ") {
                    queue.entries.push_back(ReplEntry {
                        op: ReplOp::Put,
                        path: path.to_string(),
                        id: queue.next_id,
                        in_progress: false,
                        completed: false,
                    });
                    queue.next_id += 1;
                } else if let Some(path) = line.strip_prefix("delete ") {
                    queue.entries.push_back(ReplEntry {
                        op: ReplOp::Delete,
                        path: path.to_string(),
                        id: queue.next_id,
                        in_progress: false,
                        completed: false,
                    });
                    queue.next_id += 1;
                }
            }
            if !queue.entries.is_empty() {
                log::info!("Loaded {} entries from replication log", queue.entries.len());
            }
        }

        Ok(Self {
            inner: Mutex::new(queue),
            cond: Condvar::new(),
            log_path,
        })
    }

    /// Enqueue a `put` entry. Coalesces with any existing uncompleted put for
    /// the same path (marks the earlier one completed).
    pub fn enqueue_put(&self, path: &str) {
        let mut queue = self.inner.lock().unwrap();
        queue.enqueue_put(path);
        self.persist_locked(&queue);
        self.cond.notify_one();
    }

    /// Enqueue a `delete` entry. Cross-op coalescing: mark earlier pending
    /// (not in_progress) puts for the same path as completed.
    pub fn enqueue_delete(&self, path: &str) {
        let mut queue = self.inner.lock().unwrap();
        queue.enqueue_delete(path);
        self.persist_locked(&queue);
        self.cond.notify_one();
    }

    /// Block until there is an uncompleted, not-in-progress entry, then mark it
    /// as in_progress and return a clone of it (and its stable id). The caller
    /// must call `mark_completed` after processing.
    /// Returns `None` if woken up with no entries (e.g. shutdown signal).
    pub fn wait_next(&self) -> Option<(u64, ReplEntry)> {
        let mut queue = self.inner.lock().unwrap();
        loop {
            if let Some(result) = queue.claim_next() {
                return Some(result);
            }
            // Use wait_timeout to avoid permanent blocking
            let (guard, _timeout) = self
                .cond
                .wait_timeout(queue, std::time::Duration::from_millis(200))
                .unwrap();
            queue = guard;

            // After wakeup, check again — if still nothing, return None
            // so the caller can check for shutdown
            if !queue.has_claimable() {
                return None;
            }
        }
    }

    /// Try to get next uncompleted, not-in-progress entry without blocking.
    /// Marks it as in_progress. Returns None if there are no available entries.
    pub fn try_next(&self) -> Option<(u64, ReplEntry)> {
        let mut queue = self.inner.lock().unwrap();
        queue.claim_next()
    }

    /// Mark entry with the given stable `id` as completed and garbage-collect
    /// leading completed entries.
    pub fn mark_completed(&self, id: u64) {
        let mut queue = self.inner.lock().unwrap();
        queue.mark_completed(id);
        self.persist_locked(&queue);
    }

    /// Check if there is a pending (uncompleted) `put` for the given path.
    /// Entries that are in_progress are also considered pending (being processed).
    pub fn has_pending_put(&self, path: &str) -> bool {
        let queue = self.inner.lock().unwrap();
        queue.has_pending_put(path)
    }

    /// Wake up all waiting workers (used during shutdown).
    pub fn notify_all(&self) {
        self.cond.notify_all();
    }

    // -----------------------------------------------------------------------
    // Persistence
    // -----------------------------------------------------------------------

    fn persist_locked(&self, queue: &ReplQueue) {
        if let Err(e) = self.persist_impl(queue) {
            log::error!("Failed to persist replication log: {}", e);
        }
    }

    fn persist_impl(&self, queue: &ReplQueue) -> io::Result<()> {
        let tmp = self.log_path.with_extension("log.tmp");
        {
            let mut f = fs::File::create(&tmp)?;
            for entry in &queue.entries {
                if entry.completed {
                    continue;
                }
                match entry.op {
                    ReplOp::Put => writeln!(f, "put {}", entry.path)?,
                    ReplOp::Delete => writeln!(f, "delete {}", entry.path)?,
                }
            }
            f.sync_all()?;
        }
        fs::rename(&tmp, &self.log_path)?;
        Ok(())
    }
}
