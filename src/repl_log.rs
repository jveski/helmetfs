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

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ReplOp {
    Put,
    Delete,
}

#[derive(Debug, Clone)]
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
// ReplLog
// ---------------------------------------------------------------------------

pub struct ReplLog {
    inner: Mutex<ReplLogInner>,
    cond: Condvar,
    log_path: PathBuf,
}

struct ReplLogInner {
    entries: VecDeque<ReplEntry>,
    /// Monotonically increasing counter for assigning stable entry IDs.
    next_id: u64,
}

impl ReplLog {
    /// Create a new replication log. If `log_path` exists on disk, its entries
    /// are loaded (providing crash-recovery).
    pub fn new(backing_dir: &Path) -> io::Result<Self> {
        let helmetfs_dir = backing_dir.join(".helmetfs");
        fs::create_dir_all(&helmetfs_dir)?;
        let log_path = helmetfs_dir.join("repl.log");

        let mut entries = VecDeque::new();
        let mut next_id: u64 = 0;
        if log_path.exists() {
            let file = fs::File::open(&log_path)?;
            for line in io::BufReader::new(file).lines() {
                let line = line?;
                let line = line.trim();
                if line.is_empty() {
                    continue;
                }
                if let Some(path) = line.strip_prefix("put ") {
                    entries.push_back(ReplEntry {
                        op: ReplOp::Put,
                        path: path.to_string(),
                        id: next_id,
                        in_progress: false,
                        completed: false,
                    });
                    next_id += 1;
                } else if let Some(path) = line.strip_prefix("delete ") {
                    entries.push_back(ReplEntry {
                        op: ReplOp::Delete,
                        path: path.to_string(),
                        id: next_id,
                        in_progress: false,
                        completed: false,
                    });
                    next_id += 1;
                }
            }
            if !entries.is_empty() {
                log::info!("Loaded {} entries from replication log", entries.len());
            }
        }

        Ok(Self {
            inner: Mutex::new(ReplLogInner { entries, next_id }),
            cond: Condvar::new(),
            log_path,
        })
    }

    /// Enqueue a `put` entry. Coalesces with any existing uncompleted put for
    /// the same path (marks the earlier one completed).
    pub fn enqueue_put(&self, path: &str) {
        let mut inner = self.inner.lock().unwrap();
        // Coalesce: mark earlier pending entries for the same path as completed,
        // but only if they are not currently in_progress (being actively processed).
        //
        // This coalesces both earlier puts (same-op) AND earlier deletes (cross-op).
        // A put copies the current backing content, so it fully subsumes an
        // earlier delete—the replica will end up with the latest file regardless.
        // Without cross-op coalescing, multi-worker out-of-order processing can
        // cause an earlier delete to execute after this put, leaving the replica
        // empty.
        for entry in inner.entries.iter_mut() {
            if !entry.completed && !entry.in_progress && entry.path == path {
                entry.completed = true;
            }
        }
        let id = inner.next_id;
        inner.next_id += 1;
        inner.entries.push_back(ReplEntry {
            op: ReplOp::Put,
            path: path.to_string(),
            id,
            in_progress: false,
            completed: false,
        });
        self.persist_locked(&inner);
        self.cond.notify_one();
    }

    /// Enqueue a `delete` entry. Cross-op coalescing: mark earlier pending
    /// (not in_progress) puts for the same path as completed—there is no point
    /// copying a file we are about to remove.
    pub fn enqueue_delete(&self, path: &str) {
        let mut inner = self.inner.lock().unwrap();
        for entry in inner.entries.iter_mut() {
            if !entry.completed && !entry.in_progress && entry.op == ReplOp::Put && entry.path == path {
                entry.completed = true;
            }
        }
        let id = inner.next_id;
        inner.next_id += 1;
        inner.entries.push_back(ReplEntry {
            op: ReplOp::Delete,
            path: path.to_string(),
            id,
            in_progress: false,
            completed: false,
        });
        self.persist_locked(&inner);
        self.cond.notify_one();
    }

    /// Block until there is an uncompleted, not-in-progress entry, then mark it
    /// as in_progress and return a clone of it (and its stable id). The caller
    /// must call `mark_completed` after processing.
    /// Returns `None` if woken up with no entries (e.g. shutdown signal).
    pub fn wait_next(&self) -> Option<(u64, ReplEntry)> {
        let mut inner = self.inner.lock().unwrap();
        loop {
            for entry in inner.entries.iter_mut() {
                if !entry.completed && !entry.in_progress {
                    entry.in_progress = true;
                    return Some((entry.id, entry.clone()));
                }
            }
            // Use wait_timeout to avoid permanent blocking
            let (guard, _timeout) = self
                .cond
                .wait_timeout(inner, std::time::Duration::from_millis(200))
                .unwrap();
            inner = guard;

            // After wakeup, check again — if still nothing, return None
            // so the caller can check for shutdown
            let has_pending = inner.entries.iter().any(|e| !e.completed && !e.in_progress);
            if !has_pending {
                return None;
            }
        }
    }

    /// Try to get next uncompleted, not-in-progress entry without blocking.
    /// Marks it as in_progress. Returns None if there are no available entries.
    pub fn try_next(&self) -> Option<(u64, ReplEntry)> {
        let mut inner = self.inner.lock().unwrap();
        for entry in inner.entries.iter_mut() {
            if !entry.completed && !entry.in_progress {
                entry.in_progress = true;
                return Some((entry.id, entry.clone()));
            }
        }
        None
    }

    /// Mark entry with the given stable `id` as completed and garbage-collect
    /// leading completed entries.
    pub fn mark_completed(&self, id: u64) {
        let mut inner = self.inner.lock().unwrap();
        for entry in inner.entries.iter_mut() {
            if entry.id == id {
                entry.completed = true;
                entry.in_progress = false;
                break;
            }
        }
        // GC: remove completed entries from the front
        while inner
            .entries
            .front()
            .map_or(false, |e| e.completed)
        {
            inner.entries.pop_front();
        }
        self.persist_locked(&inner);
    }

    /// Check if there is a pending (uncompleted) `put` for the given path.
    /// Entries that are in_progress are also considered pending (being processed).
    pub fn has_pending_put(&self, path: &str) -> bool {
        let inner = self.inner.lock().unwrap();
        inner
            .entries
            .iter()
            .any(|e| !e.completed && e.op == ReplOp::Put && e.path == path)
    }

    /// Wake up all waiting workers (used during shutdown).
    pub fn notify_all(&self) {
        self.cond.notify_all();
    }

    // -----------------------------------------------------------------------
    // Persistence
    // -----------------------------------------------------------------------

    fn persist_locked(&self, inner: &ReplLogInner) {
        if let Err(e) = self.persist_impl(inner) {
            log::error!("Failed to persist replication log: {}", e);
        }
    }

    fn persist_impl(&self, inner: &ReplLogInner) -> io::Result<()> {
        let tmp = self.log_path.with_extension("log.tmp");
        {
            let mut f = fs::File::create(&tmp)?;
            for entry in &inner.entries {
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
