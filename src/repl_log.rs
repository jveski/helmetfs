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
}

impl ReplLog {
    /// Create a new replication log. If `log_path` exists on disk, its entries
    /// are loaded (providing crash-recovery).
    pub fn new(backing_dir: &Path) -> io::Result<Self> {
        let helmetfs_dir = backing_dir.join(".helmetfs");
        fs::create_dir_all(&helmetfs_dir)?;
        let log_path = helmetfs_dir.join("repl.log");

        let mut entries = VecDeque::new();
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
                        completed: false,
                    });
                } else if let Some(path) = line.strip_prefix("delete ") {
                    entries.push_back(ReplEntry {
                        op: ReplOp::Delete,
                        path: path.to_string(),
                        completed: false,
                    });
                }
            }
            if !entries.is_empty() {
                log::info!("Loaded {} entries from replication log", entries.len());
            }
        }

        Ok(Self {
            inner: Mutex::new(ReplLogInner { entries }),
            cond: Condvar::new(),
            log_path,
        })
    }

    /// Enqueue a `put` entry. Coalesces with any existing uncompleted put for
    /// the same path (marks the earlier one completed).
    pub fn enqueue_put(&self, path: &str) {
        let mut inner = self.inner.lock().unwrap();
        // Coalesce: mark earlier pending puts for same path as completed
        for entry in inner.entries.iter_mut() {
            if !entry.completed && entry.op == ReplOp::Put && entry.path == path {
                entry.completed = true;
            }
        }
        inner.entries.push_back(ReplEntry {
            op: ReplOp::Put,
            path: path.to_string(),
            completed: false,
        });
        self.persist_locked(&inner);
        self.cond.notify_one();
    }

    /// Enqueue a `delete` entry. Deletes are never coalesced.
    pub fn enqueue_delete(&self, path: &str) {
        let mut inner = self.inner.lock().unwrap();
        inner.entries.push_back(ReplEntry {
            op: ReplOp::Delete,
            path: path.to_string(),
            completed: false,
        });
        self.persist_locked(&inner);
        self.cond.notify_one();
    }

    /// Block until there is an uncompleted entry, then return a clone of it
    /// (and its index). The caller must call `mark_completed` after processing.
    /// Returns `None` if woken up with no entries (e.g. shutdown signal).
    pub fn wait_next(&self) -> Option<(usize, ReplEntry)> {
        let mut inner = self.inner.lock().unwrap();
        loop {
            for (i, entry) in inner.entries.iter().enumerate() {
                if !entry.completed {
                    return Some((i, entry.clone()));
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
            let has_pending = inner.entries.iter().any(|e| !e.completed);
            if !has_pending {
                return None;
            }
        }
    }

    /// Try to get next uncompleted entry without blocking. Returns None if
    /// there are no pending entries.
    pub fn try_next(&self) -> Option<(usize, ReplEntry)> {
        let inner = self.inner.lock().unwrap();
        for (i, entry) in inner.entries.iter().enumerate() {
            if !entry.completed {
                return Some((i, entry.clone()));
            }
        }
        None
    }

    /// Mark entry at index `idx` as completed and garbage-collect leading
    /// completed entries.
    pub fn mark_completed(&self, idx: usize) {
        let mut inner = self.inner.lock().unwrap();
        if idx < inner.entries.len() {
            inner.entries[idx].completed = true;
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
