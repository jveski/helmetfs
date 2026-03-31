//! Scrub: periodic integrity checking and self-healing.
//!
//! The scrub walks all files in the backing directory, and for each file:
//!
//! 1. **Untracked** (no `.sum` sidecar): compute checksum, write `.sum`,
//!    enqueue a `put` to replicate.
//!
//! 2. **Clean** (has `.sum`, checksum matches): nothing to do.
//!
//! 3. **Corrupt** (has `.sum`, checksum mismatch): attempt self-healing from
//!    the replica — but only if:
//!    - no pending `put` in the replication log for this path,
//!    - no open write references,
//!    - not dirty,
//!    - the replica's file checksum matches the replica's `.sum`.
//!
//! Scrub timing: on startup if no `scrub.timestamp` or if it's older than
//! `scrub_interval_secs`. After each scrub completes, update the timestamp.

use std::fs;
use std::io::Write;
use std::path::Path;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::{Duration, SystemTime};

use crate::helpers;
use crate::replication::copy_file_with_sync;
use crate::state::{self, checksum_and_enqueue, FsState};

/// Run the scrub thread. Performs an initial scrub if needed, then sleeps for
/// `scrub_interval_secs` between runs.
pub fn scrub_thread(state: &Arc<FsState>) {
    log::info!("Scrub thread started");
    loop {
        if state.shutting_down.load(Ordering::Relaxed) {
            break;
        }

        if should_scrub(state) {
            log::info!("Starting scrub");
            let (checked, healed, adopted) = run_scrub(state);
            log::info!(
                "Scrub complete: checked={}, healed={}, adopted={}",
                checked,
                healed,
                adopted
            );
            update_scrub_timestamp(state);
        }

        // Sleep in small increments so we can respond to shutdown.
        // Use wall-clock target to avoid drift from scrub execution time.
        let target = std::time::Instant::now() + Duration::from_secs(state.scrub_interval_secs);
        while std::time::Instant::now() < target {
            if state.shutting_down.load(Ordering::Relaxed) {
                break;
            }
            std::thread::sleep(Duration::from_secs(1));
        }
    }
    log::info!("Scrub thread stopped");
}

/// Check if a scrub should run now.
fn should_scrub(state: &FsState) -> bool {
    let ts_path = state.backing_dir.join(".helmetfs").join("scrub.timestamp");
    match fs::metadata(&ts_path) {
        Ok(meta) => {
            let modified = meta
                .modified()
                .unwrap_or(SystemTime::UNIX_EPOCH);
            let elapsed = SystemTime::now()
                .duration_since(modified)
                .unwrap_or(Duration::from_secs(u64::MAX));
            elapsed.as_secs() >= state.scrub_interval_secs
        }
        Err(_) => true, // No timestamp file — scrub immediately
    }
}

/// Update the scrub timestamp file.
fn update_scrub_timestamp(state: &FsState) {
    let ts_path = state.backing_dir.join(".helmetfs").join("scrub.timestamp");
    if let Ok(mut f) = fs::File::create(&ts_path) {
        let _ = writeln!(f, "{}", humantime(SystemTime::now()));
    }
}

fn humantime(t: SystemTime) -> String {
    match t.duration_since(SystemTime::UNIX_EPOCH) {
        Ok(d) => d.as_secs().to_string(),
        Err(_) => "0".to_string(),
    }
}

/// Walk the backing directory and scrub all regular files.
/// Returns (checked, healed, adopted).
fn run_scrub(state: &FsState) -> (u64, u64, u64) {
    let mut checked = 0u64;
    let mut healed = 0u64;
    let mut adopted = 0u64;

    walk_dir(state, &state.backing_dir, &mut checked, &mut healed, &mut adopted);

    (checked, healed, adopted)
}

fn walk_dir(
    state: &FsState,
    dir: &Path,
    checked: &mut u64,
    healed: &mut u64,
    adopted: &mut u64,
) {
    let entries = match fs::read_dir(dir) {
        Ok(e) => e,
        Err(e) => {
            log::warn!("Scrub: failed to read dir {:?}: {}", dir, e);
            return;
        }
    };

    for entry in entries {
        if state.shutting_down.load(Ordering::Relaxed) {
            return;
        }

        let entry = match entry {
            Ok(e) => e,
            Err(_) => continue,
        };

        let path = entry.path();

        // Skip .helmetfs directory
        if let Ok(rel) = path.strip_prefix(&state.backing_dir) {
            if rel.starts_with(".helmetfs") {
                continue;
            }
        }

        let ft = match entry.file_type() {
            Ok(ft) => ft,
            Err(_) => continue,
        };

        if ft.is_dir() {
            walk_dir(state, &path, checked, healed, adopted);
            continue;
        }

        if !ft.is_file() {
            // Symlinks don't have checksums; skip
            continue;
        }

        // Skip .sum files themselves
        if path.extension().and_then(|e| e.to_str()) == Some("sum") {
            continue;
        }

        // .sum.tmp files — skip
        if path.to_str().map_or(false, |s| s.ends_with(".sum.tmp")) {
            continue;
        }

        let rel = match path.strip_prefix(&state.backing_dir) {
            Ok(r) => r.to_string_lossy().to_string(),
            Err(_) => continue,
        };

        *checked += 1;

        let sum_path = helpers::sum_path_for(&path);

        if !sum_path.exists() {
            // Untracked file — adopt it
            log::info!("Scrub: adopting untracked file: {}", rel);
            checksum_and_enqueue(state, &rel);
            *adopted += 1;
            continue;
        }

        // Has .sum — verify checksum
        let stored_sum = match helpers::read_sum_file(&sum_path) {
            Ok(s) => s,
            Err(e) => {
                log::warn!("Scrub: failed to read .sum for {}: {}", rel, e);
                continue;
            }
        };

        let actual_sum = match helpers::compute_blake3(&path) {
            Ok(s) => s,
            Err(e) => {
                log::warn!("Scrub: failed to checksum {}: {}", rel, e);
                continue;
            }
        };

        if actual_sum == stored_sum {
            // Clean — nothing to do
            continue;
        }

        // Corrupt — attempt self-healing
        log::warn!("Scrub: corruption detected in {}", rel);

        if !can_heal_file(state, &rel) {
            log::warn!("Scrub: cannot heal {} (busy or pending replication)", rel);
            continue;
        }

        if try_heal(state, &rel, &path) {
            log::info!("Scrub: healed {}", rel);
            *healed += 1;
        } else {
            log::error!("Scrub: failed to heal {}", rel);
        }
    }
}

/// Check if a file is eligible for healing.
fn can_heal_file(state: &FsState, rel: &str) -> bool {
    state::can_heal(state.repl_log.has_pending_put(rel), state.is_busy(rel))
}

/// Attempt to heal a corrupted file from the replica.
/// Returns true on success.
fn try_heal(state: &FsState, rel: &str, backing_path: &Path) -> bool {
    let replica_path = state.replica_file_path(rel);
    let replica_sum_path = helpers::sum_path_for(&replica_path);

    // Replica file must exist
    if !replica_path.exists() {
        log::warn!("Scrub: no replica copy for {}", rel);
        return false;
    }

    // Replica .sum must exist
    let replica_sum = match helpers::read_sum_file(&replica_sum_path) {
        Ok(s) => s,
        Err(_) => {
            log::warn!("Scrub: no replica .sum for {}", rel);
            return false;
        }
    };

    // Verify replica integrity: replica file checksum must match replica .sum
    let replica_actual = match helpers::compute_blake3(&replica_path) {
        Ok(s) => s,
        Err(e) => {
            log::warn!("Scrub: failed to checksum replica for {}: {}", rel, e);
            return false;
        }
    };

    if replica_actual != replica_sum {
        log::warn!("Scrub: replica itself is corrupt for {}", rel);
        return false;
    }

    // Restore from replica
    if let Err(e) = copy_file_with_sync(&replica_path, backing_path) {
        log::error!("Scrub: failed to restore {} from replica: {}", rel, e);
        return false;
    }

    // Also restore the .sum
    let backing_sum_path = helpers::sum_path_for(backing_path);
    if let Err(e) = copy_file_with_sync(&replica_sum_path, &backing_sum_path) {
        log::error!("Scrub: failed to restore .sum for {}: {}", rel, e);
        // File was restored even if .sum copy failed, still count as healed
    }

    true
}
