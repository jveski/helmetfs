//! Replication worker: processes the replication log queue.
//!
//! Each worker thread calls `repl_log.wait_next()` in a loop and either
//! copies the file to the replica (`put`) or removes it (`delete`).

use std::fs;
use std::io::{self, Read, Write};
use std::os::unix::fs::{MetadataExt, PermissionsExt};
use std::path::Path;
use std::sync::atomic::Ordering;
use std::sync::Arc;

use crate::helpers;
use crate::repl_log::ReplOp;
use crate::state::FsState;

/// Run the replication worker loop. Returns when `state.shutting_down` is set
/// and there are no more pending entries.
pub fn replication_worker(state: &Arc<FsState>) {
    log::info!("Replication worker started");
    loop {
        // Try to get next entry. wait_next() returns None after a timeout
        // if no entries are pending, allowing us to check shutdown.
        let (id, entry) = match state.repl_log.wait_next() {
            Some(pair) => pair,
            None => {
                if state.shutting_down.load(Ordering::Relaxed) {
                    // Drain any stragglers
                    while let Some((id, entry)) = state.repl_log.try_next() {
                        let _ = match entry.op {
                            ReplOp::Put => replicate_put(state, &entry.path),
                            ReplOp::Delete => replicate_delete(state, &entry.path),
                        };
                        state.repl_log.mark_completed(id);
                    }
                    break;
                }
                continue;
            }
        };

        let result = match entry.op {
            ReplOp::Put => replicate_put(state, &entry.path),
            ReplOp::Delete => replicate_delete(state, &entry.path),
        };

        match result {
            Ok(()) => {
                log::debug!("Replicated {:?} {}", entry.op, entry.path);
            }
            Err(e) => {
                log::error!("Replication failed for {:?} {}: {}", entry.op, entry.path, e);
            }
        }

        state.repl_log.mark_completed(id);
    }
    log::info!("Replication worker stopped");
}

/// Copy a file (and its .sum sidecar) from backing to replica.
fn replicate_put(state: &FsState, rel: &str) -> io::Result<()> {
    let src = state.backing_path(rel);
    let dst = state.replica_file_path(rel);

    // Handle symlinks
    let meta = fs::symlink_metadata(&src)?;
    if meta.file_type().is_symlink() {
        let target = fs::read_link(&src)?;
        // Ensure parent dir exists in replica
        if let Some(parent) = dst.parent() {
            fs::create_dir_all(parent)?;
        }
        // Remove any existing file/symlink at destination
        let _ = fs::remove_file(&dst);
        std::os::unix::fs::symlink(&target, &dst)?;
        return Ok(());
    }

    if !meta.is_file() {
        return Ok(()); // Skip directories, etc.
    }

    // Ensure parent dir exists in replica
    if let Some(parent) = dst.parent() {
        fs::create_dir_all(parent)?;
    }

    // Copy file content with sync
    copy_file_with_sync(&src, &dst)?;

    // Preserve permissions
    let perms = fs::Permissions::from_mode(meta.mode() & 0o7777);
    fs::set_permissions(&dst, perms)?;

    // Copy .sum sidecar if it exists
    let src_sum = helpers::sum_path_for(&src);
    let dst_sum = helpers::sum_path_for(&dst);
    if src_sum.exists() {
        copy_file_with_sync(&src_sum, &dst_sum)?;
    }

    Ok(())
}

/// Remove a file (and its .sum sidecar) from the replica.
///
/// Guard: if the backing file currently exists with a `.sum` sidecar, the file
/// was re-created (and checksummed) since this delete entry was enqueued.
/// Deleting the replica copy would lose the latest version, so we skip the
/// delete.  With multiple worker threads, this prevents an older delete from
/// executing after a newer put for the same path has already replicated the
/// re-created file.  (In normal operation, FUSE `unlink` removes the backing
/// file and .sum before enqueuing the delete, so this guard only fires when
/// the file was truly re-created.)
fn replicate_delete(state: &FsState, rel: &str) -> io::Result<()> {
    let src = state.backing_path(rel);
    let src_sum = helpers::sum_path_for(&src);
    if src.exists() && src_sum.exists() {
        log::debug!(
            "Skipping replica delete for {} — file was re-created",
            rel
        );
        return Ok(());
    }

    let dst = state.replica_file_path(rel);
    let dst_sum = helpers::sum_path_for(&dst);

    // Remove file — ignore NotFound
    match fs::remove_file(&dst) {
        Ok(()) => {}
        Err(e) if e.kind() == io::ErrorKind::NotFound => {}
        Err(e) => return Err(e),
    }

    // Remove .sum sidecar — ignore NotFound
    match fs::remove_file(&dst_sum) {
        Ok(()) => {}
        Err(e) if e.kind() == io::ErrorKind::NotFound => {}
        Err(e) => return Err(e),
    }

    // Try to remove empty parent dirs (best effort, like Zig version)
    if let Some(parent) = dst.parent() {
        remove_empty_parents(parent, &state.replica_dir.join("files"));
    }

    Ok(())
}

/// Copy `src` to `dst` via temp file + rename, with fsync.
pub fn copy_file_with_sync(src: &Path, dst: &Path) -> io::Result<()> {
    let mut tmp_name = dst.as_os_str().to_os_string();
    tmp_name.push(".helmetfs-tmp");
    let tmp = std::path::PathBuf::from(tmp_name);
    {
        let mut reader = fs::File::open(src)?;
        let mut writer = fs::File::create(&tmp)?;
        let mut buf = [0u8; 65536];
        loop {
            let n = reader.read(&mut buf)?;
            if n == 0 {
                break;
            }
            writer.write_all(&buf[..n])?;
        }
        writer.sync_all()?;
    }
    fs::rename(&tmp, dst)?;
    Ok(())
}

/// Remove empty parent directories up to (but not including) `stop_at`.
fn remove_empty_parents(mut dir: &Path, stop_at: &Path) {
    while dir != stop_at {
        if fs::remove_dir(dir).is_err() {
            break;
        }
        match dir.parent() {
            Some(p) => dir = p,
            None => break,
        }
    }
}
