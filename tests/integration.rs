//! Integration tests for helmetfs core logic (no FUSE mount required).
//!
//! These tests exercise the checksum, replication log, replication worker,
//! and scrub logic end-to-end using real temp directories.

use std::fs;
use std::io::Write;
use std::path::Path;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::thread;
use std::time::Duration;

use tempfile::TempDir;

// We access the library through the binary crate's public modules.
// Since helmetfs is a binary crate, we import the library modules directly.
// For integration tests to access internal modules, we re-export from main.
// Instead, since we can't do that easily with a bin crate, we'll test via
// the binary's public state module + the source files directly.
//
// Workaround: the Cargo.toml doesn't define a [lib] section, so we add one
// or just test by running the binary.  For now, we directly use the modules
// by having main.rs expose `pub mod state`.

/// Helper: create a test environment with backing + replica dirs.
fn setup() -> (TempDir, std::path::PathBuf, std::path::PathBuf) {
    let tmp = TempDir::new().unwrap();
    let backing = tmp.path().join("backing");
    let replica = tmp.path().join("replica");
    fs::create_dir_all(&backing).unwrap();
    fs::create_dir_all(replica.join("files")).unwrap();
    (tmp, backing, replica)
}

/// Helper: create FsState for testing.
fn make_state(backing: &Path, replica: &Path) -> Arc<helmetfs::state::FsState> {
    Arc::new(
        helmetfs::state::FsState::new(
            backing.to_path_buf(),
            replica.to_path_buf(),
            86400,
        )
        .unwrap(),
    )
}

/// Helper: write a file with content.
fn write_file(path: &Path, content: &str) {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent).unwrap();
    }
    let mut f = fs::File::create(path).unwrap();
    f.write_all(content.as_bytes()).unwrap();
    f.sync_all().unwrap();
}

/// Helper: read file to string.
fn read_file(path: &Path) -> String {
    fs::read_to_string(path).unwrap()
}

// =========================================================================
// Test 1: Full pipeline — create, checksum, replicate, corrupt, scrub repair
// =========================================================================

#[test]
fn test_full_pipeline() {
    let (_tmp, backing, replica) = setup();
    let state = make_state(&backing, &replica);

    // 1. Create a file in backing
    write_file(&backing.join("hello.txt"), "hello world");

    // 2. Checksum and enqueue
    helmetfs::state::checksum_and_enqueue(&state, "hello.txt");

    // Verify .sum was created
    let sum_path = backing.join("hello.txt.sum");
    assert!(sum_path.exists(), ".sum sidecar should exist");
    let sum_content = read_file(&sum_path);
    let sum_hex = sum_content.trim();
    assert_eq!(sum_hex.len(), 64, ".sum should be 64 hex chars");

    // 3. Run replication worker briefly to process the put
    let worker_state = state.clone();
    let worker = thread::spawn(move || {
        helmetfs::replication::replication_worker(&worker_state);
    });

    // Give worker time to process
    thread::sleep(Duration::from_millis(500));

    // Signal shutdown and wake worker
    state.shutting_down.store(true, Ordering::Relaxed);
    state.repl_log.notify_all();
    worker.join().unwrap();

    // Verify replica has the file
    let replica_file = replica.join("files").join("hello.txt");
    assert!(replica_file.exists(), "File should be in replica");
    assert_eq!(read_file(&replica_file), "hello world");

    // Verify replica has .sum
    let replica_sum = replica.join("files").join("hello.txt.sum");
    assert!(replica_sum.exists(), "Replica should have .sum");

    // 4. Corrupt the backing file
    write_file(&backing.join("hello.txt"), "CORRUPTED");

    // 5. Create a fresh state (simulating remount) for scrub
    let state2 = Arc::new(
        helmetfs::state::FsState::new(
            backing.to_path_buf(),
            replica.to_path_buf(),
            86400,
        )
        .unwrap(),
    );

    // Remove scrub timestamp to force immediate scrub
    let _ = fs::remove_file(backing.join(".helmetfs").join("scrub.timestamp"));

    // Run scrub thread briefly
    let scrub_state = state2.clone();
    let scrub = thread::spawn(move || {
        helmetfs::scrub::scrub_thread(&scrub_state);
    });

    thread::sleep(Duration::from_secs(2));
    state2.shutting_down.store(true, Ordering::Relaxed);
    scrub.join().unwrap();

    // Verify file was healed
    assert_eq!(
        read_file(&backing.join("hello.txt")),
        "hello world",
        "Corrupted file should be healed from replica"
    );
}

// =========================================================================
// Test 2: Replication log persistence and reload
// =========================================================================

#[test]
fn test_repl_log_persistence() {
    let (_tmp, backing, replica) = setup();

    // Create state and enqueue entries
    {
        let state = make_state(&backing, &replica);
        state.repl_log.enqueue_put("file1.txt");
        state.repl_log.enqueue_put("file2.txt");
        state.repl_log.enqueue_delete("old.txt");
    }

    // Verify log file exists
    let log_path = backing.join(".helmetfs").join("repl.log");
    assert!(log_path.exists(), "repl.log should exist on disk");

    let content = read_file(&log_path);
    assert!(content.contains("put file1.txt"), "Should contain file1 put");
    assert!(content.contains("put file2.txt"), "Should contain file2 put");
    assert!(content.contains("delete old.txt"), "Should contain old.txt delete");

    // Reload and verify entries are recovered
    let state2 = make_state(&backing, &replica);
    let (id, entry) = state2.repl_log.try_next().expect("Should have pending entry");
    assert_eq!(entry.path, "file1.txt");
    state2.repl_log.mark_completed(id);

    let (id, entry) = state2.repl_log.try_next().expect("Should have pending entry");
    assert_eq!(entry.path, "file2.txt");
    state2.repl_log.mark_completed(id);

    let (id, entry) = state2.repl_log.try_next().expect("Should have pending entry");
    assert_eq!(entry.path, "old.txt");
    state2.repl_log.mark_completed(id);

    assert!(state2.repl_log.try_next().is_none(), "Should be empty now");
}

// =========================================================================
// Test 3: Put coalescing
// =========================================================================

#[test]
fn test_put_coalescing() {
    let (_tmp, backing, replica) = setup();
    let state = make_state(&backing, &replica);

    // Enqueue multiple puts for the same file
    state.repl_log.enqueue_put("data.txt");
    state.repl_log.enqueue_put("data.txt");
    state.repl_log.enqueue_put("data.txt");

    // Only the last one should be uncompleted
    // First should be coalesced (completed)
    let (id1, entry1) = state.repl_log.try_next().unwrap();
    assert_eq!(entry1.path, "data.txt");
    // This should be the last enqueued put (the non-coalesced one)

    state.repl_log.mark_completed(id1);

    // After marking the one pending entry completed, there should be none left
    assert!(
        state.repl_log.try_next().is_none(),
        "Coalesced puts should leave only one effective entry"
    );
}

// =========================================================================
// Test 4: Rename pipeline — delete old + put new
// =========================================================================

#[test]
fn test_rename_pipeline() {
    let (_tmp, backing, replica) = setup();
    let state = make_state(&backing, &replica);

    // Create original file, checksum it, replicate it
    write_file(&backing.join("old.txt"), "rename me");
    helmetfs::state::checksum_and_enqueue(&state, "old.txt");

    // Process the put
    state.shutting_down.store(false, Ordering::Relaxed);
    let worker_state = state.clone();
    let worker = thread::spawn(move || {
        helmetfs::replication::replication_worker(&worker_state);
    });

    thread::sleep(Duration::from_millis(500));
    state.shutting_down.store(true, Ordering::Relaxed);
    state.repl_log.notify_all();
    worker.join().unwrap();

    assert!(replica.join("files/old.txt").exists());

    // Simulate rename: move file and .sum in backing
    fs::rename(backing.join("old.txt"), backing.join("new.txt")).unwrap();
    fs::rename(backing.join("old.txt.sum"), backing.join("new.txt.sum")).unwrap();

    // Enqueue delete for old, put for new
    let state2 = Arc::new(
        helmetfs::state::FsState::new(
            backing.to_path_buf(),
            replica.to_path_buf(),
            86400,
        )
        .unwrap(),
    );
    state2.repl_log.enqueue_delete("old.txt");
    state2.repl_log.enqueue_put("new.txt");

    // Process entries
    let worker_state2 = state2.clone();
    let worker2 = thread::spawn(move || {
        helmetfs::replication::replication_worker(&worker_state2);
    });

    thread::sleep(Duration::from_millis(500));
    state2.shutting_down.store(true, Ordering::Relaxed);
    state2.repl_log.notify_all();
    worker2.join().unwrap();

    // Old should be gone, new should exist in replica
    assert!(
        !replica.join("files/old.txt").exists(),
        "Old file should be removed from replica"
    );
    assert!(
        replica.join("files/new.txt").exists(),
        "New file should be in replica"
    );
    assert_eq!(
        read_file(&replica.join("files/new.txt")),
        "rename me"
    );
}

// =========================================================================
// Test 5: Mixed scrub — clean + untracked + corrupt
// =========================================================================

#[test]
fn test_mixed_scrub() {
    let (_tmp, backing, replica) = setup();
    let state = make_state(&backing, &replica);

    // File A: tracked and clean
    write_file(&backing.join("clean.txt"), "clean data");
    helmetfs::state::checksum_and_enqueue(&state, "clean.txt");

    // File B: untracked (no .sum)
    write_file(&backing.join("untracked.txt"), "untracked data");

    // Process puts to get things replicated
    let worker_state = state.clone();
    let worker = thread::spawn(move || {
        helmetfs::replication::replication_worker(&worker_state);
    });
    thread::sleep(Duration::from_millis(500));
    state.shutting_down.store(true, Ordering::Relaxed);
    state.repl_log.notify_all();
    worker.join().unwrap();

    // File C: corrupt (modify backing after checksum)
    write_file(&backing.join("corrupt.txt"), "original");
    // Create a fresh state to checksum+enqueue
    let state2 = Arc::new(
        helmetfs::state::FsState::new(
            backing.to_path_buf(),
            replica.to_path_buf(),
            86400,
        )
        .unwrap(),
    );
    helmetfs::state::checksum_and_enqueue(&state2, "corrupt.txt");

    // Replicate corrupt.txt
    let worker_state2 = state2.clone();
    let worker2 = thread::spawn(move || {
        helmetfs::replication::replication_worker(&worker_state2);
    });
    thread::sleep(Duration::from_millis(500));
    state2.shutting_down.store(true, Ordering::Relaxed);
    state2.repl_log.notify_all();
    worker2.join().unwrap();

    // Now corrupt it in backing
    write_file(&backing.join("corrupt.txt"), "DAMAGED");

    // Run scrub with fresh state
    let state3 = Arc::new(
        helmetfs::state::FsState::new(
            backing.to_path_buf(),
            replica.to_path_buf(),
            86400,
        )
        .unwrap(),
    );
    let _ = fs::remove_file(backing.join(".helmetfs").join("scrub.timestamp"));

    let scrub_state = state3.clone();
    let scrub = thread::spawn(move || {
        helmetfs::scrub::scrub_thread(&scrub_state);
    });
    thread::sleep(Duration::from_secs(2));
    state3.shutting_down.store(true, Ordering::Relaxed);
    scrub.join().unwrap();

    // Clean file should remain unchanged
    assert_eq!(read_file(&backing.join("clean.txt")), "clean data");

    // Untracked file should now have a .sum
    assert!(
        backing.join("untracked.txt.sum").exists(),
        "Untracked file should be adopted with .sum"
    );

    // Corrupt file should be healed
    assert_eq!(
        read_file(&backing.join("corrupt.txt")),
        "original",
        "Corrupt file should be healed from replica"
    );
}

// =========================================================================
// Test 6: Delete idempotency
// =========================================================================

#[test]
fn test_delete_idempotency() {
    let (_tmp, backing, replica) = setup();
    let state = make_state(&backing, &replica);

    // Create and replicate a file
    write_file(&backing.join("delme.txt"), "delete me");
    helmetfs::state::checksum_and_enqueue(&state, "delme.txt");

    let worker_state = state.clone();
    let worker = thread::spawn(move || {
        helmetfs::replication::replication_worker(&worker_state);
    });
    thread::sleep(Duration::from_millis(500));
    state.shutting_down.store(true, Ordering::Relaxed);
    state.repl_log.notify_all();
    worker.join().unwrap();

    assert!(replica.join("files/delme.txt").exists());

    // Enqueue delete twice — first remove the backing file and its .sum,
    // matching the real FUSE unlink flow.
    let state2 = Arc::new(
        helmetfs::state::FsState::new(
            backing.to_path_buf(),
            replica.to_path_buf(),
            86400,
        )
        .unwrap(),
    );
    let _ = std::fs::remove_file(backing.join("delme.txt"));
    let _ = std::fs::remove_file(backing.join("delme.txt.sum"));
    state2.repl_log.enqueue_delete("delme.txt");
    state2.repl_log.enqueue_delete("delme.txt");

    let worker_state2 = state2.clone();
    let worker2 = thread::spawn(move || {
        helmetfs::replication::replication_worker(&worker_state2);
    });
    thread::sleep(Duration::from_millis(500));
    state2.shutting_down.store(true, Ordering::Relaxed);
    state2.repl_log.notify_all();
    worker2.join().unwrap();

    // File should be gone (and no errors from double-delete)
    assert!(
        !replica.join("files/delme.txt").exists(),
        "File should be deleted from replica"
    );
    assert!(
        !replica.join("files/delme.txt.sum").exists(),
        ".sum should be deleted from replica"
    );
}
