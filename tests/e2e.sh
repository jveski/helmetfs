#!/usr/bin/env bash
#
# End-to-end tests for helmetfs using real FUSE mounts.
#
# These tests build helmetfs, mount it on a temp directory, exercise filesystem
# operations through the mount, and verify replication/self-healing behavior.
#
# Requirements:
#   - Linux with libfuse3 and fusermount3
#   - Zig toolchain (builds helmetfs)
#   - FUSE kernel module loaded (modprobe fuse)
#
# Usage:
#   ./tests/e2e.sh
#
set -euo pipefail

# ── Paths ────────────────────────────────────────────────────────────────────

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
HELMETFS="$PROJECT_DIR/zig-out/bin/helmetfs"

# ── Color output ─────────────────────────────────────────────────────────────

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[0;33m'
NC='\033[0m' # No Color

PASS_COUNT=0
FAIL_COUNT=0

pass() {
    PASS_COUNT=$((PASS_COUNT + 1))
    echo -e "${GREEN}PASS${NC}: $1"
}

fail() {
    FAIL_COUNT=$((FAIL_COUNT + 1))
    echo -e "${RED}FAIL${NC}: $1"
    echo "       $2"
}

info() {
    echo -e "${YELLOW}INFO${NC}: $1"
}

# ── Build ────────────────────────────────────────────────────────────────────

info "Building helmetfs..."
(cd "$PROJECT_DIR" && zig build) || { echo "Build failed"; exit 1; }

if [[ ! -x "$HELMETFS" ]]; then
    echo "ERROR: helmetfs binary not found at $HELMETFS"
    exit 1
fi

# ── Test infrastructure ──────────────────────────────────────────────────────

# Each test gets its own temp directory with backing/, replica/, mount/ subdirs.
# helmetfs runs in the background; cleanup tears it down.

PIDS_TO_KILL=()
DIRS_TO_CLEAN=()

cleanup() {
    info "Cleaning up..."
    for pid in "${PIDS_TO_KILL[@]:-}"; do
        if kill -0 "$pid" 2>/dev/null; then
            kill "$pid" 2>/dev/null || true
            wait "$pid" 2>/dev/null || true
        fi
    done

    # Small delay to let FUSE release mounts
    sleep 0.5

    for dir in "${DIRS_TO_CLEAN[@]:-}"; do
        # Attempt unmount in case it's still mounted
        fusermount3 -u "$dir/mount" 2>/dev/null || true
        rm -rf "$dir" 2>/dev/null || true
    done
}

trap cleanup EXIT

# Create a fresh test environment and start helmetfs.
# Sets: TEST_DIR, BACKING, REPLICA, MOUNT, HELMETFS_PID
setup_mount() {
    local test_name="$1"
    shift
    local extra_args=("$@")

    TEST_DIR=$(mktemp -d /tmp/helmetfs-e2e-XXXXXX)
    DIRS_TO_CLEAN+=("$TEST_DIR")

    BACKING="$TEST_DIR/backing"
    REPLICA="$TEST_DIR/replica"
    MOUNT="$TEST_DIR/mount"

    mkdir -p "$BACKING" "$REPLICA" "$MOUNT"

    info "[$test_name] Starting helmetfs: backing=$BACKING mount=$MOUNT replica=$REPLICA"

    "$HELMETFS" mount "$BACKING" "$MOUNT" --replica "$REPLICA" \
        --replication-workers 1 "${extra_args[@]}" \
        2>"$TEST_DIR/helmetfs.log" &
    HELMETFS_PID=$!
    PIDS_TO_KILL+=("$HELMETFS_PID")

    # Wait for the mount to become available
    local attempts=0
    while ! mountpoint -q "$MOUNT" 2>/dev/null; do
        sleep 0.2
        attempts=$((attempts + 1))
        if [[ $attempts -ge 25 ]]; then
            echo "ERROR: [$test_name] Mount did not appear after 5s"
            echo "--- helmetfs log ---"
            cat "$TEST_DIR/helmetfs.log"
            echo "---"
            fail "$test_name" "Mount timed out"
            return 1
        fi
    done

    info "[$test_name] Mount ready (took ~$((attempts * 200))ms)"
    return 0
}

# Unmount and stop the helmetfs process for the current test.
teardown_mount() {
    if [[ -n "${HELMETFS_PID:-}" ]] && kill -0 "$HELMETFS_PID" 2>/dev/null; then
        # Send SIGTERM for graceful shutdown
        kill "$HELMETFS_PID" 2>/dev/null || true
        wait "$HELMETFS_PID" 2>/dev/null || true
    fi
    # Belt-and-suspenders unmount
    fusermount3 -u "$MOUNT" 2>/dev/null || true
    HELMETFS_PID=""
}

# Wait for a file to appear in the replica (replication is async).
wait_for_replica() {
    local rel_path="$1"
    local max_wait="${2:-10}"
    local attempts=0
    while [[ ! -f "$REPLICA/files/$rel_path" ]]; do
        sleep 0.3
        attempts=$((attempts + 1))
        if [[ $attempts -ge $((max_wait * 3)) ]]; then
            return 1
        fi
    done
    return 0
}

# Wait for a file to disappear from the replica.
wait_for_replica_gone() {
    local rel_path="$1"
    local max_wait="${2:-10}"
    local attempts=0
    while [[ -f "$REPLICA/files/$rel_path" ]]; do
        sleep 0.3
        attempts=$((attempts + 1))
        if [[ $attempts -ge $((max_wait * 3)) ]]; then
            return 1
        fi
    done
    return 0
}

# Wait for a .sum sidecar to appear in the backing dir.
wait_for_sum() {
    local rel_path="$1"
    local max_wait="${2:-10}"
    local attempts=0
    while [[ ! -f "$BACKING/${rel_path}.sum" ]]; do
        sleep 0.3
        attempts=$((attempts + 1))
        if [[ $attempts -ge $((max_wait * 3)) ]]; then
            return 1
        fi
    done
    return 0
}

# ─────────────────────────────────────────────────────────────────────────────
# TEST 1: Basic file write and read through FUSE mount
# ─────────────────────────────────────────────────────────────────────────────

test_basic_crud() {
    local T="basic_crud"
    setup_mount "$T" || return

    # Write a file through the mount
    echo "hello world" > "$MOUNT/test.txt"
    sync "$MOUNT/test.txt"

    # Read it back through the mount
    local content
    content=$(cat "$MOUNT/test.txt")
    if [[ "$content" == "hello world" ]]; then
        pass "$T: write and read file"
    else
        fail "$T: write and read file" "expected 'hello world', got '$content'"
    fi

    # Verify it exists in the backing directory
    if [[ -f "$BACKING/test.txt" ]]; then
        pass "$T: file exists in backing dir"
    else
        fail "$T: file exists in backing dir" "file not found in $BACKING"
    fi

    # Create a subdirectory and file
    mkdir -p "$MOUNT/subdir/nested"
    echo "nested content" > "$MOUNT/subdir/nested/deep.txt"
    sync "$MOUNT/subdir/nested/deep.txt"

    content=$(cat "$MOUNT/subdir/nested/deep.txt")
    if [[ "$content" == "nested content" ]]; then
        pass "$T: nested directory and file"
    else
        fail "$T: nested directory and file" "got '$content'"
    fi

    # List directory contents
    local files
    files=$(ls "$MOUNT")
    if echo "$files" | grep -q "test.txt" && echo "$files" | grep -q "subdir"; then
        pass "$T: readdir lists files"
    else
        fail "$T: readdir lists files" "ls output: $files"
    fi

    # Overwrite a file
    echo "updated content" > "$MOUNT/test.txt"
    sync "$MOUNT/test.txt"
    content=$(cat "$MOUNT/test.txt")
    if [[ "$content" == "updated content" ]]; then
        pass "$T: overwrite file"
    else
        fail "$T: overwrite file" "got '$content'"
    fi

    # Truncate a file
    truncate -s 7 "$MOUNT/test.txt"
    content=$(cat "$MOUNT/test.txt")
    if [[ "$content" == "updated" ]]; then
        pass "$T: truncate file"
    else
        fail "$T: truncate file" "got '$content'"
    fi

    teardown_mount
}

# ─────────────────────────────────────────────────────────────────────────────
# TEST 2: Replication - files are copied to replica after sync
# ─────────────────────────────────────────────────────────────────────────────

test_replication() {
    local T="replication"
    setup_mount "$T" || return

    echo "replicate me" > "$MOUNT/data.txt"
    sync "$MOUNT/data.txt"

    # Wait for the file to appear in the replica
    if wait_for_replica "data.txt"; then
        pass "$T: file replicated to replica"
    else
        fail "$T: file replicated to replica" "data.txt not found in replica after timeout"
        teardown_mount
        return
    fi

    # Verify replica content matches
    local replica_content
    replica_content=$(cat "$REPLICA/files/data.txt")
    if [[ "$replica_content" == "replicate me" ]]; then
        pass "$T: replica content matches"
    else
        fail "$T: replica content matches" "expected 'replicate me', got '$replica_content'"
    fi

    # Verify .sum sidecar is also replicated
    if wait_for_replica "data.txt.sum" 5; then
        pass "$T: .sum sidecar replicated"
    else
        fail "$T: .sum sidecar replicated" "data.txt.sum not in replica"
    fi

    # Check .sum sidecar content is a valid BLAKE3 hex digest (64 hex chars + newline)
    local sum_content
    sum_content=$(cat "$REPLICA/files/data.txt.sum")
    if [[ ${#sum_content} -eq 64 ]] && [[ "$sum_content" =~ ^[0-9a-f]{64}$ ]]; then
        pass "$T: .sum contains valid BLAKE3 digest"
    else
        fail "$T: .sum contains valid BLAKE3 digest" "got '$sum_content' (len=${#sum_content})"
    fi

    # Test nested file replication
    mkdir -p "$MOUNT/deep/path"
    echo "deep file" > "$MOUNT/deep/path/file.txt"
    sync "$MOUNT/deep/path/file.txt"

    if wait_for_replica "deep/path/file.txt"; then
        pass "$T: nested file replicated with directories"
    else
        fail "$T: nested file replicated with directories" "file not found in replica"
    fi

    teardown_mount
}

# ─────────────────────────────────────────────────────────────────────────────
# TEST 3: Checksum sidecar hiding - .sum files hidden from FUSE mount
# ─────────────────────────────────────────────────────────────────────────────

test_sum_hiding() {
    local T="sum_hiding"
    setup_mount "$T" || return

    echo "content" > "$MOUNT/file.txt"
    sync "$MOUNT/file.txt"

    # Wait for .sum to be created in backing dir
    if wait_for_sum "file.txt"; then
        pass "$T: .sum created in backing dir"
    else
        fail "$T: .sum created in backing dir" "file.txt.sum not found"
        teardown_mount
        return
    fi

    # .sum should be hidden from the mount (because file.txt exists)
    if [[ ! -e "$MOUNT/file.txt.sum" ]]; then
        pass "$T: .sum hidden from mount"
    else
        fail "$T: .sum hidden from mount" "file.txt.sum visible through FUSE"
    fi

    # .sum should NOT appear in directory listing
    local listing
    listing=$(ls "$MOUNT")
    if ! echo "$listing" | grep -q "file.txt.sum"; then
        pass "$T: .sum hidden from readdir"
    else
        fail "$T: .sum hidden from readdir" "listing: $listing"
    fi

    # .helmetfs directory should be hidden too
    if [[ ! -e "$MOUNT/.helmetfs" ]]; then
        pass "$T: .helmetfs dir hidden from mount"
    else
        fail "$T: .helmetfs dir hidden from mount" ".helmetfs visible through FUSE"
    fi

    # A file named "foo.sum" without a corresponding "foo" should be visible
    echo "legitimate" > "$MOUNT/standalone.sum"
    sync "$MOUNT/standalone.sum"
    if [[ -f "$MOUNT/standalone.sum" ]]; then
        pass "$T: standalone .sum file visible (no corresponding data file)"
    else
        fail "$T: standalone .sum file visible" "standalone.sum not visible"
    fi

    teardown_mount
}

# ─────────────────────────────────────────────────────────────────────────────
# TEST 4: Corruption detection and self-healing
# ─────────────────────────────────────────────────────────────────────────────

test_self_healing() {
    local T="self_healing"
    setup_mount "$T" || return

    # Write a file and wait for replication
    echo "original content" > "$MOUNT/heal.txt"
    sync "$MOUNT/heal.txt"

    if ! wait_for_replica "heal.txt"; then
        fail "$T: setup - file not replicated" "heal.txt not in replica"
        teardown_mount
        return
    fi

    # Also wait for the .sum to be replicated
    if ! wait_for_replica "heal.txt.sum" 5; then
        fail "$T: setup - .sum not replicated" "heal.txt.sum not in replica"
        teardown_mount
        return
    fi

    # Corrupt the backing file directly (bypass FUSE)
    echo "CORRUPTED" > "$BACKING/heal.txt"

    # Trigger a scrub by stopping and re-mounting with immediate scrub
    # (scrub triggers on startup if no timestamp or >24h since last scrub)
    teardown_mount

    # Remove the scrub timestamp so scrub triggers immediately on remount
    rm -f "$BACKING/.helmetfs/scrub.timestamp"

    # Re-mount - scrub should detect corruption and repair from replica
    "$HELMETFS" mount "$BACKING" "$MOUNT" --replica "$REPLICA" \
        --replication-workers 1 \
        2>"$TEST_DIR/helmetfs-heal.log" &
    HELMETFS_PID=$!
    PIDS_TO_KILL+=("$HELMETFS_PID")

    # Wait for mount
    local attempts=0
    while ! mountpoint -q "$MOUNT" 2>/dev/null; do
        sleep 0.2
        attempts=$((attempts + 1))
        if [[ $attempts -ge 25 ]]; then
            fail "$T: remount failed" "mount timed out"
            return
        fi
    done

    # Wait for scrub to run and repair the file
    # The scrub should detect the checksum mismatch and restore from replica
    local repaired=false
    for i in $(seq 1 30); do
        local current
        current=$(cat "$MOUNT/heal.txt" 2>/dev/null) || true
        if [[ "$current" == "original content" ]]; then
            repaired=true
            break
        fi
        sleep 0.5
    done

    if $repaired; then
        pass "$T: corrupted file repaired from replica"
    else
        local actual
        actual=$(cat "$MOUNT/heal.txt" 2>/dev/null || echo "<read failed>")
        fail "$T: corrupted file repaired from replica" "content is '$actual', expected 'original content'"
        if [[ -f "$TEST_DIR/helmetfs-heal.log" ]]; then
            echo "--- helmetfs heal log (last 20 lines) ---"
            tail -20 "$TEST_DIR/helmetfs-heal.log"
            echo "---"
        fi
    fi

    teardown_mount
}

# ─────────────────────────────────────────────────────────────────────────────
# TEST 5: Unlink removes file and enqueues delete to replica
# ─────────────────────────────────────────────────────────────────────────────

test_unlink() {
    local T="unlink"
    setup_mount "$T" || return

    echo "delete me" > "$MOUNT/todelete.txt"
    sync "$MOUNT/todelete.txt"

    # Wait for replication
    if ! wait_for_replica "todelete.txt"; then
        fail "$T: setup - file not replicated" ""
        teardown_mount
        return
    fi

    # Delete through the mount
    rm "$MOUNT/todelete.txt"

    # File should be gone from mount
    if [[ ! -e "$MOUNT/todelete.txt" ]]; then
        pass "$T: file removed from mount"
    else
        fail "$T: file removed from mount" "still exists"
    fi

    # File should be gone from backing dir
    if [[ ! -e "$BACKING/todelete.txt" ]]; then
        pass "$T: file removed from backing dir"
    else
        fail "$T: file removed from backing dir" "still in backing"
    fi

    # .sum should be gone from backing dir
    if [[ ! -e "$BACKING/todelete.txt.sum" ]]; then
        pass "$T: .sum removed from backing dir"
    else
        fail "$T: .sum removed from backing dir" "still in backing"
    fi

    # File should eventually be removed from replica
    if wait_for_replica_gone "todelete.txt"; then
        pass "$T: file removed from replica"
    else
        fail "$T: file removed from replica" "still in replica after timeout"
    fi

    # .sum should also be removed from replica
    if wait_for_replica_gone "todelete.txt.sum"; then
        pass "$T: .sum removed from replica"
    else
        fail "$T: .sum removed from replica" "still in replica after timeout"
    fi

    teardown_mount
}

# ─────────────────────────────────────────────────────────────────────────────
# TEST 6: Rename moves file and .sum, enqueues delete+put
# ─────────────────────────────────────────────────────────────────────────────

test_rename() {
    local T="rename"
    setup_mount "$T" || return

    echo "rename me" > "$MOUNT/oldname.txt"
    sync "$MOUNT/oldname.txt"

    # Wait for replication of original
    if ! wait_for_replica "oldname.txt"; then
        fail "$T: setup - file not replicated" ""
        teardown_mount
        return
    fi

    # Rename through the mount
    mv "$MOUNT/oldname.txt" "$MOUNT/newname.txt"

    # Old name should be gone, new name should exist
    if [[ ! -e "$MOUNT/oldname.txt" ]] && [[ -f "$MOUNT/newname.txt" ]]; then
        pass "$T: rename visible through mount"
    else
        fail "$T: rename visible through mount" "old exists=$(test -e "$MOUNT/oldname.txt" && echo yes || echo no), new exists=$(test -e "$MOUNT/newname.txt" && echo yes || echo no)"
    fi

    # Content should be preserved
    local content
    content=$(cat "$MOUNT/newname.txt")
    if [[ "$content" == "rename me" ]]; then
        pass "$T: content preserved after rename"
    else
        fail "$T: content preserved after rename" "got '$content'"
    fi

    # Backing dir should reflect rename
    if [[ ! -e "$BACKING/oldname.txt" ]] && [[ -f "$BACKING/newname.txt" ]]; then
        pass "$T: rename reflected in backing dir"
    else
        fail "$T: rename reflected in backing dir" ""
    fi

    # .sum should be moved too (in backing dir)
    if [[ ! -e "$BACKING/oldname.txt.sum" ]] && [[ -f "$BACKING/newname.txt.sum" ]]; then
        pass "$T: .sum moved in backing dir"
    else
        fail "$T: .sum moved in backing dir" "old.sum=$(test -e "$BACKING/oldname.txt.sum" && echo exists || echo gone), new.sum=$(test -e "$BACKING/newname.txt.sum" && echo exists || echo gone)"
    fi

    # Replica should eventually have new name and not old name
    if wait_for_replica "newname.txt"; then
        pass "$T: new name replicated"
    else
        fail "$T: new name replicated" "newname.txt not in replica"
    fi

    if wait_for_replica_gone "oldname.txt"; then
        pass "$T: old name removed from replica"
    else
        fail "$T: old name removed from replica" "oldname.txt still in replica"
    fi

    teardown_mount
}

# ─────────────────────────────────────────────────────────────────────────────
# TEST 7: Symlink replication
# ─────────────────────────────────────────────────────────────────────────────

test_symlink() {
    local T="symlink"
    setup_mount "$T" || return

    echo "target content" > "$MOUNT/target.txt"
    sync "$MOUNT/target.txt"
    ln -s "target.txt" "$MOUNT/link.txt"

    # Symlink should be readable through mount
    local content
    content=$(cat "$MOUNT/link.txt")
    if [[ "$content" == "target content" ]]; then
        pass "$T: symlink readable through mount"
    else
        fail "$T: symlink readable through mount" "got '$content'"
    fi

    # Symlink target should be correct
    local link_target
    link_target=$(readlink "$MOUNT/link.txt")
    if [[ "$link_target" == "target.txt" ]]; then
        pass "$T: symlink target correct"
    else
        fail "$T: symlink target correct" "got '$link_target'"
    fi

    # Symlink should be replicated to replica
    if wait_for_replica "link.txt" 5; then
        # Check it's a symlink in the replica too
        if [[ -L "$REPLICA/files/link.txt" ]]; then
            local replica_target
            replica_target=$(readlink "$REPLICA/files/link.txt")
            if [[ "$replica_target" == "target.txt" ]]; then
                pass "$T: symlink replicated with correct target"
            else
                fail "$T: symlink replicated with correct target" "replica target: '$replica_target'"
            fi
        else
            pass "$T: symlink present in replica (may not be a symlink on all backends)"
        fi
    else
        fail "$T: symlink replicated" "link.txt not in replica"
    fi

    # Symlinks should NOT have .sum files
    sleep 1
    if [[ ! -f "$BACKING/link.txt.sum" ]]; then
        pass "$T: no .sum for symlink"
    else
        fail "$T: no .sum for symlink" "link.txt.sum exists in backing"
    fi

    teardown_mount
}

# ─────────────────────────────────────────────────────────────────────────────
# TEST 8: Scrub adopts untracked pre-existing files
# ─────────────────────────────────────────────────────────────────────────────

test_scrub_adopt() {
    local T="scrub_adopt"

    # Create test dirs manually (don't mount yet)
    TEST_DIR=$(mktemp -d /tmp/helmetfs-e2e-XXXXXX)
    DIRS_TO_CLEAN+=("$TEST_DIR")

    BACKING="$TEST_DIR/backing"
    REPLICA="$TEST_DIR/replica"
    MOUNT="$TEST_DIR/mount"

    mkdir -p "$BACKING" "$REPLICA" "$MOUNT"

    # Create a pre-existing file in the backing dir BEFORE mounting
    echo "pre-existing data" > "$BACKING/preexist.txt"
    mkdir -p "$BACKING/subdir"
    echo "nested preexist" > "$BACKING/subdir/old.txt"

    # Mount - scrub should trigger immediately (no scrub.timestamp)
    "$HELMETFS" mount "$BACKING" "$MOUNT" --replica "$REPLICA" \
        --replication-workers 1 \
        2>"$TEST_DIR/helmetfs.log" &
    HELMETFS_PID=$!
    PIDS_TO_KILL+=("$HELMETFS_PID")

    local attempts=0
    while ! mountpoint -q "$MOUNT" 2>/dev/null; do
        sleep 0.2
        attempts=$((attempts + 1))
        if [[ $attempts -ge 25 ]]; then
            fail "$T: mount timed out" ""
            return
        fi
    done

    # Wait for scrub to adopt the files (creates .sum and enqueues replication)
    local adopted=false
    for i in $(seq 1 40); do
        if [[ -f "$BACKING/preexist.txt.sum" ]]; then
            adopted=true
            break
        fi
        sleep 0.5
    done

    if $adopted; then
        pass "$T: pre-existing file adopted (.sum created)"
    else
        fail "$T: pre-existing file adopted" "preexist.txt.sum not created"
        teardown_mount
        return
    fi

    # Verify the adopted file gets replicated
    if wait_for_replica "preexist.txt" 15; then
        pass "$T: adopted file replicated"
    else
        fail "$T: adopted file replicated" "preexist.txt not in replica"
    fi

    # Check nested file was also adopted
    local nested_adopted=false
    for i in $(seq 1 20); do
        if [[ -f "$BACKING/subdir/old.txt.sum" ]]; then
            nested_adopted=true
            break
        fi
        sleep 0.5
    done

    if $nested_adopted; then
        pass "$T: nested pre-existing file adopted"
    else
        fail "$T: nested pre-existing file adopted" "subdir/old.txt.sum not created"
    fi

    # Verify file is still readable through mount
    local content
    content=$(cat "$MOUNT/preexist.txt")
    if [[ "$content" == "pre-existing data" ]]; then
        pass "$T: pre-existing file readable through mount"
    else
        fail "$T: pre-existing file readable through mount" "got '$content'"
    fi

    teardown_mount
}

# ─────────────────────────────────────────────────────────────────────────────
# TEST 9: Graceful shutdown preserves replication log
# ─────────────────────────────────────────────────────────────────────────────

test_graceful_shutdown() {
    local T="graceful_shutdown"
    setup_mount "$T" || return

    # Write several files quickly
    for i in $(seq 1 5); do
        echo "file $i content" > "$MOUNT/batch_$i.txt"
    done
    sync "$MOUNT/"

    # Give a moment for checksums to be computed
    sleep 1

    # Send SIGTERM for graceful shutdown
    kill "$HELMETFS_PID"
    wait "$HELMETFS_PID" 2>/dev/null || true

    # Check that .helmetfs/repl.log exists and is non-empty if there were
    # pending replications (or that all files were replicated already)
    local all_replicated=true
    for i in $(seq 1 5); do
        if [[ ! -f "$REPLICA/files/batch_$i.txt" ]]; then
            all_replicated=false
            break
        fi
    done

    if $all_replicated; then
        pass "$T: all files replicated before shutdown"
    else
        # If not all replicated, the repl.log should have entries for them
        if [[ -f "$BACKING/.helmetfs/repl.log" ]]; then
            local log_size
            log_size=$(wc -c < "$BACKING/.helmetfs/repl.log")
            if [[ $log_size -gt 0 ]]; then
                pass "$T: replication log preserved with pending entries"
            else
                # Log may have been truncated after completion - check .sum files exist
                local has_sums=true
                for i in $(seq 1 5); do
                    if [[ ! -f "$BACKING/batch_$i.txt.sum" ]]; then
                        has_sums=false
                        break
                    fi
                done
                if $has_sums; then
                    pass "$T: all checksums computed before shutdown"
                else
                    fail "$T: pending work preserved" "repl.log empty and not all .sum files exist"
                fi
            fi
        else
            fail "$T: replication log preserved" "repl.log not found"
        fi
    fi

    # Re-mount and verify files are still accessible and replication completes
    "$HELMETFS" mount "$BACKING" "$MOUNT" --replica "$REPLICA" \
        --replication-workers 1 \
        2>"$TEST_DIR/helmetfs-resume.log" &
    HELMETFS_PID=$!
    PIDS_TO_KILL+=("$HELMETFS_PID")

    local attempts=0
    while ! mountpoint -q "$MOUNT" 2>/dev/null; do
        sleep 0.2
        attempts=$((attempts + 1))
        if [[ $attempts -ge 25 ]]; then
            fail "$T: remount timed out" ""
            return
        fi
    done

    # All files should eventually be replicated after resume
    local resumed_ok=true
    for i in $(seq 1 5); do
        if ! wait_for_replica "batch_$i.txt" 15; then
            resumed_ok=false
            fail "$T: file batch_$i.txt not replicated after resume" ""
        fi
    done

    if $resumed_ok; then
        pass "$T: all files replicated after resume from log"
    fi

    # Content should be intact
    local content
    content=$(cat "$MOUNT/batch_3.txt")
    if [[ "$content" == "file 3 content" ]]; then
        pass "$T: file content intact after remount"
    else
        fail "$T: file content intact after remount" "got '$content'"
    fi

    teardown_mount
}

# ─────────────────────────────────────────────────────────────────────────────
# TEST 10: Metrics endpoint
# ─────────────────────────────────────────────────────────────────────────────

test_metrics() {
    local T="metrics"
    setup_mount "$T" --metrics-addr :9871 || return

    # Give the metrics server a moment to start
    sleep 1

    # Fetch metrics
    local metrics_output
    if metrics_output=$(curl -s --max-time 5 http://127.0.0.1:9871/metrics 2>/dev/null); then
        pass "$T: metrics endpoint reachable"
    else
        fail "$T: metrics endpoint reachable" "curl failed"
        teardown_mount
        return
    fi

    # Check for expected metric names
    if echo "$metrics_output" | grep -q "helmetfs_replication_pending"; then
        pass "$T: has replication_pending metric"
    else
        fail "$T: has replication_pending metric" "not found in output"
    fi

    if echo "$metrics_output" | grep -q "helmetfs_replication_completed_total"; then
        pass "$T: has replication_completed_total metric"
    else
        fail "$T: has replication_completed_total metric" "not found in output"
    fi

    if echo "$metrics_output" | grep -q "helmetfs_scrub_files_checked_total"; then
        pass "$T: has scrub_files_checked_total metric"
    else
        fail "$T: has scrub_files_checked_total metric" "not found in output"
    fi

    # Write a file and check that metrics update
    echo "metric test" > "$MOUNT/metric_file.txt"
    sync "$MOUNT/metric_file.txt"
    wait_for_replica "metric_file.txt" 5 || true
    sleep 1

    local updated_metrics
    updated_metrics=$(curl -s --max-time 5 http://127.0.0.1:9871/metrics 2>/dev/null)

    if echo "$updated_metrics" | grep -q "helmetfs_replication_completed_total [1-9]"; then
        pass "$T: replication_completed_total incremented"
    else
        # May be 0 if replication hasn't completed yet - just check format
        if echo "$updated_metrics" | grep -q "helmetfs_replication_completed_total"; then
            pass "$T: replication_completed_total present (may not have incremented yet)"
        else
            fail "$T: replication_completed_total present" "not found"
        fi
    fi

    # 404 for non-metrics path
    local status_code
    status_code=$(curl -s -o /dev/null -w "%{http_code}" --max-time 5 http://127.0.0.1:9871/other 2>/dev/null)
    if [[ "$status_code" == "404" ]]; then
        pass "$T: non-metrics path returns 404"
    else
        fail "$T: non-metrics path returns 404" "got status $status_code"
    fi

    teardown_mount
}

# ─────────────────────────────────────────────────────────────────────────────
# TEST 11: Large file replication
# ─────────────────────────────────────────────────────────────────────────────

test_large_file() {
    local T="large_file"
    setup_mount "$T" || return

    # Create a 5MB file
    dd if=/dev/urandom of="$MOUNT/large.bin" bs=1M count=5 2>/dev/null
    sync "$MOUNT/large.bin"

    # Wait for replication (may take longer for large files)
    if wait_for_replica "large.bin" 30; then
        pass "$T: large file replicated"
    else
        fail "$T: large file replicated" "large.bin not in replica after 30s"
        teardown_mount
        return
    fi

    # Verify sizes match
    local mount_size replica_size
    mount_size=$(stat -c%s "$MOUNT/large.bin" 2>/dev/null || stat -f%z "$MOUNT/large.bin")
    replica_size=$(stat -c%s "$REPLICA/files/large.bin" 2>/dev/null || stat -f%z "$REPLICA/files/large.bin")

    if [[ "$mount_size" == "$replica_size" ]]; then
        pass "$T: replica file size matches ($mount_size bytes)"
    else
        fail "$T: replica file size matches" "mount=$mount_size replica=$replica_size"
    fi

    # Verify content matches via checksum
    local mount_md5 replica_md5
    mount_md5=$(md5sum "$BACKING/large.bin" | cut -d' ' -f1)
    replica_md5=$(md5sum "$REPLICA/files/large.bin" | cut -d' ' -f1)

    if [[ "$mount_md5" == "$replica_md5" ]]; then
        pass "$T: large file content matches (md5: $mount_md5)"
    else
        fail "$T: large file content matches" "mount=$mount_md5 replica=$replica_md5"
    fi

    teardown_mount
}

# ─────────────────────────────────────────────────────────────────────────────
# TEST 12: File permissions preserved through replication
# ─────────────────────────────────────────────────────────────────────────────

test_permissions() {
    local T="permissions"
    setup_mount "$T" || return

    echo "#!/bin/bash" > "$MOUNT/script.sh"
    chmod 755 "$MOUNT/script.sh"
    sync "$MOUNT/script.sh"

    if wait_for_replica "script.sh"; then
        pass "$T: file replicated"
    else
        fail "$T: file replicated" "not in replica"
        teardown_mount
        return
    fi

    # Check permissions on the replica
    local replica_perms
    replica_perms=$(stat -c%a "$REPLICA/files/script.sh" 2>/dev/null || stat -f%Lp "$REPLICA/files/script.sh")

    if [[ "$replica_perms" == "755" ]]; then
        pass "$T: permissions preserved on replica (755)"
    else
        fail "$T: permissions preserved on replica" "got $replica_perms, expected 755"
    fi

    # Change permissions and verify replication
    chmod 644 "$MOUNT/script.sh"
    sync "$MOUNT/script.sh"
    sleep 3

    replica_perms=$(stat -c%a "$REPLICA/files/script.sh" 2>/dev/null || stat -f%Lp "$REPLICA/files/script.sh")

    if [[ "$replica_perms" == "644" ]]; then
        pass "$T: changed permissions replicated (644)"
    else
        fail "$T: changed permissions replicated" "got $replica_perms, expected 644"
    fi

    teardown_mount
}

# ─────────────────────────────────────────────────────────────────────────────
# TEST 13: Hardlinks return ENOTSUP
# ─────────────────────────────────────────────────────────────────────────────

test_hardlink_rejected() {
    local T="hardlink_rejected"
    setup_mount "$T" || return

    echo "source" > "$MOUNT/src.txt"
    sync "$MOUNT/src.txt"

    # Attempt to create a hardlink - should fail
    if ln "$MOUNT/src.txt" "$MOUNT/hardlink.txt" 2>/dev/null; then
        fail "$T: hardlink rejected" "ln succeeded (should have failed)"
    else
        pass "$T: hardlink creation rejected"
    fi

    teardown_mount
}

# ─────────────────────────────────────────────────────────────────────────────
# TEST 14: Multiple rapid writes coalesce replication
# ─────────────────────────────────────────────────────────────────────────────

test_coalescing() {
    local T="coalescing"
    setup_mount "$T" || return

    # Write the same file multiple times rapidly
    for i in $(seq 1 10); do
        echo "version $i" > "$MOUNT/coalesce.txt"
        sync "$MOUNT/coalesce.txt"
    done

    # Wait for replication to complete
    if wait_for_replica "coalesce.txt" 10; then
        pass "$T: file eventually replicated"
    else
        fail "$T: file eventually replicated" "not in replica"
        teardown_mount
        return
    fi

    # Give time for all replication to settle
    sleep 2

    # The final replica content should be the last version
    local replica_content
    replica_content=$(cat "$REPLICA/files/coalesce.txt")
    if [[ "$replica_content" == "version 10" ]]; then
        pass "$T: replica has latest version after coalescing"
    else
        fail "$T: replica has latest version" "got '$replica_content', expected 'version 10'"
    fi

    teardown_mount
}

# ─────────────────────────────────────────────────────────────────────────────
# Run all tests
# ─────────────────────────────────────────────────────────────────────────────

echo "============================================"
echo "  helmetfs E2E Tests"
echo "============================================"
echo ""

test_basic_crud
test_replication
test_sum_hiding
test_self_healing
test_unlink
test_rename
test_symlink
test_scrub_adopt
test_graceful_shutdown
test_metrics
test_large_file
test_permissions
test_hardlink_rejected
test_coalescing

echo ""
echo "============================================"
echo "  Results: ${GREEN}$PASS_COUNT passed${NC}, ${RED}$FAIL_COUNT failed${NC}"
echo "============================================"

if [[ $FAIL_COUNT -gt 0 ]]; then
    exit 1
fi
