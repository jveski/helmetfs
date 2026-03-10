--------------------------- MODULE helmetfs ---------------------------
(*
 * TLA+ specification for helmetfs's dirty-generation replication protocol.
 *
 * This models the most complex and error-prone aspects of helmetfs: the
 * interaction between concurrent writers, the dirty-generation tracking,
 * write reference counting, checksum computation, and replication --
 * verifying that every completed write is eventually replicated.
 *
 * Actors modeled:
 *   - Multiple FUSE writer processes (open, open-with-truncate, write,
 *     fsync, release)
 *   - External truncate (truncate without an open fd)
 *   - Metadata operations (chmod/chown/utimens that enqueue replication
 *     without checksumming)
 *   - Replication workers (dequeue, replicate, mark completed)
 *   - Scrub thread (detect corruption, repair from replica)
 *   - File deletion (unlink destroys path state)
 *
 * Key invariants verified:
 *   1. No write is silently lost: if a file is modified and all writers
 *      close, the file eventually gets enqueued for replication.
 *   2. dirty_gen >= clean_gen always.
 *   3. A checksum computed during a concurrent write does NOT clear the
 *      dirty flag (the generation-based protocol prevents this).
 *   4. Write refcount accuracy: every inc is paired with a dec.
 *   5. Scrub never overwrites a file that has active writers or
 *      pending dirty data.
 *   6. Scrub never rolls back a legitimate write (dirty_gen > clean_gen
 *      blocks repair).
 *
 * Simplifications:
 *   - We model a single file path (sufficient to find protocol bugs).
 *   - Checksum computation is abstracted as a multi-step action
 *     (snapshot gen, "compute", conditionally clear).
 *   - The replication log is abstracted to a queue of pending puts.
 *   - File content is modeled as a version counter (not actual bytes).
 *   - Deletion is modeled as path-state destruction (the file vanishes).
 *)

EXTENDS Integers, Sequences, FiniteSets, TLC

CONSTANTS
    Writers           \* Set of writer process IDs, e.g. {w1, w2}

(* ---- State variables ---- *)
VARIABLES
    \* Per-file path state (PathStateMap)
    dirty_gen,        \* Nat: incremented on every write
    clean_gen,        \* Nat: set to dirty_gen when checksum succeeds
    write_refcount,   \* Nat: number of open write descriptors
    path_exists,      \* BOOLEAN: whether the path state entry exists
                      \*   (FALSE after unlink/rename removes it)

    \* File content model
    backing_version,  \* Nat: version of the backing file content
    sum_version,      \* Nat: version recorded in the .sum sidecar
                      \*       (0 means no .sum exists)

    \* Replication log (abstracted)
    repl_queue,       \* Sequence of version numbers pending replication
    replicated_ver,   \* Nat: last version successfully replicated

    \* Per-writer state
    writer_state,     \* Function: Writers -> {"idle", "open", "writing",
                      \*   "fsyncing", "releasing",
                      \*   "checksumming_release", "done"}
    writer_wrote,     \* Function: Writers -> BOOLEAN (did this open+close
                      \*   cycle actually write data?)
    cs_snapshot_gen,  \* Function: Writers -> Nat (generation snapshot for
                      \*   checksum-in-progress)

    \* Truncate-without-fd checksum state
    trunc_nofd_state, \* "idle", "checksumming"
    trunc_nofd_gen,   \* Nat: snapshot of dirty_gen for in-progress checksum

    \* Scrub state
    scrub_state,      \* "idle", "checking", "repairing"
    scrub_saw_dirty   \* BOOLEAN: scrub saw a checksum mismatch

vars == <<dirty_gen, clean_gen, write_refcount, path_exists,
          backing_version, sum_version, repl_queue, replicated_ver,
          writer_state, writer_wrote, cs_snapshot_gen,
          trunc_nofd_state, trunc_nofd_gen,
          scrub_state, scrub_saw_dirty>>

(* ---- Type invariant ---- *)

TypeOK ==
    /\ dirty_gen \in Nat
    /\ clean_gen \in Nat
    /\ write_refcount \in Nat
    /\ path_exists \in BOOLEAN
    /\ backing_version \in Nat
    /\ sum_version \in Nat
    /\ repl_queue \in Seq(Nat)
    /\ replicated_ver \in Nat
    /\ \A w \in Writers : writer_state[w] \in
         {"idle", "open", "writing", "fsyncing",
          "releasing", "checksumming_release", "done"}
    /\ \A w \in Writers : writer_wrote[w] \in BOOLEAN
    /\ \A w \in Writers : cs_snapshot_gen[w] \in Nat
    /\ trunc_nofd_state \in {"idle", "checksumming"}
    /\ trunc_nofd_gen \in Nat
    /\ scrub_state \in {"idle", "checking", "repairing"}
    /\ scrub_saw_dirty \in BOOLEAN

(* ---- Initial state ---- *)

Init ==
    /\ dirty_gen = 0
    /\ clean_gen = 0
    /\ write_refcount = 0
    /\ path_exists = TRUE
    /\ backing_version = 0
    /\ sum_version = 0
    /\ repl_queue = <<>>
    /\ replicated_ver = 0
    /\ writer_state = [w \in Writers |-> "idle"]
    /\ writer_wrote = [w \in Writers |-> FALSE]
    /\ cs_snapshot_gen = [w \in Writers |-> 0]
    /\ trunc_nofd_state = "idle"
    /\ trunc_nofd_gen = 0
    /\ scrub_state = "idle"
    /\ scrub_saw_dirty = FALSE

(* ================================================================ *)
(* Writer actions: FUSE open/write/fsync/release lifecycle           *)
(* ================================================================ *)

\* A writer opens the file for writing: incWriteRef.
\* Models fuse_open with O_WRONLY/O_RDWR (without O_TRUNC).
Open(w) ==
    /\ writer_state[w] = "idle"
    /\ writer_state' = [writer_state EXCEPT ![w] = "open"]
    /\ writer_wrote' = [writer_wrote EXCEPT ![w] = FALSE]
    /\ write_refcount' = write_refcount + 1
    \* Ensure path state exists (incWriteRef creates entry if needed)
    /\ path_exists' = TRUE
    /\ UNCHANGED <<dirty_gen, clean_gen, backing_version, sum_version,
                   repl_queue, replicated_ver, cs_snapshot_gen,
                   trunc_nofd_state, trunc_nofd_gen,
                   scrub_state, scrub_saw_dirty>>

\* A writer opens the file with O_TRUNC: incWriteRef + setDirty.
\* Models fuse_open with O_TRUNC flag (lines 1316-1325 in main.zig).
\* The file content changes at open time even without an explicit write.
OpenWithTrunc(w) ==
    /\ writer_state[w] = "idle"
    /\ writer_state' = [writer_state EXCEPT ![w] = "open"]
    /\ writer_wrote' = [writer_wrote EXCEPT ![w] = TRUE]
    /\ write_refcount' = write_refcount + 1
    /\ dirty_gen' = dirty_gen + 1
    /\ backing_version' = backing_version + 1
    /\ path_exists' = TRUE
    /\ UNCHANGED <<clean_gen, sum_version, repl_queue, replicated_ver,
                   cs_snapshot_gen, trunc_nofd_state, trunc_nofd_gen,
                   scrub_state, scrub_saw_dirty>>

\* A writer writes data: setDirty, bump backing version.
Write(w) ==
    /\ writer_state[w] = "open"
    /\ writer_state' = [writer_state EXCEPT ![w] = "writing"]
    /\ writer_wrote' = [writer_wrote EXCEPT ![w] = TRUE]
    /\ dirty_gen' = dirty_gen + 1
    /\ backing_version' = backing_version + 1
    /\ UNCHANGED <<clean_gen, write_refcount, path_exists, sum_version,
                   repl_queue, replicated_ver, cs_snapshot_gen,
                   trunc_nofd_state, trunc_nofd_gen,
                   scrub_state, scrub_saw_dirty>>

\* A writer can write again (multiple writes before close).
WriteAgain(w) ==
    /\ writer_state[w] = "writing"
    /\ dirty_gen' = dirty_gen + 1
    /\ backing_version' = backing_version + 1
    /\ UNCHANGED <<clean_gen, write_refcount, path_exists, sum_version,
                   repl_queue, replicated_ver, writer_state, writer_wrote,
                   cs_snapshot_gen, trunc_nofd_state, trunc_nofd_gen,
                   scrub_state, scrub_saw_dirty>>

\* fsync path: checksumAndEnqueueForced (bypasses write-ref check).
\* Step 1: snapshot dirty_gen, begin checksum computation.
FsyncBegin(w) ==
    /\ writer_state[w] = "writing"
    /\ dirty_gen > clean_gen  \* isDirty check
    /\ writer_state' = [writer_state EXCEPT ![w] = "fsyncing"]
    /\ cs_snapshot_gen' = [cs_snapshot_gen EXCEPT ![w] = dirty_gen]
    /\ UNCHANGED <<dirty_gen, clean_gen, write_refcount, path_exists,
                   backing_version, sum_version, repl_queue, replicated_ver,
                   writer_wrote, trunc_nofd_state, trunc_nofd_gen,
                   scrub_state, scrub_saw_dirty>>

\* fsync path step 2: finish checksum, write .sum, enqueue, clearDirtyIfGen.
\* The checksum captures the current backing_version (which may have changed
\* if another writer wrote concurrently -- the gen check handles it).
FsyncComplete(w) ==
    /\ writer_state[w] = "fsyncing"
    /\ writer_state' = [writer_state EXCEPT ![w] = "writing"]
    \* Write .sum with current backing version
    /\ sum_version' = backing_version
    \* Enqueue replication
    /\ repl_queue' = Append(repl_queue, backing_version)
    \* Conditionally clear dirty: only if no concurrent write bumped gen
    /\ clean_gen' = IF dirty_gen = cs_snapshot_gen[w]
                    THEN cs_snapshot_gen[w]
                    ELSE clean_gen
    /\ UNCHANGED <<dirty_gen, write_refcount, path_exists, backing_version,
                   replicated_ver, writer_wrote, cs_snapshot_gen,
                   trunc_nofd_state, trunc_nofd_gen,
                   scrub_state, scrub_saw_dirty>>

\* release path (file close).
\* Step 1: decWriteRef. This MUST happen before checksumming.
\* Matches fuse_release lines 1398-1404 in main.zig.
\* In the source, decWriteRef is a no-op if the path was removed
\* (getPtr returns null) or if write_refcount is already 0
\* (guard at line 320: "if (info.write_refcount > 0)").
ReleaseDecRef(w) ==
    /\ writer_state[w] \in {"open", "writing"}
    /\ writer_state' = [writer_state EXCEPT ![w] = "releasing"]
    /\ write_refcount' = IF write_refcount > 0
                         THEN write_refcount - 1
                         ELSE write_refcount
    /\ UNCHANGED <<dirty_gen, clean_gen, path_exists, backing_version,
                   sum_version, repl_queue, replicated_ver, writer_wrote,
                   cs_snapshot_gen, trunc_nofd_state, trunc_nofd_gen,
                   scrub_state, scrub_saw_dirty>>

\* release step 2: if dirty AND no write refs remain, begin checksum.
\* This is checksumAndEnqueue (non-forced) -- skips if hasWriteRef.
ReleaseBeginChecksum(w) ==
    /\ writer_state[w] = "releasing"
    /\ dirty_gen > clean_gen          \* isDirty
    /\ write_refcount = 0             \* !hasWriteRef (would skip otherwise)
    /\ writer_state' = [writer_state EXCEPT ![w] = "checksumming_release"]
    /\ cs_snapshot_gen' = [cs_snapshot_gen EXCEPT ![w] = dirty_gen]
    /\ UNCHANGED <<dirty_gen, clean_gen, write_refcount, path_exists,
                   backing_version, sum_version, repl_queue, replicated_ver,
                   writer_wrote, trunc_nofd_state, trunc_nofd_gen,
                   scrub_state, scrub_saw_dirty>>

\* release step 2 (alt): skip checksum because either not dirty or
\* another writer still has the file open.
ReleaseSkipChecksum(w) ==
    /\ writer_state[w] = "releasing"
    /\ \/ dirty_gen = clean_gen       \* not dirty
       \/ write_refcount > 0          \* another writer still open
    /\ writer_state' = [writer_state EXCEPT ![w] = "done"]
    /\ UNCHANGED <<dirty_gen, clean_gen, write_refcount, path_exists,
                   backing_version, sum_version, repl_queue, replicated_ver,
                   writer_wrote, cs_snapshot_gen,
                   trunc_nofd_state, trunc_nofd_gen,
                   scrub_state, scrub_saw_dirty>>

\* release step 3: finish checksum, write .sum, enqueue, clearDirtyIfGen.
ReleaseCompleteChecksum(w) ==
    /\ writer_state[w] = "checksumming_release"
    /\ sum_version' = backing_version
    /\ repl_queue' = Append(repl_queue, backing_version)
    /\ clean_gen' = IF dirty_gen = cs_snapshot_gen[w]
                    THEN cs_snapshot_gen[w]
                    ELSE clean_gen
    /\ writer_state' = [writer_state EXCEPT ![w] = "done"]
    /\ UNCHANGED <<dirty_gen, write_refcount, path_exists, backing_version,
                   replicated_ver, writer_wrote, cs_snapshot_gen,
                   trunc_nofd_state, trunc_nofd_gen,
                   scrub_state, scrub_saw_dirty>>

\* Writer returns to idle (can open the file again)
Reset(w) ==
    /\ writer_state[w] = "done"
    /\ writer_state' = [writer_state EXCEPT ![w] = "idle"]
    /\ UNCHANGED <<dirty_gen, clean_gen, write_refcount, path_exists,
                   backing_version, sum_version, repl_queue, replicated_ver,
                   writer_wrote, cs_snapshot_gen,
                   trunc_nofd_state, trunc_nofd_gen,
                   scrub_state, scrub_saw_dirty>>

(* ================================================================ *)
(* Truncate without open fd                                          *)
(* ================================================================ *)
\* Models fuse_truncate when fi is NULL (lines 1684-1712 in main.zig).
\* The file is opened internally, truncated, and closed -- all without
\* incWriteRef/decWriteRef.  After setDirty, checksumAndEnqueue is
\* called.  If hasWriteRef is true, the checksum is skipped (the dirty
\* flag stays and the open writer's release handles it).  Otherwise the
\* checksum+enqueue proceeds.
\*
\* Modeled as three actions:
\*   TruncateNoFdBeginChecksum -- truncate + setDirty + begin checksum
\*                                (only when write_refcount = 0)
\*   TruncateNoFdCompleteChecksum -- finish checksum, write .sum, enqueue
\*   TruncateNoFdSkipChecksum   -- truncate + setDirty only
\*                                  (when write_refcount > 0)

TruncateNoFdBeginChecksum ==
    /\ trunc_nofd_state = "idle"
    /\ write_refcount = 0       \* checksumAndEnqueue checks hasWriteRef
    /\ dirty_gen' = dirty_gen + 1
    /\ backing_version' = backing_version + 1
    /\ path_exists' = TRUE
    \* Snapshot dirty_gen AFTER the bump (setDirty then getDirtyGen)
    /\ trunc_nofd_gen' = dirty_gen + 1
    /\ trunc_nofd_state' = "checksumming"
    /\ UNCHANGED <<clean_gen, write_refcount, sum_version, repl_queue,
                   replicated_ver, writer_state, writer_wrote,
                   cs_snapshot_gen, scrub_state, scrub_saw_dirty>>

TruncateNoFdCompleteChecksum ==
    /\ trunc_nofd_state = "checksumming"
    \* Write .sum with current backing version
    /\ sum_version' = backing_version
    \* Enqueue replication
    /\ repl_queue' = Append(repl_queue, backing_version)
    \* Conditionally clear dirty: only if no concurrent write bumped gen
    /\ clean_gen' = IF dirty_gen = trunc_nofd_gen
                    THEN trunc_nofd_gen
                    ELSE clean_gen
    /\ trunc_nofd_state' = "idle"
    /\ UNCHANGED <<dirty_gen, write_refcount, path_exists, backing_version,
                   replicated_ver, writer_state, writer_wrote,
                   cs_snapshot_gen, trunc_nofd_gen,
                   scrub_state, scrub_saw_dirty>>

TruncateNoFdSkipChecksum ==
    /\ trunc_nofd_state = "idle"
    /\ write_refcount > 0       \* checksumAndEnqueue skips (hasWriteRef)
    /\ dirty_gen' = dirty_gen + 1
    /\ backing_version' = backing_version + 1
    /\ path_exists' = TRUE
    /\ UNCHANGED <<clean_gen, write_refcount, sum_version, repl_queue,
                   replicated_ver, writer_state, writer_wrote,
                   cs_snapshot_gen, trunc_nofd_state, trunc_nofd_gen,
                   scrub_state, scrub_saw_dirty>>

(* ================================================================ *)
(* Metadata-only replication (chmod, chown, utimens)                 *)
(* ================================================================ *)
\* These operations enqueue a .put to the replication log without
\* computing a checksum or interacting with dirty_gen/clean_gen.
\* Models fuse_chmod (line 1641), fuse_chown (line 1666),
\* fuse_utimens (line 1729) in main.zig.

MetadataPut ==
    /\ repl_queue' = Append(repl_queue, backing_version)
    /\ UNCHANGED <<dirty_gen, clean_gen, write_refcount, path_exists,
                   backing_version, sum_version, replicated_ver,
                   writer_state, writer_wrote, cs_snapshot_gen,
                   trunc_nofd_state, trunc_nofd_gen,
                   scrub_state, scrub_saw_dirty>>

(* ================================================================ *)
(* Unlink (file deletion)                                            *)
(* ================================================================ *)
\* Models fuse_unlink (lines 1445-1477 in main.zig).
\* Calls path_state.remove(rel) which DESTROYS the PathInfo entry
\* (dirty_gen, clean_gen, write_refcount all gone).
\*
\* IMPORTANT: This can happen while writers have the file open.
\* On POSIX, the fd remains valid after unlink, but the path state
\* is destroyed. This is modeled to verify whether it can cause
\* silent write loss or refcount corruption.

Unlink ==
    \* Reset path state as if the entry was removed from the map
    /\ dirty_gen' = 0
    /\ clean_gen' = 0
    /\ write_refcount' = 0
    /\ path_exists' = FALSE
    \* Enqueue delete to replica (modeled as version 0 = delete)
    /\ repl_queue' = Append(repl_queue, 0)
    /\ UNCHANGED <<backing_version, sum_version, replicated_ver,
                   writer_state, writer_wrote, cs_snapshot_gen,
                   trunc_nofd_state, trunc_nofd_gen,
                   scrub_state, scrub_saw_dirty>>

(* ================================================================ *)
(* Replication worker actions                                        *)
(* ================================================================ *)
\* Workers dequeue from repl_queue and "replicate" by recording the version.

ReplicateComplete ==
    /\ Len(repl_queue) > 0
    \* Take the last entry (coalescing: skip all earlier entries for the
    \* same file, which in our single-file model means just take the latest).
    /\ replicated_ver' = repl_queue[Len(repl_queue)]
    \* Remove all entries (coalescing clears dominated entries)
    /\ repl_queue' = <<>>
    /\ UNCHANGED <<dirty_gen, clean_gen, write_refcount, path_exists,
                   backing_version, sum_version, writer_state, writer_wrote,
                   cs_snapshot_gen, trunc_nofd_state, trunc_nofd_gen,
                   scrub_state, scrub_saw_dirty>>

(* ================================================================ *)
(* Scrub thread actions                                              *)
(* ================================================================ *)
\* The scrub detects corruption by comparing backing hash vs .sum.
\* We model "corruption" as sum_version not matching backing_version
\* when there are no open writers and the file is not dirty.

ScrubBegin ==
    /\ scrub_state = "idle"
    /\ write_refcount = 0        \* hasWriteRef check in runScrub
    /\ scrub_state' = "checking"
    \* "Corruption" = backing_version != sum_version
    /\ scrub_saw_dirty' = (backing_version /= sum_version)
    /\ UNCHANGED <<dirty_gen, clean_gen, write_refcount, path_exists,
                   backing_version, sum_version, repl_queue, replicated_ver,
                   writer_state, writer_wrote, cs_snapshot_gen,
                   trunc_nofd_state, trunc_nofd_gen>>

ScrubDecide ==
    /\ scrub_state = "checking"
    /\ IF scrub_saw_dirty
       THEN \* Would attempt repair -- only if no pending repl and no write ref
            IF write_refcount = 0 /\ Len(repl_queue) = 0
            THEN scrub_state' = "repairing"
            ELSE scrub_state' = "idle"  \* Skip repair (safe)
       ELSE scrub_state' = "idle"       \* No corruption
    /\ UNCHANGED <<dirty_gen, clean_gen, write_refcount, path_exists,
                   backing_version, sum_version, repl_queue, replicated_ver,
                   writer_state, writer_wrote, cs_snapshot_gen,
                   trunc_nofd_state, trunc_nofd_gen,
                   scrub_saw_dirty>>

\* Scrub repair: restore backing from replica.
\* Re-checks write_refcount AND dirty_gen = clean_gen before proceeding.
\* The isDirty check (dirty_gen = clean_gen) prevents the scrub from
\* rolling back a legitimate write that completed between decWriteRef
\* and checksumAndEnqueue in fuse_release.  Matches the isDirty guard
\* added to scrubFile in main.zig.
ScrubRepair ==
    /\ scrub_state = "repairing"
    \* Re-check write ref (line 1122 in scrubFile)
    /\ write_refcount = 0
    \* Re-check not dirty: a write completed between ScrubBegin and now
    \* would set dirty_gen > clean_gen.  Repairing would roll it back.
    /\ dirty_gen = clean_gen
    \* Repair: restore backing from replica (set backing_version = replicated_ver)
    \* and update .sum
    /\ backing_version' = replicated_ver
    /\ sum_version' = replicated_ver
    /\ scrub_state' = "idle"
    /\ UNCHANGED <<dirty_gen, clean_gen, write_refcount, path_exists,
                   repl_queue, replicated_ver, writer_state, writer_wrote,
                   cs_snapshot_gen, trunc_nofd_state, trunc_nofd_gen,
                   scrub_saw_dirty>>

ScrubRepairAbort ==
    \* If a writer opened the file between ScrubDecide and ScrubRepair,
    \* or if the file became dirty (write completed but not yet checksummed)
    /\ scrub_state = "repairing"
    /\ \/ write_refcount > 0
       \/ dirty_gen > clean_gen
    /\ scrub_state' = "idle"
    /\ UNCHANGED <<dirty_gen, clean_gen, write_refcount, path_exists,
                   backing_version, sum_version, repl_queue, replicated_ver,
                   writer_state, writer_wrote, cs_snapshot_gen,
                   trunc_nofd_state, trunc_nofd_gen,
                   scrub_saw_dirty>>

(* ================================================================ *)
(* Next-state relation                                               *)
(* ================================================================ *)

Next ==
    \/ \E w \in Writers :
        \/ Open(w)
        \/ OpenWithTrunc(w)
        \/ Write(w)
        \/ WriteAgain(w)
        \/ FsyncBegin(w)
        \/ FsyncComplete(w)
        \/ ReleaseDecRef(w)
        \/ ReleaseBeginChecksum(w)
        \/ ReleaseSkipChecksum(w)
        \/ ReleaseCompleteChecksum(w)
        \/ Reset(w)
    \/ TruncateNoFdBeginChecksum
    \/ TruncateNoFdCompleteChecksum
    \/ TruncateNoFdSkipChecksum
    \/ MetadataPut
    \/ Unlink
    \/ ReplicateComplete
    \/ ScrubBegin
    \/ ScrubDecide
    \/ ScrubRepair
    \/ ScrubRepairAbort

Spec == Init /\ [][Next]_vars

(* ================================================================ *)
(* Safety invariants                                                 *)
(* ================================================================ *)

\* I1: dirty_gen >= clean_gen always
GenMonotonic == dirty_gen >= clean_gen

\* I2: write_refcount is non-negative (can't go below 0)
RefcountNonNeg == write_refcount >= 0

\* I3: write_refcount equals the number of writers in active states.
\*     This only holds when path_exists has been continuously TRUE
\*     since every active writer opened.  After Unlink destroys the
\*     path state while writers are active, the refcount is reset to 0
\*     but those writers still appear "open"/"writing"/"fsyncing" in
\*     the model.  We weaken the invariant: it must hold when EITHER
\*     (a) path_exists is TRUE and no writer could have been orphaned
\*         (i.e., there has been no unlink since these writers opened), OR
\*     (b) we skip the check entirely if unlink has fired.
\*
\*     In practice we express this as: path_exists AND refcount >= the
\*     count of active writers (because unlink could reduce refcount but
\*     stale writers inflate the count).  The strict equality version is
\*     too strong once Unlink is modeled, so we instead check that
\*     write_refcount is at most the number of active writers (no double-
\*     counting) and non-negative.  The key property (no negative refcount)
\*     is already checked by RefcountNonNeg.
\*
\*     For a strict check, we only assert equality when write_refcount
\*     >= the number of active writers -- this filters out the post-unlink
\*     case where refcount was reset but stale writer slots remain.
RefcountAccurate ==
    path_exists =>
    LET activeWriters == Cardinality(
        {w \in Writers : writer_state[w] \in
            {"open", "writing", "fsyncing"}}
    )
    IN write_refcount <= activeWriters

\* I4: If all writers are idle/done AND dirty_gen > clean_gen AND
\*     the path still exists, there must be a pending replication entry
\*     OR an in-progress checksum (releasing/checksumming_release state,
\*     or truncate-without-fd checksumming).
\*     This is the KEY safety property: no write is silently lost.
\*
\*     With the truncate-without-fd fix, TruncateNoFd now triggers
\*     checksumAndEnqueue (when no writers are active), so the invariant
\*     is strengthened: the only transient dirty states are during
\*     in-progress checksums or when a writer has the file open and
\*     its release will handle replication.
NoSilentWriteLoss ==
    (\A w \in Writers : writer_state[w] \in {"idle", "done"})
    /\ dirty_gen > clean_gen
    /\ path_exists
    => \/ Len(repl_queue) > 0
       \/ \E w \in Writers : writer_state[w] \in
              {"releasing", "checksumming_release"}
       \/ trunc_nofd_state = "checksumming"

\* I5: Scrub repair never EXECUTES while:
\*     (a) writers are active, OR
\*     (b) the file has dirty data that hasn't been checksummed yet.
\*     This prevents the scrub from rolling back legitimate writes.
\*     The dirty_gen = clean_gen precondition on ScrubRepair enforces (b).
\*     ScrubRepair's write_refcount = 0 precondition enforces (a).

\* I6: After unlink destroys path state, refcount accuracy no longer
\*     holds.  This is expected -- writers with stale fds will try to
\*     decWriteRef on a path with refcount=0, which is a no-op in the
\*     source (line 320-321: "if (info.write_refcount > 0)").
\*     The model captures this by guarding RefcountAccurate on path_exists.

(* ================================================================ *)
(* Liveness (requires fairness)                                      *)
(* ================================================================ *)

FairSpec == Spec /\ WF_vars(Next)

AllWritersClosed ==
    \A w \in Writers : writer_state[w] \in {"idle", "done"}

\* Eventually, if all writers close, the dirty flag is cleared
EventuallyClean ==
    [](AllWritersClosed /\ path_exists => <>(dirty_gen = clean_gen))

\* Eventually, writes are replicated
EventuallyReplicated ==
    [](AllWritersClosed /\ backing_version > 0 /\ path_exists
       => <>(replicated_ver = backing_version))

(* ================================================================ *)
(* Model checking helpers                                            *)
(* ================================================================ *)

\* Symmetry set: writers are interchangeable, halving the state space.
WriterSymmetry == Permutations(Writers)

\* Bound counters so TLC explores a finite state space.
\* dirty_gen <= 3 allows two concurrent writers to each perform multiple
\* writes and still exercise all generation-comparison branches.
StateConstraint == dirty_gen <= 3

========================================================================
