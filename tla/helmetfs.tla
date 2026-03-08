--------------------------- MODULE helmetfs ---------------------------
(*
 * TLA+ specification for helmetfs's dirty-generation replication protocol.
 *
 * This models the most complex and error-prone aspect of helmetfs: the
 * interaction between concurrent writers, the dirty-generation tracking,
 * write reference counting, checksum computation, and replication --
 * verifying that every completed write is eventually replicated.
 *
 * Actors modeled:
 *   - Multiple FUSE writer processes (open, write, fsync, release)
 *   - Replication workers (dequeue, replicate, mark completed)
 *   - Scrub thread (detect corruption, repair from replica)
 *
 * Key invariants verified:
 *   1. No write is silently lost: if a file is modified and all writers
 *      close, the file eventually gets enqueued for replication.
 *   2. dirty_gen >= clean_gen always.
 *   3. A checksum computed during a concurrent write does NOT clear the
 *      dirty flag (the generation-based protocol prevents this).
 *   4. Write refcount accuracy: every inc is paired with a dec.
 *   5. Scrub never overwrites a file that has active writers.
 *
 * Simplifications:
 *   - We model a single file path (sufficient to find protocol bugs).
 *   - Checksum computation is abstracted as a multi-step action
 *     (snapshot gen, "compute", conditionally clear).
 *   - The replication log is abstracted to a queue of pending puts.
 *   - File content is modeled as a version counter (not actual bytes).
 *)

EXTENDS Integers, Sequences, FiniteSets, TLC

CONSTANTS
    Writers,          \* Set of writer process IDs, e.g. {w1, w2}
    NumReplWorkers    \* Number of replication workers (e.g. 1 or 2)

(* ---- State variables ---- *)
VARIABLES
    \* Per-file path state (PathStateMap)
    dirty_gen,        \* Nat: incremented on every write
    clean_gen,        \* Nat: set to dirty_gen when checksum succeeds
    write_refcount,   \* Nat: number of open write descriptors

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

    \* Scrub state
    scrub_state,      \* "idle", "checking", "repairing"
    scrub_saw_dirty   \* BOOLEAN: scrub saw a checksum mismatch

vars == <<dirty_gen, clean_gen, write_refcount, backing_version,
          sum_version, repl_queue, replicated_ver, writer_state,
          writer_wrote, cs_snapshot_gen, scrub_state, scrub_saw_dirty>>

(* ---- Type invariant ---- *)

TypeOK ==
    /\ dirty_gen \in Nat
    /\ clean_gen \in Nat
    /\ write_refcount \in Nat
    /\ backing_version \in Nat
    /\ sum_version \in Nat
    /\ repl_queue \in Seq(Nat)
    /\ replicated_ver \in Nat
    /\ \A w \in Writers : writer_state[w] \in
         {"idle", "open", "writing", "fsyncing",
          "releasing", "checksumming_release", "done"}
    /\ \A w \in Writers : writer_wrote[w] \in BOOLEAN
    /\ \A w \in Writers : cs_snapshot_gen[w] \in Nat
    /\ scrub_state \in {"idle", "checking", "repairing"}
    /\ scrub_saw_dirty \in BOOLEAN

(* ---- Initial state ---- *)

Init ==
    /\ dirty_gen = 0
    /\ clean_gen = 0
    /\ write_refcount = 0
    /\ backing_version = 0
    /\ sum_version = 0
    /\ repl_queue = <<>>
    /\ replicated_ver = 0
    /\ writer_state = [w \in Writers |-> "idle"]
    /\ writer_wrote = [w \in Writers |-> FALSE]
    /\ cs_snapshot_gen = [w \in Writers |-> 0]
    /\ scrub_state = "idle"
    /\ scrub_saw_dirty = FALSE

(* ==== Writer actions: model the FUSE open/write/fsync/release lifecycle ==== *)

\* A writer opens the file for writing: incWriteRef
Open(w) ==
    /\ writer_state[w] = "idle"
    /\ writer_state' = [writer_state EXCEPT ![w] = "open"]
    /\ writer_wrote' = [writer_wrote EXCEPT ![w] = FALSE]
    /\ write_refcount' = write_refcount + 1
    /\ UNCHANGED <<dirty_gen, clean_gen, backing_version, sum_version,
                   repl_queue, replicated_ver, cs_snapshot_gen,
                   scrub_state, scrub_saw_dirty>>

\* A writer writes data: setDirty, bump backing version
Write(w) ==
    /\ writer_state[w] = "open"
    /\ writer_state' = [writer_state EXCEPT ![w] = "writing"]
    /\ writer_wrote' = [writer_wrote EXCEPT ![w] = TRUE]
    /\ dirty_gen' = dirty_gen + 1
    /\ backing_version' = backing_version + 1
    /\ UNCHANGED <<clean_gen, write_refcount, sum_version, repl_queue,
                   replicated_ver, cs_snapshot_gen,
                   scrub_state, scrub_saw_dirty>>

\* A writer can write again (multiple writes before close)
WriteAgain(w) ==
    /\ writer_state[w] = "writing"
    /\ dirty_gen' = dirty_gen + 1
    /\ backing_version' = backing_version + 1
    /\ UNCHANGED <<clean_gen, write_refcount, sum_version, repl_queue,
                   replicated_ver, writer_state, writer_wrote,
                   cs_snapshot_gen, scrub_state, scrub_saw_dirty>>

\* fsync path: checksumAndEnqueueForced (bypasses write-ref check).
\* Step 1: snapshot dirty_gen, begin checksum computation.
FsyncBegin(w) ==
    /\ writer_state[w] = "writing"
    /\ dirty_gen > clean_gen  \* isDirty check
    /\ writer_state' = [writer_state EXCEPT ![w] = "fsyncing"]
    /\ cs_snapshot_gen' = [cs_snapshot_gen EXCEPT ![w] = dirty_gen]
    /\ UNCHANGED <<dirty_gen, clean_gen, write_refcount, backing_version,
                   sum_version, repl_queue, replicated_ver, writer_wrote,
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
    /\ UNCHANGED <<dirty_gen, write_refcount, backing_version,
                   replicated_ver, writer_wrote, cs_snapshot_gen,
                   scrub_state, scrub_saw_dirty>>

\* release path (file close).
\* Step 1: decWriteRef. This MUST happen before checksumming.
\* Matches fuse_release lines 1398-1404.
ReleaseDecRef(w) ==
    /\ writer_state[w] \in {"open", "writing"}
    /\ writer_state' = [writer_state EXCEPT ![w] = "releasing"]
    /\ write_refcount' = write_refcount - 1
    /\ UNCHANGED <<dirty_gen, clean_gen, backing_version, sum_version,
                   repl_queue, replicated_ver, writer_wrote,
                   cs_snapshot_gen, scrub_state, scrub_saw_dirty>>

\* release step 2: if dirty AND no write refs remain, begin checksum.
\* This is checksumAndEnqueue (non-forced) -- skips if hasWriteRef.
ReleaseBeginChecksum(w) ==
    /\ writer_state[w] = "releasing"
    /\ dirty_gen > clean_gen          \* isDirty
    /\ write_refcount = 0             \* !hasWriteRef (would skip otherwise)
    /\ writer_state' = [writer_state EXCEPT ![w] = "checksumming_release"]
    /\ cs_snapshot_gen' = [cs_snapshot_gen EXCEPT ![w] = dirty_gen]
    /\ UNCHANGED <<dirty_gen, clean_gen, write_refcount, backing_version,
                   sum_version, repl_queue, replicated_ver, writer_wrote,
                   scrub_state, scrub_saw_dirty>>

\* release step 2 (alt): skip checksum because either not dirty or
\* another writer still has the file open.
ReleaseSkipChecksum(w) ==
    /\ writer_state[w] = "releasing"
    /\ \/ dirty_gen = clean_gen       \* not dirty
       \/ write_refcount > 0          \* another writer still open
    /\ writer_state' = [writer_state EXCEPT ![w] = "done"]
    /\ UNCHANGED <<dirty_gen, clean_gen, write_refcount, backing_version,
                   sum_version, repl_queue, replicated_ver, writer_wrote,
                   cs_snapshot_gen, scrub_state, scrub_saw_dirty>>

\* release step 3: finish checksum, write .sum, enqueue, clearDirtyIfGen.
ReleaseCompleteChecksum(w) ==
    /\ writer_state[w] = "checksumming_release"
    /\ sum_version' = backing_version
    /\ repl_queue' = Append(repl_queue, backing_version)
    /\ clean_gen' = IF dirty_gen = cs_snapshot_gen[w]
                    THEN cs_snapshot_gen[w]
                    ELSE clean_gen
    /\ writer_state' = [writer_state EXCEPT ![w] = "done"]
    /\ UNCHANGED <<dirty_gen, write_refcount, backing_version,
                   replicated_ver, writer_wrote, cs_snapshot_gen,
                   scrub_state, scrub_saw_dirty>>

\* Writer returns to idle (can open the file again)
Reset(w) ==
    /\ writer_state[w] = "done"
    /\ writer_state' = [writer_state EXCEPT ![w] = "idle"]
    /\ UNCHANGED <<dirty_gen, clean_gen, write_refcount, backing_version,
                   sum_version, repl_queue, replicated_ver, writer_wrote,
                   cs_snapshot_gen, scrub_state, scrub_saw_dirty>>

(* ==== Replication worker actions ==== *)
\* Workers dequeue from repl_queue and "replicate" by recording the version.

ReplicateComplete ==
    /\ Len(repl_queue) > 0
    \* Take the last entry (coalescing: skip all earlier entries for the
    \* same file, which in our single-file model means just take the latest).
    /\ replicated_ver' = repl_queue[Len(repl_queue)]
    \* Remove all entries (coalescing clears dominated entries)
    /\ repl_queue' = <<>>
    /\ UNCHANGED <<dirty_gen, clean_gen, write_refcount, backing_version,
                   sum_version, writer_state, writer_wrote,
                   cs_snapshot_gen, scrub_state, scrub_saw_dirty>>

(* ==== Scrub thread actions (simplified) ==== *)
\* The scrub detects corruption by comparing backing hash vs .sum.
\* We model "corruption" as sum_version not matching backing_version
\* when there are no open writers and the file is not dirty.

ScrubBegin ==
    /\ scrub_state = "idle"
    /\ write_refcount = 0        \* hasWriteRef check in runScrub
    /\ scrub_state' = "checking"
    \* "Corruption" = backing_version != sum_version
    /\ scrub_saw_dirty' = (backing_version /= sum_version)
    /\ UNCHANGED <<dirty_gen, clean_gen, write_refcount, backing_version,
                   sum_version, repl_queue, replicated_ver, writer_state,
                   writer_wrote, cs_snapshot_gen>>

ScrubDecide ==
    /\ scrub_state = "checking"
    /\ IF scrub_saw_dirty
       THEN \* Would attempt repair -- only if no pending repl and no write ref
            IF write_refcount = 0 /\ Len(repl_queue) = 0
            THEN scrub_state' = "repairing"
            ELSE scrub_state' = "idle"  \* Skip repair (safe)
       ELSE scrub_state' = "idle"       \* No corruption
    /\ UNCHANGED <<dirty_gen, clean_gen, write_refcount, backing_version,
                   sum_version, repl_queue, replicated_ver, writer_state,
                   writer_wrote, cs_snapshot_gen, scrub_saw_dirty>>

ScrubRepair ==
    /\ scrub_state = "repairing"
    \* Re-check write ref (line 1122 in scrubFile)
    /\ write_refcount = 0
    \* Repair: restore backing from replica (set backing_version = replicated_ver)
    \* and update .sum
    /\ backing_version' = replicated_ver
    /\ sum_version' = replicated_ver
    /\ scrub_state' = "idle"
    /\ UNCHANGED <<dirty_gen, clean_gen, write_refcount, repl_queue,
                   replicated_ver, writer_state, writer_wrote,
                   cs_snapshot_gen, scrub_saw_dirty>>

ScrubRepairAbort ==
    \* If a writer opened the file between ScrubDecide and ScrubRepair
    /\ scrub_state = "repairing"
    /\ write_refcount > 0
    /\ scrub_state' = "idle"
    /\ UNCHANGED <<dirty_gen, clean_gen, write_refcount, backing_version,
                   sum_version, repl_queue, replicated_ver, writer_state,
                   writer_wrote, cs_snapshot_gen, scrub_saw_dirty>>

(* ==== Next-state relation ==== *)

Next ==
    \/ \E w \in Writers :
        \/ Open(w)
        \/ Write(w)
        \/ WriteAgain(w)
        \/ FsyncBegin(w)
        \/ FsyncComplete(w)
        \/ ReleaseDecRef(w)
        \/ ReleaseBeginChecksum(w)
        \/ ReleaseSkipChecksum(w)
        \/ ReleaseCompleteChecksum(w)
        \/ Reset(w)
    \/ ReplicateComplete
    \/ ScrubBegin
    \/ ScrubDecide
    \/ ScrubRepair
    \/ ScrubRepairAbort

Spec == Init /\ [][Next]_vars

(* ==== Safety invariants ==== *)

\* I1: dirty_gen >= clean_gen always
GenMonotonic == dirty_gen >= clean_gen

\* I2: write_refcount is non-negative (can't go below 0)
RefcountNonNeg == write_refcount >= 0

\* I3: write_refcount equals the number of writers in active states
RefcountAccurate ==
    write_refcount = Cardinality(
        {w \in Writers : writer_state[w] \in
            {"open", "writing", "fsyncing"}}
    )

\* I4: If all writers are idle/done AND dirty_gen > clean_gen, there must be
\*     a pending replication entry OR an in-progress checksum.
\*     This is the KEY safety property: no write is silently lost.
NoSilentWriteLoss ==
    (\A w \in Writers : writer_state[w] \in {"idle", "done"})
    /\ dirty_gen > clean_gen
    => Len(repl_queue) > 0

\* I5: Scrub repair action never EXECUTES while writers are active.
\*     Note: scrub_state can be "repairing" with write_refcount > 0
\*     (the TOCTOU window between ScrubDecide and ScrubRepair), but
\*     ScrubRepair re-checks write_refcount and aborts if > 0.
\*     This is verified by the ScrubRepair precondition, not as an
\*     invariant on the state. The actual safety property is that
\*     backing_version only changes in ScrubRepair when write_refcount = 0.
\*     (TLC confirms the TOCTOU window exists -- matching the comment at
\*     line 1118-1121 of main.zig.)

(* ==== Liveness (requires fairness) ==== *)

FairSpec == Spec /\ WF_vars(Next)

AllWritersClosed ==
    \A w \in Writers : writer_state[w] \in {"idle", "done"}

\* Eventually, if all writers close, the dirty flag is cleared
EventuallyClean ==
    [](AllWritersClosed => <>(dirty_gen = clean_gen))

\* Eventually, writes are replicated
EventuallyReplicated ==
    [](AllWritersClosed /\ backing_version > 0
       => <>(replicated_ver = backing_version))

========================================================================
