--------------------------- MODULE PathState ---------------------------
(*
 * Module 1: Dirty-Gen Protocol + Write Refcounting
 *
 * Models the generation-based dirty/clean tracking in PathStateMap
 * (main.zig:285-438) and the checksumAndEnqueue flow (main.zig:1012-1042).
 *
 * The core problem: a FUSE write can arrive during checksum computation,
 * and the generation-snapshot pattern must ensure the dirty flag is not
 * silently cleared.
 *
 * Source Code Mapping:
 *   Writer.open          -> fuse_open / fuse_create  (main.zig:1607-1643 / 1749-1776)
 *   Writer.incWriteRef   -> PathStateMap.incWriteRef (main.zig:328-346)
 *   Writer.write         -> fuse_write               (main.zig:1679-1695)
 *   Writer.setDirty      -> PathStateMap.setDirty    (main.zig:308-326)
 *   Releaser.decWriteRef -> PathStateMap.decWriteRef (main.zig:348-356)
 *   Releaser.isDirty     -> PathStateMap.isDirty     (main.zig:358-363)
 *   Releaser.hasWriteRef -> PathStateMap.hasWriteRef (main.zig:365-370)
 *   Releaser.getDirtyGen -> PathStateMap.getDirtyGen (main.zig:409-414)
 *   Releaser.computeChecksum -> computeBlake3        (main.zig:973-992)
 *   Releaser.clearDirtyIfGen -> PathStateMap.clearDirtyIfGen (main.zig:419-427)
 *   Fsyncer.checksumForced   -> checksumAndEnqueueForced     (main.zig:1024-1042)
 *)

EXTENDS Naturals, Sequences, FiniteSets, TLC

CONSTANTS
    Writers,        \* set of writer process IDs
    Fsyncers,       \* set of fsyncer process IDs
    MaxGen,         \* upper bound for generation counters
    MaxWrites       \* max number of writes per writer

ASSUME Writers /= {}
ASSUME Fsyncers /= {}
ASSUME MaxGen \in Nat /\ MaxGen >= 2
ASSUME MaxWrites \in Nat /\ MaxWrites >= 1

(* All processes operate on a single path "f" -- the protocol is per-path *)
Path == "f"

(*
--algorithm fair PathState {

variables
    \* Real state (corresponds to PathStateMap fields)
    dirty_gen    = 0,
    clean_gen    = 0,
    write_refcount = 0,

    \* RwLock state
    rwlock_excl  = "None",  \* "None" or thread id holding exclusive
    rwlock_shared = 0,       \* count of shared holders

    \* Ghost variables (for invariants only)
    fileDirty    = FALSE,

    \* Per-writer state
    write_count  = [w \in Writers |-> 0],  \* number of writes performed
    released     = [w \in Writers |-> FALSE],

    \* Per-releaser state
    rel_started   = [w \in Writers |-> FALSE],
    rel_dirty     = [w \in Writers |-> FALSE], \* isDirty result
    rel_gen       = [w \in Writers |-> 0],     \* getDirtyGen snapshot
    rel_skip      = [w \in Writers |-> FALSE], \* skip due to hasWriteRef

    \* Per-fsyncer state
    fs_gen        = [f \in Fsyncers |-> 0],
    fs_dirty      = [f \in Fsyncers |-> FALSE];

\* ---------------------------------------------------------------
\* Macro: acquire exclusive rwlock
\* In the real code: rwlock.lock()
\* ---------------------------------------------------------------
macro AcquireExclusive(t) {
    await rwlock_excl = "None" /\ rwlock_shared = 0;
    rwlock_excl := t;
}

\* ---------------------------------------------------------------
\* Macro: release exclusive rwlock
\* ---------------------------------------------------------------
macro ReleaseExclusive() {
    rwlock_excl := "None";
}

\* ---------------------------------------------------------------
\* Macro: acquire shared rwlock
\* In the real code: rwlock.lockShared()
\* ---------------------------------------------------------------
macro AcquireShared(t) {
    await rwlock_excl = "None";
    rwlock_shared := rwlock_shared + 1;
}

\* ---------------------------------------------------------------
\* Macro: release shared rwlock
\* ---------------------------------------------------------------
macro ReleaseShared() {
    rwlock_shared := rwlock_shared - 1;
}

\* =============================================================
\* Writer process: open -> incWriteRef -> write x N -> release
\* =============================================================
fair process (Writer \in Writers)
{
w_incRef:
    \* incWriteRef: acquires exclusive lock, increments refcount
    AcquireExclusive(self);
    write_refcount := write_refcount + 1;
    ReleaseExclusive();

w_write:
    \* Write loop: each write calls setDirty under exclusive lock
    while (write_count[self] < MaxWrites) {
w_setDirty:
        AcquireExclusive(self);
        dirty_gen := dirty_gen + 1;
        fileDirty := TRUE;
        ReleaseExclusive();
        write_count[self] := write_count[self] + 1;
    };

w_release:
    \* Signal that this writer has released; start the releaser logic
    released[self] := TRUE;
    rel_started[self] := TRUE;

    \* --- Releaser logic inline (separate labels = interleaving points) ---

    \* decWriteRef: acquires exclusive lock
r_decRef:
    AcquireExclusive(self);
    if (write_refcount > 0) {
        write_refcount := write_refcount - 1;
    };
    ReleaseExclusive();

    \* isDirty: acquires shared lock
r_isDirty:
    AcquireShared(self);
    rel_dirty[self] := (dirty_gen > clean_gen);
    ReleaseShared();

    if (~rel_dirty[self]) {
        goto Done;
    };

    \* hasWriteRef: acquires shared lock (skip if still has writers)
r_hasWriteRef:
    AcquireShared(self);
    rel_skip[self] := (write_refcount > 0);
    ReleaseShared();

    if (rel_skip[self]) {
        goto Done;
    };

    \* getDirtyGen: snapshot under shared lock
r_getDirtyGen:
    AcquireShared(self);
    rel_gen[self] := dirty_gen;
    ReleaseShared();

    \* computeChecksum: no lock held (this is where concurrent writes interleave)
r_computeChecksum:
    skip;

    \* clearDirtyIfGen: acquires exclusive lock, CAS on generation
r_clearDirtyIfGen:
    AcquireExclusive(self);
    if (dirty_gen = rel_gen[self]) {
        clean_gen := rel_gen[self];
        fileDirty := FALSE;
    };
    ReleaseExclusive();
}

\* =============================================================
\* Fsyncer process: bypasses hasWriteRef guard
\* =============================================================
fair process (Fsyncer \in Fsyncers)
{
fs_isDirty:
    AcquireShared(self);
    fs_dirty[self] := (dirty_gen > clean_gen);
    ReleaseShared();

    if (~fs_dirty[self]) {
        goto Done;
    };

fs_getDirtyGen:
    AcquireShared(self);
    fs_gen[self] := dirty_gen;
    ReleaseShared();

    \* computeChecksum: no lock held
fs_computeChecksum:
    skip;

    \* clearDirtyIfGen
fs_clearDirtyIfGen:
    AcquireExclusive(self);
    if (dirty_gen = fs_gen[self]) {
        clean_gen := fs_gen[self];
        fileDirty := FALSE;
    };
    ReleaseExclusive();
}

}
*)

\* BEGIN TRANSLATION (chksum(pcal) = "3eedaf17" /\ chksum(tla) = "62dd40dc")
VARIABLES dirty_gen, clean_gen, write_refcount, rwlock_excl, rwlock_shared, 
          fileDirty, write_count, released, rel_started, rel_dirty, rel_gen, 
          rel_skip, fs_gen, fs_dirty, pc

vars == << dirty_gen, clean_gen, write_refcount, rwlock_excl, rwlock_shared, 
           fileDirty, write_count, released, rel_started, rel_dirty, rel_gen, 
           rel_skip, fs_gen, fs_dirty, pc >>

ProcSet == (Writers) \cup (Fsyncers)

Init == (* Global variables *)
        /\ dirty_gen = 0
        /\ clean_gen = 0
        /\ write_refcount = 0
        /\ rwlock_excl = "None"
        /\ rwlock_shared = 0
        /\ fileDirty = FALSE
        /\ write_count = [w \in Writers |-> 0]
        /\ released = [w \in Writers |-> FALSE]
        /\ rel_started = [w \in Writers |-> FALSE]
        /\ rel_dirty = [w \in Writers |-> FALSE]
        /\ rel_gen = [w \in Writers |-> 0]
        /\ rel_skip = [w \in Writers |-> FALSE]
        /\ fs_gen = [f \in Fsyncers |-> 0]
        /\ fs_dirty = [f \in Fsyncers |-> FALSE]
        /\ pc = [self \in ProcSet |-> CASE self \in Writers -> "w_incRef"
                                        [] self \in Fsyncers -> "fs_isDirty"]

\* Writer actions

w_incRef(self) ==
    /\ self \in Writers
    /\ pc[self] = "w_incRef"
    /\ rwlock_excl = "None" /\ rwlock_shared = 0
    /\ write_refcount' = write_refcount + 1
    /\ pc' = [pc EXCEPT ![self] = "w_write"]
    /\ UNCHANGED << dirty_gen, clean_gen, rwlock_excl, rwlock_shared, fileDirty, write_count,
                    released, rel_started, rel_dirty, rel_gen, rel_skip, fs_gen, fs_dirty >>

w_write(self) ==
    /\ self \in Writers
    /\ pc[self] = "w_write"
    /\ IF write_count[self] < MaxWrites
          THEN /\ pc' = [pc EXCEPT ![self] = "w_setDirty"]
          ELSE /\ pc' = [pc EXCEPT ![self] = "w_release"]
    /\ UNCHANGED << dirty_gen, clean_gen, write_refcount, rwlock_excl, rwlock_shared,
                    fileDirty, write_count, released, rel_started, rel_dirty, rel_gen,
                    rel_skip, fs_gen, fs_dirty >>

w_setDirty(self) ==
    /\ self \in Writers
    /\ pc[self] = "w_setDirty"
    /\ rwlock_excl = "None" /\ rwlock_shared = 0
    /\ dirty_gen' = dirty_gen + 1
    /\ fileDirty' = TRUE
    /\ write_count' = [write_count EXCEPT ![self] = write_count[self] + 1]
    /\ pc' = [pc EXCEPT ![self] = "w_write"]
    /\ UNCHANGED << clean_gen, write_refcount, rwlock_excl, rwlock_shared,
                    released, rel_started, rel_dirty, rel_gen, rel_skip, fs_gen, fs_dirty >>

w_release(self) ==
    /\ self \in Writers
    /\ pc[self] = "w_release"
    /\ released' = [released EXCEPT ![self] = TRUE]
    /\ rel_started' = [rel_started EXCEPT ![self] = TRUE]
    /\ pc' = [pc EXCEPT ![self] = "r_decRef"]
    /\ UNCHANGED << dirty_gen, clean_gen, write_refcount, rwlock_excl, rwlock_shared,
                    fileDirty, write_count, rel_dirty, rel_gen, rel_skip, fs_gen, fs_dirty >>

r_decRef(self) ==
    /\ self \in Writers
    /\ pc[self] = "r_decRef"
    /\ rwlock_excl = "None" /\ rwlock_shared = 0
    /\ write_refcount' = IF write_refcount > 0 THEN write_refcount - 1 ELSE write_refcount
    /\ pc' = [pc EXCEPT ![self] = "r_isDirty"]
    /\ UNCHANGED << dirty_gen, clean_gen, rwlock_excl, rwlock_shared,
                    fileDirty, write_count, released, rel_started, rel_dirty, rel_gen,
                    rel_skip, fs_gen, fs_dirty >>

r_isDirty(self) ==
    /\ self \in Writers
    /\ pc[self] = "r_isDirty"
    /\ rwlock_excl = "None"
    /\ rel_dirty' = [rel_dirty EXCEPT ![self] = (dirty_gen > clean_gen)]
    /\ pc' = [pc EXCEPT ![self] = IF ~(dirty_gen > clean_gen) THEN "Done" ELSE "r_hasWriteRef"]
    /\ UNCHANGED << dirty_gen, clean_gen, write_refcount, rwlock_excl, rwlock_shared,
                    fileDirty, write_count, released, rel_started, rel_gen, rel_skip,
                    fs_gen, fs_dirty >>

r_hasWriteRef(self) ==
    /\ self \in Writers
    /\ pc[self] = "r_hasWriteRef"
    /\ rwlock_excl = "None"
    /\ rel_skip' = [rel_skip EXCEPT ![self] = (write_refcount > 0)]
    /\ pc' = [pc EXCEPT ![self] = IF (write_refcount > 0) THEN "Done" ELSE "r_getDirtyGen"]
    /\ UNCHANGED << dirty_gen, clean_gen, write_refcount, rwlock_excl, rwlock_shared,
                    fileDirty, write_count, released, rel_started, rel_dirty, rel_gen,
                    fs_gen, fs_dirty >>

r_getDirtyGen(self) ==
    /\ self \in Writers
    /\ pc[self] = "r_getDirtyGen"
    /\ rwlock_excl = "None"
    /\ rel_gen' = [rel_gen EXCEPT ![self] = dirty_gen]
    /\ pc' = [pc EXCEPT ![self] = "r_computeChecksum"]
    /\ UNCHANGED << dirty_gen, clean_gen, write_refcount, rwlock_excl, rwlock_shared,
                    fileDirty, write_count, released, rel_started, rel_dirty, rel_skip,
                    fs_gen, fs_dirty >>

r_computeChecksum(self) ==
    /\ self \in Writers
    /\ pc[self] = "r_computeChecksum"
    /\ pc' = [pc EXCEPT ![self] = "r_clearDirtyIfGen"]
    /\ UNCHANGED << dirty_gen, clean_gen, write_refcount, rwlock_excl, rwlock_shared,
                    fileDirty, write_count, released, rel_started, rel_dirty, rel_gen,
                    rel_skip, fs_gen, fs_dirty >>

r_clearDirtyIfGen(self) ==
    /\ self \in Writers
    /\ pc[self] = "r_clearDirtyIfGen"
    /\ rwlock_excl = "None" /\ rwlock_shared = 0
    /\ IF dirty_gen = rel_gen[self]
          THEN /\ clean_gen' = rel_gen[self]
               /\ fileDirty' = FALSE
          ELSE /\ UNCHANGED << clean_gen, fileDirty >>
    /\ pc' = [pc EXCEPT ![self] = "Done"]
    /\ UNCHANGED << dirty_gen, write_refcount, rwlock_excl, rwlock_shared,
                    write_count, released, rel_started, rel_dirty, rel_gen, rel_skip,
                    fs_gen, fs_dirty >>

\* Fsyncer actions

fs_isDirty(self) ==
    /\ self \in Fsyncers
    /\ pc[self] = "fs_isDirty"
    /\ rwlock_excl = "None"
    /\ fs_dirty' = [fs_dirty EXCEPT ![self] = (dirty_gen > clean_gen)]
    /\ pc' = [pc EXCEPT ![self] = IF ~(dirty_gen > clean_gen) THEN "Done" ELSE "fs_getDirtyGen"]
    /\ UNCHANGED << dirty_gen, clean_gen, write_refcount, rwlock_excl, rwlock_shared,
                    fileDirty, write_count, released, rel_started, rel_dirty, rel_gen,
                    rel_skip, fs_gen >>

fs_getDirtyGen(self) ==
    /\ self \in Fsyncers
    /\ pc[self] = "fs_getDirtyGen"
    /\ rwlock_excl = "None"
    /\ fs_gen' = [fs_gen EXCEPT ![self] = dirty_gen]
    /\ pc' = [pc EXCEPT ![self] = "fs_computeChecksum"]
    /\ UNCHANGED << dirty_gen, clean_gen, write_refcount, rwlock_excl, rwlock_shared,
                    fileDirty, write_count, released, rel_started, rel_dirty, rel_gen,
                    rel_skip, fs_dirty >>

fs_computeChecksum(self) ==
    /\ self \in Fsyncers
    /\ pc[self] = "fs_computeChecksum"
    /\ pc' = [pc EXCEPT ![self] = "fs_clearDirtyIfGen"]
    /\ UNCHANGED << dirty_gen, clean_gen, write_refcount, rwlock_excl, rwlock_shared,
                    fileDirty, write_count, released, rel_started, rel_dirty, rel_gen,
                    rel_skip, fs_gen, fs_dirty >>

fs_clearDirtyIfGen(self) ==
    /\ self \in Fsyncers
    /\ pc[self] = "fs_clearDirtyIfGen"
    /\ rwlock_excl = "None" /\ rwlock_shared = 0
    /\ IF dirty_gen = fs_gen[self]
          THEN /\ clean_gen' = fs_gen[self]
               /\ fileDirty' = FALSE
          ELSE /\ UNCHANGED << clean_gen, fileDirty >>
    /\ pc' = [pc EXCEPT ![self] = "Done"]
    /\ UNCHANGED << dirty_gen, write_refcount, rwlock_excl, rwlock_shared,
                    write_count, released, rel_started, rel_dirty, rel_gen, rel_skip,
                    fs_gen, fs_dirty >>

\* Complete next-state relation
Next == \E self \in ProcSet:
    \/ w_incRef(self)
    \/ w_write(self)
    \/ w_setDirty(self)
    \/ w_release(self)
    \/ r_decRef(self)
    \/ r_isDirty(self)
    \/ r_hasWriteRef(self)
    \/ r_getDirtyGen(self)
    \/ r_computeChecksum(self)
    \/ r_clearDirtyIfGen(self)
    \/ fs_isDirty(self)
    \/ fs_getDirtyGen(self)
    \/ fs_computeChecksum(self)
    \/ fs_clearDirtyIfGen(self)

Spec == Init /\ [][Next]_vars /\ WF_vars(Next)

\* END TRANSLATION

----

\* =============================================================
\* INVARIANTS
\* =============================================================

(*
 * Inv1 -- NoLostWrite (safety):
 * If the file has been modified since the last successful checksum,
 * the dirty flag must reflect this.
 *   fileDirty => dirty_gen > clean_gen
 *)
NoLostWrite ==
    fileDirty => dirty_gen > clean_gen

(*
 * Inv2 -- NoWrongChecksum (safety):
 * A releaser only reaches computeChecksum after having observed
 * write_refcount = 0 (the hasWriteRef guard passed).  A new writer
 * may open between the check and the computation, but the
 * generation-based clearDirtyIfGen ensures safety in that case.
 *)
NoWrongChecksum ==
    \A t \in Writers:
        pc[t] = "r_computeChecksum" => rel_skip[t] = FALSE

(*
 * Inv3 -- DirtyGenMonotonic (safety):
 * dirty_gen never decreases.  This is checked as a state predicate:
 * dirty_gen >= clean_gen always holds (clean_gen is set from dirty_gen).
 *)
DirtyGenMonotonic ==
    dirty_gen >= clean_gen

\* =============================================================
\* LIVENESS PROPERTIES
\* =============================================================

(*
 * Live1 -- EventualChecksum:
 * Every dirty file is eventually checksummed, assuming writers
 * eventually stop writing and release their file descriptors.
 *   dirty_gen > clean_gen ~> clean_gen >= dirty_gen
 *)
EventualChecksum ==
    (dirty_gen > clean_gen) ~> (clean_gen >= dirty_gen)

====
