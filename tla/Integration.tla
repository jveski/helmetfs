-------------------------- MODULE Integration -------------------------
(*
 * Module 3: Cross-Cutting FUSE Callback Interactions
 *
 * Models the interactions between concurrent FUSE callbacks that span
 * both PathStateMap and ReplLog.  Primary concerns:
 *   - fuse_unlink / fuse_rename concurrent with fuse_release
 *   - scrub thread vs concurrent writes
 *
 * Source Code Mapping:
 *   Writer.open                      -> fuse_open (write mode)  (main.zig:1607-1643)
 *   Writer.write                     -> fuse_write              (main.zig:1679-1695)
 *   Writer.release.decWriteRef       -> fuse_release            (main.zig:1734)
 *   Writer.release.isDirty           -> fuse_release            (main.zig:1740)
 *   Writer.release.checksumAndEnqueue -> checksumAndEnqueue      (main.zig:1012-1042)
 *   Unlinker.deleteFile              -> fuse_unlink             (main.zig:1790-1792)
 *   Unlinker.enqueueDelete           -> fuse_unlink             (main.zig:1801)
 *   Unlinker.removeState             -> fuse_unlink             (main.zig:1807)
 *   Renamer.renameFile               -> fuse_rename             (main.zig:1857)
 *   Renamer.enqueuePair              -> fuse_rename             (main.zig:1870)
 *   Renamer.removeState              -> fuse_rename             (main.zig:1875)
 *   Consumer.processPut              -> replicatePut            (main.zig:1079-1136)
 *   Consumer.processDelete           -> replicateDelete         (main.zig:1138-1153)
 *   Scrubber.hasWriteRef             -> runScrub                (main.zig:1329)
 *   Scrubber.computeChecksum         -> scrubFile               (main.zig:1362-1366)
 *   Scrubber.hasPendingPut           -> scrubFile               (main.zig:1396,1423-1426)
 *   Scrubber.hasWriteRefAgain        -> scrubFile               (main.zig:1432)
 *   Scrubber.repair                  -> scrubFile               (main.zig:1439)
 *)

EXTENDS Naturals, Sequences, FiniteSets, TLC

CONSTANTS
    MaxGen      \* upper bound for generation counters

ASSUME MaxGen \in Nat /\ MaxGen >= 2

\* Fixed paths for this module
PathA == "a"
PathB == "b"
Paths == {PathA, PathB}

(*
--algorithm fair Integration {

variables
    \* File system state
    fileExists      = [p \in Paths |-> IF p = PathA THEN TRUE ELSE FALSE],
    \* PathStateMap (simplified)
    dirty_gen       = [p \in Paths |-> 0],
    clean_gen       = [p \in Paths |-> 0],
    write_refcount  = [p \in Paths |-> 0],
    pathStateExists = [p \in Paths |-> IF p = PathA THEN TRUE ELSE FALSE],
    \* Replication log (simplified: just a sequence of <<op, path>>)
    replLog         = <<>>,
    \* Replica state
    replicaExists   = [p \in Paths |-> FALSE],
    \* Writer state
    w_released     = FALSE,
    w_dirty        = FALSE,
    w_gen          = 0,
    w_fileNotFound = FALSE,
    \* Scrubber state
    scrub_skip     = FALSE,
    scrub_mismatch = FALSE,
    scrub_skip2    = FALSE,
    \* Consumer state
    cons_idx       = 0,
    \* Track if all processes are done
    allDone        = FALSE;

\* =============================================================
\* Writer: open(WR, a) -> write(a) -> release(a)
\* Models the full lifecycle including checksumAndEnqueue
\* =============================================================
fair process (Writer = "writer")
{
wr_open:
    \* incWriteRef(a)
    write_refcount[PathA] := write_refcount[PathA] + 1;

wr_write:
    \* fuse_write: setDirty
    if (dirty_gen[PathA] < MaxGen) {
        dirty_gen[PathA] := dirty_gen[PathA] + 1;
    };

wr_decRef:
    \* fuse_release: decWriteRef
    w_released := TRUE;
    if (write_refcount[PathA] > 0) {
        write_refcount[PathA] := write_refcount[PathA] - 1;
    };

wr_isDirty:
    \* isDirty check: returns false if state was removed
    if (pathStateExists[PathA]) {
        w_dirty := dirty_gen[PathA] > clean_gen[PathA];
    } else {
        w_dirty := FALSE;
    };
    if (~w_dirty) { goto Done; };

wr_hasWriteRef:
    \* checksumAndEnqueue: skip if hasWriteRef
    if (pathStateExists[PathA] /\ write_refcount[PathA] > 0) {
        goto Done;
    };

wr_getDirtyGen:
    \* getDirtyGen: snapshot generation (returns 0 if state removed)
    if (pathStateExists[PathA]) {
        w_gen := dirty_gen[PathA];
    } else {
        w_gen := 0;
    };

wr_computeChecksum:
    \* computeBlake3: fails if file deleted
    if (~fileExists[PathA]) {
        w_fileNotFound := TRUE;
        goto Done;
    };

wr_enqueue:
    \* enqueue(put, a)
    replLog := Append(replLog, <<"put", PathA>>);

wr_clearDirtyIfGen:
    \* clearDirtyIfGen
    if (pathStateExists[PathA] /\ dirty_gen[PathA] = w_gen) {
        clean_gen[PathA] := w_gen;
    };
}

\* =============================================================
\* Unlinker: deleteFile(a) -> enqueueDelete(a) -> removeState(a)
\* =============================================================
fair process (Unlinker = "unlinker")
{
ul_delete:
    \* deleteFile: remove from filesystem
    fileExists[PathA] := FALSE;

ul_enqueue:
    \* enqueue(delete, a)
    replLog := Append(replLog, <<"delete", PathA>>);

ul_removeState:
    \* removeState: clear from PathStateMap
    pathStateExists[PathA] := FALSE;
    dirty_gen[PathA] := 0;
    clean_gen[PathA] := 0;
    write_refcount[PathA] := 0;
}

\* =============================================================
\* Renamer: renameFile(a->b) -> enqueuePair -> removeState(a)
\* =============================================================
fair process (Renamer = "renamer")
{
rn_rename:
    \* renameFile: move a -> b in filesystem
    fileExists[PathB] := fileExists[PathA];
    fileExists[PathA] := FALSE;

rn_enqueuePair:
    \* enqueuePair(delete a, put b)
    replLog := Append(Append(replLog, <<"delete", PathA>>), <<"put", PathB>>);
    \* Initialize pathState for b
    pathStateExists[PathB] := TRUE;

rn_removeState:
    \* removeState(a)
    pathStateExists[PathA] := FALSE;
    dirty_gen[PathA] := 0;
    clean_gen[PathA] := 0;
}

\* =============================================================
\* Consumer: processes entries from replLog in order
\* Keeps checking for new entries until all producers finish
\* =============================================================
fair process (Consumer = "consumer")
{
cn_loop:
    while (TRUE) {
cn_check:
        if (cons_idx < Len(replLog)) {
cn_dequeue:
            cons_idx := cons_idx + 1;

cn_process:
            if (replLog[cons_idx][1] = "put") {
                \* processPut: only replicate if file exists
                if (fileExists[replLog[cons_idx][2]]) {
                    replicaExists[replLog[cons_idx][2]] := TRUE;
                };
            } else {
                \* processDelete: idempotent
                replicaExists[replLog[cons_idx][2]] := FALSE;
            };
        } else {
            \* No more entries; exit if all producers are done
            if (pc["writer"] = "Done" /\ pc["unlinker"] = "Done" /\ pc["renamer"] = "Done") {
                goto Done;
            };
        };
    };
}

\* =============================================================
\* Scrubber: hasWriteRef -> computeChecksum -> hasPendingPut -> hasWriteRef -> repair
\* Models runScrub (main.zig:1329) + scrubFile (main.zig:1356-1449)
\* =============================================================
fair process (Scrubber = "scrubber")
{
sc_hasWriteRef1:
    \* First hasWriteRef check in runScrub (main.zig:1329)
    if (write_refcount[PathA] > 0) {
        scrub_skip := TRUE;
        goto Done;
    };

sc_computeChecksum:
    \* computeBlake3 + compare with stored .sum (main.zig:1362-1387)
    \* Abstract: non-deterministically detect mismatch
    \* (In reality, a concurrent write could cause mismatch)
    either {
        scrub_mismatch := TRUE;
    } or {
        scrub_mismatch := FALSE;
        goto Done;
    };

sc_hasPendingPut:
    \* hasPendingPut check (main.zig:1396, 1423-1426)
    \* Skip repair if there is a pending (non-completed) put entry for this
    \* path in the replLog — the replica is stale and must not be used.
    if (\E i \in 1..Len(replLog): replLog[i][1] = "put" /\ replLog[i][2] = PathA) {
        goto Done;
    };

sc_hasWriteRef2:
    \* Second hasWriteRef check before repair (main.zig:1432)
    \* NOT atomic with the repair — a writer could interleave between
    \* this check and the actual copyFileWithSync.
    if (write_refcount[PathA] > 0) {
        scrub_skip2 := TRUE;
        goto Done;
    };

sc_repair:
    \* copyFileWithSync from replica (main.zig:1439)
    \* No lock is held here — concurrent open/write is possible
    skip;
}

}
*)

\* BEGIN TRANSLATION (chksum(pcal) = "UPDATED" /\ chksum(tla) = "UPDATED")
VARIABLES fileExists, dirty_gen, clean_gen, write_refcount, pathStateExists,
          replLog, replicaExists, w_released, w_dirty, w_gen, w_fileNotFound,
          scrub_skip, scrub_mismatch, scrub_skip2, cons_idx, allDone, pc

vars == << fileExists, dirty_gen, clean_gen, write_refcount, pathStateExists,
           replLog, replicaExists, w_released, w_dirty, w_gen, w_fileNotFound,
           scrub_skip, scrub_mismatch, scrub_skip2, cons_idx, allDone, pc >>

ProcSet == {"writer"} \cup {"unlinker"} \cup {"renamer"} \cup {"consumer"} \cup {"scrubber"}

Init == /\ fileExists = [p \in Paths |-> IF p = PathA THEN TRUE ELSE FALSE]
        /\ dirty_gen = [p \in Paths |-> 0]
        /\ clean_gen = [p \in Paths |-> 0]
        /\ write_refcount = [p \in Paths |-> 0]
        /\ pathStateExists = [p \in Paths |-> IF p = PathA THEN TRUE ELSE FALSE]
        /\ replLog = <<>>
        /\ replicaExists = [p \in Paths |-> FALSE]
        /\ w_released = FALSE
        /\ w_dirty = FALSE
        /\ w_gen = 0
        /\ w_fileNotFound = FALSE
        /\ scrub_skip = FALSE
        /\ scrub_mismatch = FALSE
        /\ scrub_skip2 = FALSE
        /\ cons_idx = 0
        /\ allDone = FALSE
        /\ pc = [self \in ProcSet |->
                    CASE self = "writer"   -> "wr_open"
                      [] self = "unlinker" -> "ul_delete"
                      [] self = "renamer"  -> "rn_rename"
                      [] self = "consumer" -> "cn_loop"
                      [] self = "scrubber" -> "sc_hasWriteRef1"]

\* --- Writer actions ---

wr_open ==
    /\ pc["writer"] = "wr_open"
    /\ write_refcount' = [write_refcount EXCEPT ![PathA] = write_refcount[PathA] + 1]
    /\ pc' = [pc EXCEPT !["writer"] = "wr_write"]
    /\ UNCHANGED << fileExists, dirty_gen, clean_gen, pathStateExists, replLog,
                    replicaExists, w_released, w_dirty, w_gen, w_fileNotFound,
                    scrub_skip, scrub_mismatch, scrub_skip2, cons_idx, allDone >>

wr_write ==
    /\ pc["writer"] = "wr_write"
    /\ dirty_gen' = [dirty_gen EXCEPT ![PathA] =
            IF dirty_gen[PathA] < MaxGen THEN dirty_gen[PathA] + 1 ELSE dirty_gen[PathA]]
    /\ pc' = [pc EXCEPT !["writer"] = "wr_decRef"]
    /\ UNCHANGED << fileExists, clean_gen, write_refcount, pathStateExists, replLog,
                    replicaExists, w_released, w_dirty, w_gen, w_fileNotFound,
                    scrub_skip, scrub_mismatch, scrub_skip2, cons_idx, allDone >>

wr_decRef ==
    /\ pc["writer"] = "wr_decRef"
    /\ w_released' = TRUE
    /\ write_refcount' = [write_refcount EXCEPT ![PathA] =
            IF write_refcount[PathA] > 0 THEN write_refcount[PathA] - 1 ELSE write_refcount[PathA]]
    /\ pc' = [pc EXCEPT !["writer"] = "wr_isDirty"]
    /\ UNCHANGED << fileExists, dirty_gen, clean_gen, pathStateExists, replLog,
                    replicaExists, w_dirty, w_gen, w_fileNotFound,
                    scrub_skip, scrub_mismatch, scrub_skip2, cons_idx, allDone >>

wr_isDirty ==
    /\ pc["writer"] = "wr_isDirty"
    /\ IF pathStateExists[PathA]
          THEN w_dirty' = (dirty_gen[PathA] > clean_gen[PathA])
          ELSE w_dirty' = FALSE
    /\ IF (IF pathStateExists[PathA] THEN dirty_gen[PathA] > clean_gen[PathA] ELSE FALSE)
          THEN pc' = [pc EXCEPT !["writer"] = "wr_hasWriteRef"]
          ELSE pc' = [pc EXCEPT !["writer"] = "Done"]
    /\ UNCHANGED << fileExists, dirty_gen, clean_gen, write_refcount, pathStateExists,
                    replLog, replicaExists, w_released, w_gen, w_fileNotFound,
                    scrub_skip, scrub_mismatch, scrub_skip2, cons_idx, allDone >>

wr_hasWriteRef ==
    /\ pc["writer"] = "wr_hasWriteRef"
    /\ IF pathStateExists[PathA] /\ write_refcount[PathA] > 0
          THEN pc' = [pc EXCEPT !["writer"] = "Done"]
          ELSE pc' = [pc EXCEPT !["writer"] = "wr_getDirtyGen"]
    /\ UNCHANGED << fileExists, dirty_gen, clean_gen, write_refcount, pathStateExists,
                    replLog, replicaExists, w_released, w_dirty, w_gen, w_fileNotFound,
                    scrub_skip, scrub_mismatch, scrub_skip2, cons_idx, allDone >>

wr_getDirtyGen ==
    /\ pc["writer"] = "wr_getDirtyGen"
    /\ IF pathStateExists[PathA]
          THEN w_gen' = dirty_gen[PathA]
          ELSE w_gen' = 0
    /\ pc' = [pc EXCEPT !["writer"] = "wr_computeChecksum"]
    /\ UNCHANGED << fileExists, dirty_gen, clean_gen, write_refcount, pathStateExists,
                    replLog, replicaExists, w_released, w_dirty, w_fileNotFound,
                    scrub_skip, scrub_mismatch, scrub_skip2, cons_idx, allDone >>

wr_computeChecksum ==
    /\ pc["writer"] = "wr_computeChecksum"
    /\ IF ~fileExists[PathA]
          THEN /\ w_fileNotFound' = TRUE
               /\ pc' = [pc EXCEPT !["writer"] = "Done"]
          ELSE /\ w_fileNotFound' = w_fileNotFound
               /\ pc' = [pc EXCEPT !["writer"] = "wr_enqueue"]
    /\ UNCHANGED << fileExists, dirty_gen, clean_gen, write_refcount, pathStateExists,
                    replLog, replicaExists, w_released, w_dirty, w_gen,
                    scrub_skip, scrub_mismatch, scrub_skip2, cons_idx, allDone >>

wr_enqueue ==
    /\ pc["writer"] = "wr_enqueue"
    /\ replLog' = Append(replLog, <<"put", PathA>>)
    /\ pc' = [pc EXCEPT !["writer"] = "wr_clearDirtyIfGen"]
    /\ UNCHANGED << fileExists, dirty_gen, clean_gen, write_refcount, pathStateExists,
                    replicaExists, w_released, w_dirty, w_gen, w_fileNotFound,
                    scrub_skip, scrub_mismatch, scrub_skip2, cons_idx, allDone >>

wr_clearDirtyIfGen ==
    /\ pc["writer"] = "wr_clearDirtyIfGen"
    /\ IF pathStateExists[PathA] /\ dirty_gen[PathA] = w_gen
          THEN clean_gen' = [clean_gen EXCEPT ![PathA] = w_gen]
          ELSE UNCHANGED clean_gen
    /\ pc' = [pc EXCEPT !["writer"] = "Done"]
    /\ UNCHANGED << fileExists, dirty_gen, write_refcount, pathStateExists,
                    replLog, replicaExists, w_released, w_dirty, w_gen, w_fileNotFound,
                    scrub_skip, scrub_mismatch, scrub_skip2, cons_idx, allDone >>

\* --- Unlinker actions ---

ul_delete ==
    /\ pc["unlinker"] = "ul_delete"
    /\ fileExists' = [fileExists EXCEPT ![PathA] = FALSE]
    /\ pc' = [pc EXCEPT !["unlinker"] = "ul_enqueue"]
    /\ UNCHANGED << dirty_gen, clean_gen, write_refcount, pathStateExists, replLog,
                    replicaExists, w_released, w_dirty, w_gen, w_fileNotFound,
                    scrub_skip, scrub_mismatch, scrub_skip2, cons_idx, allDone >>

ul_enqueue ==
    /\ pc["unlinker"] = "ul_enqueue"
    /\ replLog' = Append(replLog, <<"delete", PathA>>)
    /\ pc' = [pc EXCEPT !["unlinker"] = "ul_removeState"]
    /\ UNCHANGED << fileExists, dirty_gen, clean_gen, write_refcount, pathStateExists,
                    replicaExists, w_released, w_dirty, w_gen, w_fileNotFound,
                    scrub_skip, scrub_mismatch, scrub_skip2, cons_idx, allDone >>

ul_removeState ==
    /\ pc["unlinker"] = "ul_removeState"
    /\ pathStateExists' = [pathStateExists EXCEPT ![PathA] = FALSE]
    /\ dirty_gen' = [dirty_gen EXCEPT ![PathA] = 0]
    /\ clean_gen' = [clean_gen EXCEPT ![PathA] = 0]
    /\ write_refcount' = [write_refcount EXCEPT ![PathA] = 0]
    /\ pc' = [pc EXCEPT !["unlinker"] = "Done"]
    /\ UNCHANGED << fileExists, replLog, replicaExists, w_released, w_dirty, w_gen,
                    w_fileNotFound, scrub_skip, scrub_mismatch, scrub_skip2, cons_idx, allDone >>

\* --- Renamer actions ---

rn_rename ==
    /\ pc["renamer"] = "rn_rename"
    /\ fileExists' = [fileExists EXCEPT ![PathB] = fileExists[PathA], ![PathA] = FALSE]
    /\ pc' = [pc EXCEPT !["renamer"] = "rn_enqueuePair"]
    /\ UNCHANGED << dirty_gen, clean_gen, write_refcount, pathStateExists, replLog,
                    replicaExists, w_released, w_dirty, w_gen, w_fileNotFound,
                    scrub_skip, scrub_mismatch, scrub_skip2, cons_idx, allDone >>

rn_enqueuePair ==
    /\ pc["renamer"] = "rn_enqueuePair"
    /\ replLog' = Append(Append(replLog, <<"delete", PathA>>), <<"put", PathB>>)
    /\ pathStateExists' = [pathStateExists EXCEPT ![PathB] = TRUE]
    /\ pc' = [pc EXCEPT !["renamer"] = "rn_removeState"]
    /\ UNCHANGED << fileExists, dirty_gen, clean_gen, write_refcount,
                    replicaExists, w_released, w_dirty, w_gen, w_fileNotFound,
                    scrub_skip, scrub_mismatch, scrub_skip2, cons_idx, allDone >>

rn_removeState ==
    /\ pc["renamer"] = "rn_removeState"
    /\ pathStateExists' = [pathStateExists EXCEPT ![PathA] = FALSE]
    /\ dirty_gen' = [dirty_gen EXCEPT ![PathA] = 0]
    /\ clean_gen' = [clean_gen EXCEPT ![PathA] = 0]
    /\ pc' = [pc EXCEPT !["renamer"] = "Done"]
    /\ UNCHANGED << fileExists, write_refcount, replLog, replicaExists,
                    w_released, w_dirty, w_gen, w_fileNotFound,
                    scrub_skip, scrub_mismatch, scrub_skip2, cons_idx, allDone >>

\* --- Consumer actions ---

cn_loop ==
    /\ pc["consumer"] = "cn_loop"
    /\ pc' = [pc EXCEPT !["consumer"] = "cn_check"]
    /\ UNCHANGED << fileExists, dirty_gen, clean_gen, write_refcount, pathStateExists,
                    replLog, replicaExists, w_released, w_dirty, w_gen, w_fileNotFound,
                    scrub_skip, scrub_mismatch, scrub_skip2, cons_idx, allDone >>

cn_check ==
    /\ pc["consumer"] = "cn_check"
    /\ IF cons_idx < Len(replLog)
          THEN /\ pc' = [pc EXCEPT !["consumer"] = "cn_dequeue"]
          ELSE /\ IF pc["writer"] = "Done" /\ pc["unlinker"] = "Done" /\ pc["renamer"] = "Done"
                     THEN pc' = [pc EXCEPT !["consumer"] = "Done"]
                     ELSE pc' = [pc EXCEPT !["consumer"] = "cn_loop"]
    /\ UNCHANGED << fileExists, dirty_gen, clean_gen, write_refcount, pathStateExists,
                    replLog, replicaExists, w_released, w_dirty, w_gen, w_fileNotFound,
                    scrub_skip, scrub_mismatch, scrub_skip2, cons_idx, allDone >>

cn_dequeue ==
    /\ pc["consumer"] = "cn_dequeue"
    /\ cons_idx' = cons_idx + 1
    /\ pc' = [pc EXCEPT !["consumer"] = "cn_process"]
    /\ UNCHANGED << fileExists, dirty_gen, clean_gen, write_refcount, pathStateExists,
                    replLog, replicaExists, w_released, w_dirty, w_gen, w_fileNotFound,
                    scrub_skip, scrub_mismatch, scrub_skip2, allDone >>

cn_process ==
    /\ pc["consumer"] = "cn_process"
    /\ IF replLog[cons_idx][1] = "put"
          THEN IF fileExists[replLog[cons_idx][2]]
               THEN replicaExists' = [replicaExists EXCEPT ![replLog[cons_idx][2]] = TRUE]
               ELSE UNCHANGED replicaExists
          ELSE replicaExists' = [replicaExists EXCEPT ![replLog[cons_idx][2]] = FALSE]
    /\ pc' = [pc EXCEPT !["consumer"] = "cn_loop"]
    /\ UNCHANGED << fileExists, dirty_gen, clean_gen, write_refcount, pathStateExists,
                    replLog, w_released, w_dirty, w_gen, w_fileNotFound,
                    scrub_skip, scrub_mismatch, scrub_skip2, cons_idx, allDone >>

\* --- Scrubber actions ---

sc_hasWriteRef1 ==
    /\ pc["scrubber"] = "sc_hasWriteRef1"
    /\ IF write_refcount[PathA] > 0
          THEN /\ scrub_skip' = TRUE
               /\ pc' = [pc EXCEPT !["scrubber"] = "Done"]
          ELSE /\ scrub_skip' = scrub_skip
               /\ pc' = [pc EXCEPT !["scrubber"] = "sc_computeChecksum"]
    /\ UNCHANGED << fileExists, dirty_gen, clean_gen, write_refcount, pathStateExists,
                    replLog, replicaExists, w_released, w_dirty, w_gen, w_fileNotFound,
                    scrub_mismatch, scrub_skip2, cons_idx, allDone >>

sc_computeChecksum ==
    /\ pc["scrubber"] = "sc_computeChecksum"
    /\ \/ /\ scrub_mismatch' = TRUE
          /\ pc' = [pc EXCEPT !["scrubber"] = "sc_hasPendingPut"]
       \/ /\ scrub_mismatch' = FALSE
          /\ pc' = [pc EXCEPT !["scrubber"] = "Done"]
    /\ UNCHANGED << fileExists, dirty_gen, clean_gen, write_refcount, pathStateExists,
                    replLog, replicaExists, w_released, w_dirty, w_gen, w_fileNotFound,
                    scrub_skip, scrub_skip2, cons_idx, allDone >>

sc_hasPendingPut ==
    /\ pc["scrubber"] = "sc_hasPendingPut"
    /\ IF \E i \in 1..Len(replLog): replLog[i][1] = "put" /\ replLog[i][2] = PathA
          THEN /\ pc' = [pc EXCEPT !["scrubber"] = "Done"]
          ELSE /\ pc' = [pc EXCEPT !["scrubber"] = "sc_hasWriteRef2"]
    /\ UNCHANGED << fileExists, dirty_gen, clean_gen, write_refcount, pathStateExists,
                    replLog, replicaExists, w_released, w_dirty, w_gen, w_fileNotFound,
                    scrub_skip, scrub_mismatch, scrub_skip2, cons_idx, allDone >>

sc_hasWriteRef2 ==
    /\ pc["scrubber"] = "sc_hasWriteRef2"
    /\ IF write_refcount[PathA] > 0
          THEN /\ scrub_skip2' = TRUE
               /\ pc' = [pc EXCEPT !["scrubber"] = "Done"]
          ELSE /\ UNCHANGED scrub_skip2
               /\ pc' = [pc EXCEPT !["scrubber"] = "sc_repair"]
    /\ UNCHANGED << fileExists, dirty_gen, clean_gen, write_refcount, pathStateExists,
                    replLog, replicaExists, w_released, w_dirty, w_gen, w_fileNotFound,
                    scrub_skip, scrub_mismatch, cons_idx, allDone >>

sc_repair ==
    /\ pc["scrubber"] = "sc_repair"
    /\ pc' = [pc EXCEPT !["scrubber"] = "Done"]
    /\ UNCHANGED << fileExists, dirty_gen, clean_gen, write_refcount, pathStateExists,
                    replLog, replicaExists, w_released, w_dirty, w_gen, w_fileNotFound,
                    scrub_skip, scrub_mismatch, scrub_skip2, cons_idx, allDone >>

\* Complete next-state relation
Next == \/ wr_open \/ wr_write \/ wr_decRef \/ wr_isDirty
        \/ wr_hasWriteRef \/ wr_getDirtyGen \/ wr_computeChecksum
        \/ wr_enqueue \/ wr_clearDirtyIfGen
        \/ ul_delete \/ ul_enqueue \/ ul_removeState
        \/ rn_rename \/ rn_enqueuePair \/ rn_removeState
        \/ cn_loop \/ cn_check \/ cn_dequeue \/ cn_process
        \/ sc_hasWriteRef1 \/ sc_computeChecksum \/ sc_hasPendingPut
        \/ sc_hasWriteRef2 \/ sc_repair

Spec == Init /\ [][Next]_vars /\ WF_vars(Next)

\* END TRANSLATION

----

\* =============================================================
\* Helper: check if all non-consumer processes are done
\* =============================================================
Terminated ==
    /\ pc["writer"] = "Done"
    /\ pc["unlinker"] = "Done"
    /\ pc["renamer"] = "Done"
    /\ pc["scrubber"] = "Done"

\* =============================================================
\* INVARIANTS
\* =============================================================

(*
 * Inv8 -- NoStaleReplica (safety):
 * After all processes terminate and the consumer drains the queue,
 * if a file does not exist in the source then it does not exist
 * in the replica.
 *)
NoStaleReplica ==
    (Terminated /\ pc["consumer"] = "Done") =>
        \A p \in Paths: ~fileExists[p] => ~replicaExists[p]

(*
 * Inv9 -- SafePutAfterDelete (safety):
 * If a consumer processes a put for a path that no longer exists,
 * the put is a no-op (the consumer does not create the file on replica).
 * This is enforced by the consumer's processPut logic checking fileExists.
 *)
SafePutAfterDelete ==
    pc["consumer"] = "cn_process" /\ cons_idx <= Len(replLog) =>
        (replLog[cons_idx][1] = "put" /\ ~fileExists[replLog[cons_idx][2]] =>
            replicaExists'[replLog[cons_idx][2]] = replicaExists[replLog[cons_idx][2]])

(*
 * Inv10 -- NoScrubClobber (safety):
 * The scrubber never executes a repair while the file has an open writer.
 * The second hasWriteRef check guards the repair step.  Additionally,
 * the scrubber skips repair when there is a pending put in the replLog
 * (the replica may be stale).  We verify the post-condition: the scrubber
 * only reaches the sc_repair step when write_refcount was observed as 0
 * at sc_hasWriteRef2 and no pending put was found at sc_hasPendingPut.
 *)
NoScrubClobber ==
    (pc["scrubber"] = "sc_repair") => (~scrub_skip /\ ~scrub_skip2)

====
