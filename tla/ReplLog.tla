---------------------------- MODULE ReplLog ----------------------------
(*
 * Module 2: Replication Queue + Shutdown
 *
 * Models the ReplLog producer-consumer queue (main.zig:458-838),
 * the replication worker loop (main.zig:1048-1077), and the shutdown
 * protocol (main.zig:226-256).
 *
 * Source Code Mapping:
 *   Producer.enqueue       -> ReplLog.enqueue         (main.zig:559-580)
 *   PairProducer.enqueuePair -> ReplLog.enqueuePair   (main.zig:582-623)
 *   Consumer.dequeueNext   -> ReplLog.dequeueNext     (main.zig:656-698)
 *   Consumer.markCompleted -> ReplLog.markCompleted    (main.zig:700-717)
 *   Consumer.coalesce      -> coalescing in dequeueNext (main.zig:666-679)
 *   Consumer.checkDep      -> dependency check         (main.zig:682-688)
 *   Shutdown.setFlag       -> FsState.stopWorkers      (main.zig:227)
 *   Shutdown.broadcast     -> cond.broadcast under mutex (main.zig:231-235)
 *)

EXTENDS Naturals, Integers, Sequences, FiniteSets, TLC

CONSTANTS
    Paths,          \* set of path names
    Producers,      \* set of producer process IDs
    PairProds,      \* set of pair-producer process IDs
    Consumers,      \* set of consumer process IDs
    MaxOps          \* max operations per producer

ASSUME Paths /= {}
ASSUME Producers /= {}
ASSUME Consumers /= {}
ASSUME MaxOps \in Nat /\ MaxOps >= 1

\* Helper: Range of a sequence
Range(s) == {s[i] : i \in 1..Len(s)}

(*
--algorithm fair ReplLog {

variables
    \* The replication log: sequence of records
    entries   = <<>>,
    next_id   = 0,

    \* Mutex + condition variable
    mutex     = "free",
    waiting   = {},       \* threads blocked on cond.wait
    signaled  = {},       \* threads woken by signal/broadcast

    \* Shutdown flag (atomic)
    shutdown  = FALSE,

    \* Per-producer state
    prod_ops  = [p \in Producers |-> 0],

    \* Per-pair-producer state
    pair_done = [p \in PairProds |-> FALSE],

    \* Per-consumer state: what entry is currently in-flight (-1 = none)
    cons_entry = [c \in Consumers |-> -1],
    cons_done  = [c \in Consumers |-> FALSE];

\* ---------------------------------------------------------------
\* Macros for mutex
\* ---------------------------------------------------------------
macro AcquireMutex(t) {
    await mutex = "free";
    mutex := t;
}

macro ReleaseMutex() {
    mutex := "free";
}

\* ---------------------------------------------------------------
\* Macros for condition variable
\* ---------------------------------------------------------------
macro CondSignal() {
    if (waiting /= {}) {
        with (t \in waiting) {
            waiting := waiting \ {t};
            signaled := signaled \cup {t};
        };
    };
}

macro CondBroadcast() {
    signaled := signaled \cup waiting;
    waiting := {};
}

\* =============================================================
\* Producer: repeatedly enqueue single entries
\* =============================================================
fair process (Producer \in Producers)
variables p_op = "put", p_path = "a";
{
p_loop:
    while (prod_ops[self] < MaxOps /\ ~shutdown) {
p_choose:
        \* Non-deterministically choose operation and path
        with (op \in {"put", "delete"}, path \in Paths) {
            p_op := op;
            p_path := path;
        };

p_acquire:
        AcquireMutex(self);

p_append:
        entries := Append(entries, [
            id         |-> next_id,
            op         |-> p_op,
            path       |-> p_path,
            completed  |-> FALSE,
            in_flight  |-> FALSE,
            depends_on |-> -1,
            coalesced  |-> FALSE
        ]);
        next_id := next_id + 1;

p_signal:
        CondSignal();

p_release:
        ReleaseMutex();
        prod_ops[self] := prod_ops[self] + 1;
    };
}

\* =============================================================
\* PairProducer: enqueue a pair with dependency (models rename)
\* =============================================================
fair process (PairProducer \in PairProds)
variables pp_id1 = 0;
{
pp_start:
    if (pair_done[self] \/ shutdown) { goto Done; };

pp_acquire:
    AcquireMutex(self);

pp_append1:
    \* First entry: delete old path
    entries := Append(entries, [
        id         |-> next_id,
        op         |-> "delete",
        path       |-> "a",
        completed  |-> FALSE,
        in_flight  |-> FALSE,
        depends_on |-> -1,
        coalesced  |-> FALSE
    ]);
    pp_id1 := next_id;
    next_id := next_id + 1;

pp_append2:
    \* Second entry: put new path, depends on first
    entries := Append(entries, [
        id         |-> next_id,
        op         |-> "put",
        path       |-> "b",
        completed  |-> FALSE,
        in_flight  |-> FALSE,
        depends_on |-> pp_id1,
        coalesced  |-> FALSE
    ]);
    next_id := next_id + 1;

pp_broadcast:
    CondBroadcast();

pp_release:
    ReleaseMutex();
    pair_done[self] := TRUE;
}

\* =============================================================
\* Consumer: dequeue -> process -> markCompleted, loop
\* =============================================================
fair process (Consumer \in Consumers)
variables c_found = FALSE, c_idx = 0;
{
c_loop:
    while (~shutdown \/ cons_entry[self] >= 0) {

c_acquire:
        AcquireMutex(self);

c_scan:
        \* Scan for eligible entry
        c_found := FALSE;
        c_idx := 1;

c_scan_loop:
        while (c_idx <= Len(entries) /\ ~c_found) {
            if (~entries[c_idx].completed /\ ~entries[c_idx].in_flight) {
                \* Check coalescing for put entries
                if (entries[c_idx].op = "put") {
                    \* Check if there's a newer non-completed put for the same path
                    if (\E j \in (c_idx+1)..Len(entries):
                            entries[j].op = "put"
                            /\ ~entries[j].completed
                            /\ entries[j].path = entries[c_idx].path) {
                        \* Coalesce: mark as completed
                        entries := [entries EXCEPT
                            ![c_idx].completed = TRUE,
                            ![c_idx].coalesced = TRUE];
                    } else {
                        \* Check dependency
                        if (entries[c_idx].depends_on >= 0) {
                            \* Find the dependency
                            if (\E j \in 1..Len(entries):
                                    entries[j].id = entries[c_idx].depends_on
                                    /\ entries[j].completed) {
                                \* Dependency satisfied
                                entries := [entries EXCEPT ![c_idx].in_flight = TRUE];
                                cons_entry[self] := entries[c_idx].id;
                                c_found := TRUE;
                            } else {
                                \* Check if dependency was truncated (not in entries)
                                if (~\E j \in 1..Len(entries):
                                        entries[j].id = entries[c_idx].depends_on) {
                                    \* Truncated -> treat as completed
                                    entries := [entries EXCEPT ![c_idx].in_flight = TRUE];
                                    cons_entry[self] := entries[c_idx].id;
                                    c_found := TRUE;
                                };
                                \* else: dependency exists but not completed, skip
                            };
                        } else {
                            \* No dependency, take it
                            entries := [entries EXCEPT ![c_idx].in_flight = TRUE];
                            cons_entry[self] := entries[c_idx].id;
                            c_found := TRUE;
                        };
                    };
                } else {
                    \* delete entry: check dependency only
                    if (entries[c_idx].depends_on >= 0) {
                        if (\E j \in 1..Len(entries):
                                entries[j].id = entries[c_idx].depends_on
                                /\ entries[j].completed) {
                            entries := [entries EXCEPT ![c_idx].in_flight = TRUE];
                            cons_entry[self] := entries[c_idx].id;
                            c_found := TRUE;
                        } else {
                            if (~\E j \in 1..Len(entries):
                                    entries[j].id = entries[c_idx].depends_on) {
                                entries := [entries EXCEPT ![c_idx].in_flight = TRUE];
                                cons_entry[self] := entries[c_idx].id;
                                c_found := TRUE;
                            };
                        };
                    } else {
                        entries := [entries EXCEPT ![c_idx].in_flight = TRUE];
                        cons_entry[self] := entries[c_idx].id;
                        c_found := TRUE;
                    };
                };
            };
            if (~c_found) { c_idx := c_idx + 1; };
        };

c_check_found:
        if (~c_found) {
            \* No work available
            if (shutdown) {
c_release_exit:
                ReleaseMutex();
                cons_done[self] := TRUE;
                goto Done;
            } else {
c_wait:
                \* cond.wait: release mutex, wait for signal, reacquire
                waiting := waiting \cup {self};
                ReleaseMutex();

c_wait_signal:
                await self \in signaled \/ shutdown;
                signaled := signaled \ {self};
                waiting := waiting \ {self};

c_reacquire:
                AcquireMutex(self);

c_rescan_release:
                \* Release and go back to scan
                ReleaseMutex();
                goto c_acquire;
            };
        };

c_release_work:
        ReleaseMutex();

        \* "Do work" (abstract -- no actual I/O)
c_doWork:
        skip;

        \* markCompleted
c_mark_acquire:
        AcquireMutex(self);

c_markCompleted:
        \* Find the entry and mark completed
        c_idx := 1;

c_mark_loop:
        while (c_idx <= Len(entries)) {
            if (entries[c_idx].id = cons_entry[self]) {
                entries := [entries EXCEPT
                    ![c_idx].completed = TRUE,
                    ![c_idx].in_flight = FALSE];
                c_idx := Len(entries) + 1; \* break
            } else {
                c_idx := c_idx + 1;
            };
        };

c_mark_release:
        ReleaseMutex();
        cons_entry[self] := -1;
    };

c_final:
    cons_done[self] := TRUE;
}

\* =============================================================
\* Shutdown: set flag, broadcast under mutex
\* =============================================================
fair process (Shutdown = "shutdown_proc")
{
s_wait:
    \* Wait until all producers are done
    await \A p \in Producers: prod_ops[p] >= MaxOps \/ shutdown;
    await \A p \in PairProds: pair_done[p] \/ shutdown;

s_setFlag:
    shutdown := TRUE;

s_acquire:
    AcquireMutex(self);

s_broadcast:
    CondBroadcast();

s_release:
    ReleaseMutex();
}

}
*)

\* BEGIN TRANSLATION (chksum(pcal) = "c1d3e36a" /\ chksum(tla) = "4b923c93")
VARIABLES entries, next_id, mutex, waiting, signaled, shutdown, prod_ops, 
          pair_done, cons_entry, cons_done, pc, p_op, p_path, pp_id1, 
          c_found, c_idx

vars == << entries, next_id, mutex, waiting, signaled, shutdown, prod_ops, 
           pair_done, cons_entry, cons_done, pc, p_op, p_path, pp_id1, 
           c_found, c_idx >>

ProcSet == (Producers) \cup (PairProds) \cup (Consumers) \cup {"shutdown_proc"}

Init == (* Global variables *)
        /\ entries = <<>>
        /\ next_id = 0
        /\ mutex = "free"
        /\ waiting = {}
        /\ signaled = {}
        /\ shutdown = FALSE
        /\ prod_ops = [p \in Producers |-> 0]
        /\ pair_done = [p \in PairProds |-> FALSE]
        /\ cons_entry = [c \in Consumers |-> -1]
        /\ cons_done = [c \in Consumers |-> FALSE]
        (* Per-process variables *)
        /\ p_op = [self \in Producers |-> "put"]
        /\ p_path = [self \in Producers |-> "a"]
        /\ pp_id1 = [self \in PairProds |-> 0]
        /\ c_found = [self \in Consumers |-> FALSE]
        /\ c_idx = [self \in Consumers |-> 0]
        /\ pc = [self \in ProcSet |->
                    CASE self \in Producers -> "p_loop"
                      [] self \in PairProds -> "pp_start"
                      [] self \in Consumers -> "c_loop"
                      [] self = "shutdown_proc" -> "s_wait"]

\* --- Producer actions ---

p_loop(self) ==
    /\ self \in Producers
    /\ pc[self] = "p_loop"
    /\ IF prod_ops[self] < MaxOps /\ ~shutdown
          THEN /\ pc' = [pc EXCEPT ![self] = "p_choose"]
          ELSE /\ pc' = [pc EXCEPT ![self] = "Done"]
    /\ UNCHANGED << entries, next_id, mutex, waiting, signaled, shutdown,
                    prod_ops, pair_done, cons_entry, cons_done,
                    p_op, p_path, pp_id1, c_found, c_idx >>

p_choose(self) ==
    /\ self \in Producers
    /\ pc[self] = "p_choose"
    /\ \E op \in {"put", "delete"}, path \in Paths:
        /\ p_op' = [p_op EXCEPT ![self] = op]
        /\ p_path' = [p_path EXCEPT ![self] = path]
    /\ pc' = [pc EXCEPT ![self] = "p_acquire"]
    /\ UNCHANGED << entries, next_id, mutex, waiting, signaled, shutdown,
                    prod_ops, pair_done, cons_entry, cons_done,
                    pp_id1, c_found, c_idx >>

p_acquire(self) ==
    /\ self \in Producers
    /\ pc[self] = "p_acquire"
    /\ mutex = "free"
    /\ mutex' = self
    /\ pc' = [pc EXCEPT ![self] = "p_append"]
    /\ UNCHANGED << entries, next_id, waiting, signaled, shutdown,
                    prod_ops, pair_done, cons_entry, cons_done,
                    p_op, p_path, pp_id1, c_found, c_idx >>

p_append(self) ==
    /\ self \in Producers
    /\ pc[self] = "p_append"
    /\ entries' = Append(entries, [
            id         |-> next_id,
            op         |-> p_op[self],
            path       |-> p_path[self],
            completed  |-> FALSE,
            in_flight  |-> FALSE,
            depends_on |-> -1,
            coalesced  |-> FALSE])
    /\ next_id' = next_id + 1
    /\ pc' = [pc EXCEPT ![self] = "p_signal"]
    /\ UNCHANGED << mutex, waiting, signaled, shutdown,
                    prod_ops, pair_done, cons_entry, cons_done,
                    p_op, p_path, pp_id1, c_found, c_idx >>

p_signal(self) ==
    /\ self \in Producers
    /\ pc[self] = "p_signal"
    /\ IF waiting /= {}
          THEN \E t \in waiting:
                /\ waiting' = waiting \ {t}
                /\ signaled' = signaled \cup {t}
          ELSE UNCHANGED << waiting, signaled >>
    /\ pc' = [pc EXCEPT ![self] = "p_release"]
    /\ UNCHANGED << entries, next_id, mutex, shutdown,
                    prod_ops, pair_done, cons_entry, cons_done,
                    p_op, p_path, pp_id1, c_found, c_idx >>

p_release(self) ==
    /\ self \in Producers
    /\ pc[self] = "p_release"
    /\ mutex' = "free"
    /\ prod_ops' = [prod_ops EXCEPT ![self] = prod_ops[self] + 1]
    /\ pc' = [pc EXCEPT ![self] = "p_loop"]
    /\ UNCHANGED << entries, next_id, waiting, signaled, shutdown,
                    pair_done, cons_entry, cons_done,
                    p_op, p_path, pp_id1, c_found, c_idx >>

\* --- PairProducer actions ---

pp_start(self) ==
    /\ self \in PairProds
    /\ pc[self] = "pp_start"
    /\ IF pair_done[self] \/ shutdown
          THEN /\ pc' = [pc EXCEPT ![self] = "Done"]
          ELSE /\ pc' = [pc EXCEPT ![self] = "pp_acquire"]
    /\ UNCHANGED << entries, next_id, mutex, waiting, signaled, shutdown,
                    prod_ops, pair_done, cons_entry, cons_done,
                    p_op, p_path, pp_id1, c_found, c_idx >>

pp_acquire(self) ==
    /\ self \in PairProds
    /\ pc[self] = "pp_acquire"
    /\ mutex = "free"
    /\ mutex' = self
    /\ pc' = [pc EXCEPT ![self] = "pp_append1"]
    /\ UNCHANGED << entries, next_id, waiting, signaled, shutdown,
                    prod_ops, pair_done, cons_entry, cons_done,
                    p_op, p_path, pp_id1, c_found, c_idx >>

pp_append1(self) ==
    /\ self \in PairProds
    /\ pc[self] = "pp_append1"
    /\ entries' = Append(entries, [
            id         |-> next_id,
            op         |-> "delete",
            path       |-> "a",
            completed  |-> FALSE,
            in_flight  |-> FALSE,
            depends_on |-> -1,
            coalesced  |-> FALSE])
    /\ pp_id1' = [pp_id1 EXCEPT ![self] = next_id]
    /\ next_id' = next_id + 1
    /\ pc' = [pc EXCEPT ![self] = "pp_append2"]
    /\ UNCHANGED << mutex, waiting, signaled, shutdown,
                    prod_ops, pair_done, cons_entry, cons_done,
                    p_op, p_path, c_found, c_idx >>

pp_append2(self) ==
    /\ self \in PairProds
    /\ pc[self] = "pp_append2"
    /\ entries' = Append(entries, [
            id         |-> next_id,
            op         |-> "put",
            path       |-> "b",
            completed  |-> FALSE,
            in_flight  |-> FALSE,
            depends_on |-> pp_id1[self],
            coalesced  |-> FALSE])
    /\ next_id' = next_id + 1
    /\ pc' = [pc EXCEPT ![self] = "pp_broadcast"]
    /\ UNCHANGED << mutex, waiting, signaled, shutdown,
                    prod_ops, pair_done, cons_entry, cons_done,
                    p_op, p_path, pp_id1, c_found, c_idx >>

pp_broadcast(self) ==
    /\ self \in PairProds
    /\ pc[self] = "pp_broadcast"
    /\ signaled' = signaled \cup waiting
    /\ waiting' = {}
    /\ pc' = [pc EXCEPT ![self] = "pp_release"]
    /\ UNCHANGED << entries, next_id, mutex, shutdown,
                    prod_ops, pair_done, cons_entry, cons_done,
                    p_op, p_path, pp_id1, c_found, c_idx >>

pp_release(self) ==
    /\ self \in PairProds
    /\ pc[self] = "pp_release"
    /\ mutex' = "free"
    /\ pair_done' = [pair_done EXCEPT ![self] = TRUE]
    /\ pc' = [pc EXCEPT ![self] = "Done"]
    /\ UNCHANGED << entries, next_id, waiting, signaled, shutdown,
                    prod_ops, cons_entry, cons_done,
                    p_op, p_path, pp_id1, c_found, c_idx >>

\* --- Consumer actions ---

c_loop(self) ==
    /\ self \in Consumers
    /\ pc[self] = "c_loop"
    /\ IF ~shutdown \/ cons_entry[self] >= 0
          THEN /\ pc' = [pc EXCEPT ![self] = "c_acquire"]
          ELSE /\ pc' = [pc EXCEPT ![self] = "c_final"]
    /\ UNCHANGED << entries, next_id, mutex, waiting, signaled, shutdown,
                    prod_ops, pair_done, cons_entry, cons_done,
                    p_op, p_path, pp_id1, c_found, c_idx >>

c_acquire(self) ==
    /\ self \in Consumers
    /\ pc[self] = "c_acquire"
    /\ mutex = "free"
    /\ mutex' = self
    /\ pc' = [pc EXCEPT ![self] = "c_scan"]
    /\ UNCHANGED << entries, next_id, waiting, signaled, shutdown,
                    prod_ops, pair_done, cons_entry, cons_done,
                    p_op, p_path, pp_id1, c_found, c_idx >>

c_scan(self) ==
    /\ self \in Consumers
    /\ pc[self] = "c_scan"
    /\ c_found' = [c_found EXCEPT ![self] = FALSE]
    /\ c_idx' = [c_idx EXCEPT ![self] = 1]
    /\ pc' = [pc EXCEPT ![self] = "c_scan_loop"]
    /\ UNCHANGED << entries, next_id, mutex, waiting, signaled, shutdown,
                    prod_ops, pair_done, cons_entry, cons_done,
                    p_op, p_path, pp_id1 >>

c_scan_loop(self) ==
    /\ self \in Consumers
    /\ pc[self] = "c_scan_loop"
    /\ IF c_idx[self] <= Len(entries) /\ ~c_found[self]
       THEN
            /\ IF ~entries[c_idx[self]].completed /\ ~entries[c_idx[self]].in_flight
               THEN
                    /\ IF entries[c_idx[self]].op = "put"
                       THEN
                            \* Check coalescing
                            /\ IF \E j \in (c_idx[self]+1)..Len(entries):
                                    entries[j].op = "put"
                                    /\ ~entries[j].completed
                                    /\ entries[j].path = entries[c_idx[self]].path
                               THEN
                                    \* Coalesce
                                    /\ entries' = [entries EXCEPT
                                          ![c_idx[self]].completed = TRUE,
                                          ![c_idx[self]].coalesced = TRUE]
                                    /\ UNCHANGED << cons_entry, c_found >>
                                    /\ c_idx' = [c_idx EXCEPT ![self] = c_idx[self] + 1]
                               ELSE
                                    \* Check dependency
                                     /\ IF entries[c_idx[self]].depends_on >= 0
                                       THEN
                                            IF \E j \in 1..Len(entries):
                                                    entries[j].id = entries[c_idx[self]].depends_on
                                                    /\ entries[j].completed
                                            THEN \* Dep satisfied
                                                /\ entries' = [entries EXCEPT ![c_idx[self]].in_flight = TRUE]
                                                /\ cons_entry' = [cons_entry EXCEPT ![self] = entries[c_idx[self]].id]
                                                /\ c_found' = [c_found EXCEPT ![self] = TRUE]
                                                /\ UNCHANGED c_idx
                                            ELSE IF ~\E j \in 1..Len(entries):
                                                        entries[j].id = entries[c_idx[self]].depends_on
                                                 THEN \* Truncated
                                                    /\ entries' = [entries EXCEPT ![c_idx[self]].in_flight = TRUE]
                                                    /\ cons_entry' = [cons_entry EXCEPT ![self] = entries[c_idx[self]].id]
                                                    /\ c_found' = [c_found EXCEPT ![self] = TRUE]
                                                    /\ UNCHANGED c_idx
                                                 ELSE \* Dep not done, skip
                                                    /\ c_idx' = [c_idx EXCEPT ![self] = c_idx[self] + 1]
                                                    /\ UNCHANGED << entries, cons_entry, c_found >>
                                       ELSE
                                            \* No dependency, take it
                                            /\ entries' = [entries EXCEPT ![c_idx[self]].in_flight = TRUE]
                                            /\ cons_entry' = [cons_entry EXCEPT ![self] = entries[c_idx[self]].id]
                                            /\ c_found' = [c_found EXCEPT ![self] = TRUE]
                                            /\ UNCHANGED c_idx
                       ELSE
                            \* delete entry: check dependency only
                            /\ IF entries[c_idx[self]].depends_on >= 0
                               THEN
                                    IF \E j \in 1..Len(entries):
                                            entries[j].id = entries[c_idx[self]].depends_on
                                            /\ entries[j].completed
                                    THEN
                                        /\ entries' = [entries EXCEPT ![c_idx[self]].in_flight = TRUE]
                                        /\ cons_entry' = [cons_entry EXCEPT ![self] = entries[c_idx[self]].id]
                                        /\ c_found' = [c_found EXCEPT ![self] = TRUE]
                                        /\ UNCHANGED c_idx
                                    ELSE IF ~\E j \in 1..Len(entries):
                                                entries[j].id = entries[c_idx[self]].depends_on
                                         THEN
                                            /\ entries' = [entries EXCEPT ![c_idx[self]].in_flight = TRUE]
                                            /\ cons_entry' = [cons_entry EXCEPT ![self] = entries[c_idx[self]].id]
                                            /\ c_found' = [c_found EXCEPT ![self] = TRUE]
                                            /\ UNCHANGED c_idx
                                         ELSE
                                            /\ c_idx' = [c_idx EXCEPT ![self] = c_idx[self] + 1]
                                            /\ UNCHANGED << entries, cons_entry, c_found >>
                               ELSE
                                    /\ entries' = [entries EXCEPT ![c_idx[self]].in_flight = TRUE]
                                    /\ cons_entry' = [cons_entry EXCEPT ![self] = entries[c_idx[self]].id]
                                    /\ c_found' = [c_found EXCEPT ![self] = TRUE]
                                    /\ UNCHANGED c_idx
               ELSE
                    \* Entry completed or in_flight, skip
                    /\ c_idx' = [c_idx EXCEPT ![self] = c_idx[self] + 1]
                    /\ UNCHANGED << entries, cons_entry, c_found >>
            /\ pc' = [pc EXCEPT ![self] = "c_scan_loop"]
       ELSE
            \* Done scanning
            /\ pc' = [pc EXCEPT ![self] = "c_check_found"]
            /\ UNCHANGED << entries, cons_entry, c_found, c_idx >>
    /\ UNCHANGED << next_id, mutex, waiting, signaled, shutdown,
                    prod_ops, pair_done, cons_done,
                    p_op, p_path, pp_id1 >>

c_check_found(self) ==
    /\ self \in Consumers
    /\ pc[self] = "c_check_found"
    /\ IF ~c_found[self]
          THEN IF shutdown
               THEN /\ pc' = [pc EXCEPT ![self] = "c_release_exit"]
               ELSE /\ pc' = [pc EXCEPT ![self] = "c_wait"]
          ELSE /\ pc' = [pc EXCEPT ![self] = "c_release_work"]
    /\ UNCHANGED << entries, next_id, mutex, waiting, signaled, shutdown,
                    prod_ops, pair_done, cons_entry, cons_done,
                    p_op, p_path, pp_id1, c_found, c_idx >>

c_release_exit(self) ==
    /\ self \in Consumers
    /\ pc[self] = "c_release_exit"
    /\ mutex' = "free"
    /\ cons_done' = [cons_done EXCEPT ![self] = TRUE]
    /\ pc' = [pc EXCEPT ![self] = "Done"]
    /\ UNCHANGED << entries, next_id, waiting, signaled, shutdown,
                    prod_ops, pair_done, cons_entry,
                    p_op, p_path, pp_id1, c_found, c_idx >>

c_wait(self) ==
    /\ self \in Consumers
    /\ pc[self] = "c_wait"
    /\ waiting' = waiting \cup {self}
    /\ mutex' = "free"
    /\ pc' = [pc EXCEPT ![self] = "c_wait_signal"]
    /\ UNCHANGED << entries, next_id, signaled, shutdown,
                    prod_ops, pair_done, cons_entry, cons_done,
                    p_op, p_path, pp_id1, c_found, c_idx >>

c_wait_signal(self) ==
    /\ self \in Consumers
    /\ pc[self] = "c_wait_signal"
    /\ self \in signaled \/ shutdown
    /\ signaled' = signaled \ {self}
    /\ waiting' = waiting \ {self}
    /\ pc' = [pc EXCEPT ![self] = "c_reacquire"]
    /\ UNCHANGED << entries, next_id, mutex, shutdown,
                    prod_ops, pair_done, cons_entry, cons_done,
                    p_op, p_path, pp_id1, c_found, c_idx >>

c_reacquire(self) ==
    /\ self \in Consumers
    /\ pc[self] = "c_reacquire"
    /\ mutex = "free"
    /\ mutex' = self
    /\ pc' = [pc EXCEPT ![self] = "c_rescan_release"]
    /\ UNCHANGED << entries, next_id, waiting, signaled, shutdown,
                    prod_ops, pair_done, cons_entry, cons_done,
                    p_op, p_path, pp_id1, c_found, c_idx >>

c_rescan_release(self) ==
    /\ self \in Consumers
    /\ pc[self] = "c_rescan_release"
    /\ mutex' = "free"
    /\ pc' = [pc EXCEPT ![self] = "c_acquire"]
    /\ UNCHANGED << entries, next_id, waiting, signaled, shutdown,
                    prod_ops, pair_done, cons_entry, cons_done,
                    p_op, p_path, pp_id1, c_found, c_idx >>

c_release_work(self) ==
    /\ self \in Consumers
    /\ pc[self] = "c_release_work"
    /\ mutex' = "free"
    /\ pc' = [pc EXCEPT ![self] = "c_doWork"]
    /\ UNCHANGED << entries, next_id, waiting, signaled, shutdown,
                    prod_ops, pair_done, cons_entry, cons_done,
                    p_op, p_path, pp_id1, c_found, c_idx >>

c_doWork(self) ==
    /\ self \in Consumers
    /\ pc[self] = "c_doWork"
    /\ pc' = [pc EXCEPT ![self] = "c_mark_acquire"]
    /\ UNCHANGED << entries, next_id, mutex, waiting, signaled, shutdown,
                    prod_ops, pair_done, cons_entry, cons_done,
                    p_op, p_path, pp_id1, c_found, c_idx >>

c_mark_acquire(self) ==
    /\ self \in Consumers
    /\ pc[self] = "c_mark_acquire"
    /\ mutex = "free"
    /\ mutex' = self
    /\ pc' = [pc EXCEPT ![self] = "c_markCompleted"]
    /\ UNCHANGED << entries, next_id, waiting, signaled, shutdown,
                    prod_ops, pair_done, cons_entry, cons_done,
                    p_op, p_path, pp_id1, c_found, c_idx >>

c_markCompleted(self) ==
    /\ self \in Consumers
    /\ pc[self] = "c_markCompleted"
    /\ c_idx' = [c_idx EXCEPT ![self] = 1]
    /\ pc' = [pc EXCEPT ![self] = "c_mark_loop"]
    /\ UNCHANGED << entries, next_id, mutex, waiting, signaled, shutdown,
                    prod_ops, pair_done, cons_entry, cons_done,
                    p_op, p_path, pp_id1, c_found >>

c_mark_loop(self) ==
    /\ self \in Consumers
    /\ pc[self] = "c_mark_loop"
    /\ IF c_idx[self] <= Len(entries)
          THEN IF entries[c_idx[self]].id = cons_entry[self]
               THEN /\ entries' = [entries EXCEPT
                        ![c_idx[self]].completed = TRUE,
                        ![c_idx[self]].in_flight = FALSE]
                    /\ c_idx' = [c_idx EXCEPT ![self] = Len(entries) + 1]
               ELSE /\ c_idx' = [c_idx EXCEPT ![self] = c_idx[self] + 1]
                    /\ UNCHANGED entries
          ELSE /\ UNCHANGED << entries, c_idx >>
    /\ IF c_idx[self] > Len(entries) \/ (c_idx[self] <= Len(entries) /\ entries[c_idx[self]].id = cons_entry[self])
          THEN /\ pc' = [pc EXCEPT ![self] = "c_mark_release"]
          ELSE /\ pc' = [pc EXCEPT ![self] = "c_mark_loop"]
    /\ UNCHANGED << next_id, mutex, waiting, signaled, shutdown,
                    prod_ops, pair_done, cons_entry, cons_done,
                    p_op, p_path, pp_id1, c_found >>

c_mark_release(self) ==
    /\ self \in Consumers
    /\ pc[self] = "c_mark_release"
    /\ mutex' = "free"
    /\ cons_entry' = [cons_entry EXCEPT ![self] = -1]
    /\ pc' = [pc EXCEPT ![self] = "c_loop"]
    /\ UNCHANGED << entries, next_id, waiting, signaled, shutdown,
                    prod_ops, pair_done, cons_done,
                    p_op, p_path, pp_id1, c_found, c_idx >>

c_final(self) ==
    /\ self \in Consumers
    /\ pc[self] = "c_final"
    /\ cons_done' = [cons_done EXCEPT ![self] = TRUE]
    /\ pc' = [pc EXCEPT ![self] = "Done"]
    /\ UNCHANGED << entries, next_id, mutex, waiting, signaled, shutdown,
                    prod_ops, pair_done, cons_entry,
                    p_op, p_path, pp_id1, c_found, c_idx >>

\* --- Shutdown actions ---

s_wait ==
    /\ pc["shutdown_proc"] = "s_wait"
    /\ \A p \in Producers: prod_ops[p] >= MaxOps \/ shutdown
    /\ \A p \in PairProds: pair_done[p] \/ shutdown
    /\ pc' = [pc EXCEPT !["shutdown_proc"] = "s_setFlag"]
    /\ UNCHANGED << entries, next_id, mutex, waiting, signaled, shutdown,
                    prod_ops, pair_done, cons_entry, cons_done,
                    p_op, p_path, pp_id1, c_found, c_idx >>

s_setFlag ==
    /\ pc["shutdown_proc"] = "s_setFlag"
    /\ shutdown' = TRUE
    /\ pc' = [pc EXCEPT !["shutdown_proc"] = "s_acquire"]
    /\ UNCHANGED << entries, next_id, mutex, waiting, signaled,
                    prod_ops, pair_done, cons_entry, cons_done,
                    p_op, p_path, pp_id1, c_found, c_idx >>

s_acquire ==
    /\ pc["shutdown_proc"] = "s_acquire"
    /\ mutex = "free"
    /\ mutex' = "shutdown_proc"
    /\ pc' = [pc EXCEPT !["shutdown_proc"] = "s_broadcast"]
    /\ UNCHANGED << entries, next_id, waiting, signaled, shutdown,
                    prod_ops, pair_done, cons_entry, cons_done,
                    p_op, p_path, pp_id1, c_found, c_idx >>

s_broadcast ==
    /\ pc["shutdown_proc"] = "s_broadcast"
    /\ signaled' = signaled \cup waiting
    /\ waiting' = {}
    /\ pc' = [pc EXCEPT !["shutdown_proc"] = "s_release"]
    /\ UNCHANGED << entries, next_id, mutex, shutdown,
                    prod_ops, pair_done, cons_entry, cons_done,
                    p_op, p_path, pp_id1, c_found, c_idx >>

s_release ==
    /\ pc["shutdown_proc"] = "s_release"
    /\ mutex' = "free"
    /\ pc' = [pc EXCEPT !["shutdown_proc"] = "Done"]
    /\ UNCHANGED << entries, next_id, waiting, signaled, shutdown,
                    prod_ops, pair_done, cons_entry, cons_done,
                    p_op, p_path, pp_id1, c_found, c_idx >>

\* Complete next-state relation
Next == \/ \E self \in Producers:
             \/ p_loop(self) \/ p_choose(self) \/ p_acquire(self)
             \/ p_append(self) \/ p_signal(self) \/ p_release(self)
        \/ \E self \in PairProds:
             \/ pp_start(self) \/ pp_acquire(self) \/ pp_append1(self)
             \/ pp_append2(self) \/ pp_broadcast(self) \/ pp_release(self)
        \/ \E self \in Consumers:
             \/ c_loop(self) \/ c_acquire(self) \/ c_scan(self)
             \/ c_scan_loop(self) \/ c_check_found(self) \/ c_release_exit(self)
             \/ c_wait(self) \/ c_wait_signal(self) \/ c_reacquire(self)
             \/ c_rescan_release(self) \/ c_release_work(self) \/ c_doWork(self)
             \/ c_mark_acquire(self) \/ c_markCompleted(self)
             \/ c_mark_loop(self) \/ c_mark_release(self) \/ c_final(self)
        \/ s_wait \/ s_setFlag \/ s_acquire \/ s_broadcast \/ s_release

Spec == Init /\ [][Next]_vars /\ WF_vars(Next)

\* END TRANSLATION

----

\* =============================================================
\* INVARIANTS
\* =============================================================

\* Helper: check if a depends_on value is an actual dependency (not -1 sentinel)
HasDep(d) == d >= 0

(*
 * Inv4 -- NoDependencyViolation (safety):
 * A consumer never processes an entry whose dependency is incomplete.
 *)
NoDependencyViolation ==
    \A i \in 1..Len(entries):
        entries[i].in_flight /\ HasDep(entries[i].depends_on) =>
            \/ \E j \in 1..Len(entries):
                entries[j].id = entries[i].depends_on /\ entries[j].completed
            \/ ~\E j \in 1..Len(entries):
                entries[j].id = entries[i].depends_on

(*
 * Inv5 -- NoDoubleProcess (safety):
 * At most one consumer holds any given entry as in-flight.
 *)
NoDoubleProcess ==
    \A i, j \in Consumers:
        i /= j =>
            ~(cons_entry[i] >= 0 /\ cons_entry[i] = cons_entry[j])

(*
 * Inv6 -- CoalesceCorrectness (safety):
 * When an entry is auto-completed via coalescing, a newer put entry
 * for the same path exists in the log (it will carry the latest data).
 *)
CoalesceCorrectness ==
    \A i \in 1..Len(entries):
        entries[i].coalesced =>
            \E j \in (i+1)..Len(entries):
                entries[j].path = entries[i].path
                /\ entries[j].op = "put"

(*
 * Inv7 -- PairAtomicity (safety):
 * Both entries produced by enqueuePair are present.
 *)
PairAtomicity ==
    \A i \in 1..Len(entries):
        HasDep(entries[i].depends_on) =>
            \E j \in 1..Len(entries):
                entries[j].id = entries[i].depends_on

\* =============================================================
\* LIVENESS PROPERTIES
\* =============================================================

(*
 * Live3 -- ShutdownTermination:
 * After shutdown is set, all consumers eventually exit.
 *)
ShutdownTermination ==
    shutdown => <>(\A c \in Consumers: pc[c] = "Done")

====
