---------------------------- MODULE BoskMongoSequoia ----------------------------
\* TLA+ specification for the bosk-mongo driver's SEQUOIA format.
\*
\* Models:
\*   - Concurrent writers submitting state updates via submitReplacement
\*   - MongoDB change stream event generation and processing
\*   - Disconnect/reconnect lifecycle
\*   - Flush synchronization
\*
\* Key safety property: when a connected bosk's event queue is empty,
\* its in-memory state must match the database state.
\* This captures "no lost updates" for the connected case.

EXTENDS Integers, Sequences, FiniteSets, TLC

(*************************************************************************)
\* CONSTANTS — set in the .cfg file
(*************************************************************************)
CONSTANTS
    Bosk,           \* Set of bosk instance IDs     e.g. {"b1","b2"}
    WriterID,       \* Set of writer IDs            e.g. {"w1","w2"}
    Path,           \* State tree paths             e.g. {"a1","a2","b1"}
    Value,          \* Possible leaf values          e.g. {"v1","v2"}
    MaxRev          \* Upper bound on revision (for TLC)

(*************************************************************************)
\* Derived values
(*************************************************************************)
NONE == "!NONE!"
Vals == Value \cup {NONE}

\* The DB state function: each path maps to a value
StateFunc == [Path -> Vals]

\* A change stream event records which paths changed.
\* updated[p] = NONE means path p did not change in this event.
Event == [ revision : 0..MaxRev, updated : StateFunc ]

(*************************************************************************)
\* VARIABLES
(*************************************************************************)
VARIABLES
    dbState,            \* StateFunc
    dbRevision,         \* 0..MaxRev
    inMemory,           \* [Bosk -> StateFunc]
    pendingEvents,      \* [Bosk -> Seq(Event)]
    cursorOpen,         \* [Bosk -> BOOLEAN]
    formatType,         \* [Bosk -> {"sequoia","disconnected"}]
    flushSeen,          \* [Bosk -> 0..MaxRev]
    wrote               \* [Bosk \times WriterID -> BOOLEAN]

vars == <<dbState, dbRevision, inMemory, pendingEvents,
          cursorOpen, formatType, flushSeen, wrote>>

(*************************************************************************)
\* Type invariant
(*************************************************************************)
TypeOK ==
    /\ dbState \in StateFunc
    /\ dbRevision \in 0..MaxRev
    /\ inMemory \in [Bosk -> StateFunc]
    /\ pendingEvents \in [Bosk -> Seq(Event)]
    /\ cursorOpen \in [Bosk -> BOOLEAN]
    /\ formatType \in [Bosk -> {"sequoia", "disconnected"}]
    /\ flushSeen \in [Bosk -> 0..MaxRev]
    /\ wrote \in [Bosk \times WriterID -> BOOLEAN]

(*************************************************************************)
\* Initial state
(*************************************************************************)
InitVal == CHOOSE v \in Value : TRUE

Init ==
    \* Database starts with all paths set to the chosen initial value
    /\ dbState      = [p \in Path |-> InitVal]
    /\ dbRevision   = 0
    \* All bosks start with a disconnected copy of initial DB
    /\ inMemory     = [b \in Bosk |-> dbState]
    /\ pendingEvents = [b \in Bosk |-> << >>]
    /\ cursorOpen   = [b \in Bosk |-> FALSE]
    /\ formatType   = [b \in Bosk |-> "disconnected"]
    /\ flushSeen    = [b \in Bosk |-> 0]
    /\ wrote        = [b \in Bosk, w \in WriterID |-> FALSE]

(*************************************************************************)
\* Helper: apply event updates to a state
(*************************************************************************)
ApplyEvent(state, event) ==
    [p \in Path |->
        IF event.updated[p] = NONE THEN state[p] ELSE event.updated[p]]

(*************************************************************************)
\* WRITER actions
(*************************************************************************)
\* A writer writes a new value to a path in the DB.
\* Only enabled if the value actually changes (to avoid no-op events).
\* Updates dbState, increments dbRevision, and queues a change event
\* for every bosk whose change stream cursor is currently open.
Write(b, w, t, v) ==
    /\ formatType[b] = "sequoia"
    /\ dbRevision < MaxRev
    /\ t \in Path
    /\ v \in Value
    /\ v # dbState[t]       \* Only useful writes — skip no-ops
    /\ LET newRev   == dbRevision + 1
           newState == [dbState EXCEPT ![t] = v]
           event    == [ revision |-> newRev,
                         updated  |-> [p \in Path |->
                             IF p = t THEN v ELSE NONE] ]
       IN
       /\ dbState' = newState
       /\ dbRevision' = newRev
       \* Queue event for all connected bosks (including the writer's own)
       /\ pendingEvents' = [b2 \in Bosk |->
            IF cursorOpen[b2]
            THEN Append(pendingEvents[b2], event)
            ELSE pendingEvents[b2]]
       \* Record that this writer has written
       /\ wrote' = [wrote EXCEPT ![<<b, w>>] = TRUE]
       /\ UNCHANGED <<inMemory, cursorOpen, formatType, flushSeen>>

\* Flush: wait until all prior updates have been applied downstream.
\* Completes when flushSeen[b] >= dbRevision, meaning inMemory matches DB.
Flush(b, w) ==
    /\ formatType[b] = "sequoia"
    /\ flushSeen[b] >= dbRevision
    /\ UNCHANGED vars

(*************************************************************************)
\* CHANGE RECEIVER actions
(*************************************************************************)
\* Process the next queued change event for bosk b.
\* If the event's revision <= flushSeen[b], skip it — it's a stale event
\* left over from before a prior reconnect. Otherwise apply the updates
\* and advance flushSeen.
ProcessEvent(b) ==
    /\ cursorOpen[b]
    /\ pendingEvents[b] # << >>
    /\ LET event == Head(pendingEvents[b])
           rest  == Tail(pendingEvents[b])
           skip  == event.revision <= flushSeen[b]
       IN
       /\ pendingEvents' = [pendingEvents EXCEPT ![b] = rest]
       /\ IF skip
          THEN UNCHANGED <<inMemory, flushSeen>>
          ELSE ( inMemory'  = [inMemory  EXCEPT ![b] = ApplyEvent(inMemory[b], event)]
                 /\ flushSeen' = [flushSeen EXCEPT ![b] = event.revision] )
       /\ UNCHANGED <<dbState, dbRevision, cursorOpen, formatType, wrote>>

(*************************************************************************)
\* CONNECTION LIFECYCLE actions
(*************************************************************************)
\* Open cursor: reload in-memory state from the current DB.
OpenCursor(b) ==
    /\ ~cursorOpen[b]
    /\ cursorOpen' = [cursorOpen EXCEPT ![b] = TRUE]
    /\ formatType' = [formatType EXCEPT ![b] = "sequoia"]
    /\ inMemory'  = [inMemory  EXCEPT ![b] = dbState]
    /\ flushSeen' = [flushSeen EXCEPT ![b] = dbRevision]
    /\ UNCHANGED <<dbState, dbRevision, pendingEvents, wrote>>

\* Close cursor: disconnect due to network error or other failure.
CloseCursor(b) ==
    /\ cursorOpen[b]
    /\ cursorOpen' = [cursorOpen EXCEPT ![b] = FALSE]
    /\ formatType' = [formatType EXCEPT ![b] = "disconnected"]
    /\ UNCHANGED <<dbState, dbRevision, inMemory, pendingEvents, flushSeen, wrote>>

(*************************************************************************)
\* Next-state relation
(*************************************************************************)
AnyWrite ==
    \E b \in Bosk, w \in WriterID, t \in Path, v \in Value :
        Write(b, w, t, v)

AnyFlush ==
    \E b \in Bosk, w \in WriterID :
        Flush(b, w)

AnyProcess ==
    \E b \in Bosk :
        ProcessEvent(b)

AnyOpen ==
    \E b \in Bosk :
        OpenCursor(b)

AnyClose ==
    \E b \in Bosk :
        CloseCursor(b)

Next ==
    \/ AnyWrite
    \/ AnyFlush
    \/ AnyProcess
    \/ AnyOpen
    \/ AnyClose

(*************************************************************************)
\* Specification
(*************************************************************************)
\* Without fairness: check safety invariants under all interleavings
Spec == Init /\ [][Next]_vars

\* With fairness: also check liveness (eventual processing)
Fairness ==
    /\ \A b \in Bosk : SF_vars(ProcessEvent(b))
    /\ \A b \in Bosk : WF_vars(AnyFlush)

SpecFair == Init /\ [][Next]_vars /\ Fairness

(*************************************************************************)
\* Safety invariants (checked by TLC)
(*************************************************************************)
\* 1. All pending event revisions are <= current DB revision
PendingRevsOK ==
    \A b \in Bosk :
        \A i \in 1..Len(pendingEvents[b]) :
            pendingEvents[b][i].revision <= dbRevision

\* 2. When a connected bosk has drained its queue, its in-memory state
\*    must match the database state exactly.
ConsistentWhenIdle ==
    \A b \in Bosk :
        (cursorOpen[b] /\ pendingEvents[b] = << >>)
            => inMemory[b] = dbState

\* 3. cursorOpen and formatType are consistent
FormatConsistent ==
    \A b \in Bosk :
        cursorOpen[b] <=> formatType[b] = "sequoia"

\* 4. flushSeen never exceeds dbRevision
FlushSeenOK ==
    \A b \in Bosk :
        flushSeen[b] <= dbRevision

Invariants ==
    /\ TypeOK
    /\ PendingRevsOK
    /\ ConsistentWhenIdle
    /\ FormatConsistent
    /\ FlushSeenOK

(*************************************************************************)
\* Liveness (temporal property)
(*************************************************************************)
\* Under fairness: whenever a connected bosk's queue is empty,
\* in-memory state matches the DB.
ConnectedConsistency ==
    \A b \in Bosk :
        []( (cursorOpen[b] /\ pendingEvents[b] = << >>)
            => (inMemory[b] = dbState) )

=============================================================================
