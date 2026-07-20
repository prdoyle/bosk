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
\* For INSERT/REPLACE events, updated gives the full new state for every path.
\* For DELETE events, updated is ignored.
EventType == {"insert", "replace", "update", "delete"}
Event == [ type : EventType, revision : 0..MaxRev, updated : StateFunc ]

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
    wrote,              \* [Bosk \times WriterID -> BOOLEAN]
    dbDeleted           \* BOOLEAN

vars == <<dbState, dbRevision, inMemory, pendingEvents,
          cursorOpen, formatType, flushSeen, wrote, dbDeleted>>

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
    /\ dbDeleted \in BOOLEAN

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
    /\ dbDeleted    = FALSE

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
\* for every bosk.
Write(b, w, t, v) ==
    /\ formatType[b] = "sequoia"
    /\ dbRevision < MaxRev
    /\ ~dbDeleted
    /\ t \in Path
    /\ v \in Value
    /\ v # dbState[t]       \* Only useful writes — skip no-ops
    /\ LET newRev   == dbRevision + 1
           newState == [dbState EXCEPT ![t] = v]
           event    == [ type |-> "update",
                          revision |-> newRev,
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
       /\ UNCHANGED <<inMemory, cursorOpen, formatType, flushSeen, dbDeleted>>

\* Flush: wait until all prior updates have been applied downstream.
\* Completes when flushSeen[b] >= dbRevision, meaning inMemory matches DB.
Flush(b, w) ==
    /\ formatType[b] = "sequoia"
    /\ flushSeen[b] >= dbRevision
    /\ UNCHANGED vars

(*************************************************************************)
\* DATABASE LIFECYCLE actions
(*************************************************************************)

\* Delete the entire database state (models external deletion or refurbish).
DeleteState ==
    /\ dbRevision < MaxRev
    /\ LET newRev == dbRevision + 1
           event  == [ type |-> "delete", revision |-> newRev,
                       updated |-> [p \in Path |-> NONE] ]
       IN
       /\ dbState'    = [p \in Path |-> NONE]
       /\ dbRevision' = newRev
       /\ dbDeleted'  = TRUE
       \* Queue DELETE event for all bosks
       /\ pendingEvents' = [b2 \in Bosk |->
            Append(pendingEvents[b2], event)]
       /\ UNCHANGED <<inMemory, cursorOpen, formatType, flushSeen, wrote>>

\* Reinitialize the database with a fresh state (models the completion
\* of a refurbish or external re-creation).
ReinitializeState ==
    /\ dbRevision < MaxRev
    /\ LET newRev == dbRevision + 1
       IN
       \E newState \in StateFunc :
       /\ \A p \in Path : newState[p] # NONE   \* No NONE values in live state
       /\ LET event == [ type |-> "replace", revision |-> newRev,
                         updated |-> newState ]
          IN
          /\ dbState'    = newState
          /\ dbRevision' = newRev
          /\ dbDeleted'  = FALSE
          \* Queue INSERT/REPLACE event for all bosks
          /\ pendingEvents' = [b2 \in Bosk |->
               Append(pendingEvents[b2], event)]
          /\ UNCHANGED <<inMemory, cursorOpen, formatType, flushSeen, wrote>>

(*************************************************************************)
\* CHANGE RECEIVER actions
(*************************************************************************)
\* Process the next queued change event for bosk b.
\* Behavior depends on event type:
\*   - INSERT/REPLACE: always apply full state (no shouldSkip check)
\*   - UPDATE: skip if revision <= flushSeen[b], else apply incremental updates
\*   - DELETE: reset flushSeen to 0 without changing inMemory
ProcessEvent(b) ==
    /\ cursorOpen[b]
    /\ pendingEvents[b] # << >>
    /\ LET event == Head(pendingEvents[b])
           rest  == Tail(pendingEvents[b])
       IN
       /\ pendingEvents' = [pendingEvents EXCEPT ![b] = rest]
       /\ CASE event.type = "delete" ->
               \* Reset flushSeen to 0; inMemory remains stale until next event
               /\ flushSeen' = [flushSeen EXCEPT ![b] = 0]
               /\ UNCHANGED inMemory
          [] event.type \in {"insert", "replace"} ->
               \* Full-document replacement: always apply, no shouldSkip
               /\ inMemory'  = [inMemory EXCEPT ![b] = event.updated]
               /\ flushSeen' = [flushSeen EXCEPT ![b] = event.revision]
          [] event.type = "update" ->
               LET skip == event.revision <= flushSeen[b]
               IN
               /\ IF skip
                  THEN UNCHANGED <<inMemory, flushSeen>>
                  ELSE ( inMemory'  = [inMemory EXCEPT ![b] = ApplyEvent(inMemory[b], event)]
                         /\ flushSeen' = [flushSeen EXCEPT ![b] = event.revision] )
       /\ UNCHANGED <<dbState, dbRevision, cursorOpen, formatType, wrote, dbDeleted>>

(*************************************************************************)
\* CONNECTION LIFECYCLE actions
(*************************************************************************)
\* Open cursor: reload in-memory state from the current DB.
\* Clear any stale events accumulated during disconnection;
\* the new cursor delivers only future events.
OpenCursor(b) ==
    /\ ~cursorOpen[b]
    /\ cursorOpen' = [cursorOpen EXCEPT ![b] = TRUE]
    /\ formatType' = [formatType EXCEPT ![b] = "sequoia"]
    /\ inMemory'  = [inMemory  EXCEPT ![b] = dbState]
    /\ flushSeen' = [flushSeen EXCEPT ![b] = dbRevision]
    /\ pendingEvents' = [pendingEvents EXCEPT ![b] = << >>]
    /\ UNCHANGED <<dbState, dbRevision, wrote, dbDeleted>>

\* Close cursor: disconnect due to network error or other failure.
CloseCursor(b) ==
    /\ cursorOpen[b]
    /\ cursorOpen' = [cursorOpen EXCEPT ![b] = FALSE]
    /\ formatType' = [formatType EXCEPT ![b] = "disconnected"]
    /\ UNCHANGED <<dbState, dbRevision, inMemory, pendingEvents, flushSeen, wrote, dbDeleted>>

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
    \/ DeleteState
    \/ ReinitializeState

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

\* 2. When the database exists and a connected bosk has drained its queue,
\*    its in-memory state must match the database state exactly.
\*    Excludes the deletion window (dbDeleted) where inMemory is intentionally stale
\*    until the next INSERT/REPLACE event arrives.
ConsistentWhenIdle ==
    \A b \in Bosk :
        (cursorOpen[b] /\ pendingEvents[b] = << >> /\ ~dbDeleted)
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
