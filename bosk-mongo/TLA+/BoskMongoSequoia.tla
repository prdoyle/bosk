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

\* Epoch is a UUID that changes whenever the document is externally replaced.
\* This prevents the "epoch problem": flushSeen matching the new dbRevision
\* but belonging to a different epoch with different state.
\*
\* NOTE: In the TLA+ model we use a monotonically increasing counter for
\* state-space tractability. In the Java implementation this MUST be a UUID
\* (e.g. java.util.UUID or BsonString), NOT a counter — the counter could
\* wrap around (as it is necessarily finite here) and re-introduce the same
\* problem we're solving for the revision field. A UUID is globally unique
\* and never repeats across unrelated re-creation events.
EpochRange == 1..3   \* Enough for double-recreate (epochs 1→2→3)

\* A change stream event records which paths changed.
\* updated[p] = NONE means path p did not change in this event.
\* For INSERT/REPLACE events, updated gives the full new state for every path.
\* For DELETE events, updated is ignored.
EventType == {"insert", "replace", "update", "delete"}
Event == [ type : EventType, revision : 0..MaxRev, updated : StateFunc, epoch : EpochRange ]

\* TLC symmetry reduction: bosk elements are interchangeable
Symmetry == Permutations(Bosk)

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
    epoch,              \* Global epoch UUID
    flushEpoch,         \* [Bosk -> 0..MaxRev] — which epoch flushSeen belongs to
    wrote,              \* [Bosk \times WriterID -> BOOLEAN]
    dbDeleted           \* BOOLEAN

vars == <<dbState, dbRevision, inMemory, pendingEvents,
          cursorOpen, formatType, flushSeen, epoch, flushEpoch, wrote, dbDeleted>>

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
    /\ epoch \in EpochRange
    /\ flushEpoch \in [Bosk -> EpochRange]
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
    /\ epoch        = 1               \* Initial epoch
    /\ flushEpoch   = [b \in Bosk |-> 1]
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
\* If dbDeleted, the write goes to MongoDB but matches 0 documents
\* (matchedCount=0) and is silently lost — no state change, no event.
Write(b, w, t, v) ==
    \/ ( /\ formatType[b] = "sequoia"
         /\ dbRevision < MaxRev
         /\ t \in Path
         /\ v \in Value
         /\ dbDeleted
         /\ wrote' = [wrote EXCEPT ![<<b, w>>] = TRUE]
         /\ UNCHANGED <<dbState, dbRevision, pendingEvents, inMemory, cursorOpen,
                       formatType, flushSeen, epoch, flushEpoch, dbDeleted>> )
    \/ ( /\ formatType[b] = "sequoia"
         /\ dbRevision < MaxRev
         /\ t \in Path
         /\ v \in Value
         /\ ~dbDeleted
         /\ v # dbState[t]       \* Only useful writes — skip no-ops
         /\ LET newRev   == dbRevision + 1
                newState == [dbState EXCEPT ![t] = v]
                event    == [ type |-> "update",
                               revision |-> newRev,
                               updated  |-> [p \in Path |->
                                   IF p = t THEN v ELSE NONE],
                               epoch |-> epoch ]
            IN
            /\ dbState' = newState
            /\ dbRevision' = newRev
            \* Queue event for all bosks (models cursor delivery from resume token)
            /\ pendingEvents' = [b2 \in Bosk |->
                 Append(pendingEvents[b2], event)]
            /\ wrote' = [wrote EXCEPT ![<<b, w>>] = TRUE]
            /\ UNCHANGED <<inMemory, cursorOpen, formatType, flushSeen,
                          epoch, flushEpoch, dbDeleted>> )

\* Flush: wait until all prior updates have been applied downstream.
\* Completes when flushSeen[b] >= dbRevision and the epoch matches,
\* meaning inMemory matches the current DB state and epoch.
Flush(b, w) ==
    /\ formatType[b] = "sequoia"
    /\ flushSeen[b] >= dbRevision
    /\ flushEpoch[b] = epoch
    /\ UNCHANGED vars

(*************************************************************************)
\* DATABASE LIFECYCLE actions
(*************************************************************************)

\* Delete the entire database state (models external deletion or refurbish).
DeleteState ==
    /\ dbRevision < MaxRev
    /\ LET newRev == dbRevision + 1
            event  == [ type |-> "delete", revision |-> newRev,
                        updated |-> [p \in Path |-> NONE],
                        epoch |-> epoch ]
       IN
       /\ dbState'    = [p \in Path |-> NONE]
       /\ dbRevision' = newRev
       /\ dbDeleted'  = TRUE
       \* Queue DELETE event for all bosks
       /\ pendingEvents' = [b2 \in Bosk |->
            Append(pendingEvents[b2], event)]
	/\ UNCHANGED <<inMemory, cursorOpen, formatType, flushSeen, wrote,
	              epoch, flushEpoch>>

\* Reinitialize the database with a fresh state (models an external replacement,
\* e.g. after a delete+recreate cycle, that resets the revision counter and
\* assigns a new epoch UUID).
ReinitializeState ==
    /\ dbDeleted    \* Only after the document has been deleted
    /\ dbRevision < MaxRev
    /\ epoch + 1 \in EpochRange   \* Ensure epoch' stays in range
    /\ LET newRev == 1   \* Reset revision counter (models external replacement)
       IN
       \E newState \in StateFunc :
       /\ \A p \in Path : newState[p] # NONE   \* No NONE values in live state
       /\ newState # dbState                   \* Must actually change the state
       /\ LET newEpoch == epoch + 1
              event == [ type |-> "replace", revision |-> newRev,
                         updated |-> newState, epoch |-> newEpoch ]
          IN
          /\ dbState'    = newState
          /\ dbRevision' = newRev
          /\ dbDeleted'  = FALSE
          /\ epoch' = newEpoch   \* Monotonically increasing (UUID in practice)
          \* Queue INSERT/REPLACE event for all bosks
          /\ pendingEvents' = [b2 \in Bosk |->
               Append(pendingEvents[b2], event)]
          /\ UNCHANGED <<inMemory, cursorOpen, formatType, flushSeen,
                        flushEpoch, wrote>>

(*************************************************************************)
\* CHANGE RECEIVER actions
(*************************************************************************)
\* Process the next queued change event for bosk b.
\* Behavior depends on event type:
\*   - INSERT/REPLACE: apply if epoch >= flushEpoch[b], else skip
\*   - UPDATE: skip if revision <= flushSeen[b] or epoch < flushEpoch[b]
\*   - DELETE: reset flushSeen to 0 without changing inMemory
ProcessEvent(b) ==
    /\ cursorOpen[b]
    /\ pendingEvents[b] # << >>
    /\ LET event == Head(pendingEvents[b])
           rest  == Tail(pendingEvents[b])
       IN
       \* DELETE event: reset flushSeen to 0; inMemory remains stale until next event
       \/ ( /\ event.type = "delete"
            /\ pendingEvents' = [pendingEvents EXCEPT ![b] = rest]
            /\ flushSeen' = [flushSeen EXCEPT ![b] = 0]
            /\ UNCHANGED <<inMemory, dbState, dbRevision, cursorOpen, formatType,
                          wrote, dbDeleted, epoch, flushEpoch>> )
        \* INSERT/REPLACE event: apply only if from current or later epoch
        \/ ( /\ event.type \in {"insert", "replace"}
             /\ event.epoch >= flushEpoch[b]
             /\ pendingEvents' = [pendingEvents EXCEPT ![b] = rest]
             /\ inMemory'  = [inMemory EXCEPT ![b] = event.updated]
             /\ flushSeen' = [flushSeen EXCEPT ![b] = event.revision]
             /\ UNCHANGED <<dbState, dbRevision, cursorOpen, formatType,
                           wrote, dbDeleted, epoch, flushEpoch>> )
       \* INSERT/REPLACE event: skip if from a prior epoch
       \/ ( /\ event.type \in {"insert", "replace"}
            /\ event.epoch < flushEpoch[b]
            /\ pendingEvents' = [pendingEvents EXCEPT ![b] = rest]
            /\ UNCHANGED <<inMemory, flushSeen, dbState, dbRevision, cursorOpen,
                          formatType, wrote, dbDeleted, epoch, flushEpoch>> )
       \* UPDATE event: skip if revision <= flushSeen[b] or epoch < flushEpoch[b]
       \/ ( /\ event.type = "update"
            /\ (event.revision <= flushSeen[b] \/ event.epoch < flushEpoch[b])
            /\ pendingEvents' = [pendingEvents EXCEPT ![b] = rest]
            /\ UNCHANGED <<inMemory, flushSeen, dbState, dbRevision, cursorOpen,
                          formatType, wrote, dbDeleted, epoch, flushEpoch>> )
       \* UPDATE event: apply if revision > flushSeen[b] and epoch >= flushEpoch[b]
       \/ ( /\ event.type = "update"
            /\ event.revision > flushSeen[b]
            /\ event.epoch >= flushEpoch[b]
            /\ pendingEvents' = [pendingEvents EXCEPT ![b] = rest]
            /\ inMemory'  = [inMemory EXCEPT ![b] = ApplyEvent(inMemory[b], event)]
            /\ flushSeen' = [flushSeen EXCEPT ![b] = event.revision]
            /\ UNCHANGED <<dbState, dbRevision, cursorOpen, formatType,
                          wrote, dbDeleted, epoch, flushEpoch>> )

(*************************************************************************)
\* CONNECTION LIFECYCLE actions
(*************************************************************************)
\* Open cursor: reload in-memory state from the current DB.
\* Stale events accumulated during disconnection remain in the queue
\* and are either skipped (UPDATE with revision <= flushSeen) or
\* applied (INSERT/REPLACE), modeling cursor resume-token replay.
\* Also resyncs flushEpoch to the current DB epoch (models reconnect).
OpenCursor(b) ==
    /\ ~cursorOpen[b]
    /\ ~dbDeleted                    \* Can't load state from a deleted document
    /\ cursorOpen' = [cursorOpen EXCEPT ![b] = TRUE]
    /\ formatType' = [formatType EXCEPT ![b] = "sequoia"]
    /\ inMemory'  = [inMemory  EXCEPT ![b] = dbState]
    /\ flushSeen' = [flushSeen EXCEPT ![b] = dbRevision]
    /\ flushEpoch' = [flushEpoch EXCEPT ![b] = epoch]
    /\ UNCHANGED <<dbState, dbRevision, pendingEvents, epoch, wrote, dbDeleted>>

\* Close cursor: disconnect due to network error or other failure.
CloseCursor(b) ==
    /\ cursorOpen[b]
    /\ cursorOpen' = [cursorOpen EXCEPT ![b] = FALSE]
    /\ formatType' = [formatType EXCEPT ![b] = "disconnected"]
	/\ UNCHANGED <<dbState, dbRevision, inMemory, pendingEvents, flushSeen, wrote,
	              dbDeleted, epoch, flushEpoch>>

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
\* 1. When the database exists and a connected bosk has drained its queue,
\*    its in-memory state must match the database state exactly.
\*    Excludes the deletion window (dbDeleted) where inMemory is intentionally stale
\*    until the next INSERT/REPLACE event arrives.
ConsistentWhenIdle ==
    \A b \in Bosk :
        (cursorOpen[b] /\ pendingEvents[b] = << >> /\ ~dbDeleted)
            => inMemory[b] = dbState

\* 2. cursorOpen and formatType are consistent
FormatConsistent ==
    \A b \in Bosk :
        cursorOpen[b] <=> formatType[b] = "sequoia"

\* 3. When a connected bosk's flushSeen is at or ahead of dbRevision,
\*    the flushEpoch matches the current epoch, and the database exists,
\*    inMemory must match dbState.
\*    The epoch check prevents the "epoch problem": after an external
\*    replacement resets the revision counter, flushSeen may be >= dbRevision
\*    but belong to a different epoch with different state.
FlushConsistent ==
    \A b \in Bosk :
        (cursorOpen[b] /\ flushSeen[b] >= dbRevision /\ flushEpoch[b] = epoch /\ ~dbDeleted)
            => inMemory[b] = dbState

Invariants ==
    /\ TypeOK
    /\ ConsistentWhenIdle
    /\ FormatConsistent
    /\ FlushConsistent

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
