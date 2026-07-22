---------------------------- MODULE BoskMongoSequoia ----------------------------
\* TLA+ specification for the bosk-mongo SEQUOIA format driver.
\*
\* Models the interaction between concurrent writers, a MongoDB collection,
\* and bosk service instances that subscribe via change streams.
\*
\*
\* BACKGROUND
\*
\* A Bosk instance ("bosk") keeps an in-memory copy of a shared state tree
\* stored in MongoDB. Writers submit changes by calling
\* BoskDriver.submitReplacement, which writes to MongoDB. MongoDB generates
\* a change-stream event for each write. The bosk receives these events via
\* an open cursor and replays them into its local copy.
\*
\* Before returning from BoskDriver.flush(), the bosk must ensure its local
\* state matches the database. It does this by tracking how many revisions
\* it has seen (flushSeen) and waiting until flushSeen >= dbRevision.
\*
\* Because dbRevision resets when the document is externally replaced,
\* the model also assigns an opaque "epoch" to each document generation
\* and tracks which generation each bosk's inMemory belongs to (flushEpoch).
\* Flush requires both flushSeen >= dbRevision and flushEpoch = epoch.
\*
\* In this model a finite set of TLA+ model values {u1, u2, u3} stands in
\* for UUIDs. Model values support only equality and inequality — no
\* ordering — which matches the properties of UUIDs.
\*
\*
\* SCENARIO
\*
\*   - Two Bosk instances (b1, b2), each with two writers (w1, w2)
\*   - Two state-tree paths (a1, a2) in a single MongoDB collection
\*   - External delete/recreate cycles interleaved with normal writes
\*   - Disconnect, reconnect, and event replay via change-stream cursors
\*
\* SAFETY PROPERTY
\*
\* The core invariant: when a connected bosk's event queue is empty,
\* its in-memory state must match the database state.
\* This captures "no lost updates" for the connected case.

EXTENDS Integers, Sequences, FiniteSets, TLC

(*************************************************************************)
\* CONSTANTS
\*
\* Defined in the .cfg file. TLC explores all states reachable from
\* any combination of these values. To keep the state space tractable,
\* we pick small finite sets that exercise the interesting interleavings.
(*************************************************************************)
CONSTANTS
    Bosk,           \* Bosk service instances      e.g. {"b1","b2"}
    WriterID,       \* Callers of submitReplacement e.g. {"w1","w2"}
    Path,           \* State tree paths             e.g. {"a1","a2"}
    Value,          \* State tree leaf values        e.g. {"v1","v2"}
    MaxRev,         \* Upper bound on the document revision counter
    u1, u2, u3      \* Model values for generation UUIDs

(*************************************************************************)
\* NONE and Vals
\*
\* NONE (the string "!NONE!") represents "no value" for a path.
\* It serves two purposes in this model:
\*
\*   1. Deleted state: after DeleteState, every path maps to NONE
\*      instead of a real Value.
\*
\*   2. Partial updates: a change-stream UPDATE event carries only
\*      the paths that actually changed. Unchanged paths have
\*      updated[p] = NONE, and ApplyEvent preserves the old value.
\*
\* Vals is the union of real leaf values and NONE, the full domain
\* of the StateFunc.
(*************************************************************************)
NONE == "!NONE!"
Vals == Value \cup {NONE}

StateFunc == [Path -> Vals]

\* EpochRange is the finite set of generation UUIDs available in this
\* model. A new epoch is assigned each time the document is externally
\* re-created (ReinitializeState). The bosk uses the epoch to tell
\* whether an event belongs to the current document generation.
\*
\* We use TLA+ model values (u1, u2, u3) rather than integers because
\* model values support only equality and inequality — no ordering,
\* no arithmetic.  This matches the properties of UUIDs in the Java
\* implementation.  Using ordered integers would over-constrain the
\* model: we would be implicitly relying on monotonicity, but a UUID
\* from a prior generation could theoretically reappear after a large
\* number of re-creations (the coupon-collector problem).  Model values
\* with equality-only comparisons avoid that assumption.
\*
\* Three model values are enough to exercise double-recreate cycles
\* (initial spawns 1, first recreate spawns 2, second recreate spawns 3).
EpochRange == {u1, u2, u3}

\* A change-stream event delivered to all connected bosks.
\*
\* The event.type maps directly to MongoDB change-stream event types:
\*   "insert":   a new document was created (full state in updated)
\*   "replace":  the document was replaced (full state in updated)
\*   "update":   partial update was applied (changed paths in updated;
\*               others are NONE, use ApplyEvent to merge)
\*   "delete":   document was removed (updated is ignored)
\*
\* The epoch field identifies which document generation this event
\* belongs to.  Events from an older generation can be discarded
\* when the bosk has already moved to a newer generation.
EventType == {"insert", "replace", "update", "delete"}
Event == [ type : EventType, revision : 0..MaxRev, updated : StateFunc, epoch : EpochRange ]

\* Symmetry reduction
\*
\* TLC normally distinguishes states that differ only by swapping
\* symmetric values.  Permutations(Bosk) tells TLC that b1 and b2
\* are interchangeable for the purposes of invariant checking:
\* a state where b1 has cursorOpen and b2 does not is equivalent
\* to one where b2 has the cursor and b1 does not.  This roughly
\* halves the state space.
Symmetry == Permutations(Bosk)

(*************************************************************************)
\* VARIABLES
\*
\* The model has one global "database" (dbState, dbRevision, epoch,
\* dbDeleted) that all bosks share.  Each bosk additionally maintains
\* its own view (inMemory, flushSeen, flushEpoch, cursorOpen) and an
\* event queue.
(*************************************************************************)
VARIABLES
    dbState,            \* The MongoDB document state: each path -> value
    dbRevision,         \* Document revision counter, bounded by MaxRev
    inMemory,           \* Per-bosk local copy of the state tree
    pendingEvents,      \* Per-bosk FIFO queue of change-stream events not yet processed
    cursorOpen,         \* Whether each bosk has an active change-stream cursor to MongoDB
    formatType,         \* "sequoia" when cursor is open, "disconnected" otherwise
    flushSeen,          \* Highest event revision each bosk has applied to inMemory
    epoch,              \* Current generation UUID assigned to the document
    flushEpoch,         \* Per-bosk: which generation its inMemory corresponds to
    wrote,              \* Whether each (bosk, writer) pair has ever submitted a write
    dbDeleted,          \* TRUE after DeleteState, FALSE otherwise
    used                \* All epochs ever assigned (monotonically growing)

vars == <<dbState, dbRevision, inMemory, pendingEvents,
          cursorOpen, formatType, flushSeen, epoch, flushEpoch, wrote,
          dbDeleted, used>>

(*************************************************************************)
\* Type invariant
\*
\* Checks that all variables stay in their declared domains.
\* This is the first line of defence against modelling errors:
\* if any action ever assigns a value outside these types, TLC
\* reports it as an invariant violation.
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
    /\ used \subseteq EpochRange

(*************************************************************************)
\* Initial state
\*
\* All paths start at the same arbitrary value.  All bosks are
\* disconnected.  The first epoch (u1) is assigned.
(*************************************************************************)
InitVal == CHOOSE v \in Value : TRUE

Init ==
    /\ dbState      = [p \in Path |-> InitVal]
    /\ dbRevision   = 0
    /\ inMemory     = [b \in Bosk |-> dbState]
    /\ pendingEvents = [b \in Bosk |-> << >>]
    /\ cursorOpen   = [b \in Bosk |-> FALSE]
    /\ formatType   = [b \in Bosk |-> "disconnected"]
    /\ flushSeen    = [b \in Bosk |-> 0]
    /\ epoch        = CHOOSE x \in EpochRange : TRUE   \* Deterministic: u1
    /\ flushEpoch   = [b \in Bosk |-> epoch]
    /\ wrote        = [b \in Bosk, w \in WriterID |-> FALSE]
    /\ dbDeleted    = FALSE
    /\ used         = {epoch}          \* u1 is now assigned

(*************************************************************************)
\* ApplyEvent
\*
\* Merges a partial UPDATE event into a state function.  For each path,
\* if event.updated[p] is not NONE, the new value replaces the old one;
\* otherwise the original value is preserved.
\*
\* INSERT/REPLACE events supply their own full state and do not need
\* ApplyEvent — they simply replace inMemory wholesale.
(*************************************************************************)
ApplyEvent(state, event) ==
    [p \in Path |->
        IF event.updated[p] = NONE THEN state[p] ELSE event.updated[p]]

(*************************************************************************)
\* WRITER ACTION: Write
\*
\* A writer calls submitReplacement to change one path in the document.
\* This models MongoDB updateOne with a match filter; the outcome
\* depends on whether the document exists.
\*
\* Two branches:
\*
\*   1. Document is deleted (dbDeleted):
\*      The write matches 0 documents and is silently lost.
\*      No state change occurs, and no event is generated.
\*      Only wrote is updated to record the attempt.
\*
\*   2. Document exists (~dbDeleted):
\*      The write succeeds.  dbState and dbRevision are updated,
\*      and an UPDATE event is queued for every bosk (including the
\*      one that wrote it).  The event carries the current epoch.
\*
\* A write is skipped entirely if the new value equals the old one
\* (v = dbState[t]).  The model also requires dbRevision < MaxRev,
\* an artificial bound to keep the state space finite.
Write(b, w, t, v) ==
    \/ ( /\ formatType[b] = "sequoia"
         /\ dbRevision < MaxRev
         /\ t \in Path
         /\ v \in Value
         /\ dbDeleted
         /\ wrote' = [wrote EXCEPT ![<<b, w>>] = TRUE]
         /\ UNCHANGED <<dbState, dbRevision, pendingEvents, inMemory, cursorOpen,
                       formatType, flushSeen, epoch, flushEpoch, dbDeleted,
                       used>> )
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
                          epoch, flushEpoch, dbDeleted, used>> )

\* Flush: the bosk declares that its in-memory state matches the database.
\*
\* Two branches:
\*
\*   1. Success: flushSeen >= dbRevision AND flushEpoch = epoch.
\*      The bosk is on the current document generation and has applied
\*      every event.  Flush succeeds (no-op in the model; in the code
\*      it signals the waiting writer).
\*
\*   2. Epoch mismatch: flushSeen >= dbRevision but flushEpoch # epoch.
\*      The database has been externally replaced (new generation) but
\*      the bosk's inMemory still holds stale state.  Instead of
\*      flushing stale state, the bosk disconnects.  On reconnect
\*      (OpenCursor) it will snapshot the current database state and
\*      resume under the new epoch.
\*
\*      In the code this maps to: FlushLock sees epoch mismatch and
\*      throws DisconnectedException; the writer retries after reconnect.
Flush(b, w) ==
    \/ ( /\ formatType[b] = "sequoia"
         /\ flushSeen[b] >= dbRevision
         /\ flushEpoch[b] = epoch
         /\ UNCHANGED vars )
    \/ ( /\ formatType[b] = "sequoia"
         /\ flushSeen[b] >= dbRevision
         /\ flushEpoch[b] # epoch
         /\ cursorOpen'  = [cursorOpen  EXCEPT ![b] = FALSE]
         /\ formatType'  = [formatType  EXCEPT ![b] = "disconnected"]
         /\ UNCHANGED <<dbState, dbRevision, inMemory, pendingEvents,
                        flushSeen, epoch, flushEpoch, wrote, dbDeleted,
                        used>> )

(*************************************************************************)
\* DATABASE LIFECYCLE actions
\*
\* These model external operations on the MongoDB collection, such as
\* a "refurbish" or "delete+recreate" by a companion process. They are
\* not initiated by any bosk or writer — they happen independently,
\* non-deterministically, whenever their preconditions hold.
(*************************************************************************)

\* Delete the entire document. All paths are set to NONE, the dbDeleted
\* flag is set, and a DELETE event is queued for every bosk.
\* The model bounds the revision counter at MaxRev, so this action is
\* disabled when dbRevision = MaxRev.
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
	              epoch, flushEpoch, used>>

\* Re-initialize the document with a fresh state and a new epoch.
\*
\* This models what happens after a delete+recreate: an external process
\* deletes the document, then creates a brand-new document with new
\* content. The revision counter resets to 1 (as MongoDB does after a
\* delete+insert), and a new epoch is assigned to distinguish this
\* document generation from the previous one.
\*
\* Preconditions:
\*   - The document must be deleted (dbDeleted)
\*   - We must be below the revision bound (dbRevision < MaxRev)
\*   - There must be an unused epoch available (used # EpochRange)
\*
\* A REPLACE event is generated with the full new state and the new
\* epoch. This event is queued for every bosk. When a bosk processes
\* it, it updates both inMemory and flushEpoch, bringing it into the
\* new generation.
ReinitializeState ==
    /\ dbDeleted    \* Only after the document has been deleted
    /\ dbRevision < MaxRev
    /\ used # EpochRange   \* At least one unused epoch remains
    /\ LET newRev == 1   \* Reset revision counter (models external replacement)
       IN
       \E newState \in StateFunc :
       /\ \A p \in Path : newState[p] # NONE   \* No NONE values in live state
       /\ newState # dbState                   \* Must actually change the state
       /\ LET newEpoch == CHOOSE x \in EpochRange : x \notin used
              event == [ type |-> "replace", revision |-> newRev,
                         updated |-> newState, epoch |-> newEpoch ]
          IN
          /\ dbState'    = newState
          /\ dbRevision' = newRev
          /\ dbDeleted'  = FALSE
          /\ epoch' = newEpoch
          /\ used' = used \cup {newEpoch}
          \* Queue INSERT/REPLACE event for all bosks
          /\ pendingEvents' = [b2 \in Bosk |->
               Append(pendingEvents[b2], event)]
          /\ UNCHANGED <<inMemory, cursorOpen, formatType, flushSeen,
                        flushEpoch, wrote>>

(*************************************************************************)
\* CHANGE RECEIVER actions
(*************************************************************************)
\* Process the next change-stream event for bosk b.
\*
\* Events are processed strictly in FIFO order (Head of the queue).
\* The behaviour depends on event type and, for INSERT/REPLACE and
\* UPDATE events, on whether the event's epoch matches the bosk's
\* current generation.
\*
\* Key design decision for INSERT/REPLACE events:
\*   These carry the COMPLETE document state. If the event's epoch
\*   matches the bosk's current flushEpoch, the bosk is on the same
\*   generation as the event and applies it normally (updating inMemory,
\*   flushSeen, and flushEpoch). If the epoch does NOT match, the
\*   database has been externally replaced: the bosk disconnects instead
\*   of applying the stale-cross-generation state. On reconnect
\*   (OpenCursor) it will snapshot the current database state.
\*   In the code this maps to: onEvent sees epoch mismatch and throws
\*   UnprocessableEventException; MainDriver catches it and disconnects.
\*
\* Key design decision for UPDATE events:
\*   These carry PARTIAL state (only changed paths). They are only
\*   applied if the event's epoch matches the bosk's flushEpoch.
\*   If the epoch does not match, the event is from a different
\*   document generation and its partial updates would corrupt the
\*   current inMemory. Events whose revision is already <= flushSeen
\*   are also skipped (already seen or superseded).
\*
\* DELETE events reset flushSeen to 0 (the document is gone) but leave
\* inMemory unchanged — the stale state persists until reconnect.
ProcessEvent(b) ==
    /\ cursorOpen[b]
    /\ pendingEvents[b] # << >>
    /\ LET event == Head(pendingEvents[b])
           rest  == Tail(pendingEvents[b])
       IN
       \* DELETE: document gone. Reset flushSeen to 0; inMemory stays stale.
       \/ ( /\ event.type = "delete"
            /\ pendingEvents' = [pendingEvents EXCEPT ![b] = rest]
            /\ flushSeen' = [flushSeen EXCEPT ![b] = 0]
            /\ UNCHANGED <<inMemory, dbState, dbRevision, cursorOpen, formatType,
                          wrote, dbDeleted, epoch, flushEpoch, used>> )
        \* INSERT/REPLACE: matching epoch — apply normally.
        \/ ( /\ event.type \in {"insert", "replace"}
             /\ event.epoch = flushEpoch[b]
             /\ pendingEvents' = [pendingEvents EXCEPT ![b] = rest]
             /\ inMemory'  = [inMemory EXCEPT ![b] = event.updated]
             /\ flushSeen' = [flushSeen EXCEPT ![b] = event.revision]
             /\ flushEpoch' = [flushEpoch EXCEPT ![b] = event.epoch]
             /\ UNCHANGED <<dbState, dbRevision, cursorOpen, formatType,
                           wrote, dbDeleted, epoch, used>> )
        \* INSERT/REPLACE: different epoch — disconnect.
        \/ ( /\ event.type \in {"insert", "replace"}
             /\ event.epoch # flushEpoch[b]
             /\ pendingEvents' = [pendingEvents EXCEPT ![b] = rest]
             /\ cursorOpen'  = [cursorOpen  EXCEPT ![b] = FALSE]
             /\ formatType'  = [formatType  EXCEPT ![b] = "disconnected"]
             /\ UNCHANGED <<dbState, dbRevision, inMemory, flushSeen, epoch,
                           flushEpoch, wrote, dbDeleted, used>> )
       \* UPDATE: skip if already seen or from a different generation.
       \/ ( /\ event.type = "update"
            /\ (event.revision <= flushSeen[b] \/ event.epoch # flushEpoch[b])
            /\ pendingEvents' = [pendingEvents EXCEPT ![b] = rest]
            /\ UNCHANGED <<inMemory, flushSeen, dbState, dbRevision, cursorOpen,
                          formatType, wrote, dbDeleted, epoch, flushEpoch,
                          used>> )
       \* UPDATE: apply if new revision and matching epoch.
       \/ ( /\ event.type = "update"
            /\ event.revision > flushSeen[b]
            /\ event.epoch = flushEpoch[b]
            /\ pendingEvents' = [pendingEvents EXCEPT ![b] = rest]
            /\ inMemory'  = [inMemory EXCEPT ![b] = ApplyEvent(inMemory[b], event)]
            /\ flushSeen' = [flushSeen EXCEPT ![b] = event.revision]
            /\ UNCHANGED <<dbState, dbRevision, cursorOpen, formatType,
                          wrote, dbDeleted, epoch, flushEpoch, used>> )

(*************************************************************************)
\* CONNECTION LIFECYCLE actions
(*************************************************************************)
\* Open a change-stream cursor: the bosk connects to MongoDB and starts
\* receiving events.
\*
\* On connect, the bosk snapshots the current database state into
\* inMemory and resyncs flushSeen/flushEpoch to match. Any events that
\* accumulated in the queue during disconnection remain there — they
\* will be processed (or skipped) when the bosk runs ProcessEvent,
\* modelling resume-token replay.
\*
\* A bosk cannot open a cursor when the document is deleted (dbDeleted).
\* In that state there is nothing to connect to.
OpenCursor(b) ==
    /\ ~cursorOpen[b]
    /\ ~dbDeleted                    \* Can't connect to a deleted document
    /\ cursorOpen' = [cursorOpen EXCEPT ![b] = TRUE]
    /\ formatType' = [formatType EXCEPT ![b] = "sequoia"]
    /\ inMemory'  = [inMemory  EXCEPT ![b] = dbState]
    /\ flushSeen' = [flushSeen EXCEPT ![b] = dbRevision]
    /\ flushEpoch' = [flushEpoch EXCEPT ![b] = epoch]
    /\ UNCHANGED <<dbState, dbRevision, pendingEvents, epoch, wrote, dbDeleted, used>>

\* Close cursor: disconnect. The bosk stops receiving events, but its
\* inMemory and queue are preserved for when it reconnects.
CloseCursor(b) ==
    /\ cursorOpen[b]
    /\ cursorOpen' = [cursorOpen EXCEPT ![b] = FALSE]
    /\ formatType' = [formatType EXCEPT ![b] = "disconnected"]
	/\ UNCHANGED <<dbState, dbRevision, inMemory, pendingEvents, flushSeen, wrote,
	              dbDeleted, epoch, flushEpoch, used>>

(*************************************************************************)
\* Next-state relation
\*
\* At each step, exactly one action fires (and stuttering steps where
\* nothing changes are also allowed). The actions are:
\*   - Writer actions (AnyWrite, AnyFlush): initiated by bosks' writers
\*   - Change receiver (AnyProcess): a bosk processes a queued event
\*   - Connection lifecycle (AnyOpen, AnyClose): connect/disconnect
\*   - Database lifecycle (DeleteState, ReinitializeState): external ops
\*
\* Without fairness (Spec), we check safety invariants under all
\* possible interleavings. With fairness (SpecFair), we also verify
\* liveness properties.
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
\*
\* These are the properties that must hold in EVERY reachable state.
\* TLC explores the full state space and reports any violation.
(*************************************************************************)

\* 1. ConsistentWhenIdle
\*    When a connected bosk has drained its event queue (every event
\*    received has been processed) AND the database document exists
\*    (~dbDeleted), the bosk's in-memory state must exactly match the
\*    database state.
\*
\*    This is the fundamental safety property: no lost updates. If it
\*    held in the connected-idle case, Flush would succeed immediately.
\*    The dbDeleted exclusion is necessary because during the deletion
\*    window, inMemory is intentionally stale (we keep the old state
\*    until a fresh INSERT/REPLACE event arrives).
ConsistentWhenIdle ==
    \A b \in Bosk :
        (cursorOpen[b] /\ pendingEvents[b] = << >> /\ ~dbDeleted)
            => inMemory[b] = dbState

\* 2. FormatConsistent
\*    cursorOpen and formatType must always agree: an open cursor means
\*    "sequoia" format and vice versa. This is a modelling consistency
\*    check — in the real code these are two facets of the same thing.
FormatConsistent ==
    \A b \in Bosk :
        cursorOpen[b] <=> formatType[b] = "sequoia"

\* 3. FlushConsistent
\*    When a connected bosk's flushSeen is at or ahead of the database
\*    revision AND its flushEpoch matches the current epoch AND the
\*    document exists, the bosk's in-memory state must match the
\*    database state.
\*
\*    This is a stronger invariant than ConsistentWhenIdle: it applies
\*    even when the event queue is NOT empty (because Flush only checks
\*    flushSeen and flushEpoch, not the queue). If this holds, then
\*    Flush(b, w) never succeeds when inMemory is stale.
\*
\*    The epoch check (flushEpoch[b] = epoch) prevents the "epoch
\*    problem": after an external replacement resets the revision
\*    counter, flushSeen might be >= the new dbRevision even though
\*    inMemory holds stale data from the old generation. When
\*    flushEpoch[b] # epoch, Flush disconnects instead of succeeding,
\*    preventing stale state from being flushed downstream.
\*    On reconnect, OpenCursor snapshots the current database state
\*    and sets flushEpoch[b] = epoch, restoring consistency.
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
\*
\* Under fair scheduling: whenever a connected bosk's queue is empty,
\* the in-memory state matches the database state.
\*
\* The fairness assumptions are:
\*   - Each bosk eventually processes its events (SF — strong fairness:
\*     if ProcessEvent is enabled infinitely often, it eventually fires).
\*   - Each bosk eventually flushes (WF — weak fairness: if Flush is
\*     continuously enabled, it eventually fires).
\*
\* NOTE: This liveness property is checked with SpecFair. Our model
\* has a known deadlock (all epochs consumed, MaxRev reached, no active
\* cursors) which violates liveness under fairness. The Gradle task uses
\* Spec (no fairness) to avoid this; the liveness specification here is
\* for manual checking with a trimmed model.
ConnectedConsistency ==
    \A b \in Bosk :
        []( (cursorOpen[b] /\ pendingEvents[b] = << >>)
            => (inMemory[b] = dbState) )

=============================================================================
