---- MODULE BoskMongo_TTrace_1784568752 ----
EXTENDS Sequences, TLCExt, BoskMongo, Toolbox, Naturals, TLC, BoskMongo_TEConstants

_expression ==
    LET BoskMongo_TEExpression == INSTANCE BoskMongo_TEExpression
    IN BoskMongo_TEExpression!expression
----

_trace ==
    LET BoskMongo_TETrace == INSTANCE BoskMongo_TETrace
    IN BoskMongo_TETrace!trace
----

_inv ==
    ~(
        TLCGet("level") = Len(_TETrace)
        /\
        dbState = ([b1 |-> "!NONE!", a1 |-> "!NONE!", a2 |-> "!NONE!"])
        /\
        flushSeen = ((b1 :> 0 @@ b2 :> 0))
        /\
        cursorOpen = ((b1 :> TRUE @@ b2 :> FALSE))
        /\
        inMemory = ((b1 :> [b1 |-> "v1", a1 |-> "v1", a2 |-> "v1"] @@ b2 :> [b1 |-> "v1", a1 |-> "v1", a2 |-> "v1"]))
        /\
        wrote = ((<<b1, w1>> :> FALSE @@ <<b1, w2>> :> FALSE @@ <<b2, w1>> :> FALSE @@ <<b2, w2>> :> FALSE))
        /\
        dbRevision = (1)
        /\
        dbDeleted = (TRUE)
        /\
        formatType = ((b1 :> "sequoia" @@ b2 :> "disconnected"))
        /\
        pendingEvents = ((b1 :> <<>> @@ b2 :> <<[type |-> "delete", revision |-> 1, updated |-> [b1 |-> "!NONE!", a1 |-> "!NONE!", a2 |-> "!NONE!"]]>>))
    )
----

_init ==
    /\ wrote = _TETrace[1].wrote
    /\ dbState = _TETrace[1].dbState
    /\ cursorOpen = _TETrace[1].cursorOpen
    /\ formatType = _TETrace[1].formatType
    /\ dbDeleted = _TETrace[1].dbDeleted
    /\ flushSeen = _TETrace[1].flushSeen
    /\ pendingEvents = _TETrace[1].pendingEvents
    /\ dbRevision = _TETrace[1].dbRevision
    /\ inMemory = _TETrace[1].inMemory
----

_next ==
    /\ \E i,j \in DOMAIN _TETrace:
        /\ \/ /\ j = i + 1
              /\ i = TLCGet("level")
        /\ wrote  = _TETrace[i].wrote
        /\ wrote' = _TETrace[j].wrote
        /\ dbState  = _TETrace[i].dbState
        /\ dbState' = _TETrace[j].dbState
        /\ cursorOpen  = _TETrace[i].cursorOpen
        /\ cursorOpen' = _TETrace[j].cursorOpen
        /\ formatType  = _TETrace[i].formatType
        /\ formatType' = _TETrace[j].formatType
        /\ dbDeleted  = _TETrace[i].dbDeleted
        /\ dbDeleted' = _TETrace[j].dbDeleted
        /\ flushSeen  = _TETrace[i].flushSeen
        /\ flushSeen' = _TETrace[j].flushSeen
        /\ pendingEvents  = _TETrace[i].pendingEvents
        /\ pendingEvents' = _TETrace[j].pendingEvents
        /\ dbRevision  = _TETrace[i].dbRevision
        /\ dbRevision' = _TETrace[j].dbRevision
        /\ inMemory  = _TETrace[i].inMemory
        /\ inMemory' = _TETrace[j].inMemory

\* Uncomment the ASSUME below to write the states of the error trace
\* to the given file in Json format. Note that you can pass any tuple
\* to `JsonSerialize`. For example, a sub-sequence of _TETrace.
    \* ASSUME
    \*     LET J == INSTANCE Json
    \*         IN J!JsonSerialize("BoskMongo_TTrace_1784568752.json", _TETrace)

=============================================================================

 Note that you can extract this module `BoskMongo_TEExpression`
  to a dedicated file to reuse `expression` (the module in the 
  dedicated `BoskMongo_TEExpression.tla` file takes precedence 
  over the module `BoskMongo_TEExpression` below).

---- MODULE BoskMongo_TEExpression ----
EXTENDS Sequences, TLCExt, BoskMongo, Toolbox, Naturals, TLC, BoskMongo_TEConstants

expression == 
    [
        \* To hide variables of the `BoskMongo` spec from the error trace,
        \* remove the variables below.  The trace will be written in the order
        \* of the fields of this record.
        wrote |-> wrote
        ,dbState |-> dbState
        ,cursorOpen |-> cursorOpen
        ,formatType |-> formatType
        ,dbDeleted |-> dbDeleted
        ,flushSeen |-> flushSeen
        ,pendingEvents |-> pendingEvents
        ,dbRevision |-> dbRevision
        ,inMemory |-> inMemory
        
        \* Put additional constant-, state-, and action-level expressions here:
        \* ,_stateNumber |-> _TEPosition
        \* ,_wroteUnchanged |-> wrote = wrote'
        
        \* Format the `wrote` variable as Json value.
        \* ,_wroteJson |->
        \*     LET J == INSTANCE Json
        \*     IN J!ToJson(wrote)
        
        \* Lastly, you may build expressions over arbitrary sets of states by
        \* leveraging the _TETrace operator.  For example, this is how to
        \* count the number of times a spec variable changed up to the current
        \* state in the trace.
        \* ,_wroteModCount |->
        \*     LET F[s \in DOMAIN _TETrace] ==
        \*         IF s = 1 THEN 0
        \*         ELSE IF _TETrace[s].wrote # _TETrace[s-1].wrote
        \*             THEN 1 + F[s-1] ELSE F[s-1]
        \*     IN F[_TEPosition - 1]
    ]

=============================================================================



Parsing and semantic processing can take forever if the trace below is long.
 In this case, it is advised to uncomment the module below to deserialize the
 trace from a generated binary file.

\*
\*---- MODULE BoskMongo_TETrace ----
\*EXTENDS IOUtils, BoskMongo, TLC, BoskMongo_TEConstants
\*
\*trace == IODeserialize("BoskMongo_TTrace_1784568752.bin", TRUE)
\*
\*=============================================================================
\*

---- MODULE BoskMongo_TETrace ----
EXTENDS BoskMongo, TLC, BoskMongo_TEConstants

trace == 
    <<
    ([dbState |-> [b1 |-> "v1", a1 |-> "v1", a2 |-> "v1"],flushSeen |-> (b1 :> 0 @@ b2 :> 0),cursorOpen |-> (b1 :> FALSE @@ b2 :> FALSE),inMemory |-> (b1 :> [b1 |-> "v1", a1 |-> "v1", a2 |-> "v1"] @@ b2 :> [b1 |-> "v1", a1 |-> "v1", a2 |-> "v1"]),wrote |-> (<<b1, w1>> :> FALSE @@ <<b1, w2>> :> FALSE @@ <<b2, w1>> :> FALSE @@ <<b2, w2>> :> FALSE),dbRevision |-> 0,dbDeleted |-> FALSE,formatType |-> (b1 :> "disconnected" @@ b2 :> "disconnected"),pendingEvents |-> (b1 :> <<>> @@ b2 :> <<>>)]),
    ([dbState |-> [b1 |-> "v1", a1 |-> "v1", a2 |-> "v1"],flushSeen |-> (b1 :> 0 @@ b2 :> 0),cursorOpen |-> (b1 :> TRUE @@ b2 :> FALSE),inMemory |-> (b1 :> [b1 |-> "v1", a1 |-> "v1", a2 |-> "v1"] @@ b2 :> [b1 |-> "v1", a1 |-> "v1", a2 |-> "v1"]),wrote |-> (<<b1, w1>> :> FALSE @@ <<b1, w2>> :> FALSE @@ <<b2, w1>> :> FALSE @@ <<b2, w2>> :> FALSE),dbRevision |-> 0,dbDeleted |-> FALSE,formatType |-> (b1 :> "sequoia" @@ b2 :> "disconnected"),pendingEvents |-> (b1 :> <<>> @@ b2 :> <<>>)]),
    ([dbState |-> [b1 |-> "!NONE!", a1 |-> "!NONE!", a2 |-> "!NONE!"],flushSeen |-> (b1 :> 0 @@ b2 :> 0),cursorOpen |-> (b1 :> TRUE @@ b2 :> FALSE),inMemory |-> (b1 :> [b1 |-> "v1", a1 |-> "v1", a2 |-> "v1"] @@ b2 :> [b1 |-> "v1", a1 |-> "v1", a2 |-> "v1"]),wrote |-> (<<b1, w1>> :> FALSE @@ <<b1, w2>> :> FALSE @@ <<b2, w1>> :> FALSE @@ <<b2, w2>> :> FALSE),dbRevision |-> 1,dbDeleted |-> TRUE,formatType |-> (b1 :> "sequoia" @@ b2 :> "disconnected"),pendingEvents |-> (b1 :> <<[type |-> "delete", revision |-> 1, updated |-> [b1 |-> "!NONE!", a1 |-> "!NONE!", a2 |-> "!NONE!"]]>> @@ b2 :> <<[type |-> "delete", revision |-> 1, updated |-> [b1 |-> "!NONE!", a1 |-> "!NONE!", a2 |-> "!NONE!"]]>>)]),
    ([dbState |-> [b1 |-> "!NONE!", a1 |-> "!NONE!", a2 |-> "!NONE!"],flushSeen |-> (b1 :> 0 @@ b2 :> 0),cursorOpen |-> (b1 :> TRUE @@ b2 :> FALSE),inMemory |-> (b1 :> [b1 |-> "v1", a1 |-> "v1", a2 |-> "v1"] @@ b2 :> [b1 |-> "v1", a1 |-> "v1", a2 |-> "v1"]),wrote |-> (<<b1, w1>> :> FALSE @@ <<b1, w2>> :> FALSE @@ <<b2, w1>> :> FALSE @@ <<b2, w2>> :> FALSE),dbRevision |-> 1,dbDeleted |-> TRUE,formatType |-> (b1 :> "sequoia" @@ b2 :> "disconnected"),pendingEvents |-> (b1 :> <<>> @@ b2 :> <<[type |-> "delete", revision |-> 1, updated |-> [b1 |-> "!NONE!", a1 |-> "!NONE!", a2 |-> "!NONE!"]]>>)])
    >>
----


=============================================================================

---- MODULE BoskMongo_TEConstants ----
EXTENDS BoskMongo

CONSTANTS b1, b2, w1, w2

=============================================================================

---- CONFIG BoskMongo_TTrace_1784568752 ----
CONSTANTS
    Bosk = { b1 , b2 }
    WriterID = { w1 , w2 }
    Path = { "a1" , "a2" , "b1" }
    Value = { "v1" , "v2" }
    MaxRev = 4
    MaxPending = 3
    b2 = b2
    w2 = w2
    w1 = w1
    b1 = b1

INVARIANT
    _inv

CHECK_DEADLOCK
    \* CHECK_DEADLOCK off because of PROPERTY or INVARIANT above.
    FALSE

INIT
    _init

NEXT
    _next

CONSTANT
    _TETrace <- _trace

ALIAS
    _expression
=============================================================================
\* Generated on Mon Jul 20 13:32:33 EDT 2026