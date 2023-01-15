-------------------------------- MODULE mongo1 --------------------------------
EXTENDS Sequences, Integers
CONSTANTS instances \* a set of distinct integer IDs corresponding to Bosk.instanceID
VARIABLES bosks, mongo

\*======== Types ========

\* The state tree (aka the "configuration")
Config_t == [field1: STRING, field2: STRING]
Config(f1,f2) == [field1 |-> f1, field2 |-> f2]

\* For now, just updating the whole config all at once
Update_t == Config_t
Update(f1,f2) == Config(f1,f2)

Bosk_t == [
	config: Config_t,  \* The state tree
	qin: Seq(Update_t) \* Change stream events that have not yet been applied
]
Bosk(c,q) == [config |-> c, qin |-> q]

Mongo_t == [
    state: Bosk_t
]
Mongo(s) == [state |-> s]

TypeOK ==
	/\ bosks \in [instances -> Bosk_t]
	/\ mongo \in Mongo_t

\*======== State evolution ========

\* Return a bosk with the given update queued
SubmittedBosk(b,u) ==
	[config |-> b.config, qin |-> Append(b.qin, u)]

\* Return a bosk with the oldest queued update applied
UpdatedBosk(b) ==
	[config |-> Head(b.qin), qin |-> Tail(b.qin)]

\* Submit an update to Mongo
Submit(u) ==
	/\ mongo' = Mongo(SubmittedBosk(mongo.state, u))

Broadcast(u) ==
	/\ u /= mongo.state.config \* only broadcast if config has changed
	/\ bosks' = [i \in instances |-> SubmittedBosk(bosks[i], u)]

NoBroadcast(u) ==
	/\ u = mongo.state.config \* only skip broadcast if config has not changed
	/\ UNCHANGED bosks

\* Broadcast an update to all bosks, unless it's a no-op
BroadcastIfNeeded(u) ==
	\/ Broadcast(u)
	\/ NoBroadcast(u)

\* Atomically apply the oldest queued update and also broadcast to all instances
UpdateMongo ==
	/\ Len(mongo.state.qin) >= 1
	/\ mongo' = Mongo(UpdatedBosk(mongo.state))
	/\ BroadcastIfNeeded(Head(mongo.state.qin))

\* Apply the oldest queued update to the given bosk instance
UpdateInstance(i) ==
	/\ Len(bosks[i].qin) >= 1
	/\ bosks' = [bosks EXCEPT ![i] = UpdatedBosk(@)]
	/\ UNCHANGED mongo

UpdateSomeInstance ==
	\E i \in instances: UpdateInstance(i)

SubmitSomeUpdate ==
	\/ Submit(Update("a", "a"))
	\/ Submit(Update("b", "b"))

NextOptions ==
	\/ (SubmitSomeUpdate /\ UNCHANGED bosks)
	\/ UpdateMongo
	\/ UpdateSomeInstance

QLimit == 4

SearchBoundaries == \* Limit our exploration of the state space 
	/\ Len(mongo'.state.qin) <= QLimit
	/\ \A i \in instances: Len(bosks'[i].qin) <= QLimit

Next ==
	/\ NextOptions
	/\ SearchBoundaries

\*======== Initial state ========

InitialBosk ==
	Bosk(Config("init", "init"), <<>>)

Init ==
	/\ bosks = [i \in instances |-> InitialBosk]
	/\ mongo = Mongo(InitialBosk)

\* Invariants

BosksConverge == \* Whenever the queue is empty, the bosk's state equals Mongo's 
	\A i \in instances: (Len(bosks[i].qin) = 0) => (bosks[i].config = mongo.config)

Invariant ==
	/\ BosksConverge
	/\ TypeOK

=============================================================================
\* Modification History
\* Last modified Sun May 31 13:32:01 EDT 2020 by pdoyle
\* Created Sun May 31 11:30:46 EDT 2020 by pdoyle
