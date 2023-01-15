-------------------------------- MODULE mongo1 --------------------------------
EXTENDS Sequences, Integers
CONSTANTS servers
VARIABLES bosks, mongo

\*======== Types ========

Config_t == [field1: STRING, field2: STRING]
Config(f1,f2) == [field1 |-> f1, field2 |-> f2]

\* For now, just updating the whole config all at once
Update_t == Config_t
Update(f1,f2) == Config(f1,f2)

Bosk_t == [config: Config_t, qin: Seq(Update_t)]
Bosk(c,q) == [config |-> c, qin |-> q]

Mongo_t == Bosk_t \* For now, Mongo looks like just another Bosk

TypeOK ==
	/\ bosks \in [servers -> Bosk_t]
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
	/\ mongo' = SubmittedBosk(mongo, u)

Broadcast(u) ==
	/\ u /= mongo.config
	/\ bosks' = [s \in servers |-> SubmittedBosk(bosks[s], u)]

NoBroadcast(u) ==
	/\ u = mongo.config
	/\ UNCHANGED bosks

\* Broadcast an update to all bosks, unless it's a no-op
BroadcastIfNeeded(u) ==
	\/ Broadcast(u)
	\/ NoBroadcast(u)

\* Atomically apply the oldest queued update and also broadcast to all servers
UpdateMongo ==
	/\ Len(mongo.qin) >= 1
	/\ mongo' = UpdatedBosk(mongo)
	/\ BroadcastIfNeeded(Head(mongo.qin))

\* Apply the oldest queued update to the given server's bosk
UpdateServer(s) ==
	/\ Len(bosks[s].qin) >= 1
	/\ bosks' = [bosks EXCEPT ![s] = UpdatedBosk(@)]
	/\ UNCHANGED mongo

UpdateSomeServer ==
	\E s \in servers: UpdateServer(s)

SubmitSomeUpdate ==
	\/ Submit(Update("a", "a"))
	\/ Submit(Update("b", "b"))

NextOptions ==
	\/ (SubmitSomeUpdate /\ UNCHANGED bosks)
	\/ UpdateMongo
	\/ UpdateSomeServer

QLimit == 4

SearchBoundaries == \* Limit our exploration of the state space 
	/\ Len(mongo'.qin) <= QLimit
	/\ \A s \in servers: Len(bosks'[s].qin) <= QLimit

Next ==
	/\ NextOptions
	/\ SearchBoundaries

\*======== Initial state ========

InitialBosk ==
	Bosk(Config("init", "init"), <<>>)

Init ==
	/\ bosks = [s \in servers |-> InitialBosk]
	/\ mongo = InitialBosk

\* Invariants

BosksConverge == \* Whenever the queue is empty, the bosk's state equals Mongo's 
	\A s \in servers: (Len(bosks[s].qin) = 0) => (bosks[s].config = mongo.config)

Invariant ==
	/\ BosksConverge
	/\ TypeOK

=============================================================================
\* Modification History
\* Last modified Sun May 31 13:32:01 EDT 2020 by pdoyle
\* Created Sun May 31 11:30:46 EDT 2020 by pdoyle
