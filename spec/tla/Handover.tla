------------------------------ MODULE Handover ------------------------------
(* A keyed actor's ownership above the lease, for one key (keys are independent): the cluster's
   side of spec/paxos-leases.md and guide/cluster.md "Spawn actors on other nodes, one per key".
   The lease is what PaxosLeases promises (spec/tla/PaxosLeases.tla checks it): one holder at a
   time, each grant a larger epoch, the holder believing it no longer than the acceptors keep it.
   Time is in ticks and every deadline is a countdown, so time need not be bounded. A node's own
   actions can be delayed while time passes: a paused process, a starved runtime.

   Nodes are 1..N, and the view places the key on the lowest node that is up and not leaving
   (rendezvous on one key is a fixed order). Views that disagree are covered by starts on any
   node (Stray), which the lease alone must keep safe.

   Cuts that are not a clean split (up to MaxCuts changes, each to any values): whether the callers reach
   each node (cr), whether a node reaches a majority of the acceptors (grant, OwnershipAuthority::can_grant;
   without it its acquires and renewals fail and it refuses starts Isolated; the code learns a change from its
   requests' answers, a lag left out), and whether the nodes reach
   each other (apart; a node's own view then places the key on itself). The callers walk the order of their
   view past the nodes they cannot reach or that cannot grant (Entry; the code asks up to three, which N = 2
   never reaches), and a start that lands past the owner in the node's own view is marked (fell), which
   wakes its rebalance pass. A key moves to its owner only once the owner says it would take keys (the
   axon.owns probe: open, not isolated, able to grant, and reached). A start refused Held names the holder and the caller asks it (CallerServed).
   A cut does not touch a hand-off already asked for: the code's respawn asks again until operation_timeout.

   Abstractions. A handler runs at one instant (a pause inside one, or in on_turn_end, which runs
   after the turn's last fence check, is what the store's own epoch check is for); on_stop writes
   nothing here, and like a handler cut off midway only the store's check covers what it writes. The
   actor's build and its insert in the slot happen at the acquire's answer, which comes up to AcqDelay
   ticks after the acceptors granted it. The acceptors extend a renewal when it is sent, the worst case
   for the holder; a renewal either extends the hold, the acceptors and the lease entry or none of
   them (renew_loop timing out while the round still extends the hold is left out: the entry then
   lapses first, which only stops the actor sooner). One watcher, which may ask any node, and only what
   it shows is checked, not its Stopped. A leave (system.rs coordinate) drains its actors at once,
   where the code keeps them running until the extensions' leaving hooks return (while their lease
   entry is open no newer epoch exists, so nothing here depends on it); then runtime.shutdown stops
   every actor (StopAll); each key is handed off once its own actor has ended (Vacate), and
   finish_leaving shuts the fences and closes. A
   move and a leave share one hand-off flag (moveOut), so a leave starting during a move's join
   stands for both. A crash is seen by the survivors at once, and one flag stands for an eager restart
   due, answered by whichever node owns the key: the restarted node's own sweep, the minute held_by
   keeps listing a lapsed lease (b7f11c6), and the lapsed leases a node asks for while its sweep is
   armed, which the acceptors' logs keep across a restart of all of them (PaxosLeases.tla
   checks they are listed: Listed), are not modelled. Renewals never answer Lost: while a lease
   entry is open its hold and the acceptors' lease are too (Grace < Hold <= AccLease, and no acceptor
   forgets), so renew_loop's handling of Lost is not checked. Fewer starts than the code makes, which
   only weakens the liveness checks: a start gives up only while the key cannot be taken (the code
   also at operation_timeout or Unavailable), and a rebalance pass moves only a running actor (the
   code also respawns a stopping one, with its hand-off request). Not modelled: the isolation sweep,
   idle stops, a running actor ending on its own (a failure, a panic: a review run allowing it found
   nothing), supervised restarts inside a slot, a factory that fails, other keys except as renewals
   that carry this key's hold (OtherKeys), and the messages themselves (callers are a flag: someone
   keeps asking the owner, so what a caller is answered, f792594's draining actor, is not checked).

   Gaps a config may leave open, each named in Open; every base config closes them all, and
   mutations/Handover-<gap>.cfg opens one. All are past bugs, closed in the code as in the base configs:
     "HandOffOnce"        before 71fe559, a hand-off (a leave's, or a rebalance's move) asked the owner in the
                          node's view once, and none if that was the node itself again; a node that no longer owned
                          the key when the request landed refused it NotOwner. Now respawn asks the owner its view
                          names again after each refusal, for up to operation_timeout, which the model leaves out.
     "CrashLosesRequest"  before 71fe559, respawn released the lease (vacate) before it asked the owner: a crash in
                          between lost the request. Now the key stays listable (held_by) until the owner has taken
                          the request on (it answers once the start has its place) or respawn gave up. The model's
                          request outlives a crash of the owner that took it on, which the code's does not: an owner
                          that crashes between that answer and its acquire leaves an eager key to a caller.
     "OrphanHold"         before 6e8a979, get_or_spawn gave up its acquire at operation_timeout while the round
                          went on: the proposer kept a key no one waited for, and the next start there was
                          answered from that hold with a fresh lease_grace fence.
     "WatchTrustsDue"     before 234840d, watch_slot counted a lease as held until lose() took it out, past its
                          deadline too.
     "LoseRace"           before 234840d, spawn_local checked `lost`, then looked for the lease entry under
                          another lock; lose() on the renew task could take the entry out between the two and
                          note it lost only after, so a start answered from the hold built a fresh lease_grace
                          fence on the epoch lost. Now lose() and Inner::hold take `leases` and `lost` together.
   Bug is one mutation, "none" in the design (the list at the end of the constants). *)
EXTENDS Integers, FiniteSets

CONSTANTS
    N,                  \* nodes 1..N
    Grace,              \* ClusterConfig::lease_grace
    Hold,               \* what a holder believes, PaxosLeases::hold (Grace < Hold)
    AccLease,           \* how long the acceptors keep a lease (Hold <= AccLease)
    MaxDelay,           \* a renewal's answer, operation_timeout
    AcqDelay,           \* an acquire's answer after the grant
    MaxE,               \* grants
    MaxCrashes, MaxLeaves, MaxAuthFaults, MaxStray, MaxLate,
    Punctual,           \* TRUE: time waits for a node's due renewal and loss, and a leave for its
                        \* hand-off, except MaxLate times
    Callers,            \* TRUE: callers keep asking the owner for the key
    Eager,              \* the kind is eager
    OtherKeys,          \* TRUE: the node holds other keys, whose renewals extend this key's hold too
    Open,               \* the code's gaps left open, above
    MaxCuts,            \* changes of reachability (0: every node reaches every other and the acceptors)
    Bug

ASSUME AcqDelay <= Grace

\* Mutations, one per config.
ReleaseBeforeStop == Bug = "ReleaseBeforeStop"   \* hand_off releases the lease without waiting for the actor to end
RebalanceNoJoin == Bug = "RebalanceNoJoin"       \* a rebalance move respawns without joining the drained actor
SkipTimerFence == Bug = "SkipTimerFence"         \* the driver runs a due timer without checking the fence
FenceFromReply == Bug = "FenceFromReply"         \* a renewal holds the fence lease_grace from its reply, not to `until`
FreshFenceOnReuse == Bug = "FreshFenceOnReuse"   \* a start answered from the hold builds a new lease entry over the one it has
NoFence == Bug = "NoFence"                       \* 82711c3^: no fence, only lose() stops a holder
ReacquireFromHold == Bug = "ReacquireFromHold"   \* f9c1546^: an acquire answered from a lapsed hold gets a fresh fence
NoFenceAtClose == Bug = "NoFenceAtClose"         \* f9c1546^: a leave gives its leases up without shutting their fences
RenewFromReply == Bug = "RenewFromReply"         \* 3156261^ (spec/paxos-leases.md 5.1): a renewal counts from its reply
AcquireFromAnswer == Bug = "AcquireFromAnswer"   \* a new lease counts from the acquire's answer, not when it was asked
VacateIgnoresNewer == Bug = "VacateIgnoresNewer" \* 788c12d^: the exit watcher releases though a newer actor holds the slot
WatchAnyActor == Bug = "WatchAnyActor"           \* 2fddb01^: watch_slot answers for whatever holds the slot
WatchDraining == Bug = "WatchDraining"           \* watch_slot shows a draining actor as running
StartIgnoresMove == Bug = "StartIgnoresMove"     \* f8acecd^: a start does not look at the view again when it lands
MoveBlind == Bug = "MoveBlind"                   \* a key moves to its owner without asking it first
NoRedirect == Bug = "NoRedirect"                 \* a caller refused Held does not ask the holder

Nodes == 1..N
None == 0

VARIABLES
    st,                 \* "up" | "leaving" | "closed" (finish_leaving done) | "down" | "gone"
    aH, aE, aLeft,      \* the acceptors' lease: holder, epoch, ticks left (0: free)
    aOld,               \* the holder is a previous incarnation of node aH (ballots name node/incarnation)
    lastE,              \* the last epoch granted
    hE, hLeft,          \* the proposer: the epoch a node believes it holds, ticks left
    cE, cLeft, lostE,   \* spawn.rs `leases` (epoch, until) and `lost`
    act, actE, fence,   \* the actor: "none" | "running" | "draining" | "stopping", its epoch, its Fence
    pend,               \* the epoch of an actor that ended and whose vacate has not run
    moveOut,            \* after its actor ends, the node hands the key to the owner
    starting, moved,    \* a spawn_local under way, and whether the view moved since it began
    rebal,              \* a rebalance pass is due
    rn, rnE, rnAge,     \* a renewal out: "idle" | "ok" | "fail" | "lost", the epoch it covers, its age
    want, eager,        \* a hand-off asked a node to start the key; an eager restart is due
    sE, wMax, wNode,    \* the store's highest epoch written; the highest epoch a watch showed, and where
    writeBad, watchBad, watchDead,
    crashes, leaves, afails, strays, lates,
    cr, grant, apart,   \* the callers reach node n; n reaches a majority of the acceptors; the nodes are cut apart
    cuts, fell          \* reachability changes so far; the actor on n was started past its owner in n's view

reach == <<cr, grant, apart, cuts>>
obs == <<sE, wMax, wNode, writeBad, watchBad, watchDead>>
vars == <<st, aH, aE, aLeft, aOld, lastE, hE, hLeft, cE, cLeft, lostE, act, actE, fence, pend, moveOut,
          starting, moved, rebal, rn, rnE, rnAge, want, eager, obs, crashes, leaves, afails, strays, lates,
          reach, fell>>

Max(x, y) == IF x > y THEN x ELSE y
Dec(x) == IF x > 0 THEN x - 1 ELSE 0
Candidates == {n \in Nodes : st[n] = "up"}
Min(S) == IF S = {} THEN None ELSE CHOOSE n \in S : \A m \in S : n <= m
\* The owner in the callers' views, the rendezvous order's first.
Owner == Min(Candidates)
\* The owner in n's own view, which lists the other nodes only while it reaches them.
OwnerOf(n) == Min({m \in Candidates : m = n \/ ~apart})
\* The node a caller's get_or_spawn lands on: the first of its order it reaches that does not refuse Isolated.
Entry == Min({m \in Candidates : cr[m] /\ grant[m]})
Alive(n) == st[n] \in {"up", "leaving"}             \* the cluster's tasks run
Free == aLeft = 0 \/ aH = None
\* The acceptors grant n the key: free, or n's own (a new grant, paxos/core.rs Acceptor::handle).
Takes(n) == Free \/ (aH = n /\ ~aOld)
\* A grant to n, d ticks ago.
Grant(n, d) ==
    /\ aH' = n /\ aE' = lastE + 1 /\ aLeft' = AccLease - d /\ aOld' = FALSE /\ lastE' = lastE + 1
    /\ hE' = [hE EXCEPT ![n] = lastE + 1]
    /\ hLeft' = [hLeft EXCEPT ![n] = Hold - d]
\* An actor not yet ended under an open fence; Unfenced, one that can still handle a message.
Holding(n) == st[n] \notin {"down", "gone"} /\ act[n] # "none" /\ fence[n] > 0
Unfenced(n) == Holding(n) /\ act[n] # "stopping"
\* The actor's Fence is the one in its lease entry while that entry has its epoch.
Shares(n) == act[n] # "none" /\ cE[n] = actE[n]

\* A membership change: every open node's rebalance pass is due, and every start in flight has seen the view move.
ViewMoves(s) ==
    /\ rebal' = [n \in Nodes |-> s[n] = "up"]
    /\ moved' = [n \in Nodes |-> s[n] = "up" /\ (moved[n] \/ starting[n])]

\* The key starts running on the owner at first, under epoch 1, nothing else held.
Init ==
    /\ st = [n \in Nodes |-> "up"]
    /\ aH = 1 /\ aE = 1 /\ aLeft = AccLease /\ aOld = FALSE /\ lastE = 1
    /\ hE = [n \in Nodes |-> IF n = 1 THEN 1 ELSE 0]
    /\ hLeft = [n \in Nodes |-> IF n = 1 THEN Hold ELSE 0]
    /\ cE = [n \in Nodes |-> IF n = 1 THEN 1 ELSE 0]
    /\ cLeft = [n \in Nodes |-> IF n = 1 THEN Grace ELSE 0]
    /\ lostE = [n \in Nodes |-> 0]
    /\ act = [n \in Nodes |-> IF n = 1 THEN "running" ELSE "none"]
    /\ actE = [n \in Nodes |-> IF n = 1 THEN 1 ELSE 0]
    /\ fence = [n \in Nodes |-> IF n = 1 THEN Grace ELSE 0]
    /\ pend = [n \in Nodes |-> 0]
    /\ moveOut = [n \in Nodes |-> FALSE]
    /\ starting = [n \in Nodes |-> FALSE]
    /\ moved = [n \in Nodes |-> FALSE]
    /\ rebal = [n \in Nodes |-> FALSE]
    /\ rn = [n \in Nodes |-> "idle"]
    /\ rnE = [n \in Nodes |-> 0]
    /\ rnAge = [n \in Nodes |-> 0]
    /\ want = [n \in Nodes |-> FALSE]
    /\ eager = FALSE
    /\ sE = 0 /\ wMax = 0 /\ wNode = None /\ writeBad = FALSE /\ watchBad = FALSE /\ watchDead = FALSE
    /\ crashes = 0 /\ leaves = 0 /\ afails = 0 /\ strays = 0 /\ lates = 0
    /\ cr = [n \in Nodes |-> TRUE] /\ grant = [n \in Nodes |-> TRUE] /\ apart = FALSE /\ cuts = 0
    /\ fell = [n \in Nodes |-> FALSE]

----------------------------------------------------------------------------
(* Time *)

\* What the renew loop must do before time moves on: send a renewal while one can still come back
\* in time, take its answer by operation_timeout, and stop a lease that is due (spawn.rs: renew_loop).
Due(n) ==
    /\ Alive(n)
    /\ \/ cE[n] # 0 /\ cLeft[n] = 0
       \/ rn[n] = "idle" /\ cE[n] # 0 /\ cLeft[n] > 0 /\ cLeft[n] <= MaxDelay + 1
       \/ rn[n] # "idle" /\ rnAge[n] = MaxDelay

\* Values that can no longer matter are cleared as they lapse (a lapsed hold's epoch and loss; a
\* lapsed lease's holder, except where held_by still lists it for an eager kind), to keep states few.
Advance ==
    /\ aLeft' = Dec(aLeft)
    /\ aH' = IF aLeft <= 1 /\ ~Eager THEN None ELSE aH
    /\ aOld' = (aOld /\ aLeft > 1)
    /\ hLeft' = [n \in Nodes |-> Dec(hLeft[n])]
    /\ hE' = [n \in Nodes |-> IF hLeft[n] <= 1 THEN 0 ELSE hE[n]]
    /\ lostE' = [n \in Nodes |-> IF hLeft[n] <= 1 THEN 0 ELSE lostE[n]]
    /\ cLeft' = [n \in Nodes |-> Dec(cLeft[n])]
    /\ fence' = [n \in Nodes |-> Dec(fence[n])]
    /\ rnAge' = [n \in Nodes |-> IF rn[n] # "idle" /\ rnAge[n] < MaxDelay THEN rnAge[n] + 1 ELSE rnAge[n]]
    /\ UNCHANGED <<st, aE, lastE, cE, act, actE, pend, moveOut, starting, moved, rebal,
                   rn, rnE, want, eager, obs, crashes, leaves, afails, strays, reach, fell>>

Tick ==
    /\ Punctual => \A n \in Nodes : ~Due(n)
    /\ Advance
    /\ UNCHANGED lates

\* A pause or a starved runtime: time passes a node's due work.
LateTick ==
    /\ Punctual
    /\ \E n \in Nodes : Due(n)
    /\ lates < MaxLate
    /\ lates' = lates + 1
    /\ Advance

----------------------------------------------------------------------------
(* Renewals: spawn.rs Inner::renew_loop, paxos/core.rs Proposer::renew *)

\* The loop notes `sent`, then asks. The acceptors extend from when it reaches them, at the earliest `sent`.
\* Proposer::renew renews every key the proposer holds, so another key's renewal carries a hold with no entry.
RenewSend(n) ==
    /\ Alive(n)
    /\ rn[n] = "idle"
    /\ \/ cE[n] # 0 /\ cLeft[n] > 0
       \/ OtherKeys /\ cE[n] = 0 /\ hLeft[n] > 0
    /\ LET ours == hLeft[n] > 0 /\ aH = n /\ aE = hE[n] /\ aLeft > 0
       IN \/ /\ ours
             /\ grant[n]
             /\ aLeft' = AccLease
             /\ rn' = [rn EXCEPT ![n] = "ok"]
             /\ UNCHANGED afails
          \/ /\ ours
             /\ ~grant[n]                    \* cut from a majority
             /\ rn' = [rn EXCEPT ![n] = "fail"]
             /\ UNCHANGED <<aLeft, afails>>
          \/ /\ ours
             /\ afails < MaxAuthFaults      \* no majority answered
             /\ afails' = afails + 1
             /\ rn' = [rn EXCEPT ![n] = "fail"]
             /\ UNCHANGED aLeft
          \/ /\ ~ours                        \* the hold ran out: Lost
             /\ rn' = [rn EXCEPT ![n] = "lost"]
             /\ UNCHANGED <<aLeft, afails>>
    /\ rnE' = [rnE EXCEPT ![n] = cE[n]]
    /\ rnAge' = [rnAge EXCEPT ![n] = 0]
    /\ UNCHANGED <<st, aH, aE, aOld, lastE, hE, hLeft, cE, cLeft, lostE, act, actE, fence, pend, moveOut,
                   starting, moved, rebal, want, eager, obs, crashes, leaves, strays, lates, reach, fell>>

\* The answer. A renewed lease runs to `sent + lease_grace`, and its fence with it; the holder believes
\* `sent + hold`, unless its hold ran out meanwhile, which grants nothing (paxos/core.rs Next::Granted).
RenewReply(n) ==
    /\ Alive(n)
    /\ rn[n] # "idle"
    /\ LET ok == rn[n] = "ok" /\ hLeft[n] > 0
           lost == rn[n] = "lost" \/ (rn[n] = "ok" /\ hLeft[n] = 0)
           until == IF RenewFromReply THEN Grace ELSE Grace - rnAge[n]
           mine == cE[n] = rnE[n] /\ cE[n] # 0
           extend == ok /\ mine /\ cLeft[n] < until
       IN /\ hLeft' = IF ok THEN [hLeft EXCEPT ![n] = Max(hLeft[n], Hold - rnAge[n])] ELSE hLeft
          /\ cLeft' = IF extend THEN [cLeft EXCEPT ![n] = until]
                      ELSE IF lost /\ mine THEN [cLeft EXCEPT ![n] = 0]
                      ELSE cLeft
          /\ fence' = IF extend /\ Shares(n) THEN [fence EXCEPT ![n] = IF FenceFromReply THEN Grace ELSE until]
                      ELSE fence
    /\ rn' = [rn EXCEPT ![n] = "idle"]
    /\ rnAge' = [rnAge EXCEPT ![n] = 0]
    /\ rnE' = [rnE EXCEPT ![n] = 0]
    /\ UNCHANGED <<st, aH, aE, aLeft, aOld, lastE, hE, cE, lostE, act, actE, pend, moveOut, starting, moved,
                   rebal, want, eager, obs, crashes, leaves, afails, strays, lates, reach, fell>>

\* spawn.rs Inner::lose: fence first, note the epoch lost, stop the actor.
Lose(n) ==
    /\ Alive(n)
    /\ cE[n] # 0 /\ cLeft[n] = 0
    /\ lostE' = [lostE EXCEPT ![n] = IF hLeft[n] > 0 THEN cE[n] ELSE 0]
    /\ fence' = IF Shares(n) THEN [fence EXCEPT ![n] = 0] ELSE fence
    /\ act' = IF Shares(n) THEN [act EXCEPT ![n] = "stopping"] ELSE act
    /\ cE' = [cE EXCEPT ![n] = 0]
    /\ UNCHANGED <<st, aH, aE, aLeft, aOld, lastE, hE, hLeft, cLeft, actE, pend, moveOut, starting, moved,
                   rebal, rn, rnE, rnAge, want, eager, obs, crashes, leaves, afails, strays, lates, reach, fell>>

----------------------------------------------------------------------------
(* The actor: axon driver.rs Driver::advance and MailboxBatch::next *)

\* A handler writes to the store under its epoch. The fence is checked before each message and each timer
\* (stop_check). A handler runs at one instant: a pause inside one is what the store's own check is for.
Handle(n, timer) ==
    /\ st[n] \notin {"down", "gone"}
    /\ act[n] \in (IF timer THEN {"running"} ELSE {"running", "draining"})  \* a drain drops timers
    /\ fence[n] > 0 \/ (timer /\ SkipTimerFence) \/ NoFence
    /\ writeBad' = (writeBad \/ actE[n] < sE)
    /\ sE' = Max(sE, actE[n])
    /\ UNCHANGED <<st, aH, aE, aLeft, aOld, lastE, hE, hLeft, cE, cLeft, lostE, act, actE, fence, pend, moveOut,
                   starting, moved, rebal, rn, rnE, rnAge, want, eager, wMax, wNode, watchBad, watchDead,
                   crashes, leaves, afails, strays, lates, reach, fell>>

\* Mailbox::fenced: the actor finds its fence lapsed and stops Fenced.
FenceStop(n) ==
    /\ ~NoFence
    /\ st[n] \notin {"down", "gone"}
    /\ act[n] \in {"running", "draining"}
    /\ fence[n] = 0
    /\ act' = [act EXCEPT ![n] = "stopping"]
    /\ UNCHANGED <<st, aH, aE, aLeft, aOld, lastE, hE, hLeft, cE, cLeft, lostE, actE, fence, pend, moveOut,
                   starting, moved, rebal, rn, rnE, rnAge, want, eager, obs, crashes, leaves, afails, strays, lates, reach, fell>>

\* A stopped actor ends, or a drain runs out of messages. Its slot stays until vacate.
Exit(n) ==
    /\ st[n] \notin {"down", "gone"}
    /\ act[n] \in {"stopping", "draining"}
    /\ act' = [act EXCEPT ![n] = "none"]
    /\ pend' = [pend EXCEPT ![n] = IF Alive(n) THEN actE[n] ELSE 0]   \* after finish_leaving no vacate runs
    /\ actE' = [actE EXCEPT ![n] = 0]
    /\ fence' = [fence EXCEPT ![n] = 0]
    /\ UNCHANGED <<st, aH, aE, aLeft, aOld, lastE, hE, hLeft, cE, cLeft, lostE, moveOut, starting, moved,
                   rebal, rn, rnE, rnAge, want, eager, obs, crashes, leaves, afails, strays, lates, reach, fell>>

\* spawn.rs Inner::release, and PaxosLeases::release, which sends only while it holds that epoch.
Release(n, e, ok) ==
    /\ cE' = IF cE[n] = e THEN [cE EXCEPT ![n] = 0] ELSE cE
    /\ cLeft' = IF cE[n] = e THEN [cLeft EXCEPT ![n] = 0] ELSE cLeft
    /\ lostE' = IF lostE[n] = e THEN [lostE EXCEPT ![n] = 0] ELSE lostE
    /\ IF hLeft[n] > 0 /\ hE[n] = e
       THEN /\ hLeft' = [hLeft EXCEPT ![n] = 0]
            /\ hE' = [hE EXCEPT ![n] = 0]
            /\ IF ok /\ aH = n /\ aE = e
               THEN aH' = None /\ aLeft' = 0
               ELSE UNCHANGED <<aH, aLeft>>
       ELSE UNCHANGED <<hLeft, hE, aH, aLeft>>

Follows == "HandOffOnce" \notin Open

\* A hand-off asks the owner in n's view to start the key (Inner::respawn).
Ask(n) == IF Owner # None /\ (Owner # n \/ Follows)
          THEN [want EXCEPT ![Owner] = TRUE] ELSE want

\* spawn.rs Inner::vacate, by the exit watcher or by respawn: frees the slot and the lease unless a newer
\* actor holds the slot. A move or a leave then asks the owner to start the key.
Vacate(n) ==
    /\ Alive(n)
    /\ pend[n] # 0
    /\ LET free == act[n] = "none" \/ VacateIgnoresNewer
       IN \/ /\ IF free THEN Release(n, pend[n], TRUE) ELSE UNCHANGED <<cE, cLeft, lostE, hLeft, hE, aH, aLeft>>
             /\ UNCHANGED afails
          \/ /\ free                        \* the release is lost on the way: the acceptors keep the lease
             /\ afails < MaxAuthFaults
             /\ afails' = afails + 1
             /\ Release(n, pend[n], FALSE)
    /\ pend' = [pend EXCEPT ![n] = 0]
    /\ want' = IF moveOut[n] THEN Ask(n) ELSE want
    /\ moveOut' = [moveOut EXCEPT ![n] = FALSE]
    /\ UNCHANGED <<st, aE, aOld, lastE, act, actE, fence, starting, moved, rebal, rn, rnE, rnAge, eager,
                   obs, crashes, leaves, strays, lates, reach, fell>>

\* Mutations: hand_off calls respawn before the stopped actor has ended, or a rebalance move before the
\* drained one has.
EarlyHandOff(n) ==
    /\ IF st[n] = "leaving" THEN ReleaseBeforeStop /\ act[n] = "stopping"
       ELSE st[n] = "up" /\ RebalanceNoJoin /\ act[n] = "draining"
    /\ moveOut[n]
    /\ Release(n, actE[n], TRUE)
    /\ want' = Ask(n)
    /\ moveOut' = [moveOut EXCEPT ![n] = FALSE]
    /\ UNCHANGED <<st, aE, aOld, lastE, act, actE, fence, pend, starting, moved, rebal, rn, rnE, rnAge, eager,
                   obs, crashes, leaves, afails, strays, lates, reach, fell>>

----------------------------------------------------------------------------
(* Starts: Cluster::get_or_spawn, spawn.rs Inner::spawn_local *)

Wanted(n) == IF Follows THEN \E m \in Nodes : want[m] ELSE want[n]
Answered(n) == IF Follows THEN [m \in Nodes |-> FALSE] ELSE [want EXCEPT ![n] = FALSE]
\* A start that does not land: the request it served is refused, or when the hand-off follows goes on to the owner.
Dropped(n) == IF Follows THEN want ELSE [want EXCEPT ![n] = FALSE]
\* Callers start the key where their walk lands; a hand-off and an eager restart ask the owner.
Demand(n) == (Callers /\ n = Entry) \/ (n = Owner /\ (Wanted(n) \/ eager))

\* A start waits for an actor still ending in the slot, then checks the view places the key here.
\* A node whose view is stale starts where the true owner is another (Stray). Its view catches up
\* later, which wakes its rebalance pass; taken as caught up while the start waits for the lease, the
\* case f8acecd is about (caught up after the start lands wakes the pass the same way).
StartBegin(n) ==
    /\ st[n] = "up"
    /\ grant[n]                         \* else refused Isolated at once
    /\ ~starting[n]
    /\ act[n] = "none"
    /\ IF Demand(n) THEN UNCHANGED strays
       ELSE n # Owner /\ strays < MaxStray /\ strays' = strays + 1
    /\ starting' = [starting EXCEPT ![n] = TRUE]
    /\ moved' = [moved EXCEPT ![n] = ~Demand(n)]
    \* Started past its owner: the walk passed the owner of this node's own view (spawn.rs spawn_local, which
    \* wakes the rebalance pass when the key lands on a node its view does not place it on).
    /\ fell' = [fell EXCEPT ![n] = Demand(n) /\ OwnerOf(n) # n]
    /\ UNCHANGED <<st, aH, aE, aLeft, aOld, lastE, hE, hLeft, cE, cLeft, lostE, act, actE, fence, pend, moveOut,
                   rebal, rn, rnE, rnAge, want, eager, obs, crashes, leaves, afails, lates, reach>>

\* The acquire's answer, then the actor built and put in the slot. A proposer that still believes it holds
\* the key answers from that hold at once (paxos/core.rs Proposer::acquire); a key this node gave up is Held
\* until its actor released it (`lost`). A lease entry of that epoch keeps its deadline and fence; a new one
\* runs lease_grace from when the acquire was asked (`asked`), d ticks before the answer. Held by another:
\* no step here, the start retries every HELD_RETRY or gives up. A start in flight since before a leave
\* began still acquires. A swept start overwrites a vacate still due from an older actor here, which then
\* releases nothing: that older epoch's hold has lapsed, or the start would have been answered from it.
Acquire(n) ==
    /\ Alive(n)
    /\ starting[n]
    /\ \E d \in (IF hLeft[n] > 0 THEN {0} ELSE 0..AcqDelay) :
       LET fromHold == hLeft[n] > 0
           e == IF fromHold THEN hE[n] ELSE lastE + 1
           \* Open "LoseRace": lose() takes the due entry out and notes it lost between the start's two looks.
           race == "LoseRace" \in Open /\ fromHold /\ cE[n] = e /\ cLeft[n] = 0
           reuse == fromHold /\ cE[n] = e /\ ~race /\ ~FreshFenceOnReuse
           left == IF reuse THEN cLeft[n] ELSE IF AcquireFromAnswer THEN Grace ELSE Grace - d
           \* Swept (spawn.rs `swept`): its fence already shut, or the node leaving. The start stops the
           \* actor and fails, and the exit watcher's vacate releases the lease (listed = false).
           live == st[n] = "up" /\ left > 0
       IN /\ IF fromHold THEN ReacquireFromHold \/ lostE[n] # e
             ELSE Takes(n) /\ lastE < MaxE /\ grant[n]
          /\ IF fromHold THEN UNCHANGED <<aH, aE, aLeft, aOld, lastE, hE, hLeft>>
             ELSE Grant(n, d)
          /\ cE' = [cE EXCEPT ![n] = e]
          /\ cLeft' = [cLeft EXCEPT ![n] = left]
          /\ lostE' = IF race THEN [lostE EXCEPT ![n] = e] ELSE lostE
          /\ act' = IF live THEN [act EXCEPT ![n] = "running"] ELSE act
          /\ actE' = IF live THEN [actE EXCEPT ![n] = e] ELSE actE
          /\ fence' = IF live THEN [fence EXCEPT ![n] = left] ELSE fence
          /\ pend' = IF live THEN pend ELSE [pend EXCEPT ![n] = e]
          /\ eager' = (eager /\ ~live)
          /\ want' = IF live THEN Answered(n) ELSE Dropped(n)
          \* The view moved the key elsewhere while the lease was waited for: wake the rebalance pass.
          \* A start past the owner wakes it too, to ask the owner whether it would take the key.
          /\ rebal' = IF live /\ ((moved[n] /\ OwnerOf(n) # n /\ ~StartIgnoresMove) \/ fell[n])
                      THEN [rebal EXCEPT ![n] = TRUE] ELSE rebal
    /\ starting' = [starting EXCEPT ![n] = FALSE]
    /\ moved' = [moved EXCEPT ![n] = FALSE]
    /\ UNCHANGED <<st, moveOut, rn, rnE, rnAge, obs, crashes, leaves, afails, strays, lates, reach, fell>>

\* The start gives up: operation_timeout while the key is Held.
EndStart(n) ==
    /\ Alive(n)
    /\ starting[n]
    /\ starting' = [starting EXCEPT ![n] = FALSE]
    /\ moved' = [moved EXCEPT ![n] = FALSE]
    /\ want' = Dropped(n)
    /\ UNCHANGED <<st, cE, cLeft, lostE, act, actE, fence, pend, moveOut, rebal, rn, rnE, rnAge, eager,
                   obs, crashes, leaves, afails, strays, lates, reach, fell>>

GiveUp(n) ==
    /\ IF hLeft[n] > 0 THEN lostE[n] = hE[n] /\ ~ReacquireFromHold
       ELSE ~Takes(n) \/ lastE >= MaxE \/ ~grant[n]
    /\ EndStart(n)
    /\ UNCHANGED <<aH, aE, aLeft, aOld, lastE, hE, hLeft, reach, fell>>

\* Open "OrphanHold": the start gave up while its acquire was out, and the round goes on to a grant.
AcquireLate(n) ==
    /\ "OrphanHold" \in Open
    /\ hLeft[n] = 0 /\ Takes(n) /\ lastE < MaxE
    /\ EndStart(n)
    /\ Grant(n, 0)

\* spawn.rs Inner::rebalance_loop: a key whose owner in this node's view is now another node drains, then
\* moves, once the owner answers that it would take keys (takes_keys_at); until then the pass stays due, as
\* the code's asks again every operation_timeout.
Rebalance(n) ==
    /\ st[n] = "up"
    /\ rebal[n]
    /\ LET o == OwnerOf(n)
           elsewhere == act[n] = "running" /\ o # n
           takes == st[o] = "up" /\ ~apart /\ grant[o]
           go == elsewhere /\ (MoveBlind \/ takes)
       IN /\ rebal' = [rebal EXCEPT ![n] = elsewhere /\ ~go]
          /\ IF go
             THEN /\ act' = [act EXCEPT ![n] = "draining"]
                  /\ moveOut' = [moveOut EXCEPT ![n] = TRUE]
             ELSE UNCHANGED <<act, moveOut>>
    /\ UNCHANGED <<st, aH, aE, aLeft, aOld, lastE, hE, hLeft, cE, cLeft, lostE, actE, fence, pend, starting, moved,
                   rn, rnE, rnAge, want, eager, obs, crashes, leaves, afails, strays, lates, reach, fell>>

----------------------------------------------------------------------------
(* Nodes: cluster.rs begin_leaving, finish_leaving; crashes and restarts *)

\* system.shutdown(): the node is marked draining and refuses starts, its actors drain, and each key
\* it had is listed for hand-off.
BeginLeave(n) ==
    /\ st[n] = "up"
    /\ leaves < MaxLeaves
    /\ leaves' = leaves + 1
    /\ LET s == [st EXCEPT ![n] = "leaving"]
       IN /\ st' = s
          /\ ViewMoves(s)
    /\ act' = IF act[n] = "running" THEN [act EXCEPT ![n] = "draining"] ELSE act
    /\ moveOut' = [moveOut EXCEPT ![n] = act[n] # "none" \/ pend[n] # 0]
    /\ want' = Dropped(n)
    /\ UNCHANGED <<aH, aE, aLeft, aOld, lastE, hE, hLeft, cE, cLeft, lostE, actE, fence, pend, starting,
                   rn, rnE, rnAge, eager, obs, crashes, afails, strays, lates, reach, fell>>

\* system.rs coordinate, once the drain is over or its share of the budget spent: runtime.shutdown stops
\* every actor (Runtime::stop_all), before any extension's shutdown hook runs.
StopAll(n) ==
    /\ st[n] = "leaving"
    /\ act[n] \in {"running", "draining"}
    /\ act' = [act EXCEPT ![n] = "stopping"]
    /\ UNCHANGED <<st, aH, aE, aLeft, aOld, lastE, hE, hLeft, cE, cLeft, lostE, actE, fence, pend, moveOut,
                   starting, moved, rebal, rn, rnE, rnAge, want, eager, obs, crashes, leaves, afails, strays, lates, reach, fell>>

\* finish_leaving after hand_off, or once its share of the budget is spent: shut every lease's fence,
\* close the authority (PaxosLeases::close sends a release for every key the proposer lists, its hold
\* lapsed or not), stop the renew loop. What only the cluster's tasks read is cleared: they never run here
\* again, and a start still out finds the authority closed. Before the hand-off is done only once the
\* budget is spent, which punctual time counts as a late step.
FinishLeave(n) ==
    /\ st[n] = "leaving"
    /\ act[n] \notin {"running", "draining"}
    /\ IF (act[n] = "none" /\ pend[n] = 0) \/ ~Punctual THEN UNCHANGED lates
       ELSE lates < MaxLate /\ lates' = lates + 1
    /\ st' = [st EXCEPT ![n] = "closed"]
    /\ fence' = IF Shares(n) /\ ~NoFenceAtClose THEN [fence EXCEPT ![n] = 0] ELSE fence
    /\ IF aH = n /\ ~aOld THEN aH' = None /\ aLeft' = 0 ELSE UNCHANGED <<aH, aLeft>>
    /\ hLeft' = [hLeft EXCEPT ![n] = 0]
    /\ hE' = [hE EXCEPT ![n] = 0]
    /\ cE' = [cE EXCEPT ![n] = 0]
    /\ cLeft' = [cLeft EXCEPT ![n] = 0]
    /\ lostE' = [lostE EXCEPT ![n] = 0]
    /\ pend' = [pend EXCEPT ![n] = 0]
    /\ moveOut' = [moveOut EXCEPT ![n] = FALSE]
    /\ starting' = [starting EXCEPT ![n] = FALSE]
    /\ moved' = [moved EXCEPT ![n] = FALSE]
    /\ rebal' = [rebal EXCEPT ![n] = FALSE]
    /\ rn' = [rn EXCEPT ![n] = "idle"]
    /\ rnE' = [rnE EXCEPT ![n] = 0]
    /\ rnAge' = [rnAge EXCEPT ![n] = 0]
    /\ UNCHANGED <<aE, aOld, lastE, act, actE, want, eager, obs, crashes, leaves, afails, strays, reach, fell>>

\* The runtime ends once its last actor has.
Gone(n) ==
    /\ st[n] = "closed"
    /\ act[n] = "none"
    /\ st' = [st EXCEPT ![n] = "gone"]
    /\ UNCHANGED <<aH, aE, aLeft, aOld, lastE, hE, hLeft, cE, cLeft, lostE, act, actE, fence, pend, moveOut,
                   starting, moved, rebal, rn, rnE, rnAge, want, eager, obs, crashes, leaves, afails, strays, lates, reach, fell>>

\* A process or machine gone at once. Its lease stays with the acceptors until it runs out, and they list
\* it as the node's (held_by), so an eager kind's survivors restart it (Inner::eager_loop). Open
\* "CrashLosesRequest": a hand-off request not yet sent is lost with it (with N = 2 every request out is
\* the other node's, from this one).
Crash(n) ==
    /\ st[n] \in {"up", "leaving", "closed"}
    /\ crashes < MaxCrashes
    /\ crashes' = crashes + 1
    /\ LET s == [st EXCEPT ![n] = "down"]
       IN /\ st' = s
          /\ ViewMoves(s)
    /\ aOld' = (aOld \/ (aH = n /\ aLeft > 0))
    /\ hLeft' = [hLeft EXCEPT ![n] = 0]
    /\ cE' = [cE EXCEPT ![n] = 0]
    /\ cLeft' = [cLeft EXCEPT ![n] = 0]
    /\ lostE' = [lostE EXCEPT ![n] = 0]
    /\ act' = [act EXCEPT ![n] = "none"]
    /\ fence' = [fence EXCEPT ![n] = 0]
    /\ pend' = [pend EXCEPT ![n] = 0]
    /\ moveOut' = [moveOut EXCEPT ![n] = FALSE]
    /\ starting' = [starting EXCEPT ![n] = FALSE]
    /\ rn' = [rn EXCEPT ![n] = "idle"]
    /\ rnAge' = [rnAge EXCEPT ![n] = 0]
    /\ want' \in IF "CrashLosesRequest" \in Open THEN {Dropped(n), [m \in Nodes |-> FALSE]} ELSE {Dropped(n)}
    /\ eager' = (eager \/ (Eager /\ aH = n))
    /\ hE' = [hE EXCEPT ![n] = 0]
    /\ actE' = [actE EXCEPT ![n] = 0]
    /\ rnE' = [rnE EXCEPT ![n] = 0]
    /\ UNCHANGED <<aH, aE, aLeft, lastE, obs, leaves, afails, strays, lates, reach, fell>>

\* The same node id, a new incarnation: an eager kind restarts what the acceptors still list as its.
Restart(n) ==
    /\ st[n] = "down"
    /\ LET s == [st EXCEPT ![n] = "up"]
       IN /\ st' = s
          /\ ViewMoves(s)
    /\ UNCHANGED <<aH, aE, aLeft, aOld, lastE, hE, hLeft, cE, cLeft, lostE, act, actE, fence, pend, moveOut,
                   starting, rn, rnE, rnAge, want, eager, obs, crashes, leaves, afails, strays, lates, reach, fell>>

\* Reachability changes, which every node's view sees.
Cut ==
    /\ cuts < MaxCuts
    /\ cuts' = cuts + 1
    /\ cr' \in [Nodes -> BOOLEAN]
    /\ grant' \in [Nodes -> BOOLEAN]
    /\ apart' \in BOOLEAN
    /\ ViewMoves(st)
    /\ UNCHANGED <<st, aH, aE, aLeft, aOld, lastE, hE, hLeft, cE, cLeft, lostE, act, actE, fence, pend, moveOut,
                   starting, rn, rnE, rnAge, want, eager, obs, crashes, leaves, afails, strays, lates, fell>>

----------------------------------------------------------------------------
(* Watches: Cluster::watch_key, spawn.rs Watcher::next and Inner::watch_slot *)

\* One watcher asks node o, the owner in its view, which may be any node. A node that cannot be asked, or
\* runs nothing live for the key under the lease it holds, is shown Stopped; that shows no epoch.
Watch(o) ==
    /\ LET live == act[o] = "running" /\ cE[o] = actE[o] /\ cLeft[o] > 0
           shown == IF st[o] \notin {"up", "leaving"} THEN 0
                    ELSE IF WatchAnyActor THEN (IF act[o] # "none" THEN actE[o] ELSE pend[o])
                    ELSE IF (act[o] = "running" \/ (WatchDraining /\ act[o] = "draining"))
                            /\ cE[o] = actE[o] /\ (cLeft[o] > 0 \/ "WatchTrustsDue" \in Open)
                         THEN actE[o] ELSE 0
       IN /\ shown # 0
          /\ watchBad' = (watchBad \/ shown < wMax \/ (shown = wMax /\ o # wNode))
          /\ watchDead' = (watchDead \/ ~live)
          /\ wMax' = Max(wMax, shown)
          /\ wNode' = IF shown >= wMax THEN o ELSE wNode
    /\ UNCHANGED <<st, aH, aE, aLeft, aOld, lastE, hE, hLeft, cE, cLeft, lostE, act, actE, fence, pend, moveOut,
                   starting, moved, rebal, rn, rnE, rnAge, want, eager, sE, writeBad,
                   crashes, leaves, afails, strays, lates, reach, fell>>

----------------------------------------------------------------------------

Work(n) ==
    \/ RenewSend(n) \/ RenewReply(n) \/ Lose(n) \/ FenceStop(n) \/ Exit(n) \/ Vacate(n)
    \/ StartBegin(n) \/ Acquire(n) \/ GiveUp(n) \/ AcquireLate(n) \/ Rebalance(n) \/ FinishLeave(n) \/ Gone(n)
    \/ StopAll(n) \/ EarlyHandOff(n) \/ Restart(n)

\* Handlers and watches change only what the invariants read, so the liveness specs leave them out.
Run ==
    \/ Tick \/ LateTick
    \/ \E n \in Nodes : Work(n) \/ BeginLeave(n) \/ Crash(n)
    \/ Cut

Next ==
    \/ Run
    \/ \E n \in Nodes : Watch(n) \/ Handle(n, TRUE) \/ Handle(n, FALSE)

\* Every node does its work, and time passes. Faults, restarts and stray starts need no fairness.
Fairness ==
    /\ SF_vars(Tick)
    /\ \A n \in Nodes :
        /\ WF_vars(RenewSend(n) /\ rn'[n] = "ok") /\ WF_vars(RenewSend(n) /\ rn'[n] = "lost")
        /\ WF_vars(RenewSend(n) /\ ~grant[n])     \* a cut, unlike an authority fault, keeps failing
        /\ WF_vars(RenewReply(n)) /\ WF_vars(Lose(n)) /\ WF_vars(FenceStop(n))
        /\ WF_vars(Exit(n)) /\ WF_vars(Vacate(n))
        /\ WF_vars(StartBegin(n) /\ Demand(n)) /\ WF_vars(Acquire(n)) /\ WF_vars(Rebalance(n))
        /\ WF_vars(StopAll(n)) /\ WF_vars(FinishLeave(n)) /\ WF_vars(Gone(n))

Spec == Init /\ [][Next]_vars
LiveSpec == Init /\ [][Run]_vars /\ Fairness

\* Two states that differ only in what can no longer matter are the same state: a lapsed lease's holder
\* and epoch, a lapsed hold, a renewal not out, and what only the cluster's tasks read on a node whose
\* tasks have stopped (they never run again there: a leave ends in gone, a crash clears them).
View ==
    LET A(n, x, z) == IF Alive(n) THEN x ELSE z
        U(n, x) == st[n] = "up" /\ x
    IN
    <<st, IF aLeft > 0 \/ Eager THEN aH ELSE None, IF aLeft > 0 THEN aE ELSE 0, aLeft, aLeft > 0 /\ aOld, lastE,
      [n \in Nodes |-> IF hLeft[n] > 0 THEN hE[n] ELSE 0], hLeft,
      [n \in Nodes |-> A(n, cE[n], 0)], [n \in Nodes |-> A(n, cLeft[n], 0)],
      [n \in Nodes |-> IF Alive(n) /\ hLeft[n] > 0 /\ lostE[n] = hE[n] THEN lostE[n] ELSE 0], act,
      [n \in Nodes |-> IF act[n] # "none" THEN actE[n] ELSE 0], fence,
      [n \in Nodes |-> A(n, pend[n], 0)], [n \in Nodes |-> A(n, moveOut[n], FALSE)], starting,
      [n \in Nodes |-> starting[n] /\ moved[n]], [n \in Nodes |-> U(n, rebal[n])],
      [n \in Nodes |-> A(n, rn[n], "idle")],
      [n \in Nodes |-> IF Alive(n) /\ rn[n] # "idle" THEN rnE[n] ELSE 0],
      [n \in Nodes |-> A(n, rnAge[n], 0)],
      IF Follows THEN want ELSE [n \in Nodes |-> U(n, want[n])],
      eager, obs, crashes, leaves, afails, strays, lates,
      reach, [n \in Nodes |-> act[n] # "none" /\ fell[n]]>>

\* guide/cluster.md "One owner per key": at no instant do two nodes run the key's actor able to handle
\* a message, its fence open.
OneUnfenced == \A m, n \in Nodes : m # n => ~(Unfenced(m) /\ Unfenced(n))

\* guide/cluster.md: pass the epoch to what you write to and have it refuse older ones. With handlers at
\* one instant, the fence alone keeps that refusal from ever being needed: writes never go back in epoch.
WritesInOrder == ~writeBad

\* guide/cluster.md: "Under a lease authority a move runs in order: the old actor's on_stop, the release
\* of its lease, then the new actor's start". No node runs the key while another's actor has not ended
\* and its fence is open; an actor fenced by a lost lease or a leave's close is past its lease, not moved.
MoveInOrder == \A m, n \in Nodes : m # n => ~(Unfenced(m) /\ Holding(n))

\* guide/cluster.md watch_key: "Under a lease authority the epochs shown never go back", "so no epoch is
\* ever shown on two nodes". The same epoch may show again on its node; the guide says only after a
\* Stopped for an owner that could not be asked, but a start answered from the hold after its actor ended
\* shows it again on the same node.
WatchInOrder == ~watchBad

\* guide/cluster.md watch_key: an actor that has lost its lease, or is stopping, shows as Stopped. Watch
\* shows an epoch only when `live` holds, so this guards the watch_slot filter (its mutations), not the
\* protocol.
WatchShowsLive == ~watchDead

Served == Owner # None /\ act[Owner] = "running" /\ fence[Owner] > 0

\* guide/cluster.md: a key with callers is served again. Once a node stays up, the owner runs it for good.
KeyServed == <>[](Owner # None) => <>[]Served

\* Without callers ("A clean leave hands each key to its new owner at once", and an eager kind restarts a
\* gone node's keys): the same, as long as some node was up all along. A leave or a crash that finds no
\* node to take the key leaves it for a caller: acceptors list only keys still held, not released ones.
KeyServedAlone == [](Owner # None) => <>[]Served

\* guide/cluster.md "One owner per key", under a cut that is not a clean split: the callers are answered, by
\* the node their walk lands on or by the holder it names, for good, once they reach every node that can take
\* the lease and one can. A holder the callers cannot reach, cut from a granting owner, keeps the key as long as
\* it keeps its lease, so the premise asks the callers to reach every node that can grant.
Runs(n) == st[n] = "up" /\ act[n] = "running" /\ fence[n] > 0
CallerServed ==
    /\ Entry # None
    /\ \/ Runs(Entry)
       \/ ~NoRedirect /\ aH # None /\ aH # Entry /\ aLeft > 0 /\ ~aOld /\ cr[aH] /\ Runs(aH)
Reachable == (\E n \in Candidates : grant[n]) /\ \A n \in Candidates : grant[n] => cr[n]
KeyServedReachable == <>[]Reachable => <>[]CallerServed

=============================================================================
