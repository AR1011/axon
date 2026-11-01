---------------------------- MODULE PaxosLeases ----------------------------
(* PaxosLease for one key, as in spec/paxos-leases.md §2. Keys are independent, so one is
   enough. Time is real time in ticks. A clock that drifts is modelled by its worst case: an
   acceptor keeps a lease at least AcceptorLease ticks, a holder believes at most HolderLease
   ticks. Messages are a set: never removed, so they can be lost, duplicated and reordered.
   Refusals are not modelled: a refused proposer gives up (GiveUp), and what it would learn
   from the refusal only raises its next epoch, which waiting does too.
   With Durable each acceptor logs who holds the key (spec/paxos-leases.md §4.1), written with the
   accept before it is answered, and a restart takes the lease back from the log as run out: the
   log lists it (Listed), it never decides. The log also keeps the highest epoch granted (top),
   which a release does not clear, and with LogFloor a restart's floor is at least it. Not
   modelled: the sweep forgetting a lease LISTED after it ran out, and the log's rewrite, which
   holds the same leases and top. *)
EXTENDS Integers, FiniteSets, TLC

CONSTANTS
    Acceptors,          \* model values, symmetric
    Proposers,          \* positive integers
    AcceptorLease,
    HolderLease,
    RestartWait,
    MaxSkew,            \* how far a node's clock may run ahead of real time
    MaxTime,
    MaxPrepares,        \* ballots each proposer may try
    MaxCrashes,
    HolderFromReply,    \* TRUE: the holder's timer starts at the reply, not at the send
    EpochTies,          \* TRUE: equal epochs are ordered by node, so two nodes can win with one epoch
    FloorAtCrash,       \* TRUE: a restarted acceptor's floor is its clock at the crash, not after the wait
    Durable,            \* TRUE: acceptors log the holder, and a restart lists what the log holds
    AnswerBeforeDisk,   \* TRUE: an accept is answered before it is logged, which a later step does
    LogFloor            \* TRUE: a restarted acceptor's floor is at least the highest epoch its log granted

VARIABLES
    now, skew, crashes,
    promise, acc, accEnd, warmEnd,          \* acceptors
    phase, renewing, bal, sent, holdEnd, seen, prepares,  \* proposers
    msgs,
    disk, top,                              \* each acceptor's log: the lease it holds, the highest epoch granted
    lastGrant, epochBad,                    \* history, for EpochsGrow
    granted, freed                          \* history, for Listed: the newest ballot held, and whether released

vars == <<now, skew, crashes, promise, acc, accEnd, warmEnd,
          phase, renewing, bal, sent, holdEnd, seen, prepares, msgs, disk, top, lastGrant, epochBad, granted,
          freed>>
listing == <<disk, top, granted, freed>>

Zero == [e |-> 0, p |-> 0]
NoLease == [b |-> Zero, h |-> 0]
\* An acceptor takes only a larger epoch, or the ballot it already promised: two nodes never
\* win with one epoch, so the epoch alone fences.
Admits(b, promised) ==
    IF EpochTies THEN b.e > promised.e \/ (b.e = promised.e /\ b.p >= promised.p)
    ELSE b.e > promised.e \/ b = promised
Max(x, y) == IF x > y THEN x ELSE y
Majorities == {Q \in SUBSET Acceptors : 2 * Cardinality(Q) > Cardinality(Acceptors)}
Live(a) == acc[a].h # 0 /\ now < accEnd[a]
Holding(p) == now < holdEnd[p]
\* h: the holder of the live lease a promise reports, 0 for none.
Msg(t, from, b, h, s) == [t |-> t, from |-> from, b |-> b, h |-> h, s |-> s]
Send(m) == msgs' = msgs \cup {m}

Init ==
    /\ now = 0
    /\ skew \in [Acceptors \cup Proposers -> 0..MaxSkew]
    /\ crashes = 0
    /\ promise = [a \in Acceptors |-> Zero]
    /\ acc = [a \in Acceptors |-> NoLease]
    /\ accEnd = [a \in Acceptors |-> 0]
    /\ warmEnd = [a \in Acceptors |-> 0]
    /\ phase = [p \in Proposers |-> "idle"]
    /\ renewing = [p \in Proposers |-> FALSE]
    /\ bal = [p \in Proposers |-> Zero]
    /\ sent = [p \in Proposers |-> 0]
    /\ holdEnd = [p \in Proposers |-> 0]
    /\ seen = [p \in Proposers |-> 0]
    /\ prepares = [p \in Proposers |-> 0]
    /\ msgs = {}
    /\ disk = [a \in Acceptors |-> NoLease]
    /\ top = [a \in Acceptors |-> 0]
    /\ lastGrant = 0
    /\ epochBad = FALSE
    /\ granted = Zero
    /\ freed = FALSE

Tick ==
    /\ now < MaxTime
    /\ now' = now + 1
    /\ UNCHANGED <<skew, crashes, promise, acc, accEnd, warmEnd,
                   phase, renewing, bal, sent, holdEnd, seen, prepares, msgs, lastGrant, epochBad, listing>>

----------------------------------------------------------------------------
(* Proposers *)

\* Acquire, or renew through a new ballot when the old one was overtaken.
Prepare(p) ==
    LET e == Max(now + skew[p], seen[p] + 1)
        b == [e |-> e, p |-> p]
    IN /\ phase[p] = "idle"
       /\ prepares[p] < MaxPrepares
       /\ prepares' = [prepares EXCEPT ![p] = @ + 1]
       /\ bal' = [bal EXCEPT ![p] = b]
       /\ seen' = [seen EXCEPT ![p] = e]
       /\ phase' = [phase EXCEPT ![p] = "preparing"]
       /\ renewing' = [renewing EXCEPT ![p] = Holding(p)]
       /\ Send(Msg("prepare", p, b, 0, 0))
       /\ UNCHANGED <<now, skew, crashes, promise, acc, accEnd, warmEnd, sent, holdEnd, lastGrant, epochBad, listing>>

\* A majority promised. Another node's live lease in any promise means Held.
Promised(p) ==
    /\ phase[p] = "preparing"
    /\ \E Q \in Majorities :
        LET ms == {m \in msgs : m.t = "promise" /\ m.b = bal[p] /\ m.from \in Q}
        IN /\ \A a \in Q : \E m \in ms : m.from = a
           /\ IF \E m \in ms : m.h \notin {0, p}
              THEN /\ phase' = [phase EXCEPT ![p] = "idle"]
                   /\ UNCHANGED <<sent, msgs>>
              ELSE /\ phase' = [phase EXCEPT ![p] = "proposing"]
                   /\ sent' = [sent EXCEPT ![p] = now]
                   /\ Send(Msg("propose", p, bal[p], 0, now))
    /\ UNCHANGED <<now, skew, crashes, promise, acc, accEnd, warmEnd, renewing, bal, holdEnd, seen, prepares, lastGrant,
                   epochBad, listing>>

\* Renew with the same ballot: one round trip.
Renew(p) ==
    /\ phase[p] = "idle"
    /\ Holding(p)
    /\ phase' = [phase EXCEPT ![p] = "proposing"]
    /\ renewing' = [renewing EXCEPT ![p] = TRUE]
    /\ sent' = [sent EXCEPT ![p] = now]
    /\ Send(Msg("propose", p, bal[p], 0, now))
    /\ UNCHANGED <<now, skew, crashes, promise, acc, accEnd, warmEnd, bal, holdEnd, seen, prepares, lastGrant, epochBad,
                   listing>>

\* A majority accepted the latest propose. A renew that comes back after the lease ran out
\* grants nothing: the actors have stopped, and starting them again is a new acquire.
Accepted(p) ==
    LET end == IF HolderFromReply THEN now + HolderLease ELSE sent[p] + HolderLease
        grant == ~renewing[p] /\ now < end
    IN /\ phase[p] = "proposing"
       /\ \E Q \in Majorities : \A a \in Q : Msg("accepted", a, bal[p], 0, sent[p]) \in msgs
       /\ phase' = [phase EXCEPT ![p] = "idle"]
       /\ holdEnd' = IF grant \/ (renewing[p] /\ Holding(p))
                     THEN [holdEnd EXCEPT ![p] = end]
                     ELSE holdEnd
       /\ epochBad' = (epochBad \/ (grant /\ bal[p].e <= lastGrant))
       /\ lastGrant' = IF grant THEN Max(lastGrant, bal[p].e) ELSE lastGrant
       \* A grant, or a renewal through a new ballot: the ballot a majority holds for p now.
       /\ IF (grant \/ (renewing[p] /\ Holding(p))) /\ bal[p] # granted
          THEN granted' = bal[p] /\ freed' = FALSE
          ELSE UNCHANGED <<granted, freed>>
       /\ UNCHANGED <<now, skew, crashes, promise, acc, accEnd, warmEnd, renewing, bal, sent, seen, prepares, msgs, disk,
                      top>>

GiveUp(p) ==
    /\ phase[p] # "idle"
    /\ phase' = [phase EXCEPT ![p] = "idle"]
    /\ UNCHANGED <<now, skew, crashes, promise, acc, accEnd, warmEnd, renewing, bal, sent, holdEnd, seen, prepares, msgs,
                   lastGrant, epochBad, listing>>

\* The holder stops believing first, then tells the acceptors.
Release(p) ==
    /\ phase[p] = "idle"
    /\ Holding(p)
    /\ holdEnd' = [holdEnd EXCEPT ![p] = 0]
    /\ Send(Msg("release", p, bal[p], 0, 0))
    \* The node gives the key up, whichever ballot it releases by: a renewal that failed can leave it
    \* holding an older one at some acceptors.
    /\ freed' = (freed \/ granted.p = p)
    /\ UNCHANGED <<now, skew, crashes, promise, acc, accEnd, warmEnd, phase, renewing, bal, sent, seen, prepares, lastGrant,
                   epochBad, disk, top, granted>>

----------------------------------------------------------------------------
(* Acceptors *)

OnPrepare(a) ==
    /\ now >= warmEnd[a]
    /\ \E m \in msgs :
        LET h == IF Live(a) THEN acc[a].h ELSE 0
        IN /\ m.t = "prepare"
           /\ Admits(m.b, promise[a])
           \* The proposer gives up on a promise that reports another node's lease: recording it
           \* would only refuse the holder's renewals.
           /\ promise' = IF h \notin {0, m.b.p} THEN promise ELSE [promise EXCEPT ![a] = m.b]
           /\ Send(Msg("promise", a, m.b, h, 0))
    /\ UNCHANGED <<now, skew, crashes, acc, accEnd, warmEnd, phase, renewing, bal, sent, holdEnd, seen, prepares, lastGrant,
                   epochBad, listing>>

OnPropose(a) ==
    /\ now >= warmEnd[a]
    /\ \E m \in msgs :
        /\ m.t = "propose"
        /\ Admits(m.b, promise[a])
        /\ promise' = [promise EXCEPT ![a] = m.b]
        /\ acc' = [acc EXCEPT ![a] = [b |-> m.b, h |-> m.b.p]]
        /\ accEnd' = [accEnd EXCEPT ![a] = now + AcceptorLease]
        \* Logged, and synced, before the answer leaves.
        /\ disk' = IF Durable /\ ~AnswerBeforeDisk THEN [disk EXCEPT ![a] = acc'[a]] ELSE disk
        /\ top' = IF Durable /\ ~AnswerBeforeDisk THEN [top EXCEPT ![a] = Max(@, m.b.e)] ELSE top
        /\ Send(Msg("accepted", a, m.b, 0, m.s))
    /\ UNCHANGED <<now, skew, crashes, warmEnd, phase, renewing, bal, sent, holdEnd, seen, prepares, lastGrant, epochBad,
                   granted, freed>>

OnRelease(a) ==
    /\ \E m \in msgs :
        /\ m.t = "release"
        /\ acc[a].b = m.b
        /\ acc' = [acc EXCEPT ![a] = NoLease]
        /\ disk' = IF Durable /\ ~AnswerBeforeDisk THEN [disk EXCEPT ![a] = NoLease] ELSE disk
    /\ UNCHANGED <<now, skew, crashes, promise, accEnd, warmEnd, phase, renewing, bal, sent, holdEnd, seen, prepares, msgs,
                   top, lastGrant, epochBad, granted, freed>>

\* AnswerBeforeDisk: the log catches up with what the acceptor already answered.
Persist(a) ==
    /\ Durable /\ AnswerBeforeDisk
    /\ disk[a] # acc[a]
    /\ disk' = [disk EXCEPT ![a] = acc[a]]
    /\ top' = [top EXCEPT ![a] = Max(@, acc[a].b.e)]
    /\ UNCHANGED <<now, skew, crashes, promise, acc, accEnd, warmEnd, phase, renewing, bal, sent, holdEnd, seen, prepares,
                   msgs, lastGrant, epochBad, granted, freed>>

\* Restart: forget every promise, refuse for RestartWait, promise floor = own clock when the wait
\* ends. A prepare sent before the crash can arrive after the wait, and a floor taken at the crash
\* would admit it. The lease comes back from the log, run out: listed, and released by its ballot,
\* but not a promise, which would admit that ballot's late propose. With LogFloor the floor is at
\* least the highest epoch the log granted, so epochs grow however far behind the clock restarts.
Crash(a) ==
    /\ crashes < MaxCrashes
    /\ crashes' = crashes + 1
    /\ promise' = [promise EXCEPT ![a] =
                    [e |-> Max(now + skew[a] + (IF FloorAtCrash THEN 0 ELSE RestartWait),
                               IF Durable /\ LogFloor THEN top[a] ELSE 0),
                     p |-> 0]]
    /\ acc' = [acc EXCEPT ![a] = IF Durable THEN disk[a] ELSE NoLease]
    /\ accEnd' = [accEnd EXCEPT ![a] = 0]
    /\ warmEnd' = [warmEnd EXCEPT ![a] = now + RestartWait]
    /\ UNCHANGED <<now, skew, phase, renewing, bal, sent, holdEnd, seen, prepares, msgs, lastGrant, epochBad, listing>>

----------------------------------------------------------------------------

Next ==
    \/ Tick
    \/ \E p \in Proposers : Prepare(p) \/ Promised(p) \/ Renew(p) \/ Accepted(p)
                            \/ GiveUp(p) \/ Release(p)
    \/ \E a \in Acceptors : OnPrepare(a) \/ OnPropose(a) \/ OnRelease(a) \/ Persist(a) \/ Crash(a)

Spec == Init /\ [][Next]_vars

Symmetry == Permutations(Acceptors)

\* Two states that differ only in what can no longer matter are the same state: an expired
\* lease where no log lists it, an idle proposer's timers, a reply to a round that is over.
Alive(m) ==
    LET q == m.b.p
        current == \/ phase[q] = "proposing" /\ m.s = sent[q]
                   \/ phase[q] = "idle" /\ Holding(q) /\ m.s = now
    IN CASE m.t = "promise" -> phase[q] = "preparing" /\ m.b = bal[q]
         [] m.t = "accepted" -> m.b = bal[q] /\ current
         [] OTHER -> TRUE
View ==
    <<now, skew, crashes, promise,
      [a \in Acceptors |-> IF Live(a) \/ Durable THEN acc[a] ELSE NoLease],
      [a \in Acceptors |-> IF Live(a) THEN accEnd[a] ELSE 0],
      [a \in Acceptors |-> IF now < warmEnd[a] THEN warmEnd[a] ELSE 0],
      phase,
      [p \in Proposers |-> phase[p] # "idle" /\ renewing[p]],
      [p \in Proposers |-> IF phase[p] = "idle" /\ ~Holding(p) THEN Zero ELSE bal[p]],
      [p \in Proposers |-> IF phase[p] = "idle" THEN 0 ELSE sent[p]],
      [p \in Proposers |-> IF Holding(p) THEN holdEnd[p] ELSE 0],
      seen, prepares, {m \in msgs : Alive(m)}, disk, top, lastGrant, epochBad, granted, freed>>

\* At no instant do two nodes believe they hold the key.
MutualExclusion == \A p, q \in Proposers : p # q => ~(Holding(p) /\ Holding(q))

\* Every grant of the key carries a larger epoch than every grant before it.
EpochsGrow == ~epochBad

\* What eager keys need after every acceptor restarts: the newest ballot a node held and did not
\* release is listed by every majority of warm acceptors, as that ballot or a newer one.
Listed ==
    granted # Zero /\ ~freed =>
        \A Q \in Majorities :
            (\A a \in Q : now >= warmEnd[a]) => \E a \in Q : acc[a].h # 0 /\ acc[a].b.e >= granted.e

=============================================================================
