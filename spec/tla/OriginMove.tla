---------------------------- MODULE OriginMove ----------------------------
(* One stream whose origin moves between two nodes, "a" and "b", in one generation for good
   (spec/streams.md § 2). The application keeps a history the stream is published from: a
   committed log, written by one writer at a time and committed once the other node holds it.
   The writer opens the stream with open_stream_after(last committed); a node that is no longer
   the writer closes it. One relay pulls the key from whichever node is open, asking After(its
   position); two subscribers on the relay's node take what it holds: a Live follower and one that
   opened FollowAfter(0). Faults: a node crashes (its stream and its uncommitted record are lost,
   the log is kept) or pauses (nothing moves), and comes back; the writer moves to the other node
   at any time, a lease lapsing, up to MaxMoves times; an origin's ring gives up its oldest record.

   The question: across a move, does a subscriber get every position once, in order, each with the
   record the history holds there, and is it never ended?

   Abstractions, and what they could hide:
   - Records carry the term of the writer that wrote them, so a position written twice by two
     writers shows as two values (SameRecord).
   - The history is the committed log itself: an origin serves a position its ring gave up from it
     (the RecoverySource of § 1), in one step. Pending and history_ready are a delay, not modelled.
   - The relay's ring holds every record it took: a subscriber that falls behind it is the
     slow-consumer reset, out of scope. A start behind the relay's ring is § 1's, not this model's.
   - An interest and its answer are one step (Attach, then Pull). A parent behind the relay is
     waited for, as core.rs answers a child ahead of it live and accept_batch drops what it holds.
   - The relay sees a close of its parent as a close when the parent is up, and a crash or pause
     as a loss, at any time while the parent is down (upstream_timeout folded in).
   - The standby is not itself a relay of the key (the open_read path that makes a relay the
     origin is tested in world_relay.rs), and links are not cut apart from the node.
   - The app's fence: a node that is no longer the writer commits nothing, and publishes only
     what it knew committed before it lost the lease (known). *)
EXTENDS Naturals, Sequences, FiniteSets

CONSTANTS
    MaxSeq,     \* records the log commits in all
    MaxMoves,   \* moves of the writer
    MaxFaults,  \* crashes and pauses in all
    Mutation    \* "none", or the bug a mutations/OriginMove-*.cfg brings back; each is named where it acts.
                \* TrustRelay takes RepullForgets with it: a follower's own bug shows once its relay forgets.

Nodes == {"a", "b"}
Subs == {"live", "after"}
None == "none"
Other(n) == IF n = "a" THEN "b" ELSE "a"
Pos == 1..(MaxSeq + 1)
Has(m) == Mutation = m \/ (Mutation = "TrustRelay" /\ m = "RepullForgets")

VARIABLES
    log, tail, writer, term, moves, faults, known,  \* the application's history and its writer
    up, open, pub, ringLo,                          \* each node's stream: open, its records, its ring's oldest
    rparent, rstart, rvals, rfresh,                 \* the relay: its parent, records rstart+1 .. rstart+Len(rvals)
    at, last, got, ended, flaws                     \* the subscribers; last, got and flaws are history

vars == <<log, tail, writer, term, moves, faults, known, up, open, pub, ringLo,
          rparent, rstart, rvals, rfresh, at, last, got, ended, flaws>>

RPos == rstart + Len(rvals)
Min(a, b) == IF a < b THEN a ELSE b

TypeOK ==
    /\ Len(log) <= MaxSeq /\ \A i \in 1..Len(log) : log[i] \in 1..(MaxMoves + 1)
    /\ writer \in Nodes /\ up \in [Nodes -> BOOLEAN] /\ open \in [Nodes -> BOOLEAN]
    /\ rparent \in Nodes \cup {None} /\ rfresh \in BOOLEAN
    /\ at \in [Subs -> 0..(MaxSeq + 1)] /\ last \in [Subs -> 0..(MaxSeq + 1)]
    /\ got \in [Subs -> [Pos -> 0..(MaxMoves + 1)]] /\ ended \in [Subs -> BOOLEAN]
    /\ flaws \subseteq {"repeat", "gap"}

Init ==
    /\ log = <<>> /\ tail = <<>> /\ writer = "a" /\ term = 1 /\ moves = 0 /\ faults = 0
    /\ known = [n \in Nodes |-> 0]
    /\ up = [n \in Nodes |-> TRUE]
    /\ open = [n \in Nodes |-> n = "a"]
    /\ pub = [n \in Nodes |-> <<>>]
    /\ ringLo = [n \in Nodes |-> 1]
    /\ rparent = None /\ rstart = 0 /\ rvals = <<>> /\ rfresh = FALSE
    /\ at = [s \in Subs |-> 0] /\ last = [s \in Subs |-> 0]
    /\ got = [s \in Subs |-> [p \in Pos |-> 0]] /\ ended = [s \in Subs |-> FALSE]
    /\ flaws = {}

relay == <<rparent, rstart, rvals, rfresh>>
subs == <<at, last, got, ended, flaws>>

---------------------------------------------------------------------------
\* The application's history.

Write ==
    /\ up[writer] /\ tail = <<>> /\ Len(log) < MaxSeq
    /\ tail' = <<term>>
    /\ UNCHANGED <<log, writer, term, moves, faults, known, up, open, pub, ringLo>> /\ UNCHANGED relay /\ UNCHANGED subs

\* Committed once the other node holds it: the record survives a move.
Commit ==
    /\ tail # <<>> /\ up[writer] /\ up[Other(writer)]
    /\ log' = log \o tail /\ tail' = <<>>
    /\ known' = [known EXCEPT ![writer] = Len(log) + 1]
    /\ UNCHANGED <<writer, term, moves, faults, up, open, pub, ringLo>> /\ UNCHANGED relay /\ UNCHANGED subs

\* The lease moves. The old writer's uncommitted record is dropped: it can never commit.
\* StaleOpen: the new writer's copy lags the commit by one record, which it then writes over.
Promote(n) ==
    /\ n # writer /\ up[n] /\ moves < MaxMoves
    /\ writer' = n /\ term' = term + 1 /\ moves' = moves + 1 /\ tail' = <<>>
    /\ log' = IF Has("StaleOpen") /\ log # <<>> THEN SubSeq(log, 1, Len(log) - 1) ELSE log
    /\ known' = [known EXCEPT ![n] = Len(log')]
    /\ UNCHANGED <<faults, up, open, pub, ringLo>> /\ UNCHANGED relay /\ UNCHANGED subs

---------------------------------------------------------------------------
\* Each node's stream.

\* open_stream_after(last committed): the stream goes on in its generation, its ring empty.
OpenAfter(n) ==
    /\ n = writer /\ up[n] /\ ~open[n]
    /\ open' = [open EXCEPT ![n] = TRUE]
    /\ pub' = [pub EXCEPT ![n] = log]
    /\ ringLo' = [ringLo EXCEPT ![n] = Len(log) + 1]
    /\ UNCHANGED <<log, tail, writer, term, moves, faults, known, up>> /\ UNCHANGED relay /\ UNCHANGED subs

\* Publishes only what the history holds. PublishBeforeCommit: the writer publishes its record first.
Publish(n) ==
    /\ up[n] /\ open[n]
    /\ \/ /\ Len(pub[n]) < Min(known[n], Len(log))
          /\ pub' = [pub EXCEPT ![n] = Append(@, log[Len(pub[n]) + 1])]
       \/ /\ Has("PublishBeforeCommit") /\ n = writer /\ tail # <<>> /\ Len(pub[n]) = Len(log)
          /\ pub' = [pub EXCEPT ![n] = Append(@, tail[1])]
    /\ UNCHANGED <<log, tail, writer, term, moves, faults, known, up, open, ringLo>> /\ UNCHANGED relay /\ UNCHANGED subs

\* A node that lost the lease closes the stream (close_stream). NeverClose: it keeps it open.
Close(n) ==
    /\ up[n] /\ open[n] /\ n # writer /\ ~Has("NeverClose")
    /\ open' = [open EXCEPT ![n] = FALSE] /\ pub' = [pub EXCEPT ![n] = <<>>]
    /\ UNCHANGED <<log, tail, writer, term, moves, faults, known, up, ringLo>> /\ UNCHANGED relay /\ UNCHANGED subs

RollRing(n) ==
    /\ open[n] /\ ringLo[n] <= Len(pub[n])
    /\ ringLo' = [ringLo EXCEPT ![n] = @ + 1]
    /\ UNCHANGED <<log, tail, writer, term, moves, faults, known, up, open, pub>> /\ UNCHANGED relay /\ UNCHANGED subs

Crash(n) ==
    /\ up[n] /\ faults < MaxFaults
    /\ up' = [up EXCEPT ![n] = FALSE] /\ open' = [open EXCEPT ![n] = FALSE] /\ pub' = [pub EXCEPT ![n] = <<>>]
    /\ tail' = IF n = writer THEN <<>> ELSE tail
    /\ faults' = faults + 1
    /\ UNCHANGED <<log, writer, term, moves, known, ringLo>> /\ UNCHANGED relay /\ UNCHANGED subs

Pause(n) ==
    /\ up[n] /\ faults < MaxFaults
    /\ up' = [up EXCEPT ![n] = FALSE] /\ faults' = faults + 1
    /\ UNCHANGED <<log, tail, writer, term, moves, known, open, pub, ringLo>> /\ UNCHANGED relay /\ UNCHANGED subs

Resume(n) ==
    /\ ~up[n]
    /\ up' = [up EXCEPT ![n] = TRUE]
    /\ UNCHANGED <<log, tail, writer, term, moves, faults, known, open, pub, ringLo>> /\ UNCHANGED relay /\ UNCHANGED subs

origin == <<log, tail, writer, term, moves, faults, known, up, open, pub, ringLo>>

---------------------------------------------------------------------------
\* The relay.

\* TrustRelay: a follower takes the relay's new position as its own (today's Ready { start }).
Adopt(start) ==
    at' = IF Has("TrustRelay") THEN [s \in Subs |-> IF ended[s] THEN at[s] ELSE start] ELSE at

\* Asks the parent After(its position). RepullForgets: after a close it forgot that position and
\* asks First, which a parent whose ring gave up record 1 answers from its live edge.
Attach(p) ==
    /\ rparent = None /\ up[p] /\ open[p]
    /\ rparent' = p /\ rfresh' = FALSE
    /\ IF rfresh
       THEN LET start == IF ringLo[p] = 1 THEN 0 ELSE Len(pub[p])
            IN /\ rstart' = start /\ rvals' = <<>> /\ Adopt(start)
       ELSE /\ UNCHANGED <<rstart, rvals>> /\ at' = at
    /\ UNCHANGED origin /\ UNCHANGED <<last, got, ended, flaws>>

\* The next record, from the ring or from the history behind it. NoSource: only from the ring; a
\* parent refuses HistoryUnavailable and the relay takes the parent's live edge.
Pull ==
    /\ rparent # None /\ up[rparent] /\ open[rparent]
    /\ RPos + 1 <= Len(pub[rparent])
    /\ IF RPos + 1 >= ringLo[rparent] \/ ~Has("NoSource")
       THEN /\ rvals' = Append(rvals, pub[rparent][RPos + 1]) /\ UNCHANGED rstart /\ at' = at
       ELSE /\ rstart' = Len(pub[rparent]) /\ rvals' = <<>> /\ Adopt(Len(pub[rparent]))
    /\ UNCHANGED origin /\ UNCHANGED <<rparent, rfresh, last, got, ended, flaws>>

\* The parent closed (it is up) or was lost. Subscribers park. CloseEnds: a close ends the
\* subscriber that holds a cursor, End(StreamClosed), as a plain After does today.
ParentGone ==
    /\ rparent # None /\ (~up[rparent] \/ ~open[rparent])
    /\ rparent' = None
    /\ LET closed == up[rparent] /\ ~open[rparent]
       IN /\ rfresh' = (rfresh \/ (closed /\ Has("RepullForgets")))
          /\ ended' = IF closed /\ Has("CloseEnds") THEN [ended EXCEPT !["after"] = TRUE] ELSE ended
    /\ UNCHANGED origin /\ UNCHANGED <<rstart, rvals, at, last, got, flaws>>

---------------------------------------------------------------------------
\* The subscribers, at the hub of the relay's node.

Take(s, q) ==
    /\ at' = [at EXCEPT ![s] = q]
    /\ last' = [last EXCEPT ![s] = IF q > @ THEN q ELSE @]
    /\ got' = [got EXCEPT ![s][q] = IF @ = 0 THEN rvals[q - rstart] ELSE @]
    /\ flaws' = flaws \cup (IF q <= last[s] THEN {"repeat"} ELSE {})
                      \cup (IF q > last[s] + 1 THEN {"gap"} ELSE {})

\* The relay holds rstart+1 .. RPos. A follower behind rstart is moved past records: it ends with
\* ResetRequired(HistoryUnavailable) (spec/streams.md § 3), or with TrustRelay is moved in silence.
Deliver(s) ==
    /\ ~ended[s]
    /\ IF at[s] < rstart
       THEN IF Has("TrustRelay")
            THEN RPos > rstart /\ Take(s, rstart + 1) /\ UNCHANGED ended
            ELSE ended' = [ended EXCEPT ![s] = TRUE] /\ UNCHANGED <<at, last, got, flaws>>
       ELSE at[s] < RPos /\ Take(s, at[s] + 1) /\ UNCHANGED ended
    /\ UNCHANGED origin /\ UNCHANGED relay

---------------------------------------------------------------------------

Next ==
    \/ Write \/ Commit \/ \E n \in Nodes : Promote(n)
    \/ \E n \in Nodes : OpenAfter(n) \/ Publish(n) \/ Close(n) \/ RollRing(n) \/ Crash(n) \/ Pause(n) \/ Resume(n)
    \/ \E p \in Nodes : Attach(p)
    \/ Pull \/ ParentGone
    \/ \E s \in Subs : Deliver(s)

Fairness ==
    /\ WF_vars(Commit) /\ WF_vars(Pull) /\ WF_vars(ParentGone) /\ WF_vars(\E p \in Nodes : Attach(p))
    /\ \A n \in Nodes : WF_vars(OpenAfter(n)) /\ WF_vars(Publish(n)) /\ WF_vars(Close(n)) /\ WF_vars(Resume(n))
    /\ \A s \in Subs : WF_vars(Deliver(s))

Spec == Init /\ [][Next]_vars /\ Fairness

---------------------------------------------------------------------------

NoRepeat == "repeat" \notin flaws
NoGap == "gap" \notin flaws
\* What a subscriber got at a position is what the history holds there.
SameRecord == \A s \in Subs : \A p \in Pos : got[s][p] # 0 /\ p <= Len(log) => got[s][p] = log[p]
\* A move ends no subscriber: one generation, so no GenerationChanged, and no close is an end.
NoEnd == \A s \in Subs : ~ended[s]
\* Once the faults and moves are over, every subscriber reaches the last committed record.
CatchesUp == <>[](\A s \in Subs : last[s] = Len(log))

=============================================================================
