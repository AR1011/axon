----------------------------- MODULE QuotaGrant -----------------------------
(* The grants of one Permits key, as in spec/quotas.md §2.3 to §2.6: nodes 1..N take and give
   back permits and report to every replica; each replica keeps a row per node, divides the limit
   max-min fair (core.rs: targets) and moves each grant towards that within its budget (regrant);
   a node admits on the smallest fresh grant of a majority, or on the fallback. A replica may
   restart and pull the others' rows. Keys are independent, so one is enough.
   Time is real time in ticks. Drift is folded into two constants, as in PaxosLeases: a node
   believes a grant at most Hold ticks after it sent the report it answers, a replica counts a
   node's grants at least Lease ticks after it last heard from it.
   A report goes to each replica and its answer comes back at once (as in model.rs, a message not
   made late arrives before anything else happens), but for the copies a report's fault plan names,
   up to MaxFaults in all: the report or its answer is in flight, to arrive in any order at any
   later point, or never, which is a lost message (Late narrows this, for larger bounds). Up to
   MaxDups arrivals leave the message in flight, so it can arrive again. Sequence numbers do the
   rest. A replica may also push a node a grant set since its last message to the node, where
   regrant flagged it, at any point after, answering the last report it took from the node, and
   the node's reply acknowledges the push where it took it in (§2.3); up to MaxPushes in all. A
   report acknowledges the last answer the node took in. A replica keeps, per node, the messages
   that carried the entry: an acknowledgement settles the entry only where that message carried
   it. A message may leave a pending entry out where other keys fill it (RESEND, MAX_ITEMS), but an
   answer never leaves out the entry its report named; up to MaxLeft in all. A node that takes in
   a message after the grants of the link lapsed drops them, as the replica may have freed them.
   A node reports every change before time moves on (Tick), as model.rs does: a node silent with
   usage the replicas never heard of is §5's process pause, whose excess that row allows, and is
   not checked.
   Abstractions, and what they could hide:
   - Permits only. A rate differs in how a unit comes back (its window slides, core.rs: Used::Rate)
     and its usage reads high by up to a bucket (§2.1); neither is modelled.
   - Deliveries are ordered but for the faulted copies: with no faults, every report reaches every
     replica, and every answer its node, in one global order. Reports of two nodes reaching two
     replicas in opposite orders, or an answer on the wire while another report is taken in, each
     spend a fault. So QuotaGrantRestart.cfg checks a restart under that one order only.
   - A report after a take or a release carries the entry; any other carries it or not (a resync
     or a heartbeat), chosen freely: the node's resync rules (Link::resync) are not modelled, so
     every subset of them is covered.
   - Which pending entries a message carries past its bound (the cursor) is chosen freely, so every
     order is covered, and an entry may be left out for good: that each goes again in the end is
     liveness, held by a test (core/tests.rs), not here. The records past UNACKED are kept.
   - Demand: a refusal adds its cost, up to M; it lapses at the next tick at the node and at the
     replicas (a tick is a lease, five heartbeats; Entry::asking and asks keep two). The most held
     at once beyond usage (Entry::add's peak) is not added. Demand never enters the budget (§2.1),
     only the division, so it moves no bound; plans for larger demand are not explored.
   - Handover is a restart: a new process young for every key until it pulled (§2.6), which each
     node's view shows at a point of its own (ViewSees). The view never moves the key between live
     replicas, so a pull that pages rows as they change (d18d6f1) is not here. No node restarts, no
     rows dropped for silence (15 s) or after ROW_KEEP (120 s), which are past these bounds.
   - No replica pauses (Replica::paused), no eviction, no Denied::Limit vs Share (the used total a
     grant carries): the floor of a key new to a node (Entry::admit) admits on any refusal of its
     grants, not only one for a share, and for a tick after the one the entry was made in, where the
     code allows a heartbeat (a fifth of a tick). A push's reply is lost or arrives at once. A node looks up when the report a grant answers left
     (Link::commit over SENT_KEPT reports); here the report and its answer carry that time.
   - Hold = Lease = 1 and MaxTime = 1: every grant of tick 0 lapses at tick 1 at once, so grants of
     different ticks never overlap. Three mutations are not caught at any bound tried within 15
     minutes, so have no config: a stale grants message accepted (Link::accept without
     gseq > applied); an acknowledgement counted for a predecessor incarnation (before f1997e9;
     tried with a restart, alone and with two late reports); a pulled row carrying the grant, not
     the hold.
   Bug, a mutation to switch on, one at a time (mutations/QuotaGrant-<Bug>.cfg); "none" is the code:
     FloorAtUsage        targets floors a row at its usage (before b78e21b)
     RaiseFromUsage      a raise grows from usage even over the limit (model.rs's mutation, §7; before
                         f1997e9 a raise grew from held, never from usage)
     LargestGrant        a node admits on the largest fresh grant
     LowerAtSend         a lower counts from when it is sent
     SmallQuorum         one fresh grant is enough
     UntilAtArrival      a grant holds from when it arrived, not from when its report left
     UntilInclusive      a grant still counts at its until
     DropOldGen          the older hold generation is dropped at the mark, not kept
     AckEarly            a grants message acknowledges the one after it too
     PullNoHold          a pulled row carries no hold
     PushAcked           a push counts as acknowledged when it is sent
     AckUnheard          a push is acknowledged where the replica has not heard the node's usage
     FloorUncounted      the floor of a key new to a node is not counted with the fallback
     AckLeftOut          an acknowledgement settles the entry of a message that left it out
     LapsedKept          a grant that lapsed counts again after a message that left it out
   d724167 (room split in node order) is fairness of raises, not a bound. *)
EXTENDS Integers, FiniteSets, TLC

CONSTANTS
    N,                  \* nodes 1..N; node order matters (remainders go to the first rows)
    Replicas,           \* model values, symmetric
    M,                  \* the limit
    Costs,              \* what a take may cost
    Hold,               \* a node believes a grant this long after the report it answers left
    Lease,              \* a replica counts a node's grants this long after it last heard from it
    MaxTime,
    MaxTakes,           \* takes and releases, over all nodes
    MaxReports,         \* reports per node
    MaxRestarts,
    MaxDups,            \* arrivals that leave a message in flight
    MaxPushes,          \* grants a replica pushes unasked, over all replicas
    MaxFaults,          \* copies of reports lost or late
    MaxLeft,            \* messages that leave a pending entry out, over all
    Late,               \* what a fault does: "any" (a report or its answer late), "reports" (only
                        \* a report late), "lost" (a report never arrives)
    Taken,              \* TRUE: each node starts with an entry it took and gave back, not yet reported,
                        \* made long before (no floor)
    PulledRaise,        \* TRUE: as built; FALSE: regrant raises no pulled row over the limit (the gap)
    Bug

Nodes == 1..N
Quorum == IF Bug = "SmallQuorum" THEN 1 ELSE Cardinality(Replicas) \div 2 + 1
Fallback == (M + N - 1) \div N                  \* core.rs: Entry::bound, ceil(max / nodes)
None == -1
Max(x, y) == IF x > y THEN x ELSE y
Min(x, y) == IF x < y THEN x ELSE y
RECURSIVE SumSet(_, _)
SumSet(f, S) == IF S = {} THEN 0 ELSE LET x == CHOOSE x \in S : TRUE IN f[x] + SumSet(f, S \ {x})
\* What happens to each copy of one report: nothing, its report late, or its answer late.
Copies(F, what) == {r \in Replicas : F[r] \in what}
Plans(k) == {F \in [Replicas -> {"none", "report"} \cup (IF Late = "any" THEN {"answer"} ELSE {})] :
                Cardinality(Copies(F, {"report", "answer"})) <= k}

VARIABLES
    now,
    rep, pulledFrom, restarts,              \* replicas
    held, fb, dem, ent, born, owe, seq, link, grant,   \* nodes
    msgs, dups, faults, takes, pushes, lefts

vars == <<now, rep, pulledFrom, restarts, held, fb, dem, ent, born, owe, seq, link, grant, msgs, dups, faults, takes,
          pushes, lefts>>

\* core.rs: PeerState, Row (one key: pending, fresh and keys are flags), Replica.
\* sent: when the last report taken from the node left, which a push answers (the node looks it up).
\* carried: the grants messages to the node that carried the entry (PeerState::sent).
NoPeer == [known |-> FALSE, seq |-> 0, sent |-> None, heard |-> 0, pending |-> FALSE, fresh |-> FALSE, carried |-> {},
           haskey |-> FALSE, push |-> FALSE]
NewPeer(t) == [NoPeer EXCEPT !.known = TRUE, !.heard = t]
NoRow == [on |-> FALSE, used |-> 0, demand |-> 0, grant |-> 0, h0 |-> 0, h1 |-> 0,
          mark |-> 0, changed |-> 0, pulled |-> None]
NewReplica(inc, pulling) ==
    [inc |-> inc, pulling |-> pulling, gseq |-> 0,
     peer |-> [n \in Nodes |-> NoPeer], row |-> [n \in Nodes |-> NoRow]]
\* core.rs: Link, for the incarnation the view shows (Link::retarget starts it over). took: the last
\* answer taken in, which the next report acknowledges.
NewLink(inc) == [inc |-> inc, applied |-> 0, took |-> 0, until |-> 0, young |-> FALSE]

Init ==
    /\ now = 0
    /\ rep = [r \in Replicas |-> NewReplica(1, FALSE)]
    /\ pulledFrom = [r \in Replicas |-> {}]
    /\ restarts = 0
    /\ held = [n \in Nodes |-> 0]
    /\ fb = [n \in Nodes |-> 0]
    /\ dem = [n \in Nodes |-> 0]
    /\ ent = [n \in Nodes |-> Taken]
    /\ born = [n \in Nodes |-> None]
    /\ owe = [n \in Nodes |-> Taken]
    /\ seq = [n \in Nodes |-> 0]
    /\ link = [n \in Nodes |-> [r \in Replicas |-> NewLink(1)]]
    /\ grant = [n \in Nodes |-> [r \in Replicas |-> [v |-> 0, inc |-> 0]]]
    /\ msgs = {}
    /\ dups = 0
    /\ faults = 0
    /\ takes = 0
    /\ pushes = 0
    /\ lefts = 0

----------------------------------------------------------------------------
(* The division: core.rs targets(max, rows). U usage, D demand or None for a row not to grant,
   S the rows. Every row that asks gets one level, never more than its usage and demand; the
   first rows short of their need get the remainder; what no row needs is spread over every row. *)
Targets(max, U, D, S) ==
    LET floor == Bug = "FloorAtUsage"
        Sh == {i \in S : D[i] # None}
        need == [i \in Nodes |-> U[i] + Max(D[i], 0)]
        budget == Max(max - SumSet(U, S \ Sh), 0)
        lo(l, i) == IF floor THEN Max(U[i], Min(l, need[i])) ELSE Min(l, need[i])
        at(l) == SumSet([i \in Nodes |-> lo(l, i)], Sh)
        level == CHOOSE l \in 0..max : at(l) <= budget /\ (l = max \/ at(l + 1) > budget)
        short(i) == i \in Sh /\ level < need[i] /\ (~floor \/ U[i] <= level)
        left0 == budget - at(level)
        first == [i \in Nodes |->
                    IF i \notin Sh THEN 0
                    ELSE lo(level, i) + IF short(i) /\ Cardinality({j \in 1..(i - 1) : short(j)}) < left0
                                        THEN 1 ELSE 0]
        left == left0 - Min(left0, Cardinality({i \in Nodes : short(i)}))
        k(i) == Cardinality({j \in Sh : j < i})
        n == Cardinality(Sh)
    IN IF floor /\ at(0) >= budget
       THEN [i \in Nodes |-> IF i \in Sh THEN U[i] ELSE 0]
       ELSE [i \in Nodes |-> IF i \in Sh
                             THEN first[i] + left \div n + (IF k(i) < left % n THEN 1 ELSE 0)
                             ELSE 0]

Live(R, i, t) == R.peer[i].known /\ t < R.peer[i].heard + Lease     \* PeerState::live
Held(w) == Max(w.h0, w.h1)                                          \* Row::held
Have(w) == Max(w.used, Held(w))
Rows(R) == {i \in Nodes : R.row[i].on}
\* core.rs: asks. ROW_KEEP (quiet) is past these bounds; demand lapses at Tick.
Asks(R, t) == [i \in Nodes |-> IF i \in Rows(R) /\ Live(R, i, t) THEN R.row[i].demand ELSE None]
Usage(R) == [i \in Nodes |-> R.row[i].used]

\* core.rs: regrant, its loop over the rows in node order, room shrinking as it goes.
RECURSIVE Moves(_, _, _, _, _, _, _, _)
Moves(R, i, room, plan, share, fits, next, t) ==
    IF i > N THEN R
    ELSE IF ~R.row[i].on \/ ~Live(R, i, t) THEN Moves(R, i + 1, room, plan, share, fits, next, t)
    ELSE LET w == R.row[i]
             base == IF fits \/ Bug = "RaiseFromUsage" THEN Have(w)
                     ELSE IF ~PulledRaise /\ w.pulled # None THEN w.grant
                     ELSE Held(w)
             g == IF plan[i] < w.grant THEN plan[i]
                  ELSE Max(Min(plan[i], base + Min(share[i], room)), w.grant)
             w2 == IF g = w.grant THEN w
                   ELSE IF Bug = "LowerAtSend" /\ g < w.grant
                   THEN [w EXCEPT !.grant = g, !.h0 = g, !.h1 = g, !.changed = next]
                   ELSE [w EXCEPT !.grant = g, !.h1 = Max(w.h1, g), !.changed = next]
             R2 == [R EXCEPT !.row[i] = w2, !.peer[i].pending = @ \/ g # w.grant, !.peer[i].fresh = @ \/ g # w.grant]
         IN Moves(R2, i + 1, room - Max(g - Have(w), 0), plan, share, fits, next, t)

\* core.rs: regrant. The sum over nodes of max(usage, held) stays within the limit; a node
\* silent for a lease counts its usage alone. Where a row that wants more is left short of its
\* plan, the nodes that hold more than their grant are flagged for a push, as is a raise of a row
\* that wants it.
Regrant(R, next, t) ==
    LET S == Rows(R)
        D == Asks(R, t)
        plan == Targets(M, Usage(R), D, S)
        counted == [i \in Nodes |-> IF Live(R, i, t) THEN Have(R.row[i]) ELSE R.row[i].used]
        total == SumSet(counted, S)
        raise == [i \in Nodes |-> IF i \in S /\ Live(R, i, t) /\ plan[i] >= R.row[i].grant
                                  THEN Max(plan[i] - Have(R.row[i]), 0) ELSE None]
        share == Targets(Max(M - total, 0), [i \in Nodes |-> 0], raise, S)
        R2 == Moves(R, 1, Max(M - total, 0), plan, share, total <= M, next, t)
        live(i) == i \in S /\ Live(R, i, t)
        wants(i) == D[i] # None /\ (D[i] > 0 \/ R.row[i].used > R.row[i].grant)
        short == \E i \in Nodes : live(i) /\ wants(i) /\ R2.row[i].grant < plan[i]
        push(i) == R2.peer[i].push \/ (live(i) /\ wants(i) /\ R2.row[i].grant > R.row[i].grant)
                   \/ (live(i) /\ short /\ Held(R2.row[i]) > R2.row[i].grant)
    IN [R2 EXCEPT !.peer = [i \in Nodes |-> [R2.peer[i] EXCEPT !.push = push(i)]]]

\* core.rs: PeerState::settle. Node n took in grants message a; the last one sent was last. Where
\* a carried the entry, a row not changed since ends its hold, and one changed drops the older
\* generation once a passed its mark. The replica, and whether the hold fell.
Settle(R, n, a, last) ==
    LET p == R.peer[n]
        w1 == R.row[n]
        hit == a \in p.carried \/ Bug = "AckLeftOut"
        through == a + IF Bug = "AckEarly" THEN 1 ELSE 0
        keep == p.pending /\ w1.on /\ (w1.pulled # None \/ w1.changed > through)
        w2 == IF ~p.pending \/ ~w1.on \/ w1.pulled # None THEN w1
              ELSE IF w1.changed <= through THEN [w1 EXCEPT !.h0 = 0, !.h1 = w1.grant, !.mark = last]
              ELSE IF a > w1.mark
              THEN [w1 EXCEPT !.h0 = IF Bug = "DropOldGen" THEN 0 ELSE w1.h1, !.h1 = w1.grant, !.mark = last]
              ELSE w1
    IN IF ~hit THEN [rep |-> R, fell |-> FALSE]
       ELSE [rep |-> [R EXCEPT !.peer[n].carried = @ \ {a}, !.peer[n].pending = keep, !.peer[n].fresh = @ /\ keep,
                              !.row[n] = w2],
             fell |-> Held(w2) < Held(w1)]

\* core.rs: Replica::report, after wake, for a report newer than the last it took from the node.
\* A hold that fell gives its room to the rows that wait for it at once.
Report(R1, n, m, t) ==
    LET young == R1.pulling
        next == R1.gseq + 1
        p0 == IF R1.peer[n].known THEN R1.peer[n] ELSE NewPeer(t)
        freed == ~(t < p0.heard + Lease)
        w0 == R1.row[n]
        free == freed /\ p0.haskey /\ w0.on
        w1 == IF free THEN [w0 EXCEPT !.grant = 0, !.h0 = 0, !.h1 = 0, !.changed = next, !.pulled = None]
              ELSE w0
        p1 == [p0 EXCEPT !.pending = @ \/ free, !.fresh = @ \/ free]
        S == Settle([R1 EXCEPT !.peer[n] = p1, !.row[n] = w1], n, IF m.rinc = R1.inc THEN m.acked ELSE 0, R1.gseq)
        keep == S.rep.peer[n].pending
        w2 == S.rep.row[n]
        w3 == IF ~m.item THEN w2
              ELSE [w2 EXCEPT !.on = TRUE, !.used = m.used, !.demand = m.demand, !.pulled = None,
                              !.changed = IF keep THEN @ ELSE next]
        p3 == [S.rep.peer[n] EXCEPT !.seq = m.seq, !.sent = m.sent, !.heard = t,
                                    !.pending = keep \/ m.item, !.haskey = @ \/ m.item]
        R3 == [S.rep EXCEPT !.peer[n] = p3, !.row[n] = w3]
        touched == IF freed THEN p3.haskey ELSE m.item \/ S.fell
        R4 == IF ~young /\ touched THEN Regrant(R3, next, t) ELSE R3
    IN [R4 EXCEPT !.gseq = next, !.peer[n].push = FALSE]

\* core.rs: Replica::ack, for the reply to a push: node n took in push a, from the replica's
\* incarnation rinc. A hold that fell gives its room to the rows that wait for it.
Acked(R, n, rinc, a) ==
    IF rinc # R.inc \/ ~R.peer[n].known THEN R
    ELSE LET S == Settle(R, n, a, R.gseq)
         IN IF S.fell /\ ~R.pulling THEN Regrant(S.rep, R.gseq + 1, now) ELSE S.rep

----------------------------------------------------------------------------
(* Nodes *)

\* core.rs: Entry::bound. The smallest fresh grant once a majority gave one, else the fallback.
Fresh(n) == {r \in Replicas : grant[n][r].inc = link[n][r].inc /\ ~link[n][r].young
                              /\ (now < link[n][r].until \/ (Bug = "UntilInclusive" /\ now = link[n][r].until))}
Bound(n) ==
    LET F == Fresh(n)
        vs == {grant[n][r].v : r \in F}
    IN IF Cardinality(F) >= Quorum
       THEN [grant |-> TRUE,
             v |-> IF Bug = "LargestGrant" THEN CHOOSE v \in vs : \A x \in vs : v >= x
                   ELSE CHOOSE v \in vs : \A x \in vs : v <= x]
       ELSE [grant |-> FALSE, v |-> Fallback]

\* core.rs: Table::take -> Table::entry (made on first use), Entry::admit, Entry::add. A refusal
\* adds to demand. Either marks the entry for the next report (Entry::touched; check sets dirty).
\* An entry made lately (Entry::new_here) admits up to the fallback where its grants refuse, as
\* the fallback, and the refusal still adds to demand.
Take(n, c) ==
    LET b == Bound(n)
        fits == held[n] + c <= b.v /\ c <= M
        floor == ~fits /\ b.grant /\ born[n] # None /\ now <= born[n] + 1 /\ held[n] + c <= Fallback
        counted == ~b.grant \/ (~fits /\ Bug # "FloorUncounted")
    IN /\ takes < MaxTakes
       /\ takes' = takes + 1
       /\ ent' = [ent EXCEPT ![n] = TRUE]
       /\ born' = IF ent[n] THEN born ELSE [born EXCEPT ![n] = now]
       /\ owe' = [owe EXCEPT ![n] = TRUE]
       /\ held' = IF fits \/ floor THEN [held EXCEPT ![n] = @ + c] ELSE held
       /\ fb' = IF (fits \/ floor) /\ counted THEN [fb EXCEPT ![n] = @ + c] ELSE fb
       /\ dem' = IF fits THEN dem ELSE [dem EXCEPT ![n] = Min(@ + c, M)]
       /\ UNCHANGED <<now, rep, pulledFrom, restarts, seq, link, grant, msgs, dups, faults, pushes, lefts>>

\* Dropping a Permit: Table::release -> Entry::release.
Release(n) ==
    /\ takes < MaxTakes
    /\ held[n] > 0
    /\ takes' = takes + 1
    /\ owe' = [owe EXCEPT ![n] = TRUE]
    /\ held' = [held EXCEPT ![n] = @ - 1]
    /\ fb' = [fb EXCEPT ![n] = Min(@, held[n] - 1)]
    /\ UNCHANGED <<now, rep, pulledFrom, restarts, dem, ent, born, seq, link, grant, msgs, dups, faults, pushes, lefts>>

\* Node::answer -> Replica::report: the replica takes in and answers a report newer than the last
\* it took from the node, with the entry where it is pending, but where left and the report did
\* not name it. The replica's new state, and its answer if any. (Replica::wake is in Pull: the
\* tick after pulled comes within REPORT_EVERY, inside a tick here.)
Answer(R1, m, t, left) ==
    LET R2 == Report(R1, m.n, m, t)
        has == R2.peer[m.n].pending /\ R2.row[m.n].on /\ (m.item \/ ~left)
        R3 == [R2 EXCEPT !.peer[m.n].carried = IF has THEN @ \cup {R2.gseq} ELSE @, !.peer[m.n].fresh = @ /\ ~has]
    IN [rep |-> IF m.seq > R1.peer[m.n].seq THEN R3 ELSE R1,
        any |-> m.seq > R1.peer[m.n].seq,
        g |-> [t |-> "grants", n |-> m.n, r |-> m.r, inc |-> R2.inc, sent |-> m.sent, answers |-> m.seq,
               gseq |-> R2.gseq, young |-> R1.pulling, has |-> has, v |-> R2.row[m.n].grant, push |-> FALSE]]

\* An answer that could leave the entry out.
CanLeave(a, m) == a.any /\ a.g.has /\ ~m.item

\* Table::apply for one replica's grants: Link::accept, Entry::set_grant, Link::commit. The node's
\* link to the replica and its grant. A push to a node the replica took no report from answers none.
\* Where the link's grants lapsed, the replica may have freed them, and a message that left the
\* entry out must not make the old grant count again: Link::accept drops it.
TakeIn(l0, g0, m) ==
    LET ok == m.inc = l0.inc /\ m.gseq > l0.applied
        lapsed == ok /\ now >= l0.until /\ Bug # "LapsedKept"
    IN [l |-> IF ok THEN [l0 EXCEPT !.applied = m.gseq, !.took = IF m.push THEN @ ELSE m.gseq, !.young = m.young,
                                    !.until = IF m.sent = None THEN 0
                                              ELSE (IF Bug = "UntilAtArrival" THEN now ELSE m.sent) + Hold]
              ELSE l0,
        g |-> IF ok /\ m.has THEN [v |-> m.v, inc |-> l0.inc] ELSE IF lapsed THEN [v |-> 0, inc |-> 0] ELSE g0]

\* Node::round -> Link::send, Entry::report: one report to every replica, with the entry or not.
\* Each copy goes there and back at once, but those the plan F names: the report or its answer is
\* late (in flight, to arrive at any later point, or never). The answers of L leave the entry out.
SendReport(n, item) ==
    LET s == seq[n] + 1
        copy(r) == [t |-> "report", n |-> n, r |-> r, seq |-> s, sent |-> now, rinc |-> link[n][r].inc,
                    acked |-> link[n][r].took, item |-> item,
                    used |-> IF item THEN held[n] ELSE 0, demand |-> IF item THEN dem[n] ELSE 0]
        \* functions, not operators, so TLC evaluates each answer once for every plan
        a0 == [r \in Replicas |-> Answer(rep[r], copy(r), now, FALSE)]
        a1 == [r \in Replicas |-> Answer(rep[r], copy(r), now, TRUE)]
    IN /\ ent[n]
       /\ seq[n] < MaxReports
       /\ seq' = [seq EXCEPT ![n] = s]
       /\ owe' = IF item THEN [owe EXCEPT ![n] = FALSE] ELSE owe
       /\ \E F \in Plans(MaxFaults - faults),
            L \in {X \in SUBSET {r \in Replicas : CanLeave(a0[r], copy(r))} : Cardinality(X) <= MaxLeft - lefts} :
            LET a == [r \in Replicas |-> IF r \in L /\ F[r] # "report" THEN a1[r] ELSE a0[r]]
                in == [r \in Replicas |-> TakeIn(link[n][r], grant[n][r], a[r].g)]
                back(r) == F[r] = "none" /\ a[r].any
            IN /\ faults' = faults + Cardinality(Copies(F, {"report", "answer"}))
               /\ lefts' = lefts + Cardinality({r \in L : F[r] # "report"})
               /\ rep' = [r \in Replicas |-> IF F[r] # "report" THEN a[r].rep ELSE rep[r]]
               /\ link' = [link EXCEPT ![n] = [r \in Replicas |-> IF back(r) THEN in[r].l ELSE @[r]]]
               /\ grant' = [grant EXCEPT ![n] = [r \in Replicas |-> IF back(r) THEN in[r].g ELSE @[r]]]
               /\ msgs' = msgs \cup {copy(r) : r \in IF Late = "lost" THEN {} ELSE Copies(F, {"report"})}
                               \cup {a[r].g : r \in {x \in Copies(F, {"answer"}) : a[x].any}}
       /\ UNCHANGED <<now, pulledFrom, restarts, held, fb, dem, ent, born, dups, takes, pushes>>

\* core.rs: Table::pushed. A push answers node n's last report, and nothing changed since: the
\* replica knows its usage.
Heard(n, m) == m.answers = seq[n] /\ ~owe[n]

\* The node's reply to push g acknowledges it where the node took it in, as link l shows, and Heard.
PushAck(n, g, l) == l.inc = g.inc /\ l.applied = g.gseq /\ (Heard(n, g) \/ Bug = "AckUnheard")

\* core.rs: Replica::pushes -> Replica::grants, then Table::pushed and Replica::ack. Replica r
\* sends node n, flagged by regrant, the grant set since its last message to n unasked (or none,
\* where left), answering the last report it took from n (one grants message a report cycle at
\* most is not modelled); the node takes it in, or not where it answers an older report than its
\* last two (here, either way), and its reply acknowledges the push (PushAck). A fault: the push
\* late, or lost (Late = "lost"), or the reply lost (Late = "any").
Push(r, n) ==
    \E left \in IF lefts < MaxLeft THEN BOOLEAN ELSE {FALSE} :
    LET R0 == [rep[r] EXCEPT !.gseq = @ + 1, !.peer[n].push = FALSE]
        R == [R0 EXCEPT !.peer[n].fresh = left, !.peer[n].carried = IF left THEN @ ELSE @ \cup {R0.gseq}]
        g == [t |-> "grants", n |-> n, r |-> r, inc |-> R.inc, sent |-> R.peer[n].sent, answers |-> R.peer[n].seq,
              gseq |-> R.gseq, young |-> FALSE, has |-> ~left, v |-> R.row[n].grant, push |-> TRUE]
        in == TakeIn(link[n][r], grant[n][r], g)
        plans == {"none", "declined"} \cup IF faults >= MaxFaults THEN {}
                               ELSE CASE Late = "any" -> {"push", "reply"}
                                      [] Late = "lost" -> {"lost"}
                                      [] OTHER -> {}
    IN /\ pushes < MaxPushes
       /\ ~rep[r].pulling
       /\ Live(rep[r], n, now)
       /\ rep[r].peer[n].push /\ rep[r].peer[n].fresh /\ rep[r].peer[n].pending /\ rep[r].row[n].on
       /\ pushes' = pushes + 1
       /\ lefts' = IF left THEN lefts + 1 ELSE lefts
       /\ \E F \in plans :
            LET back == F \in {"none", "reply"}
            IN /\ faults' = IF F \in {"none", "declined"} THEN faults ELSE faults + 1
               /\ msgs' = IF F = "push" THEN msgs \cup {g} ELSE msgs
               /\ link' = IF back THEN [link EXCEPT ![n][r] = in.l] ELSE link
               /\ grant' = IF back THEN [grant EXCEPT ![n][r] = in.g] ELSE grant
               /\ rep' = [rep EXCEPT ![r] = IF Bug = "PushAcked" THEN Acked(R, n, R.inc, R.gseq)
                                          ELSE IF F = "none" /\ PushAck(n, g, in.l) THEN Acked(R, n, g.inc, g.gseq)
                                          ELSE R]
       /\ UNCHANGED <<now, pulledFrom, restarts, held, fb, dem, ent, born, owe, seq, dups, takes>>

\* A late message arrives: a report at its replica, whose answer comes straight back, or grants at
\* their node, whose reply to a push comes straight back. Up to MaxDups arrivals leave the message
\* in flight.
Deliver(m) ==
    \E again \in (IF dups < MaxDups THEN BOOLEAN ELSE {FALSE}),
      took \in (IF m.t = "grants" /\ m.push THEN BOOLEAN ELSE {TRUE}),
      left \in (IF m.t = "report" /\ lefts < MaxLeft /\ CanLeave(Answer(rep[m.r], m, now, FALSE), m)
                THEN BOOLEAN ELSE {FALSE}) :
        LET n == m.n
            r == m.r
            a == Answer(rep[r], m, now, left)
            g == IF m.t = "report" THEN a.g ELSE m
            in == IF took THEN TakeIn(link[n][r], grant[n][r], g) ELSE [l |-> link[n][r], g |-> grant[n][r]]
            back == m.t = "grants" \/ a.any
        IN /\ dups' = IF again THEN dups + 1 ELSE dups
           /\ msgs' = IF again THEN msgs ELSE msgs \ {m}
           /\ lefts' = IF left THEN lefts + 1 ELSE lefts
           /\ rep' = IF m.t = "report" THEN [rep EXCEPT ![r] = a.rep]
                     ELSE IF m.push /\ took /\ PushAck(n, m, in.l)
                     THEN [rep EXCEPT ![r] = Acked(rep[r], n, m.inc, m.gseq)]
                     ELSE rep
           /\ link' = IF back THEN [link EXCEPT ![n][r] = in.l] ELSE link
           /\ grant' = IF back THEN [grant EXCEPT ![n][r] = in.g] ELSE grant
           /\ UNCHANGED <<now, pulledFrom, restarts, held, fb, dem, ent, born, owe, seq, takes, faults, pushes>>

\* Table::relink (and Table::link in Node::round) -> Link::retarget: the node's view shows the
\* replica's new incarnation, at any point after its restart. Grants filed before count no more.
ViewSees(n, r) ==
    /\ link[n][r].inc < rep[r].inc
    /\ link' = [link EXCEPT ![n][r] = NewLink(rep[r].inc)]
    /\ UNCHANGED <<now, rep, pulledFrom, restarts, held, fb, dem, ent, born, owe, seq, grant, msgs, dups, faults, takes,
                   pushes, lefts>>

----------------------------------------------------------------------------
(* Replicas *)

\* A node reports what changed within REPORT_EVERY (100 ms), well inside a tick, so time moves
\* only once every change was sent (as model.rs does). A node silent past that is a paused one,
\* whose usage was reported. Demand lapses: a tick is longer than two heartbeats.
Tick ==
    /\ now < MaxTime
    /\ \A n \in Nodes : ~owe[n]
    /\ now' = now + 1
    /\ dem' = [n \in Nodes |-> 0]
    /\ rep' = [r \in Replicas |-> [rep[r] EXCEPT !.row = [i \in Nodes |-> [rep[r].row[i] EXCEPT !.demand = 0]]]]
    /\ UNCHANGED <<pulledFrom, restarts, held, fb, ent, born, owe, seq, link, grant, msgs, dups, faults, takes, pushes,
                   lefts>>

\* A new process: no rows, young until it pulled from every other replica (Node::new).
Restart(r) ==
    /\ restarts < MaxRestarts
    /\ restarts' = restarts + 1
    /\ rep' = [rep EXCEPT ![r] = NewReplica(rep[r].inc + 1, TRUE)]
    /\ pulledFrom' = [pulledFrom EXCEPT ![r] = {}]
    /\ UNCHANGED <<now, held, fb, dem, ent, born, owe, seq, link, grant, msgs, dups, faults, takes, pushes, lefts>>

\* Replica::pull at J, Replica::absorb at R: a pulled row carries what the node may hold on J and
\* the larger usage, and its hold ends at an acknowledgement once the node reported the key here.
Absorb(R, J, t) ==
    LET next == R.gseq + 1
        row(i) == LET w == R.row[i]
                      jh == IF Bug = "PullNoHold" THEN 0 ELSE Held(J.row[i])
                      w1 == IF ~w.on
                            THEN [NoRow EXCEPT !.on = TRUE, !.used = J.row[i].used,
                                               !.demand = J.row[i].demand, !.h1 = jh]
                            ELSE [w EXCEPT !.h1 = Max(w.h1, jh), !.used = Max(w.used, J.row[i].used)]
                  IN IF Held(w1) > w1.grant THEN [w1 EXCEPT !.changed = next, !.pulled = t] ELSE w1
        peer(i) == LET p == IF R.peer[i].known THEN R.peer[i] ELSE NewPeer(t)
                       set == Held(row(i)) > row(i).grant
                   IN [p EXCEPT !.haskey = TRUE, !.pending = @ \/ set, !.fresh = @ \/ set]
    IN [R EXCEPT !.row = [i \in Nodes |-> IF J.row[i].on THEN row(i) ELSE R.row[i]],
                 !.peer = [i \in Nodes |-> IF J.row[i].on THEN peer(i) ELSE R.peer[i]]]

\* Node::page -> Replica::absorb; once every other replica answered, Replica::pulled and the next
\* Replica::tick -> Replica::wake, which grants every row again.
Pull(r, j) ==
    /\ rep[r].pulling
    /\ j # r
    /\ j \notin pulledFrom[r]
    /\ pulledFrom' = [pulledFrom EXCEPT ![r] = @ \cup {j}]
    /\ LET R == Absorb(rep[r], rep[j], now)
           done == pulledFrom'[r] = Replicas \ {r}
       IN rep' = [rep EXCEPT ![r] = IF done THEN Regrant([R EXCEPT !.pulling = FALSE], R.gseq + 1, now) ELSE R]
    /\ UNCHANGED <<now, restarts, held, fb, dem, ent, born, owe, seq, link, grant, msgs, dups, faults, takes, pushes,
                   lefts>>

----------------------------------------------------------------------------

Next ==
    \/ Tick
    \/ \E n \in Nodes : \/ \E c \in Costs : Take(n, c)
                        \/ Release(n)
                        \/ \E item \in (IF owe[n] THEN {TRUE} ELSE BOOLEAN) : SendReport(n, item)
                        \/ \E r \in Replicas : ViewSees(n, r)
    \/ \E m \in msgs : Deliver(m)
    \/ \E r \in Replicas, n \in Nodes : Push(r, n)
    \/ \E r \in Replicas : Restart(r) \/ \E j \in Replicas : Pull(r, j)

Spec == Init /\ [][Next]_vars

Symmetry == Permutations(Replicas)

\* Two states that differ only in messages that can no longer change anything are the same state.
\* Grants the node's link has passed are ignored for good, but for the push it took in last, whose
\* reply acknowledges it again. A report no newer than what its
\* replica took from the node is ignored for as long as no restart can empty the replica, and its
\* arrival changes nothing.
Dead(m) ==
    IF m.t = "grants"
    THEN m.inc < link[m.n][m.r].inc
         \/ (m.inc = link[m.n][m.r].inc /\ (m.gseq < link[m.n][m.r].applied \/ (~m.push /\ m.gseq = link[m.n][m.r].applied)))
    ELSE restarts = MaxRestarts /\ m.seq <= rep[m.r].peer[m.n].seq
View == <<now, rep, pulledFrom, restarts, held, fb, dem, ent, born, owe, seq, link, grant,
          {m \in msgs : ~Dead(m)}, dups, faults, takes, pushes, lefts>>

----------------------------------------------------------------------------
(* Invariants *)

\* The most a node could hold without another message: its permits, or all its grant allows.
Reach(n) == LET b == Bound(n) IN IF b.grant THEN Max(held[n], b.v) ELSE held[n]

\* §5's rows of 0 over the limit for message loss, duplication and reorder and for a replica that
\* crashes and restarts, as §7 states the bound for model.rs: what the nodes hold or could still
\* take on their grants exceeds M by no more than the permits they hold that were taken on the
\* fallback, or on the floor of a key new to the node (§2.5, §5's new key). Drift is folded into Hold and Lease; permits have no window.
WithinLimit == SumSet([n \in Nodes |-> Reach(n)], Nodes) <= M + SumSet(fb, Nodes)

\* §5, a new key: the fallback and the floor admit at most a = ceil(M / N) per node.
FallbackBound == SumSet(fb, Nodes) <= N * ((M + N - 1) \div N)

\* §2.3's level, on the model's own Targets: at a replica that grants, a row planned less than its
\* usage and demand is planned no less than any other row that asks, but for a remainder of one.
\* Where remainders and unneeded room go is not checked.
MaxMinFair ==
    \A r \in Replicas : ~rep[r].pulling =>
        LET R == rep[r]
            D == Asks(R, now)
            plan == Targets(M, Usage(R), D, Rows(R))
        IN \A i, j \in {x \in Rows(R) : D[x] # None} :
               plan[i] < R.row[i].used + D[i] => plan[i] + 1 >= plan[j]

\* Witnesses, for QuotaGrantWitness.cfg: each must be violated, which shows its bounds reach it.
WFallback == \A n \in Nodes : fb[n] = 0
WOneFresh == \A n \in Nodes : Cardinality(Fresh(n)) # 1
WShift == \A r \in Replicas, n \in Nodes : rep[r].row[n].h0 = 0
WLate == \A m \in msgs : m.sent = now
WPulled == \A r \in Replicas, n \in Nodes : rep[r].row[n].pulled = None
WOldIncarnation == \A n \in Nodes, r \in Replicas : r \in Fresh(n) => link[n][r].inc = rep[r].inc
WSilentHeld == \A r \in Replicas, n \in Nodes :
    ~(rep[r].row[n].on /\ rep[r].peer[n].known /\ ~Live(rep[r], n, now) /\ Held(rep[r].row[n]) > 0)

=============================================================================
