--------------------------- MODULE SeedMembership ---------------------------
(* Seed discovery as in spec/seed-discovery.md §2 and §4, and as crates/axon-cluster/src/seed/core.rs
   builds it: seed servers (Server) keep the records registered over their own sessions, members
   (Node) keep a session to every seed on their list, and a member's view is the union of the lists
   of the seeds it has synced with. Time is in ticks of one second, the keepalive interval. The
   timings (SeedTimings) are constants; the promises checked against them are formulas of the
   constants that give the design's own numbers (§2.2, §8) at the shipped timings, so a mutation of
   a timing is a Bug that changes the rule (Hold, Grace, NoWithdraw) and keeps the promise. A seed in Odd holds
   a silent member for less, and nodes do not count it (core.rs: its SYNCED carries other timings);
   an odd seed differs in nothing else here.

   Abstractions, and what they could hide:
   - A seed's frames on a session whose link is up arrive in the same step as the action that
     caused them (Step); on a cut link they wait, and are lost with the session. A node's copy of
     a seed's list is replaced by the seed's list, so a Put and a Remove that cancel inside one
     step are not seen, and no state shows one node with a seed's change and another without it.
     A member's REGISTER and LEAVE (DeliverUp) and the reset a seed sees when the member's end is
     gone (SeedReset) keep interleavings of their own; a member sees a seed's reset at once.
   - Frames a node queues on a cut link collapse to the last two (REGISTER and LEAVE alternate).
   - Record versions (set_record of a changed record), metadata, validation, capacity, observers,
     the 60 s incarnation bound, duplicate seed identities, batching and the view cache (dirty,
     shown, revision: the view here is rebuilt from scratch) are not modelled. A registration is
     (member, incarnation); the same incarnation again is always "known".
   - A session's silence is a cut link: keepalives are not frames here. A cut is both ways; a
     one-way cut, where the seed sees the member's close and holds CloseGrace, not IdleHold, is not
     modelled. Either end of a session on a link cut at t sees it idle at some tick in
     [t + Idle - 1, t + Idle], each end on its own. The code's window is one tick wider (the last
     read may be up to a keepalive before the cut, and idle is checked on the keepalive tick:
     axon-net session.rs), so a machine's removal here is by t + Idle + IdleHold (9 at the shipped
     timings) where the code's is a tick later.
     A crashed process resets its sessions; a dead machine sends nothing, so its sessions go silent
     from the crash and no seed sees a reset until it boots again. A boot leaves cuts as they are:
     a partition outlasts a restart. A seed's crash is a process crash; a seed machine going silent
     is not modelled, nor is a seed that is also a member.
   - Redials: a session that can be dialled opens within RedialMax ticks. The code's backoff tops
     at 2 s with 20 % jitter, plus a connect of up to a keepalive (axon-net net.rs): about 3.4 s.
     Every time is rounded down to its tick, and what lands in one tick lands in any order, so
     RedialMax is 3; -Wan.cfg allows 6.
   - A cut heals after one of CutFor ticks only (3: at the idle timeout, so a session may end or
     not; 5: before the hold runs out; 10: after the seeds removed the member; 7, 12 and 20 in
     -Wan.cfg): orderings that need other lengths are not explored.
   - Faults happen in [FaultFrom, FaultUntil], at most MaxFaults of them, after a clean start, so
     two faults are at most FaultUntil - FaultFrom ticks apart; the cold start itself is checked
     without faults, with every seed starting at 0.
   - Retire is an action of its own, due as soon as the slot may retire; the code retires in the
     same reconcile. The model also has the orders where something lands first.
   - Isolation (heartbeat_interval, heartbeat_grace in cluster.rs, §2.2's first rows) is outside
     this model. IdleHeld checks the seeds' side of §2.2: the seeds nodes count remove a silent
     member no earlier than SeedsRemove (8 s at the shipped timings) after it went silent.
   - With 2 seeds a seed down or warming leaves every member's list without a usable majority, so
     NoSpuriousLoss excuses every member then; WarmKeeps is what checks that seeds restarting
     lose nobody.
   - Not modelled: set_record(None) (a node that unregisters; LEAVE is still sent, by a withdraw),
     and the REFUSED reasons other than Stale.
   - Code lines the bounds leave unchecked, each deleted and found to pass: reconcile's
     `minority_since = None` when attached (a node back to a majority by set_seeds and out of it
     again needs a third fault); frame's `minority_since = None` on a new SYNCED (a SYNCED that
     does not attach the node needs a list of 3 or more with seeds warming at different times);
     frame's `sent = None` on a Remove of the node's own boot (a seed drops a member whose session
     it still holds only after a one-way cut); reconcile keeping a dropped seed until the node
     is live (it only keeps the view of a node that is not live, which no invariant reads); and the
     thaw ending before WarmUp once the last view is listed again (on -Seeds3.cfg: Settle allows the
     whole hold, and sim.rs's one_seed_down_is_invisible_and_two_freeze_everyone fails without it). *)
EXTENDS Integers, FiniteSets, Sequences, TLC

CONSTANTS
    Members,            \* model values, symmetric
    Seeds,              \* model values
    L0, L1,             \* each member's seed list before and after set_seeds
    MaxInc,             \* incarnations are 1..MaxInc; a boot picks another one, lower when its clock is behind
    BootDown,           \* whether a boot may pick a lower incarnation
    Idle,               \* SeedTimings::idle: a session silent this long ends (keepalive 1 s)
    IdleHold,           \* SeedTimings::idle_hold: a member whose session went idle is removed this much later
    CloseGrace,         \* SeedTimings::closed_grace: ... whose session was closed
    WarmUp,             \* SeedTimings::warm_up: a new seed withholds SYNCED this long
    Odd,                \* seeds with a shorter idle_hold, which nodes do not count
    WithdrawAfter,      \* core.rs WITHDRAW_AFTER: attached to a minority this long, a node sends LEAVE
    Retry,              \* core.rs RETRY: after REFUSED, a node registers again this much later
    RedialMax,          \* a lost session is dialled again within this
    CutFor,             \* a cut link heals after one of these many ticks
    MaxDown,            \* a crashed seed starts again within this
    FaultFrom, FaultUntil, MaxFaults,
    Settle,             \* after the last fault, members agree within this
    MaxTime,
    Bug                 \* "none", or the rule a mutation breaks (see its config)

VARIABLES
    now,
    nd,             \* per member: Node, and its sessions' ends
    sd,             \* per seed: Server, and its sessions' ends
    cut,            \* per member and seed: -1, or the tick the link was cut
    faults, lastFault,
    crashed,        \* per member: it crashed
    minor,          \* per member: -1, or since when its list has lacked a majority it can use (Minority)
    excused,        \* per member: it has been in a minority long enough to leave views, this tick
    gainedAt,       \* per member: when it last gained a SYNCED
    lostBad,        \* a live view lost or changed a member it had no reason to
    dropBy, dropInc \* per member: the tick by which its crashed boot must be out of every view

vars == <<now, nd, sd, cut, faults, lastFault, crashed, minor, excused, gainedAt, lostBad, dropBy, dropInc>>

\* The design's numbers, as formulas of the timings; at the shipped ones (4, 5, 2, withdraw 4) they
\* are §2.2's and §8's. §2.2: sessions go idle "3-5 s" after a cut (IdleSeen 3); §4.3: a closed
\* session's record is removed after 2 s, the earliest a seed lets go of a member it lost
\* (LeaveGrace 2); §8 invariant 5: "leaves every live view within 3 s; a dead one within 11 s"
\* (ProcessGone, DeadGone); invariant 9: "in no live view after 10 s" (MinorityGone); §2.2: the
\* seeds remove a silent member "8-10 s" after it went silent (SeedsRemove 8).
IdleSeen == Idle - 1
LeaveGrace == CloseGrace
ProcessGone == CloseGrace + 1
DeadGone == Idle + IdleHold + 2
MinorityGone == Idle + WithdrawAfter + 2
SeedsRemove == Idle + IdleHold - 1

\* What the seeds hold, and the mutations of it: no hold, a hold of 2 s, a closed session held
\* for nothing or as long as an idle one. An odd seed holds the least SeedTimings::check accepts
\* at the shipped grace.
Hold(s) == CASE Bug \in {"HoldZero", "DropAtInterval"} -> 0
             [] Bug = "ShortHold" -> 2
             [] s \in Odd -> CloseGrace + 1
             [] OTHER -> IdleHold
Grace == CASE Bug = "CloseZero" -> 0 [] Bug = "ProcessAsIdle" -> IdleHold [] OTHER -> CloseGrace
\* The seeds nodes count: every one that sent SYNCED, but for an odd one (Node::frame).
Counted == IF Bug = "HoldBelowValidated" THEN Seeds ELSE Seeds \ Odd

ASSUME Settle >= MaxDown + WarmUp + 1 /\ MaxTime >= FaultUntil + Settle /\ Odd \subseteq Seeds
\* A cut long enough for a withdraw, then the redial and the thaw's hold: a member of the last view
\* that crashed meanwhile never registers again, so the hold runs out.
ASSUME \A c \in CutFor : Settle >= c + RedialMax + WarmUp + 1

Max(x, y) == IF x > y THEN x ELSE y
MaxSet(X) == CHOOSE x \in X : \A y \in X : y <= x
MinSet(X) == CHOOSE x \in X : \A y \in X : x <= y
Ids == 1..3
NoList == [x \in Members |-> 0]
NoMem == [inc |-> 0, ses |-> 0, rm |-> -1, early |-> FALSE]
Append2(q, m) == IF Len(q) = 2 THEN <<q[2], m>> ELSE Append(q, m)

----------------------------------------------------------------------------
(* Node, core.rs *)

Cur(st) == IF st.switched THEN L1 ELSE L0
\* synced: the slot had a SYNCED; an odd seed's (Slot::odd) does not count.
Counts(st, s) == st.synced[s] /\ s \in Counted
Reached(st) == {s \in Cur(st) : st.sess[s] # 0 /\ Counts(st, s)}
\* Node::attached, Node::listed(0): counted over the current list, synced sessions only.
Attached(st) == 2 * Cardinality(Reached(st)) > Cardinality(Cur(st))
Listed(m, st) == 2 * Cardinality({s \in Reached(st) : st.nlist[s][m] = st.inc}) > Cardinality(Cur(st))
\* Node::live: a record is always set here. A synced seed that lists a greater boot of m keeps that
\* boot in the view, so a boot under a lower incarnation waits until it is gone. Nor is a node
\* attached again while it thaws (Thaw).
Superseded(m, st) == \E s \in st.slots : Counts(st, s) /\ st.nlist[s][m] > st.inc
LiveSt(m, st) ==
    /\ st.up /\ Attached(st) /\ Listed(m, st) /\ (Bug = "LowerBoot" \/ ~Superseded(m, st))
    /\ (Bug = "NoThaw" \/ st.thaw < 0)
\* Node::view: the union of every synced slot's list, retired ones too; the greatest incarnation wins.
ViewSt(st) == [x \in Members |->
    MaxSet({st.nlist[s][x] : s \in {r \in st.slots : Counts(st, r)}} \cup {0})]
Live(m) == LiveSt(m, nd[m])
View(m) == ViewSt(nd[m])
\* Node::reconcile: retire = retired && (live || peer.is_none())
Retiring(m, st, s) ==
    st.up /\ s \in st.slots /\ s \notin Cur(st) /\ (LiveSt(m, st) \/ st.sess[s] = 0)

\* Node::reconcile: the thaw, then the slots it keeps (a retiring slot is Retire's). Node.thaw is Thawed,
\* Owed (not attached), or the tick it runs out once attached again; it ends sooner when every member
\* of its last view is listed again. The code's scan of the view resumes where it stopped, so a member
\* listed again and then removed counts as back; here every member must be listed at once.
Thawed == -1
Owed == -2
Thaw(st, t) ==
    LET until == IF ~Attached(st) THEN Owed ELSE IF st.thaw = Owed THEN t + WarmUp ELSE st.thaw
        back == \A x \in Members : st.last[x] # 0 => ViewSt(st)[x] # 0
    IN [st EXCEPT !.thaw = IF until >= 0 /\ (t >= until \/ back) THEN Thawed ELSE until]
Reconcile(m, st0, t) ==
    IF ~st0.up THEN st0 ELSE
    LET st == Thaw(st0, t)
        old == Bug = "OldWithdraw"
        trig == IF old THEN LiveSt(m, st) ELSE Attached(st)
        ms == IF trig THEN -1 ELSE IF st.minSince = -1 THEN t ELSE st.minSince
        withdrawn == Bug # "NoWithdraw" /\ ~trig /\ t >= ms + WithdrawAfter
        act(s) == s \in st.slots /\ st.sess[s] # 0 /\ ~Retiring(m, st, s)
        wanted(s) == ~(withdrawn /\ (old \/ Bug = "WithdrawAll" \/ st.synced[s]))
                     /\ (Counts(st, s) \/ ~st.synced[s])
        reg(s) == act(s) /\ wanted(s) /\ (Bug # "RegisterOnlySynced" \/ st.synced[s])
                  /\ ~st.sent[s] /\ t >= st.retryAt[s]
        lv(s) == act(s) /\ ~wanted(s) /\ st.sent[s]
    IN [st EXCEPT
          !.minSince = ms,
          !.sent = [s \in Seeds |-> IF reg(s) THEN TRUE ELSE IF lv(s) THEN FALSE ELSE st.sent[s]],
          !.pend = [s \in Seeds |-> IF reg(s) THEN Append2(st.pend[s], "reg")
                                    ELSE IF lv(s) THEN Append2(st.pend[s], "leave")
                                    ELSE st.pend[s]]]

\* Node::lost resets the slots to their defaults.
Fresh(st, T, t) ==
    LET f(old, new) == [s \in Seeds |-> IF s \in T THEN new ELSE old[s]] IN
    [st EXCEPT !.sess = f(@, 0), !.synced = f(@, FALSE), !.nlist = f(@, NoList),
               !.sent = f(@, FALSE), !.pend = f(@, <<>>), !.retryAt = f(@, 0),
               !.redialBy = f(@, t + RedialMax)]

Booted(st, i) ==
    [up |-> TRUE, inc |-> i, switched |-> st.switched, slots |-> Cur(st),
     sess |-> [s \in Seeds |-> 0], synced |-> [s \in Seeds |-> FALSE],
     nlist |-> [s \in Seeds |-> NoList], sent |-> [s \in Seeds |-> FALSE],
     pend |-> [s \in Seeds |-> <<>>], retryAt |-> [s \in Seeds |-> 0],
     redialBy |-> [s \in Seeds |-> now + RedialMax], minSince |-> -1, dead |-> -1,
     thaw |-> Thawed, last |-> NoList]

----------------------------------------------------------------------------
(* Server, core.rs *)

\* Server::register: Stale below the listed boot; the same boot is known (its session owns the
\* record now, and a pending removal is off); a greater one replaces it and closes the old boot's session.
Register(q, m, i, inc) ==
    LET old == q.mem[m] IN
    IF old.inc > inc THEN [q |-> q, refused |-> TRUE]
    ELSE IF old.inc = inc
    THEN [q |-> [q EXCEPT !.mem[m] = [old EXCEPT !.ses = i,
                                                  !.rm = IF Bug = "KnownKeepsRemoval" THEN @ ELSE -1,
                                                  !.early = IF Bug = "KnownKeepsRemoval" THEN @ ELSE FALSE]],
          refused |-> FALSE]
    ELSE LET q1 == IF old.inc # 0 /\ old.ses # i THEN [q EXCEPT !.ses[m] = @ \ {old.ses}] ELSE q
         IN [q |-> [q1 EXCEPT !.mem[m] = [NoMem EXCEPT !.inc = inc, !.ses = i]], refused |-> FALSE]

\* Server::leave: only the session that owns the record can end it.
Leave(q, m, i) == IF q.mem[m].inc # 0 /\ q.mem[m].ses = i THEN [q EXCEPT !.mem[m] = NoMem] ELSE q

\* Server::closed: only the owning session ending starts a removal. quiet: -1 for a reset, else
\* since when the session was silent (idle = true); early records, for IdleHeld, a removal set
\* sooner than §2.2 allows.
Closed(s, q, m, i, quiet) ==
    LET q1 == [q EXCEPT !.ses[m] = @ \ {i}] IN
    IF q.mem[m].inc # 0 /\ (q.mem[m].ses = i \/ Bug = "ClosedIgnoresOwner")
    THEN LET rm == now + (IF quiet # -1 THEN Hold(s) ELSE Grace) IN
         [q1 EXCEPT !.mem[m].rm = rm, !.mem[m].early = (quiet # -1 /\ rm < quiet + SeedsRemove)]
    ELSE q1

\* Frames a retiring slot still sends: what was queued, then LEAVE.
RECURSIVE Apply(_, _, _, _, _)
Apply(q, m, i, inc, msgs) ==
    IF msgs = <<>> THEN q
    ELSE Apply(IF Head(msgs) = "reg" THEN Register(q, m, i, inc).q ELSE Leave(q, m, i),
               m, i, inc, Tail(msgs))

----------------------------------------------------------------------------

Linked(m, s) == cut[m][s] = -1
\* A viewer holds no session over a silent link, whose copy of a seed's list may be stale.
NoStale(n) == \A s \in Seeds : Linked(n, s) \/ nd[n].sess[s] = 0
\* Since when nothing has come from m on its link to s: the link was cut, or its machine died.
Since(m, s) ==
    LET c == cut[m][s]
        d == nd[m].dead
    IN IF c = -1 THEN d ELSE IF d = -1 \/ c < d THEN c ELSE d
Silent(m, s, late) == Since(m, s) # -1 /\ now >= Since(m, s) + Idle - (IF late THEN 0 ELSE 1)

\* Every action ends with this, so frames on a linked session arrive before anything else happens:
\* 1. Node::lost for each member end whose seed end is gone;
\* 2. Node::frame(CHANGES, SYNCED): each held copy becomes its seed's list. A Remove of the node's
\*    own boot clears sent, so it registers again; a new SYNCED restarts the withdraw wait;
\* 3. Node::reconcile at t;
\* 4. Node::view: last is the view the node showed while it was last live.
\* Each stage is bound to a value (\E x \in {e}) so that TLC evaluates it once.
Step(ndA, sdA, cutA, t) ==
    \E n1 \in {[m \in Members |-> IF ~ndA[m].up THEN ndA[m] ELSE
                 Fresh(ndA[m], {s \in Seeds : ndA[m].sess[s] # 0 /\ cutA[m][s] = -1
                                             /\ ndA[m].sess[s] \notin sdA[s].ses[m]}, t)]} :
    \E n2 \in {[m \in Members |-> IF ~n1[m].up THEN n1[m] ELSE
                 LET st == n1[m]
                     on == {s \in Seeds : st.sess[s] # 0 /\ cutA[m][s] = -1}
                     L == [s \in Seeds |-> [x \in Members |-> sdA[s].mem[x].inc]]
                     gained == \E s \in on \cap Counted : sdA[s].warm /\ ~st.synced[s]
                 IN [st EXCEPT
                       !.nlist = [s \in Seeds |-> IF s \in on THEN L[s] ELSE st.nlist[s]],
                       !.synced = [s \in Seeds |-> st.synced[s] \/ (s \in on /\ sdA[s].warm)],
                       !.sent = [s \in Seeds |->
                                   IF s \in on /\ st.nlist[s][m] = st.inc /\ L[s][m] # st.inc
                                   THEN FALSE ELSE st.sent[s]],
                       !.minSince = IF gained /\ Bug # "OldWithdraw"
                                    THEN -1 ELSE st.minSince]]} :
    \E n3 \in {[m \in Members |-> Reconcile(m, n2[m], t)]} :
    /\ nd' = [m \in Members |-> [n3[m] EXCEPT !.last = IF LiveSt(m, n3[m]) THEN ViewSt(n3[m]) ELSE @]]
    /\ sd' = sdA

\* The seeds a running member can use at t: up, past the warm-up, with a session open to it, on a
\* link that carries frames or has been silent for less than a session can take (§2.2: "3-5 s").
\* Its list lacks a majority of them: §2.2's minority seen from outside, so it starts at the
\* earliest the node could notice, not when it does.
Usable(m, n0, s0, c0, t) ==
    {s \in Counted : s0[s].up /\ s0[s].warm /\ n0[m].sess[s] # 0 /\ (c0[m][s] = -1 \/ t < c0[m][s] + IdleSeen)}
Minority(m, n0, s0, c0, t) ==
    n0[m].up /\ 2 * Cardinality(Usable(m, n0, s0, c0, t) \cap Cur(n0[m])) <= Cardinality(Cur(n0[m]))

\* Every action ends with this too: the minorities, and the views each member shows, checked
\* against the ones before. A member may leave a view once it crashed, or once its list has
\* lacked a usable majority for LeaveGrace, and to the end of that tick: a removal that was due
\* can land in the tick the member returns, before its REGISTER; and while a LEAVE it queued then
\* still waits on a cut link, followed by the REGISTER of its return. §8 invariant 2's "partitioned
\* away" is that minority: after a cut long enough to end a session, the seed's grace (2 s) and the
\* node's redial race, and a seed may drop the member for a moment. A node attached again shows no
\* view without a running member of its last one for WarmUp (Thawing), excused or not: the others
\* cut off with it register again as each is attached again.
Thawing(n) == now' < Max(nd[n].thaw, nd'[n].thaw)
Post ==
    /\ minor' = [m \in Members |->
                   IF ~Minority(m, nd', sd', cut', now') THEN -1
                   ELSE IF minor[m] = -1 THEN now' ELSE minor[m]]
    /\ gainedAt' = [m \in Members |->
                      IF \E s \in Counted : nd'[m].synced[s] /\ ~nd[m].synced[s] THEN now' ELSE gainedAt[m]]
    /\ excused' = [m \in Members |->
                     \/ minor'[m] # -1 /\ now' >= minor'[m] + LeaveGrace
                     \/ excused[m] /\ (now' = now \/ \E s \in Seeds : \E i \in DOMAIN nd'[m].pend[s] :
                                                     nd'[m].pend[s][i] = "leave")]
    /\ lostBad' = (lostBad \/ \E n \in Members : LiveSt(n, nd'[n]) /\ \E v \in {ViewSt(nd'[n])} :
                     \E x \in Members :
                       /\ nd[n].last[x] # 0 /\ ~crashed'[x]
                       /\ \/ v[x] # nd[n].last[x] /\ ~excused'[x]
                          \/ Thawing(n) /\ v[x] = 0)

Init ==
    /\ now = 0
    /\ \E incs \in [Members -> 1..MaxInc] :
        nd = [m \in Members |-> Booted([switched |-> FALSE], incs[m])]
    /\ sd = [s \in Seeds |-> [up |-> TRUE, start |-> 0, warm |-> FALSE, down |-> 0,
                              mem |-> [x \in Members |-> NoMem], ses |-> [x \in Members |-> {}]]]
    /\ cut = [m \in Members |-> [s \in Seeds |-> -1]]
    /\ faults = 0
    /\ lastFault = 0
    /\ crashed = [m \in Members |-> FALSE]
    /\ minor = [m \in Members |-> 0]
    /\ excused = [m \in Members |-> FALSE]
    /\ gainedAt = [m \in Members |-> 0]
    /\ lostBad = FALSE
    /\ dropBy = [m \in Members |-> -1]
    /\ dropInc = [m \in Members |-> 0]

----------------------------------------------------------------------------
(* Sessions and frames. Each is due: time does not pass while one can happen. *)

\* mod.rs dial, then Node::opened and Server::open (which sends the list).
CanOpen(m, s) ==
    /\ nd[m].up /\ s \in Cur(nd[m]) /\ s \in nd[m].slots /\ nd[m].sess[s] = 0
    /\ sd[s].up /\ Linked(m, s) /\ (Bug # "NoRedial" \/ nd[m].redialBy[s] <= MaxTime)
Open(m, s) ==
    /\ CanOpen(m, s)
    /\ LET free == Ids \ (sd[s].ses[m] \cup {sd[s].mem[m].ses}) IN
       /\ free # {}
       /\ LET i == MinSet(free) IN
          Step([nd EXCEPT ![m].sess[s] = i], [sd EXCEPT ![s].ses[m] = @ \cup {i}], cut, now)
    /\ UNCHANGED <<now, cut, faults, lastFault, crashed, dropBy, dropInc>>
    /\ Post

\* Server::frame(REGISTER | LEAVE); a REFUSED answer is Node::frame(REFUSED).
CanDeliverUp(m, s) ==
    LET st == nd[m] IN
    st.up /\ st.sess[s] # 0 /\ st.sess[s] \in sd[s].ses[m] /\ Linked(m, s) /\ st.pend[s] # <<>>
DeliverUp(m, s) ==
    /\ CanDeliverUp(m, s)
    /\ LET st == nd[m]
           i == st.sess[s]
           st1 == [st EXCEPT !.pend[s] = Tail(@)]
       IN IF Head(st.pend[s]) = "reg"
          THEN LET r == Register(sd[s], m, i, st.inc) IN
               Step([nd EXCEPT ![m] = IF r.refused /\ Bug # "RefusedNoRetry"
                                     THEN [st1 EXCEPT !.sent[s] = FALSE, !.retryAt[s] = now + Retry]
                                     ELSE st1],
                    [sd EXCEPT ![s] = r.q], cut, now)
          ELSE Step([nd EXCEPT ![m] = st1], [sd EXCEPT ![s] = Leave(sd[s], m, i)], cut, now)
    /\ UNCHANGED <<now, cut, faults, lastFault, crashed, dropBy, dropInc>>
    /\ Post

\* Server::closed(idle = false): the member's end of a session is gone (its process crashed, it
\* dropped the session, it retired the seed) and the link carries the reset.
CanReset(s, m, i) ==
    sd[s].up /\ Linked(m, s) /\ nd[m].dead = -1 /\ i \in sd[s].ses[m] /\ (~nd[m].up \/ nd[m].sess[s] # i)
SeedReset(s, m, i) ==
    /\ CanReset(s, m, i)
    /\ Step(nd, [sd EXCEPT ![s] = Closed(s, sd[s], m, i, -1)], cut, now)
    /\ UNCHANGED <<now, cut, faults, lastFault, crashed, dropBy, dropInc>>
    /\ Post

\* Node::lost, as mod.rs reports a session whose link has been silent too long.
NodeLost(m, s) ==
    /\ nd[m].up /\ nd[m].sess[s] # 0 /\ Silent(m, s, FALSE)
    /\ Step([nd EXCEPT ![m] = Fresh(nd[m], {s}, now)], sd, cut, now)
    /\ UNCHANGED <<now, cut, faults, lastFault, crashed, dropBy, dropInc>>
    /\ Post

\* Server::closed(idle = true), as mod.rs read() reports a session that went silent.
SeedIdle(s, m, i) ==
    /\ sd[s].up /\ i \in sd[s].ses[m] /\ Silent(m, s, FALSE)
    /\ Step(nd, [sd EXCEPT ![s] = Closed(s, sd[s], m, i, Since(m, s))], cut, now)
    /\ UNCHANGED <<now, cut, faults, lastFault, crashed, dropBy, dropInc>>
    /\ Post

\* Server::tick: past the warm-up, SYNCED to every session.
SeedWarm(s) ==
    /\ sd[s].up /\ ~sd[s].warm /\ now >= sd[s].start + WarmUp
    /\ Step(nd, [sd EXCEPT ![s].warm = TRUE], cut, now)
    /\ UNCHANGED <<now, cut, faults, lastFault, crashed, dropBy, dropInc>>
    /\ Post

\* Server::tick: a removal that is due.
SeedRemove(s, m) ==
    /\ sd[s].up /\ sd[s].mem[m].inc # 0 /\ sd[s].mem[m].rm # -1 /\ sd[s].mem[m].rm <= now
    /\ Step(nd, [sd EXCEPT ![s].mem[m] = NoMem], cut, now)
    /\ UNCHANGED <<now, cut, faults, lastFault, crashed, dropBy, dropInc>>
    /\ Post

\* Node::reconcile's retire, and mod.rs's NodeOut::Retire: what is queued and a LEAVE are flushed
\* if the link carries them, then the session is dropped.
Retire(m, s) ==
    /\ Retiring(m, nd[m], s)
    /\ LET st == nd[m]
           i == st.sess[s]
           msgs == st.pend[s] \o (IF st.sent[s] THEN <<"leave">> ELSE <<>>)
       IN Step([nd EXCEPT ![m] = [Fresh(st, {s}, now) EXCEPT !.slots = @ \ {s}]],
               IF i # 0 /\ i \in sd[s].ses[m] /\ Linked(m, s)
               THEN [sd EXCEPT ![s] = Apply(sd[s], m, i, st.inc, msgs)]
               ELSE sd,
               cut, now)
    /\ UNCHANGED <<now, cut, faults, lastFault, crashed, dropBy, dropInc>>
    /\ Post

Due ==
    \/ \E m \in Members, s \in Seeds :
        \/ CanOpen(m, s) /\ now >= nd[m].redialBy[s]
        \/ CanDeliverUp(m, s)
        \/ \E i \in Ids : CanReset(s, m, i)
        \/ nd[m].up /\ nd[m].sess[s] # 0 /\ Silent(m, s, TRUE)
        \/ sd[s].up /\ sd[s].ses[m] # {} /\ Silent(m, s, TRUE)
        \/ Retiring(m, nd[m], s)
        \/ sd[s].up /\ sd[s].mem[m].inc # 0 /\ sd[s].mem[m].rm # -1 /\ sd[s].mem[m].rm <= now
        \/ ~Linked(m, s) /\ now >= cut[m][s] + MaxSet(CutFor)
    \/ \E s \in Seeds :
        \/ sd[s].up /\ ~sd[s].warm /\ now >= sd[s].start + WarmUp
        \/ ~sd[s].up /\ now >= sd[s].down + MaxDown
Quiet == ~Due

\* Both state machines tick every 100 ms or faster: Node::tick is reconcile.
Tick ==
    /\ now < MaxTime
    /\ Quiet
    /\ now' = now + 1
    /\ Step(nd, sd, cut, now + 1)
    /\ UNCHANGED <<cut, faults, lastFault, crashed, dropBy, dropInc>>
    /\ Post

----------------------------------------------------------------------------
(* Faults. Each sets lastFault; the recoveries it forces (a heal, a seed's start) fall inside
   Settle, as the ASSUMEs say. *)

Fault == now >= FaultFrom /\ now <= FaultUntil /\ faults < MaxFaults

\* Some of a member's links go silent: a blip, or a partition.
Cut(m, T) ==
    /\ Fault /\ nd[m].up
    /\ T # {} /\ \A s \in T : Linked(m, s)
    /\ cut' = [cut EXCEPT ![m] = [s \in Seeds |-> IF s \in T THEN now ELSE @[s]]]
    /\ faults' = faults + 1
    /\ lastFault' = now
    /\ UNCHANGED <<now, nd, sd, crashed, dropBy, dropInc>>
    /\ Post

\* The link carries frames again: what waited arrives, and an end whose peer is gone is reset.
Heal(m, s) ==
    /\ ~Linked(m, s) /\ now - cut[m][s] \in CutFor
    /\ cut' = [cut EXCEPT ![m][s] = -1]
    /\ Step([nd EXCEPT ![m].redialBy[s] = IF Bug = "NoRedial" THEN MaxTime + 1 ELSE now + RedialMax],
            sd, cut', now)
    /\ UNCHANGED <<now, faults, lastFault, crashed, dropBy, dropInc>>
    /\ Post

\* A seed restarts without state; its sessions reset.
SeedCrash(s) ==
    /\ Fault /\ sd[s].up
    /\ Step(nd, [sd EXCEPT ![s] = [up |-> FALSE, start |-> 0, warm |-> FALSE, down |-> now,
                                   mem |-> [x \in Members |-> NoMem], ses |-> [x \in Members |-> {}]]],
            cut, now)
    /\ faults' = faults + 1
    /\ lastFault' = now
    /\ UNCHANGED <<now, cut, crashed, dropBy, dropInc>>
    /\ Post

SeedStart(s) ==
    /\ ~sd[s].up
    /\ Step([m \in Members |-> [nd[m] EXCEPT !.redialBy[s] = now + RedialMax]],
            [sd EXCEPT ![s].up = TRUE, ![s].start = now], cut, now)
    /\ UNCHANGED <<now, cut, faults, lastFault, crashed, dropBy, dropInc>>
    /\ Post

\* §8 invariant 5 tells a crashed node, whose every seed sees the reset and starts the grace, from a
\* dead one, which some seed has to hear go silent.
Resets(m) == \A s \in Seeds :
    LET e == sd[s].mem[m] IN
    e.inc # nd[m].inc \/ (e.rm = -1 /\ Linked(m, s) /\ e.ses \in sd[s].ses[m])

Crash(m, machine) ==
    /\ Fault /\ nd[m].up
    /\ Step([nd EXCEPT ![m].up = FALSE, ![m].dead = IF machine THEN now ELSE -1], sd, cut, now)
    /\ dropBy' = [dropBy EXCEPT ![m] = now + IF ~machine /\ Resets(m) THEN ProcessGone ELSE DeadGone]
    /\ dropInc' = [dropInc EXCEPT ![m] = nd[m].inc]
    /\ crashed' = [crashed EXCEPT ![m] = TRUE]
    /\ faults' = faults + 1
    /\ lastFault' = now
    /\ UNCHANGED <<now, cut>>
    /\ Post

\* A boot under the same id with another incarnation, lower when its clock is behind. The seed list
\* is the last one it was given.
Boot(m) ==
    /\ Fault /\ ~nd[m].up
    /\ \E i \in {j \in 1..MaxInc : j # nd[m].inc /\ (BootDown \/ j > nd[m].inc)} :
        Step([nd EXCEPT ![m] = Booted(nd[m], i)], sd, cut, now)
    /\ faults' = faults + 1
    /\ lastFault' = now
    /\ UNCHANGED <<now, cut, crashed, dropBy, dropInc>>
    /\ Post

\* Node::set_seeds: the majority counts over the new list at once; dropped seeds stay until live.
SetSeeds(m) ==
    /\ Fault /\ nd[m].up /\ ~nd[m].switched /\ L1 # L0
    /\ LET st == nd[m]
           added == L1 \ st.slots
       IN Step([nd EXCEPT ![m] = [st EXCEPT !.switched = TRUE, !.slots = @ \cup L1,
                    !.redialBy = [s \in Seeds |-> IF s \in added THEN now + RedialMax ELSE @[s]]]],
               sd, cut, now)
    /\ faults' = faults + 1
    /\ lastFault' = now
    /\ UNCHANGED <<now, cut, crashed, dropBy, dropInc>>
    /\ Post

----------------------------------------------------------------------------

\* The horizon: a state there may stay, so that CHECK_DEADLOCK finds any state before it with
\* something due and no action to take.
Done == now = MaxTime /\ UNCHANGED vars

Next ==
    \/ Tick \/ Done
    \/ \E m \in Members, s \in Seeds :
        \/ Open(m, s) \/ DeliverUp(m, s) \/ NodeLost(m, s) \/ Retire(m, s)
        \/ SeedRemove(s, m) \/ Heal(m, s)
        \/ \E i \in Ids : SeedIdle(s, m, i) \/ SeedReset(s, m, i)
    \/ \E s \in Seeds : SeedWarm(s) \/ SeedCrash(s) \/ SeedStart(s)
    \/ \E m \in Members :
        \/ Crash(m, FALSE) \/ Crash(m, TRUE) \/ Boot(m) \/ SetSeeds(m)
        \/ \E T \in SUBSET Seeds : Cut(m, T)

Spec == Init /\ [][Next]_vars

Symmetry == Permutations(Members)

\* Two states that differ only in what can no longer matter are the same state: a timer that has
\* run out, a warm seed's start, a down member's sessions, a crashed member's excuse (crashed
\* stays set, and excuses it anyway), a SYNCED gained before the minority began (a minority that
\* begins later begins after it), faults once none can come. Under SYMMETRY the distinct count
\* varies by a few states between runs: TLC picks a state's representative by its full value, then
\* takes this view of it. Without SYMMETRY two runs give the same count.
Past(t) == IF t <= now THEN 0 ELSE t
NodeView(m) ==
    LET st == nd[m] IN
    IF ~st.up
    THEN [inc |-> st.inc, switched |-> st.switched,
          dead |-> IF \E s \in Seeds : sd[s].ses[m] # {} THEN st.dead ELSE -1]
    ELSE [st EXCEPT
            !.redialBy = [s \in Seeds |-> IF s \in st.slots /\ st.sess[s] = 0 THEN Past(@[s]) ELSE 0],
            !.retryAt = [s \in Seeds |-> Past(@[s])],
            !.minSince = IF @ # -1 /\ @ + WithdrawAfter <= now THEN -2 ELSE @]
SeedView(s) ==
    LET q == sd[s] IN
    [q EXCEPT !.start = IF q.warm THEN 0 ELSE @, !.down = IF q.up THEN 0 ELSE @]
Fingerprint ==
    <<now, [m \in Members |-> NodeView(m)], [s \in Seeds |-> SeedView(s)],
      cut,
      IF now > FaultUntil THEN MaxFaults ELSE faults,
      Max(lastFault, now - Settle), crashed,
      [m \in Members |-> IF minor[m] # -1 /\ minor[m] + MinorityGone <= now THEN -2 ELSE minor[m]],
      [m \in Members |-> excused[m] /\ ~crashed[m]],
      [m \in Members |-> IF minor[m] = -1 \/ gainedAt[m] <= minor[m] \/ gainedAt[m] + MinorityGone <= now
                         THEN -1 ELSE gainedAt[m]], lostBad,
      [m \in Members |-> IF dropBy[m] # -1 /\ dropBy[m] <= now THEN 0 ELSE dropBy[m]], dropInc>>

----------------------------------------------------------------------------
(* Promises *)

\* §2.1 "A node's link to one seed blips: nothing visible", §8 invariants 2 and 3: a live view
\* loses or replaces a member only after that member crashed, or its list lacked a usable majority
\* for LeaveGrace (Post). A cut to a minority of its seeds, or a shorter one to more, changes no view.
\* §2.2: back from a cut, a node's view keeps the others cut off with it while they register again,
\* for WarmUp (SeedMembership-NoThaw.cfg).
NoSpuriousLoss == ~lostBad

\* §4.2 "Only that session ending starts a removal", §4.3 "unless the same boot has registered
\* again": a seed never holds a removal for a record whose session is still open.
RemovalNeedsOwnerGone == \A s \in Seeds, m \in Members :
    sd[s].mem[m].rm # -1 => sd[s].mem[m].ses \notin sd[s].ses[m]

\* §8 invariant 1: a live node's view holds its own boot. SeedMembership-LowerBoot.cfg has a boot
\* under a lower incarnation listed by 2 of 3 seeds while the third still lists the old, greater one.
SelfInView == \A n \in Members : Live(n) => View(n)[n] = nd[n].inc

\* §2: two live nodes share a seed that lists both, so they see each other.
LiveAgree == Quiet => \A a, b \in Members : (Live(a) /\ Live(b)) => View(a)[b] = nd[b].inc

\* §8 invariant 5: a crashed boot is out of every live view within 3 s, a dead one within 11 s.
\* DroppedInTime asks it of viewers with no session on a silent link. A viewer that holds one counts
\* that seed's last list until the session idles, which nothing it hears can shorten: DroppedStale
\* gives every viewer that much more, Idle - 1 here where idle comes a tick early (header); Idle - 2
\* fails.
Dropped(n, late) == \A x \in Members : (dropBy[x] # -1 /\ now >= dropBy[x] + late) => View(n)[x] # dropInc[x]
DroppedInTime == Quiet => \A n \in Members : (Live(n) /\ NoStale(n)) => Dropped(n, 0)
DroppedStale == Quiet => \A n \in Members : Live(n) => Dropped(n, Idle - 1)

\* §2.2 "the seeds remove it 8-10 s" after it is cut off: the hold that gives a node time to
\* stop its keyed actors first (isolation itself is not modelled). Asked of the seeds nodes
\* count: an odd seed drops it sooner and is in no view (SeedMembership-Odd.cfg), and
\* SeedMembership-HoldBelowValidated.cfg counts it.
IdleHeld == \A s \in Counted, m \in Members : sd[s].mem[m].rm # -1 => ~sd[s].mem[m].early

\* §2.2 "Seeds still warming up keep its registration", §4.2 "by the end of the warm-up the list
\* is whole": a warming seed holds every running member with a session open to it. With 2 seeds
\* NoSpuriousLoss excuses every member while a seed is down or warming, so this is what checks
\* §8 invariant 2's "all seeds restarting".
WarmKeeps == Quiet => \A x \in Members, s \in Seeds :
    LET st == nd[x] IN
    (/\ st.up /\ s \in st.slots /\ s \in Cur(st) /\ st.sess[s] # 0 /\ st.sess[s] \in sd[s].ses[x]
     /\ Linked(x, s) /\ sd[s].up /\ ~sd[s].warm /\ st.retryAt[s] <= now)
        => sd[s].mem[x].inc # 0

\* §8 invariant 9: a running member whose list has lacked a usable majority for 10 s, counted from
\* the last SYNCED it gained as §2.2's wait is, is in no live view without a stale copy. Except
\* (Pending) while a seed still holds its record with a removal set: a seed that kept the
\* registration while warming, then lost the member to a cut, removes it after the idle timeout
\* and the hold, later than 10 s after a minority that began before the cut. A gap in §8's text,
\* not caught by sim.rs, whose minority test is one cut.
Pending(x) == \E s \in Seeds : sd[s].mem[x].inc = nd[x].inc /\ sd[s].mem[x].rm # -1
MinorityOut == Quiet => \A n, x \in Members :
    (Live(n) /\ NoStale(n) /\ minor[x] # -1 /\ now >= Max(minor[x], gainedAt[x]) + MinorityGone /\ ~Pending(x))
        => View(n)[x] # nd[x].inc

\* §8 invariant 1 at rest: Settle ticks after the last fault, every running member is live and
\* sees exactly the running members' boots.
Converged == (Quiet /\ now >= lastFault + Settle) => \A n \in Members :
    nd[n].up => (Live(n) /\ View(n) = [x \in Members |-> IF nd[x].up THEN nd[x].inc ELSE 0])

=============================================================================
