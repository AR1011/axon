---------------------------- MODULE PushWindow ----------------------------
(* Windowed stream pushes from one parent to one child (spec/push-window.md §2, §7): a channel
   per Downstream, numbered pushes, several in flight and several of one stream among them,
   go-back-N on a failure in rounds, a stream's items in flight charged against its branch until
   the child has applied them, a child that retires the parent boots it left and forgets them
   when the parent answers an interest out of sync. The parent is the stream's origin and keeps
   every record; the child relays to a leaf, whose position `have` it asks from.
   Both restart; a restart of the child forgets its channel state, a restart of the parent its
   Downstream and its channel counter.

   Abstractions, and what they could hide:
   - Records are integers per stream and an item is a run of them; a push's size is its item
     count, a branch's charge is counted in items (a reset item counts one), and Window counts
     pushes. The round trip, the rate samples and the window's growth are not modelled, nor the
     node cap: one parent has one child here, so the cap's bytes, the floor and the fair share
     (the_node_cap_is_shared_fairly_between_busy_channels) are the core tests' (§10). A bug in
     the rate estimate, in the cap's byte counting, or a charge right in items and wrong in bytes
     would not show here.
   - A branch at the parent has its position `pos` and `q` items queued, records pos-q+1..pos. A
     branch behind the origin reads from the store up to its charge's bound in the same step that
     opens it or lowers its charge (Filled: a replay, which a full branch holds back; read at
     once, not one record at a time, so a replay that a publish overtakes is not explored, nor
     the code's reset of a replaying branch, hub.rs Sub::offer, which ends as the edge's reset
     below does); a branch at the origin's edge takes each publish, and one whose charge would
     pass MaxBranch is reset as a slow consumer: its queue becomes one reset item and its epoch
     goes up, so what it had in flight is no longer its charge. A reset or end item, once in a
     push, drops the branch (core.rs: next_push's `done`); the child's next interest opens it
     again at its `have` with the next epoch. A branch made again starts from the epoch after its
     last, a new Downstream from epoch 1: an answer of another channel is ignored, so their
     epochs never meet.
   - The child takes a reset item as a reset of its feed that keeps its position: it asks again
     from `have`. Ready, CaughtUp, Path and `unwanted` are not modelled: the child wants every
     stream all the time. Its gap check (accept()'s RecoveryError::Gap) is left out, so a parent
     that skips records shows as a gap in `got` (InOrder) where the code would reset the leaf.
   - Interests go to the parent node, whichever boot it is. Revisions and deltas reduce to one
     bit: an interest is whole, or a delta that the parent takes only while it holds the child's
     boot; otherwise it answers out of sync (§2.1), and the child forgets its channel state for
     the parent, its retired boots with it, and asks again whole. The code also answers out of
     sync while it holds the child's boot, when the delta's base is not the revision it holds
     (core.rs, after a lost answer); here a delta to the held boot is always taken, and the state
     such an answer leaves is the one Forget leaves. The child takes the answer to an interest in
     the parent's step (Answered says why that loses nothing).
   - Messages are a set. A go-back starts the channel's next round, and its resends carry it
     (PushId::round); an answer carries the round of its push, and one of an earlier round only
     frees (§2.3). Rounds are only ever compared with the channel's current one, so a message
     says only whether it is of the current round (cur), and a go-back turns the channel's pushes
     and answers in flight old: the old copies of one seq are one message (a second copy finds
     the child past it, unless it forgot the channel in between, one fault more). Nothing is
     duplicated in the network; a push arrives twice only as a resend. Answers and interests
     arrive in any order, and old pushes and pushes of other channels and boots: a late one may
     arrive at any time. A push overtakes an earlier one of its channel and round (current, or
     old) only as a fault, as a loss is: one round's pushes go on one session, which keeps their
     order.
     Unbounded, a push 2 that always overtakes push 1 at a child with no channel state would
     retire every new channel in turn (§2.2's third line) and Done could not hold; one session
     never does that.
   - A call's failure is Fail (a fault: a go-back with nothing lost) or Timeout (fair, once the
     channel has unacked pushes and nothing of it is in flight: a push or its answer was lost).
     Expire is downstream_expiry, a fault, taken while pushes are unacked. A push to a child boot
     that has gone is refused StaleNode (Stale, fair); the pushes and the interest in flight to a
     child boot that has gone are lost with it, and the answers and interests in flight to a
     parent boot that has gone. Forget, a fault, is the parent leaving the child's view and coming
     back while the child follows its live channel: the child forgets its channel state while the
     parent keeps its Downstream, and may go back on what it answered. The code forgets on every
     leave (parents_left), also while the child follows no channel or an old one; each such case
     costs at most one channel more, which MaxChan does not count.
   - A behaviour has at most MaxFaults faults in all (a loss, a reorder in a round, Fail, Expire,
     Forget, a restart of either node) and MaxBoots boots of each node; publishes stop at MaxRec
     a stream. Interests go only while the parent lacks what they would make, and an answer no
     Downstream will take leaves the network at once (Prune): both only cut states. *)
EXTENDS Integers, Sequences

CONSTANTS
    Streams,
    MaxRec,             \* records published per stream
    Window,             \* pushes unacked on a channel, at most
    MaxInflight,        \* a stream's items in unacked pushes of its epoch, at most: the model's half of its branch
    MaxBranch,          \* a branch's charge, at most: CoreConfig::branch
    MaxBoots,           \* boots of each node
    MaxChan,            \* channels a parent boot makes, at most, in the faults allowed (FewChannels)
    MaxFaults,
    Mutation            \* "none", or the rule a mutations/PushWindow-*.cfg removes; each is named where it acts

ASSUME Window >= 1 /\ MaxInflight >= 1 /\ MaxBranch >= MaxInflight

VARIABLES
    pboot, cboot, pub,      \* boots; records published
    ds, nextCh,             \* the parent's Downstream (ch 0: none), its channel counter
    cs, retired, synced,    \* the child's channel state (boot 0: none), its retired parent boots (a set: with
                            \* MaxBoots 2 it holds one, so MAX_RETIRED's cap is the core tests'), and whether
                            \* its interests are in sync with the parent (FALSE: the next one is whole)
    have, got,              \* the leaf's position per stream, and what it received in order
    net, faults,
    applog                  \* history: the seqs the child applied on its channel

vars == <<pboot, cboot, pub, ds, nextCh, cs, retired, synced, have, got, net, faults, applog>>
parentVars == <<ds, nextCh>>
childVars == <<cs, retired, synced, applog>>
leafVars == <<have, got>>

Zero == [s \in Streams |-> 0]
NoItem == [k |-> "none", lo |-> 0, hi |-> 0, ep |-> 0]
NoItems == [s \in Streams |-> NoItem]
\* forgot is history: the child forgot this Downstream's channel (Forget) since it was made.
NoDs == [cb |-> 0, ch |-> 0, next |-> 1, acked |-> 0, un |-> <<>>, open |-> [s \in Streams |-> "none"],
         pos |-> Zero, q |-> Zero, charge |-> Zero, ep |-> Zero, forgot |-> FALSE]
NoCs == [boot |-> 0, ch |-> 0, applied |-> 0]
Up == ds.ch # 0
Unacked == Up /\ ds.acked < ds.next - 1
\* A push or an answer of d's channel.
OfChannel(m, d) == m.t \in {"push", "ans"} /\ m.pb = pboot /\ m.ch = d.ch
Max(a, b) == IF a > b THEN a ELSE b
Min(a, b) == IF a < b THEN a ELSE b
Range(f) == {f[i] : i \in DOMAIN f}
Run(lo, hi) == [i \in 1..(hi - lo + 1) |-> lo + i - 1]

Msg(t, pb, cb, ch, seq, cur, items, st, ap, h, wh) ==
    [t |-> t, pb |-> pb, cb |-> cb, ch |-> ch, seq |-> seq, cur |-> cur, items |-> items, st |-> st, ap |-> ap,
     h |-> h, wh |-> wh]
PushMsg(cb, ch, seq, items) == Msg("push", pboot, cb, ch, seq, TRUE, items, "", 0, Zero, FALSE)
AnsMsg(m, st, ap) == Msg("ans", m.pb, 0, m.ch, m.seq, m.cur, NoItems, st, ap, Zero, FALSE)
IntMsg(cb, h, wh) == Msg("int", pboot, cb, 0, 0, FALSE, NoItems, "", 0, h, wh)

RECURSIVE SumF(_, _, _)
SumF(f, lo, hi) == IF lo > hi THEN 0 ELSE f[lo] + SumF(f, lo + 1, hi)

Size(it) == IF it.k = "recs" THEN it.hi - it.lo + 1 ELSE 0
\* A stream's items of its branch's current epoch in d's unacked pushes acked+1..to.
Charged(d, s, to) ==
    SumF([j \in d.acked + 1 .. to |-> IF d.un[j][s].ep = d.ep[s] THEN Size(d.un[j][s]) ELSE 0], d.acked + 1, to)
InFlight(d, s) == Charged(d, s, d.next - 1)
\* What the parent holds for a branch: its queue and its items in flight.
Holds(d, s) == d.q[s] + (IF d.open[s] = "reset" THEN 1 ELSE 0) + InFlight(d, s)
\* Each open branch's charge less amt[s].
Release(d, amt) == [s \in Streams |-> IF d.open[s] = "none" THEN 0 ELSE d.charge[s] - amt[s]]

Init ==
    /\ pboot = 1 /\ cboot = 1 /\ pub = Zero
    /\ ds = NoDs /\ nextCh = 1
    /\ cs = NoCs /\ retired = {} /\ synced = FALSE
    /\ have = Zero /\ got = [s \in Streams |-> <<>>]
    /\ net = {} /\ faults = 0
    /\ applog = <<>>

----------------------------------------------------------------------------
(* The parent *)

\* A branch behind the origin reads from the store while its charge allows (a replay, which a full
\* branch holds back), here at once whenever the charge goes down or it opens.
Filled(d) ==
    LET k(s) == IF d.open[s] = "open" THEN Max(0, Min(pub[s] - d.pos[s], MaxBranch - d.charge[s])) ELSE 0
    IN [d EXCEPT !.pos = [s \in Streams |-> d.pos[s] + k(s)],
                 !.q = [s \in Streams |-> d.q[s] + k(s)],
                 !.charge = [s \in Streams |-> d.charge[s] + k(s)]]

\* §2.3: free every unacked push at or below a and release what each charged its branches, to
\* the epoch it charged. Mutation ReleaseAcrossEpochs: to the branch's current epoch, whichever
\* epoch the push charged.
Free(d, a) ==
    LET to == Min(a, d.next - 1)
        freed(s) == IF Mutation = "ReleaseAcrossEpochs"
                    THEN SumF([j \in d.acked + 1 .. to |-> Size(d.un[j][s])], d.acked + 1, to)
                    ELSE Charged(d, s, to)
    IN Filled([d EXCEPT !.acked = to,
                        !.un = [j \in to + 1 .. d.next - 1 |-> d.un[j]],
                        !.charge = Release(d, [s \in Streams |-> freed(s)])])

\* §2.3: go back, in the channel's next round, sending every unacked push again, in order and still
\* charged; what the channel has in flight is of an earlier round now. A set of messages takes them
\* all at once. Mutation ResendFromNext: from acked + 2. Mutation ReleaseOnGoBack: what they
\* charged is released.
GoBack(d, n) ==
    LET from == d.acked + (IF Mutation = "ResendFromNext" THEN 2 ELSE 1)
    IN /\ ds' = IF Mutation = "ReleaseOnGoBack"
                THEN Filled([d EXCEPT !.charge = Release(d, [s \in Streams |-> InFlight(d, s)])])
                ELSE d
       /\ net' = {IF OfChannel(m, d) THEN [m EXCEPT !.cur = FALSE] ELSE m : m \in n}
                 \cup {PushMsg(d.cb, d.ch, j, d.un[j]) : j \in IF d.cb = cboot THEN from .. d.next - 1 ELSE {}}

\* An answer no Downstream of this boot will take again (its channel is gone, and ids are not
\* reused) leaves the network at once: its arrival would be ignored, so that is the same as
\* arriving now. Under ReuseChannel an old channel's answers can come back to life.
Prune(n, d) ==
    {m \in n : m.t # "ans" \/ (m.pb = pboot /\ (Mutation = "ReuseChannel" \/ (d.ch # 0 /\ m.ch = d.ch)))}

DropDs(n) == ds' = NoDs /\ net' = Prune(n, NoDs)

\* core.rs: next_push. A new push within Window (mutation NoWindow: without), taking from each
\* branch all of its queue that keeps the stream's items in flight at most MaxInflight (mutation
\* NoStreamLimit: all of it), or its reset item, which drops the branch once encoded. The items
\* taken stay charged (mutation UncountedInflight: they leave the charge, today's poll). Mutation
\* AckOnSend: the push counts as acknowledged once sent.
Send ==
    /\ Up /\ ds.cb = cboot
    /\ ds.next - 1 - ds.acked < Window \/ Mutation = "NoWindow"
    /\ LET k == [s \in Streams |->
                    CASE ds.open[s] = "open" ->
                            IF Mutation = "NoStreamLimit" THEN ds.q[s]
                            ELSE Max(0, Min(ds.q[s], MaxInflight - InFlight(ds, s)))
                      [] ds.open[s] = "reset" -> 1
                      [] OTHER -> 0]
           seq == ds.next
           gone(s) == ds.open[s] = "reset"
           first(s) == ds.pos[s] - ds.q[s] + 1
           items == [s \in Streams |->
                        CASE k[s] = 0 -> NoItem
                          [] gone(s) -> [k |-> "reset", lo |-> 0, hi |-> 0, ep |-> ds.ep[s]]
                          [] OTHER -> [k |-> "recs", lo |-> first(s), hi |-> first(s) + k[s] - 1, ep |-> ds.ep[s]]]
           d == [ds EXCEPT
                    !.next = seq + 1,
                    !.un = [j \in ds.acked + 1 .. seq |-> IF j = seq THEN items ELSE ds.un[j]],
                    !.open = [s \in Streams |-> IF gone(s) THEN "none" ELSE ds.open[s]],
                    !.q = [s \in Streams |-> IF gone(s) THEN 0 ELSE ds.q[s] - k[s]],
                    !.charge = [s \in Streams |-> CASE gone(s) -> 0
                                                    [] Mutation = "UncountedInflight" -> ds.charge[s] - k[s]
                                                    [] OTHER -> ds.charge[s]],
                    !.pos = [s \in Streams |-> IF gone(s) THEN 0 ELSE ds.pos[s]]]
       IN /\ \E s \in Streams : k[s] > 0
          /\ ds' = CASE Mutation = "AckOnSend" -> Free(d, seq)
                     [] Mutation = "UncountedInflight" -> Filled(d)
                     [] OTHER -> d
          /\ net' = net \cup {PushMsg(ds.cb, ds.ch, seq, items)}
    /\ UNCHANGED <<pboot, cboot, pub, nextCh, faults>>
    /\ UNCHANGED <<childVars, leafVars>>

\* hub.rs: Sub::enqueue and Sub::fits, queued plus in flight: a branch at the origin's edge takes
\* the record, or is reset as a slow consumer (SlowConsumerPolicy::Reset).
Publish(s) ==
    /\ pub[s] < MaxRec
    /\ pub' = [pub EXCEPT ![s] = @ + 1]
    /\ ds' = IF Up /\ ds.open[s] = "open" /\ ds.pos[s] = pub[s]
             THEN IF ds.charge[s] + 1 > MaxBranch
                  THEN [ds EXCEPT !.open[s] = "reset", !.ep[s] = @ + 1, !.q[s] = 0, !.charge[s] = 1, !.pos[s] = 0]
                  ELSE [ds EXCEPT !.pos[s] = @ + 1, !.q[s] = @ + 1, !.charge[s] = @ + 1]
             ELSE ds
    /\ UNCHANGED <<pboot, cboot, nextCh, net, faults>>
    /\ UNCHANGED <<childVars, leafVars>>

\* §2.3, core.rs: push_answered, an answer to seq k on channel ch, its applied cut to what was
\* sent. One below acked is late, unless k is above acked and of the current round: then
\* the child went back on what it answered, and the Downstream goes (mutation IgnoreWentBack: it
\* is ignored too). A gap goes back only for the current round. Mutation RetiredAsApplied:
\* Retired acknowledges k.
DeliverAnswer(m) ==
    /\ m \in net /\ m.t = "ans"
    /\ LET n == net \ {m}
           ap == Min(m.ap, ds.next - 1)
       IN IF ~Up \/ m.pb # pboot \/ m.ch # ds.ch
          THEN net' = n /\ UNCHANGED ds                            \* a channel no longer kept
          ELSE CASE m.st = "retired" /\ Mutation # "RetiredAsApplied" -> DropDs(n)
                 [] m.st = "retired" -> ds' = (IF m.seq < ds.acked THEN ds ELSE Free(ds, m.seq)) /\ net' = n
                 [] ap < ds.acked ->
                      IF m.seq > ds.acked /\ m.cur /\ Mutation # "IgnoreWentBack"
                      THEN DropDs(n)
                      ELSE net' = n /\ UNCHANGED ds
                 [] m.st = "applied" \/ ap >= m.seq \/ ~m.cur -> ds' = Free(ds, ap) /\ net' = n
                 [] OTHER -> GoBack(Free(ds, ap), n)
    /\ UNCHANGED <<pboot, cboot, pub, nextCh, faults>>
    /\ UNCHANGED <<childVars, leafVars>>

\* A call of the current round fails with nothing lost: the session reset, or the call timed out
\* while its answer was on the way. A go-back from acked + 1.
Fail ==
    /\ Unacked /\ ds.cb = cboot /\ faults < MaxFaults
    /\ faults' = faults + 1
    /\ GoBack(ds, net)
    /\ UNCHANGED <<pboot, cboot, pub, nextCh>>
    /\ UNCHANGED <<childVars, leafVars>>

\* A call times out: the channel has unacked pushes and nothing of it is in flight.
Timeout ==
    /\ Unacked /\ ds.cb = cboot
    /\ ~\E m \in net : OfChannel(m, ds)
    /\ GoBack(ds, net)
    /\ UNCHANGED <<pboot, cboot, pub, nextCh, faults>>
    /\ UNCHANGED <<childVars, leafVars>>

\* downstream_expiry, taken while pushes are unacked (where the Downstream's loss can matter).
Expire ==
    /\ Unacked /\ faults < MaxFaults
    /\ faults' = faults + 1
    /\ DropDs(net)
    /\ UNCHANGED <<pboot, cboot, pub, nextCh>>
    /\ UNCHANGED <<childVars, leafVars>>

\* A push to a child boot that has gone is refused StaleNode: the Downstream goes at once.
Stale ==
    /\ Up /\ ds.cb # cboot
    /\ DropDs(net)
    /\ UNCHANGED <<pboot, cboot, pub, nextCh, faults>>
    /\ UNCHANGED <<childVars, leafVars>>

\* §2.1, core.rs: interest_result. The child takes the answer in the same step: until it does,
\* the parent does nothing more for it (its next interest waits for the answer), so a later
\* arrival is the same as this one with the child's other steps first. Out of sync, the child
\* forgets its channel state for the parent, retired boots too (mutation NoForgetOnOutOfSync: it
\* keeps them), and its next interest is whole.
ForgetParent == cs' = NoCs /\ retired' = {} /\ applog' = <<>>

Answered(ok) ==
    /\ synced' = ok
    /\ IF ~ok /\ Mutation # "NoForgetOnOutOfSync" THEN ForgetParent ELSE UNCHANGED <<cs, retired, applog>>

\* core.rs: handle_interest. A Downstream of another child boot goes. One that holds this child
\* boot opens the branches it lacks at `have`, in their next epoch. Without one, a delta is
\* answered out of sync; a whole interest makes one, with the next channel id of this parent boot
\* (mutation ReuseChannel: the last one again), its branches at the interest's `have` (mutation
\* CreateFromEdge: at the origin's edge).
DeliverInterest(m) ==
    /\ m \in net /\ m.t = "int"
    /\ LET n == net \ {m} IN
       IF Up /\ ds.cb = m.cb
       THEN /\ ds' = Filled([ds EXCEPT
                        !.open = [s \in Streams |-> IF ds.open[s] = "none" THEN "open" ELSE ds.open[s]],
                        !.pos = [s \in Streams |-> IF ds.open[s] = "none" THEN m.h[s] ELSE ds.pos[s]],
                        !.ep = [s \in Streams |-> IF ds.open[s] = "none" THEN ds.ep[s] + 1 ELSE ds.ep[s]]])
            /\ net' = n /\ Answered(TRUE)
            /\ UNCHANGED nextCh
       ELSE IF ~m.wh
       THEN DropDs(n) /\ Answered(FALSE) /\ UNCHANGED nextCh
       ELSE LET ch == IF Mutation = "ReuseChannel" /\ nextCh > 1 THEN nextCh - 1 ELSE nextCh
                d == Filled([NoDs EXCEPT !.cb = m.cb, !.ch = ch, !.open = [s \in Streams |-> "open"],
                                         !.pos = IF Mutation = "CreateFromEdge" THEN pub ELSE m.h,
                                         !.ep = [s \in Streams |-> 1]])
            IN /\ ds' = d /\ net' = Prune(n, d) /\ Answered(TRUE)
               /\ nextCh' = IF ch = nextCh THEN nextCh + 1 ELSE nextCh
    /\ UNCHANGED <<pboot, cboot, pub, faults>>
    /\ UNCHANGED leafVars

ParentRestart ==
    /\ pboot < MaxBoots /\ faults < MaxFaults
    /\ pboot' = pboot + 1 /\ ds' = NoDs /\ nextCh' = 1 /\ faults' = faults + 1
    /\ net' = {m \in net : ~(m.t \in {"ans", "int"} /\ m.pb = pboot)}
    /\ UNCHANGED <<cboot, pub>>
    /\ UNCHANGED <<childVars, leafVars>>

----------------------------------------------------------------------------
(* The child *)

\* The leaf takes a push's records above its position (mutation NoPositionCheck: every one).
Apply(items) ==
    LET from(s) == IF Mutation = "NoPositionCheck" THEN items[s].lo ELSE Max(items[s].lo, have[s] + 1)
    IN /\ got' = [s \in Streams |-> IF items[s].k = "recs" THEN got[s] \o Run(from(s), items[s].hi) ELSE got[s]]
       /\ have' = [s \in Streams |-> IF items[s].k = "recs" THEN Max(have[s], items[s].hi) ELSE have[s]]

\* §2.2 on the channel the child follows, c: only the next seq is applied (mutation AcceptAhead:
\* any above it; ApplyDuplicate: one at or below it again too). Mutation AckAboveApplied: the
\* answer says one more than the child applied. n is the network without the push.
Current(m, c, log, n) ==
    LET a == c.applied
        take == m.seq = a + 1 \/ (Mutation = "AcceptAhead" /\ m.seq > a)
        use == take \/ (Mutation = "ApplyDuplicate" /\ m.seq <= a)
        say(x) == IF Mutation = "AckAboveApplied" THEN x + 1 ELSE x
    IN /\ cs' = IF take THEN [c EXCEPT !.applied = m.seq] ELSE c
       /\ applog' = IF use THEN Append(log, m.seq) ELSE log
       /\ IF use THEN Apply(m.items) ELSE UNCHANGED leafVars
       /\ net' = Prune(n \cup {AnsMsg(m, IF use THEN "applied" ELSE "skipped", say(IF take THEN m.seq ELSE a))}, ds)

RetiredAns(m, n) ==
    /\ net' = Prune(n \cup {AnsMsg(m, "retired", 0)}, ds)
    /\ UNCHANGED <<childVars, leafVars>>

\* §2.2, core.rs: handle_push for push (pb, ch, seq). With no state for the parent only a seq 1
\* is followed. A retired boot, or an older channel of the boot followed, is Retired. A new boot
\* retires the one followed; it and a newer channel are followed from any seq. Mutation
\* KeepAppliedOnNewBoot: a new boot's channel is followed from the old boot's applied.
Child(m, n) ==
    LET new == [boot |-> m.pb, ch |-> m.ch, applied |-> 0] IN
    CASE cs.boot = 0 -> IF m.seq = 1 THEN Current(m, new, <<>>, n) /\ UNCHANGED <<retired, synced>>
                        ELSE RetiredAns(m, n)
      [] m.pb \in retired \/ (m.pb = cs.boot /\ m.ch < cs.ch) -> RetiredAns(m, n)
      [] m.pb # cs.boot ->
           /\ retired' = retired \cup {cs.boot}
           /\ Current(m, IF Mutation = "KeepAppliedOnNewBoot" THEN [new EXCEPT !.applied = cs.applied] ELSE new,
                      <<>>, n)
           /\ UNCHANGED synced
      [] m.ch > cs.ch -> Current(m, new, <<>>, n) /\ UNCHANGED <<retired, synced>>
      [] OTHER -> Current(m, cs, applog, n) /\ UNCHANGED <<retired, synced>>

\* A push arrives. Overtaking an earlier push of its round still in flight is a fault, and so,
\* the old rounds being one here, is an old push overtaking an older seq.
DeliverPush(m) ==
    /\ m \in net /\ m.t = "push" /\ m.cb = cboot
    /\ LET early == \E e \in net : e.t = "push" /\ e.cur = m.cur /\ e.pb = m.pb /\ e.cb = m.cb /\ e.ch = m.ch
                                   /\ e.seq < m.seq
       IN /\ early => faults < MaxFaults
          /\ faults' = IF early THEN faults + 1 ELSE faults
          /\ Child(m, net \ {m})
    /\ UNCHANGED <<pboot, cboot, pub, parentVars>>

\* The child asks with its leaf's position, one interest in flight at a time: it may be stale by
\* the time it arrives, and it is whole once the parent answered one out of sync. It asks while
\* the parent lacks its Downstream or one of its branches; an interest that would change nothing
\* is left out.
Interest ==
    /\ ~Up \/ ds.cb # cboot \/ \E s \in Streams : ds.open[s] = "none"
    /\ ~\E m \in net : m.t = "int" /\ m.cb = cboot
    /\ net' = net \cup {IntMsg(cboot, have, ~synced)}
    /\ UNCHANGED <<pboot, cboot, pub, faults>>
    /\ UNCHANGED <<parentVars, childVars, leafVars>>

\* The parent leaves the child's view and comes back: the child forgets its channel state for it,
\* here only while it follows the parent's live channel (an out-of-sync answer forgets the rest).
Forget ==
    /\ Up /\ cs.boot = pboot /\ cs.ch = ds.ch /\ faults < MaxFaults
    /\ ForgetParent /\ faults' = faults + 1 /\ ds' = [ds EXCEPT !.forgot = TRUE]
    /\ UNCHANGED <<pboot, cboot, pub, net, synced, nextCh>>
    /\ UNCHANGED leafVars

\* The child's process restarts: its channel state goes, and the pushes in flight to it and its
\* interest in flight are lost. Its leaf keeps its position and asks the new boot from it.
ChildRestart ==
    /\ cboot < MaxBoots /\ faults < MaxFaults
    /\ cboot' = cboot + 1 /\ ForgetParent /\ synced' = FALSE
    /\ faults' = faults + 1
    /\ net' = {m \in net : ~(m.t \in {"push", "int"} /\ m.cb = cboot)}
    /\ UNCHANGED <<pboot, pub>>
    /\ UNCHANGED <<parentVars, leafVars>>

Lose(m) ==
    /\ m \in net /\ faults < MaxFaults
    /\ net' = net \ {m} /\ faults' = faults + 1
    /\ UNCHANGED <<pboot, cboot, pub>>
    /\ UNCHANGED <<parentVars, childVars, leafVars>>

----------------------------------------------------------------------------

Next ==
    \/ Send \/ Fail \/ Timeout \/ Expire \/ Stale \/ Interest \/ Forget \/ ParentRestart \/ ChildRestart
    \/ \E s \in Streams : Publish(s)
    \/ \E m \in net : DeliverPush(m) \/ DeliverAnswer(m) \/ DeliverInterest(m) \/ Lose(m)

\* Fail, Expire, Forget, the restarts, loss and reorder are faults, and not fair; the rest is
\* each node's own work or a message that arrives.
Fairness ==
    /\ WF_vars(Send) /\ WF_vars(Timeout) /\ WF_vars(Stale) /\ WF_vars(Interest)
    /\ WF_vars(\E m \in net : DeliverPush(m))
    /\ WF_vars(\E m \in net : DeliverAnswer(m))
    /\ WF_vars(\E m \in net : DeliverInterest(m))

Spec == Init /\ [][Next]_vars /\ Fairness

\* guide/streams.md: every record once and in order. The leaf received each stream's records
\* 1..n in order: no duplicate, no gap.
InOrder == \A s \in Streams : got[s] = Run(1, Len(got[s]))

\* §2.2: the child applied seqs 1..applied of the channel it follows, each once and in order.
ChannelExact == cs.boot # 0 => applog = Run(1, cs.applied)

\* A cumulative ack never runs ahead of the child on the channel it follows, unless the child
\* forgot it since the Downstream was made (then it may follow it again from seq 1).
AckSound == Up /\ ds.cb = cboot /\ cs.boot = pboot /\ cs.ch = ds.ch /\ ~ds.forgot => ds.acked <= cs.applied

\* A Downstream is made again only after a fault, a restart, or a late push of a boot the child
\* does not hold retired (§16, risk 1: an out-of-sync answer or a Forget empties the list, so each
\* such push of the old boot costs one): a parent boot makes at most MaxChan channels in a
\* behaviour. A child that answers every new channel Retired passes it.
FewChannels == nextCh - 1 <= MaxChan

WindowBound == Up => ds.next - 1 - ds.acked <= Window

\* §2.5 Taken: a stream's charge is its queue and its items in flight of its current epoch.
CountExact == Up => \A s \in Streams : ds.open[s] # "none" => ds.charge[s] = Holds(ds, s)

\* §2.5: what the parent holds for a branch, queued and in flight, is at most the branch's bound.
BranchBound == Up => \A s \in Streams : ds.open[s] # "none" => Holds(ds, s) <= MaxBranch

\* §2.5 Bounded, as Send states it: a stream's items in flight are at most MaxInflight. The code
\* takes whole batches while under half and does not hold back a branch's first push, so it can
\* pass half by one batch; this and NoStreamLimit check the rule, not the code's batching.
StreamInflight == Up => \A s \in Streams : ds.open[s] = "open" => InFlight(ds, s) <= MaxInflight

\* Every published record reaches the leaf, and stays: faults are bounded, and the origin keeps
\* every record, so a reset, an expiry or a restart is followed by the records again.
Done == <>[](\A s \in Streams : Range(got[s]) = 1..pub[s])

Item == [k : {"none", "recs", "reset"}, lo : 0..MaxRec, hi : 0..MaxRec, ep : Nat]
TypeOK ==
    /\ pboot \in 1..MaxBoots /\ cboot \in 1..MaxBoots /\ pub \in [Streams -> 0..MaxRec]
    /\ nextCh \in Nat \ {0} /\ faults \in 0..MaxFaults
    /\ ds.ch \in 0..nextCh - 1 /\ ds.cb \in 0..MaxBoots /\ ds.acked <= ds.next - 1
    /\ \A s \in Streams : /\ ds.open[s] \in {"none", "open", "reset"}
                          /\ ds.q[s] \in 0..MaxBranch /\ ds.charge[s] \in 0..MaxBranch
                          /\ ds.pos[s] \in 0..pub[s] /\ ds.q[s] <= ds.pos[s]
    /\ \A j \in DOMAIN ds.un : ds.un[j] \in [Streams -> Item]
    /\ cs.boot \in 0..MaxBoots /\ cs.ch \in Nat /\ retired \subseteq 1..MaxBoots /\ synced \in BOOLEAN
    /\ \A s \in Streams : have[s] \in 0..pub[s]

=============================================================================
