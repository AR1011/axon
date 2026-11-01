---------------------------- MODULE RelayGiveUp ----------------------------
(* One stream key pulled by a leaf node, with a Live follower, from two relays and the origin
   (crates/axon-stream/src/core.rs, RelayCore). The leaf's candidates are the relays, then the
   origin (node.rs: candidates, the tiers of world_relay.rs); a relay's only parent is the origin.
   Links are cut one at a time, both ways, and heal all at once, as World::heal does; a relay
   crashes and comes back as a new incarnation, empty, and takes the key up live for a follower of
   its own; the origin publishes and opens new generations. Time is ticks. The leaf's view of who
   is up lags the links by at most ViewLag ticks; upstream_timeout is UT ticks.

   The question is the one 8049630 answers: when does the leaf give up a position no candidate
   holds (RelayCore::reconcile, the `broken` keys), and does it ever reset while a node it can
   reach holds what it needs?

   Abstractions, and what they could hide:
   - An interest and its answer are one step (RelayCore::reconcile + handle_interest +
     interest_result), and so is a relay's pull from the origin. A stale answer, computed on the
     parent's state before a change and applied after it (a40f2b0), is not modelled.
   - A branch at the parent holds what the parent's ring holds: a branch that falls behind the ring
     is reset as a slow consumer (SlowConsumerPolicy::Reset), where the real branch has its own
     bound (CoreConfig::branch). So a relay falls behind the origin's ring as soon as the ring rolls
     past it; in the code that needs its branch's queue to fill, a slow or stalled relay link. The
     KeepUp constraint takes them away in the StaleReset and StaleRefusal mutations. A relay's
     branch gives its Ready at the start of the relay's ring, not where the relay adopted (hub.rs:
     adopt_relay_stream): where the ring rolled on in between, the leaf refuses a Ready the code
     would continue from its queue. That only adds refusals, which never count against the code.
   - A relay gives up its own position as soon as the origin refuses it: its wait for
     upstream_timeout is left out, which moves its reset earlier. While cut from the origin it
     may at any time be orphaned (RelayOrphan), not only after upstream_timeout: the leaf's branch
     ends with UpstreamLost, which the leaf counts as an Unavailable refusal that never makes the
     key broken and that a view change clears (membership_changed). The leaf's own
     orphaning, OriginLost and `stranded` are not modelled, and nothing checks when OriginLost is
     told. `stranded` keeps a waiting key from being orphaned; before 8049630 a broken key was
     rebranched in the same reconcile, so GiveUpAtOnce brings back the leaf's behaviour before it.
     Closed keys, UnknownStream and Forbidden are not modelled.
   - The leaf never moves back to a more preferred parent while its current one feeds it
     (hold_down, `joining`). So a refusal from a joining link, which counts in reconcile's every()
     like any other, and a Reset(GenerationChanged) on a joining link, which becomes a
     HistoryUnavailable refusal, cannot happen here.
   - A refusal lasts 1..MaxBackoff ticks, chosen freely: strikes and doubling are folded into
     that choice. Without the doubling, candidates could refuse in turn, each backoff ending before
     the next refusal, and never all at once; Recovers rests instead on no tick passing while an
     answer is owed (Tick), where the code rests on the doubling. An expired refusal is forgotten;
     only the keys every parent reports closed or unreachable read it, and those are not modelled.
   - The view follows the links: a node enters it only while connected, leaves it only while not,
     and is in it once connected for ViewLag ticks. The code reads the membership view, which can
     drop a node whose link is up or see it back later than upstream_timeout after a heal;
     NoNeedlessReset holds only if ViewLag < UT (RelayGiveUp-SlowView.cfg, ViewLag = UT, fails).
     UT = 2 leaves one tick between candidates appearing at different times.
   - A candidate that leaves and comes back renews its available_since and drops its refusal, also
     when both happen between two turns of the node.rs loop, which then reads a view with the node
     in it: the loop passes the nodes its membership events took out to parents_left (Flap).
   - A restarted relay appears in the leaf's view only after its old incarnation left it; it is a
     new PeerId, so nothing of the old one's refusal or available_since applies to it.
   - The origin opens a new generation in place (restart_stream): its branches are reset with
     GenerationChanged, and it is not a new incarnation in the view. A batch of a new generation
     therefore always follows the Reset on its branch, and accept()'s GenerationMismatch path is
     not reached (OnRecord asserts it).
   - A relay's view of the origin is its link. Rendezvous order is fixed: r1, then r2.
   - `fresh` and a relay's `wanted_from` are cleared once a position is taken: both are read only
     without one.
   - Records carry no content, only positions, and the follower sees what the leaf's hub holds:
     (lgen, lpos), reset to none by every reset. A branch sends its parent's records in order from
     its Ready, as the code's queue does, so the leaf never sees a gap from its own parent and
     accept()'s gap refusal (OnRecord) is not reached; NoSilentGap is a check that the Ready and
     interest checks, and a relay's own gap check, are wired in (NoGapChecks, RelayNoGap).
   - Recovers holds only because Tick waits while an answer or a give-up is owed; the code relies
     on the doubling of retry_refused and on retry_failed instead. Removing those guards leaves
     every safety invariant passing. Recovers cannot tell a delay from a livelock: time also
     stops for a give-up that would change nothing, so a mutation that keeps the refusals after
     giving up fails it, where the code only waits out the backoff. A behaviour has at most
     MaxCuts cuts in all, not per heal. *)
EXTENDS Integers, FiniteSets, Sequences, TLC

CONSTANTS
    Relays,             \* a subset of {"r1", "r2"}
    K,                  \* records each ring holds
    MaxSeq,             \* records the origin publishes per generation
    MaxGen,             \* generations the origin opens
    UT,                 \* upstream_timeout, ticks
    ViewLag,            \* the leaf sees a node it is connected to within this many ticks
    MaxBackoff,         \* the longest a refusal lasts, ticks
    MaxCuts,            \* cuts in a whole behaviour, Flap included
    MaxCrashes,
    Mutation,           \* "none", or the bug a mutations/RelayGiveUp-*.cfg brings back; each is named where it acts
    Gaps                \* what the code does by design that an invariant counts, {} to pass; each is named where it acts

ASSUME ViewLag <= UT /\ K >= 1 /\ MaxBackoff >= 1

VARIABLES
    og, ohi,                                \* origin: generation, last record published
    rup, rgen, rlo, rhi, rwant, rfresh,     \* relays: ring rlo..rhi of generation rgen (0: no position)
    lk, ro, cuts, crashes,                  \* links leaf-parent and relay-origin
    inView, since, age, linkAge,            \* the leaf's view; ticks since each candidate's available_since, since it appeared (a ghost), since each link came up
    cur, open, bpos, bdead,                 \* the leaf's current link, its branch: open, last sent (-1 before its Ready), a terminal item queued
    lgen, lpos, ghigh, fresh, rst, rrem,    \* the leaf's feed: position (lgen 0: none), feed.generation, feed.fresh, feed.refused
    bad, needless, missed1, skipped, ahead  \* history, for the invariants

vars == <<og, ohi, rup, rgen, rlo, rhi, rwant, rfresh, lk, ro, cuts, crashes, inView, since, age, linkAge,
          cur, open, bpos, bdead, lgen, lpos, ghigh, fresh, rst, rrem, bad, needless, missed1, skipped, ahead>>
originVars == <<og, ohi>>
relayVars == <<rup, rgen, rlo, rhi, rwant, rfresh>>
netVars == <<lk, ro, cuts, crashes>>
viewVars == <<inView, since, age, linkAge>>
leafVars == <<cur, open, bpos, bdead, lgen, lpos, ghigh, fresh, rst, rrem, bad, needless, missed1, skipped, ahead>>

Parents == Relays \cup {"o"}
Order == SelectSeq(<<"r1", "r2", "o">>, LAMBDA n : n \in Parents)
Min(S) == CHOOSE x \in S : \A y \in S : x <= y
Max(S) == CHOOSE x \in S : \A y \in S : x >= y
Live == [k |-> "live", g |-> 0, s |-> 0]
First == [k |-> "first", g |-> 0, s |-> 0]
After(g, s) == [k |-> "after", g |-> g, s |-> s]

\* core.rs: earlier
Earlier(a, b) ==
    \/ a.k = "first" /\ b.k # "first"
    \/ a.k = "after" /\ (b.k = "live" \/ (b.k = "after" /\ a.g = b.g /\ a.s < b.s))

OLo == IF ohi >= K THEN ohi - K + 1 ELSE 1
PGen(n) == IF n = "o" THEN og ELSE rgen[n]
PLo(n) == IF n = "o" THEN OLo ELSE rlo[n]
PHi(n) == IF n = "o" THEN ohi ELSE rhi[n]
Connected(n) == lk[n] /\ IF n = "o" THEN TRUE ELSE rup[n]
\* n can go on from record s of generation g: its ring holds s + 1, or it stands at s.
Serves(n, g, s) == PGen(n) = g /\ PLo(n) <= s + 1 /\ s <= PHi(n)
\* A relay without a position, or behind the leaf, that reaches an origin holding the leaf's next
\* record: asked for the leaf's position it would fetch it (open_branch: want_from, or a child
\* ahead starts live).
CouldServe(n) ==
    /\ n \in Relays /\ rup[n] /\ ro[n] /\ lgen = og /\ OLo <= lpos + 1
    /\ rgen[n] = 0 \/ (rgen[n] = lgen /\ rhi[n] <= lpos)
\* The leaf waits on an empty relay for the Ready of a start the origin holds.
WaitsOn(r) ==
    /\ rgen[r] = 0 /\ cur = r /\ open /\ bpos = -1 /\ bdead = "none"
    /\ lgen # 0 /\ lgen = og /\ OLo <= lpos + 1

Candidates == {n \in Parents : inView[n]}
Newest(f) == IF Candidates = {} THEN UT ELSE Min({f[n] : n \in Candidates})
\* A reset loses records while the generation goes on and a node the leaf has been connected to
\* since no later than its newest candidate appeared (the ghost `age`, not the code's
\* available_since) holds them or could fetch them: that node should have been asked before the
\* leaf gave up. A candidate's refusal excuses it only if it was of the leaf's current position
\* and made while the candidate was in view ("refused"). Not excused: one made while the node
\* held or could fetch the position ("wrongly"), one kept across a leave and return ("kept", only
\* under KeepRefusal), and, under StaleRefusal, one of an older position ("stale"): feed.refused
\* keeps the position refused, and reconcile asks a parent that refused another one again when
\* the key has no parent, so a stale refusal never counts in every().
Counts(n) == rst[n] \in {"none", "wrongly", "kept"} \/ (rst[n] = "stale" /\ Mutation = "StaleRefusal")
Needless ==
    /\ lgen # 0 /\ lgen = og
    /\ \E n \in Parents :
        /\ Connected(n) /\ (Serves(n, lgen, lpos) \/ CouldServe(n)) /\ linkAge[n] >= Newest(age)
        /\ Counts(n)
\* A new generation is taken from its record 1 while a ring on the path holds it: the parent's, or
\* the origin's behind a relay. guide/streams.md promises only the parent's: after a parent refuses
\* record 1 the leaf asks for the live edge, by design (BestEffort, e7ca994). fresh "gaveup" is feed.gave_up: the leaf gave up
\* after taking a generation, with or without a position, and takes a newer one from its record 1 too.
MissesOne(n, g, s) ==
    /\ 0 < ghigh /\ ghigh < g /\ s > 0
    /\ \/ PGen(n) = g /\ PLo(n) = 1
       \/ og = g /\ OLo = 1 /\ (n = "o" \/ ro[n])
    /\ \/ fresh \in {"yes", "lost", "gaveup", "no"}
       \/ fresh = "spent" /\ "BestEffort" \in Gaps

Init ==
    /\ og = 1 /\ ohi = 0
    /\ rup = [r \in Relays |-> TRUE]
    /\ rgen = [r \in Relays |-> 0] /\ rlo = [r \in Relays |-> 0] /\ rhi = [r \in Relays |-> 0]
    /\ rwant = [r \in Relays |-> Live] /\ rfresh = [r \in Relays |-> FALSE]
    /\ lk = [n \in Parents |-> TRUE] /\ ro = [r \in Relays |-> TRUE]
    /\ cuts = 0 /\ crashes = 0
    /\ inView = [n \in Parents |-> TRUE] /\ since = [n \in Parents |-> UT] /\ age = since
    /\ linkAge = [n \in Parents |-> UT]
    /\ cur = "none" /\ open = FALSE /\ bpos = -1 /\ bdead = "none"
    /\ lgen = 0 /\ lpos = 0 /\ ghigh = 0 /\ fresh = "no"
    /\ rst = [n \in Parents |-> "none"] /\ rrem = [n \in Parents |-> 0]
    /\ bad = FALSE /\ needless = FALSE /\ missed1 = FALSE /\ skipped = FALSE /\ ahead = FALSE

----------------------------------------------------------------------------
(* The leaf. Each operator below sets every leaf variable. *)

\* core.rs: RelayCore::refuse. A parent that cannot give record 1 of a new generation is asked
\* again for the live edge; any other refusal drops the link and backs the parent off. st "reset"
\* is a Reset(HistoryUnavailable) item the parent queued on the branch, judged by the parent's
\* state when the leaf takes it, as any refusal is. st "unavail" is Unavailable: it
\* lasts one tick, standing for the first one after a view change, asked again at once, and for
\* longer backoffs, which only delay a give-up that Unavailable already blocks.
Refuse(n, st) ==
    IF fresh = "yes" /\ st \in {"HU", "reset"} /\ lgen = 0
    THEN /\ fresh' = "spent" /\ open' = FALSE /\ bpos' = -1 /\ bdead' = "none"
         /\ UNCHANGED <<cur, lgen, lpos, ghigh, rst, rrem, bad, needless, missed1, skipped, ahead>>
    ELSE /\ cur' = "none" /\ open' = FALSE /\ bpos' = -1 /\ bdead' = "none"
         /\ IF st = "unavail"
            THEN rst' = [rst EXCEPT ![n] = "unavail"] /\ rrem' = [rrem EXCEPT ![n] = 1] /\ UNCHANGED ahead
            \* The status is not kept: reconcile reads HistoryUnavailable and GenerationMismatch alike.
            ELSE /\ rst' = [rst EXCEPT ![n] =
                             IF lgen # 0 /\ (Serves(n, lgen, lpos) \/ CouldServe(n))
                             THEN "wrongly" ELSE "refused"]
                 /\ \E b \in 1..MaxBackoff : rrem' = [rrem EXCEPT ![n] = b]
                 /\ ahead' = (ahead \/ (st # "reset" /\ lgen # 0 /\ PGen(n) > lgen))
         /\ UNCHANGED <<lgen, lpos, ghigh, fresh, bad, needless, missed1, skipped>>

\* A refusal of the position the leaf held is of an older one once the position moves.
Stale == [n \in Parents |-> IF rst[n] = "refused" THEN "stale" ELSE rst[n]]

\* core.rs: RelayCore::local_reset for GenerationChanged, then rebranch: the same parent is asked
\* again, for record 1 of the new generation. Mutation NewGenLive, before 7143010 (its local_reset
\* half; its Ready half, a newer Ready refused HistoryUnavailable, is not brought back): from the
\* live edge. A reset that loses a position the generation still has is needless where `counted`:
\* under SpuriousGC a parent behind the leaf answers GenerationMismatch too. A relay's own
\* GenerationChanged on the leaf's branch is not counted: with the leaf on the origin's generation
\* it comes only from the relay asking the origin for an older start a child left (the StaleWant
\* mutation).
ResetGC(counted) ==
    /\ lgen' = 0 /\ lpos' = 0 /\ fresh' = IF Mutation = "NewGenLive" THEN "lost" ELSE "yes"
    /\ needless' = (needless \/ (counted /\ Needless))
    /\ open' = FALSE /\ bpos' = -1 /\ bdead' = "none" /\ rst' = Stale
    /\ UNCHANGED <<cur, ghigh, rrem, bad, missed1, skipped, ahead>>

\* core.rs: RelayCore::handle_push, Item::Ready (the branch's first item) of generation g at s,
\* and hub.rs: adopt_relay_stream when the leaf has no position. Mutation OlderGen: no refusal of
\* a generation older than feed.generation. Mutation ReadyStrict: a Ready at the leaf's own
\* position is refused as if past it. After giving a position up (feed.gave_up), a Ready of a newer
\* generation than the leaf held, past its record 1, is not adopted: that generation is asked from
\* its record 1. Mutation NotFresh, before 5f19599: it is adopted where it stands.
OnReady(n, g, s) ==
    IF g < ghigh /\ Mutation # "OlderGen" THEN Refuse(n, "GM")
    ELSE IF lgen # 0 /\ lgen < g THEN ResetGC(TRUE)
    ELSE IF lgen = 0 /\ fresh = "gaveup" /\ ghigh < g /\ s > 0 /\ Mutation # "NotFresh"
         THEN /\ fresh' = "yes" /\ ghigh' = g /\ open' = FALSE /\ bpos' = -1 /\ bdead' = "none"
              /\ UNCHANGED <<cur, lgen, lpos, rst, rrem, bad, needless, missed1, skipped, ahead>>
    ELSE IF lgen = 0
         THEN /\ lgen' = g /\ lpos' = s /\ fresh' = "no"
              /\ ghigh' = IF g > ghigh THEN g ELSE ghigh
              /\ missed1' = (missed1 \/ MissesOne(n, g, s))
              /\ open' = TRUE /\ bpos' = s /\ rst' = Stale
              /\ UNCHANGED <<cur, bdead, rrem, bad, needless, skipped, ahead>>
    ELSE IF (lpos < s \/ (Mutation = "ReadyStrict" /\ lpos = s)) /\ Mutation # "NoGapChecks"
         THEN Refuse(n, "HU")
    ELSE /\ open' = TRUE /\ bpos' = s
         /\ UNCHANGED <<cur, bdead, lgen, lpos, ghigh, fresh, rst, rrem, bad, needless, missed1, skipped, ahead>>

\* core.rs: RelayCore::handle_push, Item::Batch, then accept() and hub.rs: accept_batch, for
\* record s of generation g. Mutation NoGapChecks: no RecoveryError::Gap (not reached here: a
\* branch is sent in order from its Ready).
OnRecord(n, g, s) ==
    IF g < ghigh /\ Mutation # "OlderGen" THEN Refuse(n, "GM")
    ELSE /\ Assert(lgen = g, "a batch arrived before its generation's Ready or after its Reset")
         /\ IF s > lpos + 1 /\ Mutation # "NoGapChecks" THEN Refuse(n, "HU")
            ELSE /\ lpos' = IF s > lpos THEN s ELSE lpos     \* at or before lpos: a duplicate, dropped
                 /\ rst' = IF s > lpos THEN Stale ELSE rst
                 /\ bad' = (bad \/ s > lpos + 1)
                 /\ open' = TRUE /\ bpos' = s
                 /\ UNCHANGED <<cur, bdead, lgen, ghigh, fresh, rrem, needless, missed1, skipped, ahead>>

\* core.rs: RelayCore::reconcile, for the key. A new link when a candidate is not backed off; when
\* every candidate refused the position (HistoryUnavailable or GenerationMismatch, not Unavailable), the key is
\* `broken` once none has appeared for upstream_timeout (the newest available_since):
\* local_reset(HistoryUnavailable), and every refusal is retried at once. Mutations: GiveUpAtOnce,
\* before 8049630 (its wait half); OldestCandidate, the oldest available_since; StaleSince,
\* available_since kept for a candidate that leaves and comes back (no refused.retain); NeverGiveUp;
\* StaleRefusal, before 5f19599: a refusal of an older position still backs its parent off.
Asked(n) == rst[n] = "none" \/ (rst[n] = "stale" /\ Mutation # "StaleRefusal")
Desired == LET c == SelectSeq(Order, LAMBDA n : inView[n] /\ Asked(n))
           IN IF c = <<>> THEN "none" ELSE Head(c)
Settled ==
    CASE Mutation = "NeverGiveUp" -> FALSE
      [] Mutation = "GiveUpAtOnce" -> TRUE
      [] Mutation = "OldestCandidate" -> Candidates = {} \/ Max({since[n] : n \in Candidates}) >= UT
      [] OTHER -> Newest(since) >= UT
Broken == Candidates # {} /\ Settled /\ \A n \in Candidates : rst[n] # "unavail"
Reconcile ==
    /\ cur = "none"
    /\ IF Desired # "none"
       THEN /\ cur' = Desired /\ open' = FALSE /\ bpos' = -1 /\ bdead' = "none"
            /\ UNCHANGED <<lgen, lpos, ghigh, fresh, rst, rrem, bad, needless, missed1, skipped, ahead>>
       ELSE /\ Broken
            /\ lgen' = 0 /\ lpos' = 0 /\ fresh' = IF ghigh > 0 THEN "gaveup" ELSE "no"
            /\ needless' = (needless \/ Needless)
            /\ rst' = [n \in Parents |-> "none"] /\ rrem' = [n \in Parents |-> 0]
            /\ UNCHANGED <<cur, open, bpos, bdead, ghigh, bad, missed1, skipped, ahead>>
    /\ UNCHANGED <<originVars, relayVars, netVars, viewVars>>

\* core.rs: an interest to the current parent (reconcile), the parent's handle_interest and
\* open_branch, and interest_result, with the branch's Ready when the parent has a position.
\* Mutations: NoWantFromAfter, want_from skipped for an After start; GMRefuse, a
\* GenerationMismatch on the current link refused instead of reset; BehindRefused, a child ahead
\* of its parent refused instead of started live.
Ask ==
    /\ cur # "none" /\ ~open /\ Connected(cur)
    /\ LET n == cur
           g == PGen(n)
           want == IF lgen # 0 THEN After(lgen, lpos) ELSE IF fresh = "yes" THEN First ELSE Live
       IN IF g = 0
          THEN \* A relay without a position: the branch starts live and waits for its Ready, and
               \* the relay asks its parent from the start its one child wants now (want_from: the
               \* child's old branch is forgotten). Mutation StaleWant, before 5e7ae55: the earliest
               \* start any child asked is kept after that child moved on, First or After an older
               \* position.
               /\ open' = TRUE /\ bpos' = -1
               /\ rwant' = IF Mutation = "NoWantFromAfter" /\ want.k = "after" THEN rwant
                           ELSE IF Mutation = "StaleWant" /\ ~Earlier(want, rwant[n]) THEN rwant
                           ELSE [rwant EXCEPT ![n] = want]
               /\ UNCHANGED <<cur, bdead, lgen, lpos, ghigh, fresh, rst, rrem, bad, needless, missed1, skipped, ahead>>
               /\ UNCHANGED <<rup, rgen, rlo, rhi, rfresh>>
          ELSE /\ CASE want.k = "after" ->
                        \* interest_result: GenerationMismatch, if current, from a parent ahead. A parent
                        \* behind starts the branch live, and the leaf refuses its older Ready (open_branch).
                        \* Mutation SpuriousGC, before 5f19599: it answers GenerationMismatch too.
                        IF g > lgen THEN (IF Mutation = "GMRefuse" THEN Refuse(n, "GM") ELSE ResetGC(TRUE))
                        ELSE IF g < lgen THEN (CASE Mutation = "SpuriousGC" -> ResetGC(TRUE)
                                                 [] Mutation = "OlderGen" -> Refuse(n, "HU")
                                                 [] OTHER -> Refuse(n, "GM"))
                        ELSE IF lpos < PHi(n) /\ PLo(n) > lpos + 1 THEN Refuse(n, "HU")
                        ELSE IF Mutation = "BehindRefused" /\ lpos > PHi(n) THEN Refuse(n, "HU")
                        ELSE /\ open' = TRUE /\ bpos' = IF lpos < PHi(n) THEN lpos ELSE PHi(n)
                             /\ UNCHANGED <<cur, bdead, lgen, lpos, ghigh, fresh, rst, rrem, bad, needless, missed1, skipped, ahead>>
                    [] want.k = "first" ->
                        IF PLo(n) = 1 THEN OnReady(n, g, 0) ELSE Refuse(n, "HU")
                    [] OTHER -> OnReady(n, g, PHi(n))
               /\ UNCHANGED relayVars
    /\ UNCHANGED <<originVars, netVars, viewVars>>

\* core.rs: RelayCore::next_push at the parent and handle_push at the leaf, one item. Mutation
\* GCAsRefusal: Reset(GenerationChanged) on the current link is refused as on any other.
Push ==
    /\ cur # "none" /\ open /\ Connected(cur)
    /\ LET n == cur IN
       CASE bdead = "GC" -> IF Mutation = "GCAsRefusal" THEN Refuse(n, "HU") ELSE ResetGC(FALSE)
         [] bdead = "HU" -> Refuse(n, "reset")
         [] bdead = "Gap" ->    \* Reset(Gap): asked again from the leaf's position
              /\ open' = FALSE /\ bpos' = -1 /\ bdead' = "none"
              /\ UNCHANGED <<cur, lgen, lpos, ghigh, fresh, rst, rrem, bad, needless, missed1, skipped, ahead>>
         [] bdead = "UL" -> Refuse(n, "unavail")     \* End(UpstreamLost)
         [] bpos = -1 -> PGen(n) # 0 /\ OnReady(n, PGen(n), PLo(n) - 1)
         [] OTHER ->
              /\ bpos < PHi(n)
              /\ IF PLo(n) > bpos + 1
                 THEN \* the branch fell behind the ring: reset as a slow consumer, and rebranched
                      /\ open' = FALSE /\ bpos' = -1
                      /\ UNCHANGED <<cur, bdead, lgen, lpos, ghigh, fresh, rst, rrem, bad, needless, missed1, skipped, ahead>>
                 ELSE OnRecord(n, PGen(n), bpos + 1)
    /\ UNCHANGED <<originVars, relayVars, netVars, viewVars>>

----------------------------------------------------------------------------
(* Relays and the origin *)

\* A relay's local_reset: its subscribers, the leaf's branch among them, are reset (hub.rs:
\* reset_relay_stream, terminate_key). A relay that held a position and lost it resets its
\* branches with Gap, and each child asks again from its own position. Under the StaleWant
\* mutation the origin refuses a stale start and the relay resets the branch of a child whose own
\* start the origin holds: only a stale start can reset a waiting branch here, so that is the whole
\* of the reset half of NoStartSkipped.
RReset(r, reason) ==
    /\ rgen' = [rgen EXCEPT ![r] = 0] /\ rlo' = [rlo EXCEPT ![r] = 0] /\ rhi' = [rhi EXCEPT ![r] = 0]
    /\ rfresh' = [rfresh EXCEPT ![r] = (reason = "GC" /\ Mutation # "NewGenLive")]
    /\ rwant' = [rwant EXCEPT ![r] = Live]
    /\ bdead' = IF cur = r /\ open /\ bdead = "none" THEN reason ELSE bdead
    /\ skipped' = (skipped \/ (WaitsOn(r) /\ reason = "HU"))
    /\ UNCHANGED bad
RAdopt(r, g, s) ==
    /\ rgen' = [rgen EXCEPT ![r] = g] /\ rlo' = [rlo EXCEPT ![r] = s + 1] /\ rhi' = [rhi EXCEPT ![r] = s]
    /\ rfresh' = [rfresh EXCEPT ![r] = FALSE] /\ rwant' = [rwant EXCEPT ![r] = Live]
    /\ skipped' = (skipped \/ (WaitsOn(r) /\ s > lpos))
    /\ UNCHANGED <<bdead, bad>>

\* A relay's reconcile, interest and push from the origin, as for the leaf, with one candidate.
\* Mutation RelayNoGap: the relay takes the next record the origin no longer holds (accept()'s
\* RecoveryError::Gap at the relay). Mutation RelayAfterLive: a relay asked from a position the
\* origin holds takes the live edge. A relay that falls behind the origin's ring resets its
\* branches with Gap (local_reset); mutation StaleReset, before 5e7ae55: with HistoryUnavailable,
\* the branch of a child ahead of what it lost too.
RelayPull(r) ==
    /\ rup[r] /\ ro[r]
    /\ IF rgen[r] = 0
       THEN LET w == IF rfresh[r] THEN First ELSE rwant[r] IN
            CASE w.k = "first" ->
                   IF OLo = 1 THEN RAdopt(r, og, 0)
                   ELSE IF rfresh[r]
                        THEN /\ rfresh' = [rfresh EXCEPT ![r] = FALSE]
                             /\ UNCHANGED <<rgen, rlo, rhi, rwant, bdead, bad, skipped>>
                        ELSE RReset(r, "HU")
              [] w.k = "live" -> RAdopt(r, og, ohi)
              [] OTHER ->
                   IF og # w.g THEN RReset(r, "GC")
                   ELSE IF ohi <= w.s THEN RAdopt(r, og, ohi)
                   ELSE IF OLo <= w.s + 1 THEN RAdopt(r, og, IF Mutation = "RelayAfterLive" THEN ohi ELSE w.s)
                   ELSE RReset(r, "HU")
       ELSE IF og # rgen[r] THEN RReset(r, "GC")
       ELSE /\ rhi[r] < ohi
            /\ IF OLo <= rhi[r] + 1 \/ Mutation = "RelayNoGap"
               THEN /\ rhi' = [rhi EXCEPT ![r] = @ + 1]
                    /\ rlo' = [rlo EXCEPT ![r] = IF rhi[r] + 1 - rlo[r] >= K THEN @ + 1 ELSE @]
                    /\ bad' = (bad \/ OLo > rhi[r] + 1)
                    /\ UNCHANGED <<rgen, rwant, rfresh, bdead, skipped>>
               ELSE RReset(r, IF Mutation = "StaleReset" THEN "HU" ELSE "Gap")
    /\ UNCHANGED <<originVars, rup, netVars, viewVars, cur, open, bpos, lgen, lpos, ghigh, fresh, rst, rrem, needless, missed1, ahead>>

\* core.rs: reconcile's orphans at a relay cut from the origin (upstream_timeout not counted): the
\* leaf's branch ends with UpstreamLost. A branch asked of an orphaned relay (open_branch answers
\* Unavailable) is the same: it opens and ends here.
RelayOrphan(r) ==
    /\ rup[r] /\ ~ro[r] /\ cur = r /\ open /\ bdead = "none"
    /\ bdead' = "UL"
    /\ UNCHANGED <<originVars, relayVars, netVars, viewVars,
                   cur, open, bpos, lgen, lpos, ghigh, fresh, rst, rrem, bad, needless, missed1, skipped, ahead>>

RelayCrash(r) ==
    /\ rup[r] /\ crashes < MaxCrashes
    /\ crashes' = crashes + 1
    /\ rup' = [rup EXCEPT ![r] = FALSE]
    /\ rgen' = [rgen EXCEPT ![r] = 0] /\ rlo' = [rlo EXCEPT ![r] = 0] /\ rhi' = [rhi EXCEPT ![r] = 0]
    /\ rwant' = [rwant EXCEPT ![r] = Live] /\ rfresh' = [rfresh EXCEPT ![r] = FALSE]
    /\ linkAge' = [linkAge EXCEPT ![r] = 0]
    /\ UNCHANGED <<originVars, lk, ro, cuts, inView, since, age, leafVars>>

\* A new incarnation, empty, with a Live follower: it takes the key up at the live edge.
RelayRestart(r) ==
    /\ ~rup[r] /\ ~inView[r]
    /\ rup' = [rup EXCEPT ![r] = TRUE]
    /\ UNCHANGED <<originVars, rgen, rlo, rhi, rwant, rfresh, netVars, viewVars, leafVars>>

Publish ==
    /\ ohi < MaxSeq
    /\ ohi' = ohi + 1
    /\ UNCHANGED <<og, relayVars, netVars, viewVars, leafVars>>

\* core.rs: RelayCore::restart_stream: the origin's branches are reset with GenerationChanged.
OriginRestart ==
    /\ og < MaxGen
    /\ og' = og + 1 /\ ohi' = 0
    /\ bdead' = IF cur = "o" /\ open /\ bdead = "none" THEN "GC" ELSE bdead
    /\ UNCHANGED <<relayVars, netVars, viewVars, cur, open, bpos, lgen, lpos, ghigh, fresh, rst, rrem, bad, needless, missed1, skipped, ahead>>

----------------------------------------------------------------------------
(* The network and the leaf's view *)

CutLeaf(n) ==
    /\ lk[n] /\ cuts < MaxCuts
    /\ lk' = [lk EXCEPT ![n] = FALSE] /\ cuts' = cuts + 1
    /\ linkAge' = [linkAge EXCEPT ![n] = 0]
    /\ UNCHANGED <<originVars, relayVars, ro, crashes, inView, since, age, leafVars>>

CutRelay(r) ==
    /\ ro[r] /\ cuts < MaxCuts
    /\ ro' = [ro EXCEPT ![r] = FALSE] /\ cuts' = cuts + 1
    /\ UNCHANGED <<originVars, relayVars, lk, crashes, viewVars, leafVars>>

Mended(m) == rst[m] = "unavail"     \* membership_changed drops it

\* World::heal. A link that comes up starts its age at 0, which linkAge already holds.
HealAll ==
    /\ \E n \in Parents : ~lk[n]
       \/ \E r \in Relays : ~ro[r]
    /\ lk' = [n \in Parents |-> TRUE] /\ ro' = [r \in Relays |-> TRUE]
    /\ UNCHANGED <<originVars, relayVars, cuts, crashes, viewVars, leafVars>>

\* n's link is cut and healed, and n leaves the view and comes back, between two turns of the
\* node.rs loop, so reconcile never sees it gone. node.rs: the loop passes the nodes its membership
\* events took out to parents_left, which renews n's available_since and drops its refusal. The
\* parent dropped the branch with the session. Mutation Coalesced: no parents_left, both stay.
Flap(n) ==
    /\ inView[n] /\ Connected(n) /\ cuts < MaxCuts
    /\ cuts' = cuts + 1
    /\ linkAge' = [linkAge EXCEPT ![n] = 0] /\ age' = [age EXCEPT ![n] = 0]
    /\ since' = IF Mutation = "Coalesced" THEN since ELSE [since EXCEPT ![n] = 0]
    /\ IF cur = n THEN open' = FALSE /\ bpos' = -1 /\ bdead' = "none" ELSE UNCHANGED <<open, bpos, bdead>>
    /\ LET drop(m) == Mended(m) \/ (m = n /\ Mutation # "Coalesced")
       IN /\ rst' = [m \in Parents |-> IF drop(m) THEN "none" ELSE rst[m]]
          /\ rrem' = [m \in Parents |-> IF drop(m) THEN 0 ELSE rrem[m]]
    /\ UNCHANGED <<originVars, relayVars, lk, ro, crashes, inView,
                   cur, lgen, lpos, ghigh, fresh, bad, needless, missed1, skipped, ahead>>

\* core.rs: reconcile, when the candidates change: available_since.retain drops a candidate that
\* left, and or_insert sets it for one that appears; membership_changed drops every Unavailable
\* refusal.
Renew(n) ==
    /\ age' = [age EXCEPT ![n] = 0]
    /\ since' = IF Mutation = "StaleSince" THEN since ELSE [since EXCEPT ![n] = 0]

\* Under KeepRefusal a refusal kept across the absence comes back with the candidate.
ViewUp(n) ==
    /\ Connected(n) /\ ~inView[n]
    /\ inView' = [inView EXCEPT ![n] = TRUE] /\ Renew(n)
    /\ rst' = [m \in Parents |-> IF Mended(m) THEN "none"
                                 ELSE IF m = n /\ rst[m] # "none" THEN "kept" ELSE rst[m]]
    /\ rrem' = [m \in Parents |-> IF Mended(m) THEN 0 ELSE rrem[m]]
    /\ UNCHANGED <<originVars, relayVars, netVars, linkAge,
                   cur, open, bpos, bdead, lgen, lpos, ghigh, fresh, bad, needless, missed1, skipped, ahead>>

\* core.rs: reconcile drops a current link to a parent that is no longer a candidate
\* (parent_lost), and its refusal (refused.retain). Mutation KeepRefusal: no refused.retain.
ViewDown(n) ==
    /\ ~Connected(n) /\ inView[n]
    /\ inView' = [inView EXCEPT ![n] = FALSE] /\ Renew(n)
    /\ IF cur = n
       THEN cur' = "none" /\ open' = FALSE /\ bpos' = -1 /\ bdead' = "none"
       ELSE UNCHANGED <<cur, open, bpos, bdead>>
    /\ LET drop(m) == Mended(m) \/ (m = n /\ Mutation # "KeepRefusal")
       IN /\ rst' = [m \in Parents |-> IF drop(m) THEN "none" ELSE rst[m]]
          /\ rrem' = [m \in Parents |-> IF drop(m) THEN 0 ELSE rrem[m]]
    /\ UNCHANGED <<originVars, relayVars, netVars, linkAge, lgen, lpos, ghigh, fresh, bad, needless, missed1, skipped, ahead>>

\* Time passes only while the view is at most ViewLag ticks behind the links, and not while the leaf
\* owes a reconcile (a refusal wakes the key, wake_key), an interest to a reachable parent, or a
\* branch's first item from a parent that can give it (a relay asks the origin first): each takes a
\* turn or a round trip, well inside the shortest backoff (retry_refused, 2 s).
Grow(f) == [n \in Parents |-> IF inView[n] /\ f[n] < UT THEN f[n] + 1 ELSE f[n]]
Tick ==
    /\ \A n \in Parents : Connected(n) /\ linkAge[n] >= ViewLag => inView[n]
    /\ cur # "none" \/ (Desired = "none" /\ ~Broken)
    /\ ~(cur # "none" /\ Connected(cur) /\ ~open)
    /\ ~(cur # "none" /\ Connected(cur) /\ open /\ bpos = -1 /\ (cur = "o" \/ ro[cur]))
    /\ since' = Grow(since) /\ age' = Grow(age)
    /\ linkAge' = [n \in Parents |-> IF Connected(n) /\ linkAge[n] < UT THEN linkAge[n] + 1 ELSE linkAge[n]]
    /\ rrem' = [n \in Parents |-> IF rrem[n] > 0 THEN rrem[n] - 1 ELSE 0]
    /\ rst' = [n \in Parents |-> IF rrem[n] > 1 THEN rst[n] ELSE "none"]
    /\ UNCHANGED <<originVars, relayVars, netVars, inView, cur, open, bpos, bdead, lgen, lpos, ghigh, fresh, bad, needless, missed1, skipped, ahead>>

----------------------------------------------------------------------------

Next ==
    \/ Tick \/ Reconcile \/ Ask \/ Push \/ Publish \/ OriginRestart \/ HealAll
    \/ \E r \in Relays : RelayPull(r) \/ RelayOrphan(r) \/ RelayCrash(r) \/ RelayRestart(r) \/ CutRelay(r)
    \/ \E n \in Parents : CutLeaf(n) \/ ViewUp(n) \/ ViewDown(n) \/ Flap(n)

Fairness ==
    /\ WF_vars(Tick) /\ WF_vars(Reconcile) /\ WF_vars(Ask) /\ WF_vars(Push)
    /\ \A r \in Relays : WF_vars(RelayPull(r))
    /\ \A n \in Parents : WF_vars(ViewUp(n)) /\ WF_vars(ViewDown(n))

Spec == Init /\ [][Next]_vars /\ Fairness

\* guide/streams.md: "A subscriber gets every record once and in order, or one explicit
\* ResetRequired(reason) telling it to start over. Never a duplicate, never a silent gap." The
\* leaf's follower sees what its hub accepts, and the leaf and each relay take a record only when
\* they hold the one before it. A wiring check: see the abstractions.
NoSilentGap == ~bad

\* guide/streams.md, Per-stream fallthrough: "If every candidate lacks the history, and none has
\* appeared for upstream_timeout (3 s), the stream rejoins live ... The wait is for a heal, after
\* which a relay that restarted and took the stream up live can answer before the origin, which
\* holds the history, is back in view." If the view follows the links within upstream_timeout
\* (see the abstractions), the leaf never gives up records a node it reached no later than its
\* newest candidate holds or could fetch, nor resets for a generation that has not changed. It
\* checks the wait (GiveUpAtOnce, OldestCandidate, StaleSince, Coalesced), that no parent refuses a
\* position it holds or could fetch (ReadyStrict, BehindRefused, KeepRefusal, StaleRefusal,
\* StaleReset) and that a parent behind the leaf does not reset it (SpuriousGC).
NoNeedlessReset == ~needless

\* guide/streams.md: "Every node takes a new generation from its record 1 while its parent's ring
\* still holds it", and this checks the origin's behind a relay too. See MissesOne.
NewGenFromOne == ~missed1

\* The follower's generations never go back; core.rs: handle_push refuses a Ready or a batch
\* older than feed.generation, and only that guard holds it (OlderGen).
GenNeverBack == lgen = 0 \/ lgen >= ghigh

\* README: a relay "waits for one ahead of it, one no ring on the path holds ends with
\* ResetRequired(HistoryUnavailable)": a relay with no position, asked from a position the origin
\* holds, neither takes up the key past it nor resets the branch.
NoStartSkipped == ~skipped

\* guide/streams.md: Live "follows the key: a restart or re-open is a new Ready, not an end": a
\* parent on a newer generation than the leaf's is followed into it, never refused.
GenAheadNeverRefused == ~ahead

\* After the last heal the follower gets the records again, up to the origin's last.
AllUp == (\A n \in Parents : lk[n]) /\ (\A r \in Relays : ro[r])
Recovers == <>[]AllUp => <>[](lgen = og /\ lpos = ohi)

\* mutations/RelayGiveUp-SpuriousGC.cfg's constraint: the origin restarts only while the relays
\* are cut from it, so a relay stays on the old generation because it cannot hear of the new one,
\* not because a push over a link that is up never arrives.
RelaysCutAtRestart == og' = og \/ \A r \in Relays : ~ro[r]

\* For the StaleReset and StaleRefusal mutations: no ring rolls past a consumer on a link that is
\* up, so every lost record comes from a cut or a crash, not from a branch or relay that fell
\* behind on its own.
KeepUp ==
    /\ cur # "none" /\ open /\ bpos >= 0 /\ Connected(cur) /\ PLo(cur) <= bpos + 1
       => (IF cur = "o" THEN OLo' ELSE rlo'[cur]) <= bpos + 1
    /\ \A r \in Relays : rup[r] /\ ro[r] /\ rgen[r] = og /\ OLo <= rhi[r] + 1 /\ og' = og
       => OLo' <= rhi[r] + 1

TypeOK ==
    /\ og \in 1..MaxGen /\ ohi \in 0..MaxSeq
    /\ cur \in Parents \cup {"none"} /\ bdead \in {"none", "GC", "HU", "UL", "Gap"}
    /\ lgen \in 0..MaxGen /\ lpos \in 0..MaxSeq /\ ghigh \in 0..MaxGen
    /\ fresh \in {"no", "yes", "spent", "lost", "gaveup"}
    /\ \A n \in Parents : rst[n] \in {"none", "refused", "stale", "wrongly", "kept", "unavail"} /\ rrem[n] \in 0..MaxBackoff
    /\ \A r \in Relays : rgen[r] = 0 \/ (rlo[r] >= 1 /\ rlo[r] <= rhi[r] + 1 /\ rhi[r] - rlo[r] < K)

=============================================================================
