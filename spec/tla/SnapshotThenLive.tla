-------------------------- MODULE SnapshotThenLive --------------------------
(* A compacted stream and SubscribeOptions { start: SnapshotThenLive }, as in
   crates/axon/guide/streams.md § Compacted streams, at an origin that keeps the stream on disk
   and through one relay. One stream, one generation (the origin resumes it after a crash, as
   with a DiskConfig). One subscriber on the origin, RelaySubs on the relay.

   Abstractions, and what they could hide:
   - One record per publish. With one parent every position a relay or a snapshot takes (a
     Ready's start, a snapshot's through, the relay's cursor) falls between the origin's batches,
     so no batch is ever split; SequencedBatch::after trimming inside one is not checked.
   - The relay's ring holds RingSize records (recovery.rs MemoryStore::append, by count, not
     bytes). The origin's ring holds all its durable history, but OriginRefuse may refuse any
     branch that would replay from it, as eviction or a restart that refilled only the newest
     segments would.
   - A branch's replay at the origin is queued in one step when the branch opens: it reads a
     history that only grows, and live batches queue behind it. The relay subscriber's replay
     runs one record a read, with live batches held behind it.
   - A subscriber's queue is folded into its consumer: events are applied as they are queued, in
     queue order. Polling, queue capacity, SlowConsumer and GlobalCapacity are not modelled, and
     neither is the order of Ready and CaughtUp or Ready's start (the code takes Ready's start
     and the snapshot's through from one value). A second Snapshot in one subscription counts
     as bad.
   - Allowed more than the code does, which safety tolerates: ReleaseHeld queues the state at any
     time after the branch opens (the code does it after CaughtUp, on a poll, or at once for a
     branch not replaying); Resend goes back to any earlier item (the code resends the last
     unacknowledged push); Write may run again before Sync (the code has one flush in flight).
   - Pushes arrive in order, each at least once. Items of an older epoch are dropped, so a
     rebranch clears the channel.
   - The relay has one parent: no joining link, no switch, no chain of relays. A Ready, a batch or
     a snapshot past the relay's position cannot happen then (TLC's coverage shows them unreached),
     so the relay's own gap and Ready checks could be dropped unseen. Its refusal and reset paths
     (core.rs refuse, reconcile's broken path) run through OriginRefuse.
   - The relay never crashes and follows the key live from the start. The origin's own subscriber
     does not subscribe inside the restart window (Restart).
   - Access and every limit allow everything; End(Revoked), CompactLimits, generations (and so the
     state's reset in restart_stream) and close_stream by the producer are not modelled.
   - A write puts every admitted record in the page cache; a sync makes them durable and hands
     them to the hub (core.rs disk_written -> hub.rs commit_admitted). A restart rebuilds the
     hub's state from what the files hold; segments, STATE frames and retention are not modelled
     apart from StaleResume. *)
EXTENDS Integers, Sequences, FiniteSets, TLC

CONSTANTS
    Keys,           \* model values, symmetric
    Vals,           \* model values, symmetric
    Absent,         \* a record with this value removes its key; a key not in the state
    MaxRecords,
    MaxCrashes,
    MaxResends,
    RingSize,       \* records the relay's ring holds
    ReplayOffset,   \* 1: a replay after n starts at n + 1 (hub.rs drive_recovery); 0 repeats n
    SplitSnapshot,  \* TRUE: a snapshot's state is read at an earlier position than its label
    SnapAhead,      \* TRUE: a snapshot's state is read at a later position than its label
    ReadUnsynced,   \* TRUE: a restarted origin reads back unsynced bytes without syncing them (e5bd1cb)
    StaleResume,    \* TRUE: a restart rebuilds the state at an earlier position than it resumes after
    NoDedup,        \* TRUE: a relay takes a batch it already holds again (hub.rs accept_batch)
    NoCoverCheck,   \* TRUE: a relay's replay takes what its ring still holds, past what it evicted
    StaleCut,       \* TRUE: a relay subscriber replays only to the cursor it subscribed at
    TailFirst,      \* TRUE: live batches held behind a replay are delivered ahead of it
    StallRelay,     \* TRUE: a relay stops feeding its live subscriber
    EarlyRelease,   \* TRUE: a branch queues the state ahead of its replay (release_held unfiltered)
    RelaySubs,      \* SnapshotThenLive subscribers on the relay
    SnapToLive,     \* TRUE: accept_snapshot restarts a subscriber already past Phase::Snapshot
    KeepSubs,       \* TRUE: reset_relay_stream drops the position but leaves the subscribers
    ReadPastCut,    \* TRUE: MemoryStore::read_after ignores its through bound
    LabelAtRelease, \* TRUE: release_held labels the held state with the cursor at release
    StopAtReset     \* TRUE: accept_snapshot returns after the first subscriber it resets

VARIABLES
    log,            \* records admitted at the origin, position i at log[i]
    cached,         \* written to the files, not yet synced: survives a process crash
    synced,         \* survives a machine crash
    durable,        \* the hub's cursor: what subscribers may see
    hst,            \* the hub's compacted state (recovery.rs MemoryState)
    up, crashes,
    oc,             \* the origin's subscriber
    link, chan, next, held, resends,  \* the origin's branch to the relay
    rpos, rbase, ring, wantsSnap,     \* the relay
    fresh,          \* the relay, without a position, asks from record 1 (Feed::fresh)
    rc,             \* the relay's subscribers
    bad             \* a subscriber saw a record twice, missed one, or a second Snapshot

vars == <<log, cached, synced, durable, hst, up, crashes, oc, link, chan, next, held, resends,
          rpos, rbase, ring, wantsSnap, fresh, rc, bad>>
origin == <<log, cached, synced, durable, hst, up, crashes, oc>>
relay == <<rpos, rbase, ring, wantsSnap, fresh, rc>>
Subs == 1..RelaySubs

Records == [k : Keys, v : Vals \cup {Absent}]
Empty == [k \in Keys |-> Absent]
Apply(st, r) == [st EXCEPT ![r.k] = r.v]
RECURSIVE StateOf(_, _)
\* recovery.rs MemoryState::apply: the latest record of each key, an empty value removing it.
StateOf(l, n) == IF n = 0 THEN Empty ELSE Apply(StateOf(l, n - 1), l[n])
Max(x, y) == IF x > y THEN x ELSE y

\* ph: none, wait (Phase::Snapshot), rec (Phase::Recovering), live, done (a reset). pos and view:
\* what the consumer has applied. rec only: nx the replay's next_after, cut, and tail, the live
\* batches held behind the replay.
NoSub == [ph |-> "none", pos |-> 0, view |-> Empty, nx |-> 0, cut |-> 0, tail |-> <<>>]
Done == [NoSub EXCEPT !.ph = "done"]
EndAll(r) == [i \in Subs |-> IF r[i].ph = "none" THEN r[i] ELSE Done]
Live(p, st) == [NoSub EXCEPT !.ph = "live", !.pos = p, !.view = st]
NoSnap == [t |-> "none"]
Ready(s) == [t |-> "ready", s |-> s]
Batch(s, r) == [t |-> "batch", s |-> s, r |-> r]
CaughtUp == [t |-> "caughtup"]
Snap(s, st) == [t |-> "snap", s |-> s, st |-> st]
Batches(l, lo, hi) == [i \in 1..(hi - lo + 1) |-> Batch(lo + i - 1, l[lo + i - 1])]

\* hub.rs Stream::snapshot reads the state and the cursor at once. The mutations read the state at
\* another position than the cursor they label it with.
SnapState(d) ==
    IF SplitSnapshot THEN {StateOf(log, j) : j \in 0..d}
    ELSE IF SnapAhead THEN {StateOf(log, j) : j \in d..Len(log)}
    ELSE {hst}

\* A consumer applying batches in order, and noting any that does not follow its position.
RECURSIVE Run(_, _)
Run(c, bs) == IF bs = <<>> THEN c
              ELSE Run([c EXCEPT !.pos = Head(bs).s, !.view = Apply(@, Head(bs).r)], Tail(bs))
RECURSIVE Skips(_, _)
Skips(p, bs) == IF bs = <<>> THEN FALSE ELSE Head(bs).s # p + 1 \/ Skips(Head(bs).s, Tail(bs))

rcur == rbase + Len(ring)

\* core.rs reconcile's Want and open_branch: a child behind the cut replays after its cursor; one
\* without a position asks from record 1 if fresh; anything else starts live at the cut.
Start == IF rpos /\ rcur < durable THEN rcur ELSE IF ~rpos /\ fresh THEN 0 ELSE durable

Init ==
    /\ log = <<>> /\ cached = 0 /\ synced = 0 /\ durable = 0 /\ hst = Empty
    /\ up = TRUE /\ crashes = 0
    /\ oc = NoSub
    \* The relay already follows the key for someone: its feed asks the origin for a branch.
    /\ link = "down" /\ chan = <<>> /\ next = 1 /\ held = NoSnap /\ resends = 0
    /\ rpos = FALSE /\ rbase = 0 /\ ring = <<>> /\ wantsSnap = FALSE /\ fresh = FALSE
    /\ rc = [i \in Subs |-> NoSub]
    /\ bad = FALSE

\* core.rs RelayCore::rebranch: a new epoch; the parent starts the branch over, older items are unwanted.
Rebranch ==
    /\ link' = "down" /\ chan' = <<>> /\ next' = 1 /\ held' = NoSnap

\* The relay's position and ring go; its subscribers become subs.
DropRelay(subs) == rpos' = FALSE /\ rbase' = 0 /\ ring' = <<>> /\ rc' = subs

\* core.rs refuse: the link is dropped and asked again after retry_refused, or, once the parent has
\* refused for upstream_timeout, reconcile's broken path calls local_reset -> hub.rs
\* reset_relay_stream: the position and the ring go, every subscriber of the key is reset.
Refused ==
    /\ Rebranch
    /\ \/ UNCHANGED <<rpos, rbase, ring, rc>>
       \/ DropRelay(IF KeepSubs THEN rc ELSE EndAll(rc))
    /\ UNCHANGED <<wantsSnap, fresh, bad>>

----------------------------------------------------------------------------
(* Origin *)

\* core.rs RelayCore::publish_admitted -> disk.rs DiskStore::admit: held until it is synced.
Publish ==
    /\ up /\ Len(log) < MaxRecords
    /\ \E r \in Records : log' = Append(log, r)
    /\ UNCHANGED <<cached, synced, durable, hst, up, crashes, oc, link, chan, next, held, resends,
                   relay, bad>>

\* core.rs take_disk_writes, Files::apply: the bytes reach the page cache.
Write ==
    /\ up /\ cached < Len(log)
    /\ cached' = Len(log)
    /\ UNCHANGED <<log, synced, durable, hst, up, crashes, oc, link, chan, next, held, resends,
                   relay, bad>>

\* The write's sync, then core.rs disk_written -> hub.rs commit_admitted -> deliver: the state
\* takes the records, the origin's subscriber and the branch get them.
Sync ==
    /\ up /\ synced < cached
    /\ LET bs == Batches(log, durable + 1, cached)
       IN /\ synced' = cached
          /\ durable' = cached
          /\ hst' = Run([view |-> hst, pos |-> durable], bs).view
          /\ oc' = IF oc.ph = "live" THEN Run(oc, bs) ELSE oc
          /\ bad' = (bad \/ (oc.ph = "live" /\ Skips(oc.pos, bs)))
          /\ chan' = IF link = "open" THEN chan \o bs ELSE chan
    /\ UNCHANGED <<log, cached, up, crashes, link, next, held, resends, relay>>

\* hub.rs StreamHub::admit, SnapshotThenLive at the origin: Ready, the snapshot through the cut,
\* CaughtUp at once (the replay after the cut is empty), then live.
SubscribeOrigin ==
    /\ up /\ oc.ph = "none"
    /\ \E st \in SnapState(durable) : oc' = Live(durable, st)
    /\ UNCHANGED <<log, cached, synced, durable, hst, up, crashes, link, chan, next, held, resends,
                   relay, bad>>

\* core.rs open_branch -> hub.rs admit, then want_snapshot. A child behind the cut replays after
\* its cursor; one at or past it, or with no position, starts live. The state is taken now, through
\* the cut, and held until the branch stops replaying (hub.rs send_snapshot, Sub::release_held).
OpenBranch ==
    /\ up /\ link = "down"
    /\ LET replay == Batches(log, Max(1, Start + ReplayOffset), durable) \o <<CaughtUp>>
       IN \E st \in SnapState(durable) :
          IF EarlyRelease /\ wantsSnap
          THEN /\ chan' = <<Ready(Start), Snap(durable, st)>> \o replay
               /\ held' = NoSnap
          ELSE /\ chan' = <<Ready(Start)>> \o replay
               /\ held' = IF wantsSnap THEN Snap(durable, st) ELSE NoSnap
    /\ link' = "open" /\ next' = 1
    /\ UNCHANGED <<log, cached, synced, durable, hst, up, crashes, oc, resends, relay, bad>>

\* core.rs open_branch -> hub.rs admit returning HistoryUnavailable: the origin's ring no longer
\* covers the replay. The relay's core.rs refuse: a fresh one asks again for what it wants, without
\* a strike; one with a position goes through Refused.
OriginRefuse ==
    /\ up /\ link = "down" /\ Start < durable
    /\ IF rpos THEN Refused
       ELSE /\ fresh' = FALSE /\ Rebranch
            /\ UNCHANGED <<rpos, rbase, ring, wantsSnap, rc, bad>>
    /\ UNCHANGED <<origin, resends>>

\* hub.rs Sub::release_held: queued behind whatever the branch holds, once it has stopped replaying.
ReleaseHeld ==
    /\ up /\ link = "open" /\ held # NoSnap
    /\ chan' = Append(chan, IF LabelAtRelease THEN [held EXCEPT !.s = durable] ELSE held)
    /\ held' = NoSnap
    /\ UNCHANGED <<log, cached, synced, durable, hst, up, crashes, oc, link, next, resends,
                   relay, bad>>

\* The process dies: what was admitted and not written is gone, the page cache stays. Its
\* subscriber and its branches go with it.
CrashProcess ==
    /\ up /\ crashes < MaxCrashes
    /\ crashes' = crashes + 1
    /\ up' = FALSE
    /\ log' = SubSeq(log, 1, cached)
    /\ oc' = NoSub
    /\ Rebranch
    /\ UNCHANGED <<cached, synced, durable, hst, resends, relay, bad>>

\* testkit Crash::Machine: every file is cut to its synced length.
CrashMachine ==
    /\ crashes < MaxCrashes
    /\ crashes' = crashes + 1
    /\ up' = FALSE
    /\ log' = SubSeq(log, 1, synced)
    /\ cached' = synced
    /\ oc' = NoSub
    /\ Rebranch
    /\ UNCHANGED <<synced, durable, hst, resends, relay, bad>>

\* core.rs open_read with the same generation: disk.rs DiskRead resumes after what the files hold
\* (Files::read syncs a file before it returns it) and rebuilds the state from them, which hub.rs
\* resume_stream installs. Or the relay's Want arrives first: open_branch's ensure_stream opens a
\* relay stream for the key (cfg.relay, on by default), open_read closes it with End(StreamClosed),
\* and the relay's handle_push closes its own stream, ending its subscribers, and reopens it empty,
\* fresh.
Restart ==
    /\ ~up
    /\ up' = TRUE
    /\ durable' = cached
    /\ synced' = IF ReadUnsynced THEN synced ELSE cached
    /\ \E j \in (IF StaleResume THEN 0..cached ELSE {cached}) : hst' = StateOf(log, j)
    /\ \/ UNCHANGED relay
       \/ DropRelay(EndAll(rc)) /\ fresh' = TRUE /\ UNCHANGED wantsSnap
    /\ UNCHANGED <<log, cached, crashes, oc, link, chan, next, held, resends, bad>>

----------------------------------------------------------------------------
(* Relay *)

\* core.rs RelayCore::handle_push, one item.
Receive ==
    /\ link = "open" /\ next <= Len(chan)
    /\ LET m == chan[next] IN
       CASE m.t = "ready" ->
            \* hub.rs adopt_relay_stream; a branch that starts past this node's position is refused.
            IF ~rpos
            THEN /\ rpos' = TRUE /\ rbase' = m.s /\ ring' = <<>>
                 /\ fresh' = FALSE  \* read only without a position; local_reset clears it in the code
                 /\ next' = next + 1
                 /\ UNCHANGED <<link, chan, held, wantsSnap, rc, bad>>
            ELSE IF rcur < m.s
            THEN Refused
            ELSE /\ next' = next + 1
                 /\ UNCHANGED <<link, chan, held, relay, bad>>
         [] m.t = "batch" ->
            \* core.rs accept -> hub.rs accept_batch: a gap is refused, overlap dropped by position.
            \* Sub::offer: a live subscriber takes it, a replaying one holds it behind the replay, one
            \* waiting for the state drops it. Then MemoryStore::append, evicting past RingSize.
            LET base == IF rpos THEN rbase ELSE m.s - 1
                cur == IF rpos THEN rcur ELSE m.s - 1
                grown == Append(IF rpos THEN ring ELSE <<>>, m.r)
                evict == Len(grown) > RingSize
                feeds(c) == (c.ph = "live" /\ ~StallRelay) \/ (c.ph = "rec" /\ TailFirst)
                again(c) == NoDedup /\ c.ph = "live"
            IN IF m.s > cur + 1
               THEN Refused
               ELSE IF m.s <= cur
               THEN /\ next' = next + 1
                    /\ rc' = [i \in Subs |-> IF again(rc[i]) THEN Run(rc[i], <<m>>) ELSE rc[i]]
                    /\ bad' = (bad \/ \E i \in Subs : again(rc[i]) /\ Skips(rc[i].pos, <<m>>))
                    /\ UNCHANGED <<link, chan, held, rpos, rbase, ring, wantsSnap, fresh>>
               ELSE /\ rpos' = TRUE
                    /\ ring' = IF evict THEN Tail(grown) ELSE grown
                    /\ rbase' = IF evict THEN base + 1 ELSE base
                    /\ rc' = [i \in Subs |->
                                IF feeds(rc[i]) THEN Run(rc[i], <<m>>)
                                ELSE IF rc[i].ph = "rec" THEN [rc[i] EXCEPT !.tail = Append(@, m)]
                                ELSE rc[i]]
                    /\ bad' = (bad \/ \E i \in Subs : feeds(rc[i]) /\ Skips(rc[i].pos, <<m>>))
                    /\ next' = next + 1
                    /\ UNCHANGED <<link, chan, held, wantsSnap, fresh>>
         [] m.t = "snap" ->
            \* hub.rs accept_snapshot: at or behind this node's position, or asked for again. Each
            \* waiting subscriber, in order, starts from it and replays the rest from the relay's
            \* ring, or is reset if the ring no longer covers it. A consumer given a second
            \* Snapshot notes it.
            IF ~rpos \/ m.s > rcur
            THEN /\ IF wantsSnap THEN Rebranch
                    ELSE next' = next + 1 /\ UNCHANGED <<link, chan, held>>
                 /\ UNCHANGED <<relay, bad>>
            ELSE LET cut(c) == IF StaleCut /\ c.cut < rcur THEN c.cut ELSE rcur
                     resets(c) == c.ph = "wait" /\ m.s < cut(c) /\ m.s < rbase /\ ~NoCoverCheck
                     takes(i) == /\ rc[i].ph = "wait" \/ (SnapToLive /\ rc[i].ph \in {"live", "rec"})
                                 /\ ~(StopAtReset /\ \E j \in 1..(i - 1) : resets(rc[j]))
                     from(c) == IF m.s >= cut(c) THEN Live(m.s, m.st)
                                ELSE IF resets(c) THEN Done
                                \* The ring cannot give record rbase, so ReplayOffset = 0 repeats
                                \* only a record past it.
                                ELSE [NoSub EXCEPT !.ph = "rec", !.pos = m.s, !.view = m.st, !.cut = cut(c),
                                      !.nx = IF m.s > rbase THEN m.s + ReplayOffset - 1 ELSE m.s]
                 IN /\ wantsSnap' = FALSE
                    /\ next' = next + 1
                    /\ rc' = [i \in Subs |-> IF takes(i) THEN from(rc[i]) ELSE rc[i]]
                    /\ bad' = (bad \/ \E i \in Subs : takes(i) /\ rc[i].ph # "wait")
                    /\ UNCHANGED <<link, chan, held, rpos, rbase, ring, fresh>>
         [] OTHER ->  \* CaughtUp: only a joining link acts on it
            /\ next' = next + 1
            /\ UNCHANGED <<link, chan, held, relay, bad>>
    /\ UNCHANGED <<origin, resends>>

\* hub.rs drive_recovery, one record a read from the relay's ring (MemoryStore::read_after, which
\* refuses a range it no longer covers: a reset). At the cut: CaughtUp, then the held live batches.
DriveRecovery ==
    /\ \E i \in Subs :
       LET c == rc[i] IN
       /\ c.ph = "rec"
       /\ IF rbase > c.nx /\ ~NoCoverCheck
          THEN rc' = [rc EXCEPT ![i] = Done] /\ UNCHANGED bad
          ELSE LET s == Max(c.nx, rbase) + 1
                   b == Batch(s, ring[s - rbase])
                   d == [c EXCEPT !.pos = s, !.view = Apply(@, b.r), !.nx = s]
                   last == s >= (IF ReadPastCut THEN rcur ELSE c.cut)
                   bs == IF last THEN <<b>> \o c.tail ELSE <<b>>
               IN /\ rc' = [rc EXCEPT ![i] = IF last THEN Run(Live(s, d.view), c.tail) ELSE d]
                  /\ bad' = (bad \/ Skips(c.pos, bs))
    /\ UNCHANGED <<origin, link, chan, next, held, resends, rpos, rbase, ring, wantsSnap, fresh>>

\* The driver resends a push until it is acknowledged.
Resend ==
    /\ link = "open" /\ resends < MaxResends /\ next > 1
    /\ resends' = resends + 1
    /\ \E i \in 1..(next - 1) : next' = i
    /\ UNCHANGED <<origin, link, chan, held, relay, bad>>

\* core.rs RelayCore::subscribe -> hub.rs admit (Phase::Snapshot on a relay stream), ask_snapshot,
\* which asks again only when no snapshot is outstanding.
SubscribeRelay ==
    /\ \E i \in Subs :
       /\ rc[i].ph = "none"
       /\ rc' = [rc EXCEPT ![i] = [NoSub EXCEPT !.ph = "wait", !.cut = IF StaleCut THEN rcur ELSE 0]]
    /\ wantsSnap' = TRUE
    /\ IF wantsSnap THEN UNCHANGED <<link, chan, next, held>> ELSE Rebranch
    /\ UNCHANGED <<origin, resends, rpos, rbase, ring, fresh, bad>>

\* core.rs unsubscribe -> hub.rs forget; the feed still wants the snapshot it asked for.
UnsubscribeRelay ==
    /\ \E i \in Subs : rc[i].ph # "none" /\ rc' = [rc EXCEPT ![i] = NoSub]
    /\ UNCHANGED <<origin, link, chan, next, held, resends, rpos, rbase, ring, wantsSnap, fresh, bad>>

----------------------------------------------------------------------------

Next ==
    \/ Publish \/ Write \/ Sync \/ SubscribeOrigin \/ OpenBranch \/ OriginRefuse \/ ReleaseHeld
    \/ CrashProcess \/ CrashMachine \/ Restart
    \/ Receive \/ DriveRecovery \/ Resend \/ SubscribeRelay \/ UnsubscribeRelay

Spec == Init /\ [][Next]_vars

\* Publishes, crashes, refusals, resends and unsubscribes may stop; the rest keeps going.
LiveSpec == Spec /\ WF_vars(Write) /\ WF_vars(Sync) /\ WF_vars(Restart) /\ WF_vars(OpenBranch)
                 /\ WF_vars(ReleaseHeld) /\ WF_vars(Receive) /\ WF_vars(DriveRecovery)

Symmetry == Permutations(Keys) \cup Permutations(Vals)

Consumers == {oc} \cup {rc[i] : i \in Subs}

\* guide streams.md § Hub: "within a generation a subscriber gets every record once, in order, or
\* an explicit reset. Never a duplicate, never a silent gap." And § Compacted streams: one Snapshot,
\* before the replay.
Contiguous == ~bad

\* guide streams.md § Compacted streams: the Snapshot, then every record after it, with nothing
\* between the state and the records that follow it missed or seen twice. After every event a
\* subscriber's state is the origin's at the subscriber's position.
StateMatches ==
    \A c \in Consumers : c.ph \in {"live", "rec"} /\ c.pos <= Len(log) => c.view = StateOf(log, c.pos)

\* A check of the model, not a promise as stated: the node offers every record to a live
\* subscriber. With the queue folded into the consumer, that puts a caught-up one at its node's
\* position; in the code it trails by its queue, up to BranchLimits, then SlowConsumer. It stands
\* for the half of "gets every record" (guide streams.md § Hub) that safety can see.
AtTip ==
    /\ oc.ph = "live" => oc.pos = durable
    /\ \A i \in Subs : rc[i].ph = "live" => rc[i].pos = rcur

\* guide streams.md § History on disk: "what any subscriber sees is durable".
SeenIsDurable == oc.pos <= synced /\ rcur <= synced

\* The promise tests/world_disk.rs checks: an origin resumes past every record it made durable or a
\* subscriber had seen. What the relay and each subscriber hold is still the origin's history.
SeenIsKept ==
    /\ \A c \in Consumers : c.ph \in {"live", "rec"} => c.pos <= Len(log)
    /\ rpos => rcur <= Len(log) /\ \A i \in 1..Len(ring) : ring[i] = log[rbase + i]

\* A relay subscriber waiting for the state or replaying after it goes live or is reset.
StateArrives == \A i \in Subs : rc[i].ph \in {"wait", "rec"} ~> rc[i].ph \in {"live", "done", "none"}

=============================================================================
