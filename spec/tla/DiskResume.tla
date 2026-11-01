---------------------------- MODULE DiskResume ----------------------------
(* One stream kept on disk at its origin, as crates/axon-stream/src/disk.rs writes it and resumes
   it (guide streams.md § History on disk), and one follower that sees only what the hub delivers.
   One generation: every boot opens the same one. A record is one publish, alone in its batch and
   frame, and carries the publish's number as its value, so a position written twice with two
   values shows. A file is named by its segment's first position and the incarnation that created
   it (the name's created milliseconds), and segments are read in that order; a frame is
   <<position, value>>.
   The disk is the World's (crates/axon-testkit/src/world.rs, Disk): what was appended since a
   file's last sync is lost to a machine crash and kept by a process crash. A machine crash may also
   keep part of the unsynced tail, as a real disk may, and end it with a torn frame. A write runs its
   ops one at a time, as OsFiles::apply_now does: it may fail at any op, and a process crash may
   land between two ops or inside an append, tearing its frame.
   Abstracted, and what it could hide:
   - the ring holds all a boot resumed and delivered: no HistoryUnavailable or ResetRequired, which
     only take follower behaviours away;
   - retention removes any number of the oldest segments at any write, not at most once a second
     and by bytes or age; Behind, rates and quotas refuse nothing;
   - open_stream and StreamNode::start are one step, so the store's first write never runs between
     them (it may in the code, which only opens the window of ProofRemoved sooner);
   - the follower's relay is folded into it: it takes the next position from the origin's hub, and
     drops a position it has, as accept_batch does;
   - creating (but under MetaReorder), truncating and removing a file are durable at once. OsFiles
     syncs a truncated file at once (39577f5; TruncLost before it), and a creation or a removal only
     through the directory's sync at the end of a write, so a file system that reorders metadata
     could bring back a removed file or lose a created one (MetaReorder);
   - a file's name holds the incarnation that created it in place of created_ms: names never repeat
     and a later file sorts after an earlier one at the same first position. The store names a new
     segment no earlier than one past the newest created_ms it found at open (39577f5; ClockBack
     before it), so that holds for files a boot found whatever the clock does;
   - with one generation, commit_admitted never refuses a batch (it guards restart_stream and
     close_stream), and DiskRead::read takes every segment, so its bounded newest-first read and
     open_origin's first - 1 are checked only by RingContiguous;
   - "durable" is what the synced prefixes of the files would resume at (DurableLast): a machine
     crash that keeps more, a torn frame or a removed file, is explored by MachineCrash instead;
   - no Compacted policy (STATE frames, the scan's checks of them, DiskRead's state rebuild), and
     one record per batch, so the scan's partial overlap (batch.after) is not reached; the code's
     retries repeat whole frames too;
   - no restart_stream, close_stream or older generations: the id filters in take and written, and
     the removal of older generations at a boot, are unchecked;
   - OsFiles::list syncs the directory (e5bd1cb): the half of that fix MetaReorder cannot tell from
     a file system without ordered metadata, so it is unchecked. *)
EXTENDS Integers, Sequences, FiniteSets, TLC

CONSTANTS
    MaxPub,             \* publishes in all; positions and segment names are 1..MaxPub
    SegFrames,          \* frames a segment takes before the next publish rolls (segment_bytes)
    MaxCrashes,
    MaxFails,           \* failed writes in all
    Gaps,               \* by name, each a past bug (in mutations/) or a gap the code still has, or an
                        \* assumption about the file system dropped; {} the store as it is.
                        \* "ProofRemoved": a boot removes the first segment when it holds no whole
                        \*   frame, though its name is all that proves the positions before it (294e305)
                        \* "RetainUnsynced": retention may remove a segment the same write appends to, its
                        \*   own new frames or a failed write's
                        \* "DropNode": a node may start on the directory while the write of a
                        \*   StreamNode dropped in the process still runs, and that write land after
                        \*   the start (before 7769068)
                        \* "MetaReorder": a file created since the directory's last sync may vanish in a
                        \*   machine crash while a later removal stays
                        \* "TruncLost": a truncation is lost to a machine crash until its file is synced,
                        \*   and OsFiles synced it only when the same write appended to it (39577f5)
                        \* "ClockBack": a new segment named by the clock alone, which may repeat a
                        \*   millisecond across restarts, takes the name of a file a boot left (39577f5)
                        \* "CutLanded": a failed write cuts back every file it appended to, though a Remove
                        \*   or Truncate after the appends took effect (0f300d62)
    Bug                 \* one mutation by name, "none" in the passing configs

VARIABLES
    files,                          \* <<first, incarnation>> -> [ex, c: frames in the page cache, s: synced,
                                    \* ent: entry synced, tr: what an unsynced truncation cut from the synced frames]
    up, crashes, fails, published,
    segs, next, unwritten, pending, flush, phase, pre, failing,  \* DiskStore and the writer: lost with the process
    cursor, ring,                   \* StreamHub: the last position delivered, and what it serves
    seen,                           \* the follower's records, by position
    stale,                          \* the ops of a dropped node's write still running, or <<>>
    lastDurable                     \* history: the last position a write that returned Ok made durable

vars == <<files, up, crashes, fails, published, segs, next, unwritten, pending, flush, phase, pre,
          failing, cursor, ring, seen, stale, lastDurable>>

Gap(g) == g \in Gaps
Pos == 1..MaxPub
Id == Pos \X (0..MaxCrashes)
\* The files in the order DiskStore::open sorts them: by first position, then by creation.
Order == [i \in 1..(MaxPub * (MaxCrashes + 1)) |-> <<(i - 1) \div (MaxCrashes + 1) + 1, (i - 1) % (MaxCrashes + 1)>>]
Name(sg) == <<sg.first, sg.inc>>
Torn == <<0, 0>>
NoFile == [ex |-> FALSE, c |-> <<>>, s |-> 0, ent |-> TRUE, tr |-> <<>>]
NewFile == [NoFile EXCEPT !.ex = TRUE, !.ent = ~Gap("MetaReorder")]
\* dir: the write creates or removes a file, so OsFiles syncs the directory (under MetaReorder only).
NoFlush == [ops |-> <<>>, bats |-> <<>>, dir |-> FALSE]
NoPre == [x \in Id |-> -1]
Min(a, b) == IF a < b THEN a ELSE b
Least(S) == CHOOSE i \in S : \A j \in S : i <= j
Before(x, y) == x[1] < y[1] \/ (x[1] = y[1] /\ x[2] < y[2])
Op(t, seg, pos, val) == [t |-> t, seg |-> seg, pos |-> pos, val |-> val]

Init ==
    /\ files = [x \in Id |-> NoFile]
    /\ up = TRUE
    /\ crashes = 0
    /\ fails = 0
    /\ published = 0
    /\ segs = <<>>
    /\ next = 1
    /\ unwritten = <<>>
    /\ pending = <<>>
    /\ flush = NoFlush
    /\ phase = "idle"
    /\ pre = NoPre
    /\ failing = FALSE
    /\ cursor = 0
    /\ ring = {}
    /\ seen = <<>>
    /\ stale = <<>>
    /\ lastDurable = 0

----------------------------------------------------------------------------
(* The files *)

\* disk.rs: OsFiles::apply_now, one op. Truncate and Remove of a missing file do nothing (e5bd1cb);
\* set_len past the end pads with zeros, a torn frame. set_len is not synced.
ApplyOp(fs, op) ==
    LET f == fs[op.seg] IN
    CASE op.t = "append" ->
            [fs EXCEPT ![op.seg] = [(IF f.ex THEN f ELSE NewFile) EXCEPT !.c = @ \o <<<<op.pos, op.val>>>>]]
      [] op.t = "trunc" ->
            IF f.ex
            THEN [fs EXCEPT ![op.seg] = [f EXCEPT !.c = IF Len(@) >= op.pos THEN SubSeq(@, 1, op.pos)
                                                       ELSE @ \o [i \in 1..(op.pos - Len(@)) |-> Torn],
                                                 !.s = Min(@, op.pos),
                                                 !.tr = IF Gap("TruncLost") /\ @ = <<>> /\ f.s > op.pos
                                                        THEN SubSeq(f.c, 1, f.s) ELSE @]]
            ELSE fs
      [] op.t = "remove" -> [fs EXCEPT ![op.seg] = NoFile]

\* OsFiles::apply_now: sync_data of each file appended to (pr >= 0), then the directory's sync_all
\* when dir.
SyncAppended(fs, pr, dir) ==
    [x \in Id |-> [fs[x] EXCEPT !.s = IF pr[x] >= 0 THEN Len(fs[x].c) ELSE @,
                               !.tr = IF pr[x] >= 0 THEN <<>> ELSE @,
                               !.ent = @ \/ dir]]

\* The ops in order; pre: each appended file's length before its first append, which a failed
\* write cuts it back to. An op that changes a file, a Remove of one that exists or a Truncate that
\* cuts, first syncs what was appended before it, which then stays though the write fails.
RECURSIVE Run(_, _, _)
Run(fs, pr, ops) ==
    IF ops = <<>> THEN <<fs, pr>>
    ELSE LET op == Head(ops)
             landed == ~Gap("CutLanded") /\ fs[op.seg].ex /\ (op.t = "remove" \/ (op.t = "trunc" /\ Len(fs[op.seg].c) > op.pos))
             pr2 == IF landed THEN NoPre
                    ELSE IF op.t = "append" /\ pr[op.seg] < 0
                    THEN [pr EXCEPT ![op.seg] = Len(fs[op.seg].c)] ELSE pr
         IN Run(ApplyOp(IF landed THEN SyncAppended(fs, pr, FALSE) ELSE fs, op), pr2, Tail(ops))

\* The file op i names is missing when it runs.
Gone(fs, ops, i) == ~Run(fs, NoPre, SubSeq(ops, 1, i - 1))[1][ops[i].seg].ex

\* The Truncates of a file that is gone, by index: before e5bd1cb each failed the write there.
Missing(fs, ops) ==
    IF Bug # "TruncateMissing" THEN {}
    ELSE {i \in 1..Len(ops) : ops[i].t = "trunc" /\ Gone(fs, ops, i)}

\* OsFiles::apply_now: entries_changed, from a Remove or an Append to a file missing before it.
\* Only MetaReorder reads it.
EntChanged(fs, ops) ==
    Gap("MetaReorder") /\ \E i \in 1..Len(ops) : ops[i].t = "remove" \/ (ops[i].t = "append" /\ Gone(fs, ops, i))

\* world.rs: Disk::lose_unsynced, and what a real disk may keep of the unsynced tail, torn or not.
LoseOpts(f) ==
    IF ~f.ex THEN {f}
    ELSE {[f EXCEPT !.c = SubSeq(f.c, 1, k), !.s = k, !.tr = <<>>] : k \in f.s..Len(f.c)}
         \cup {[f EXCEPT !.c = SubSeq(f.c, 1, k) \o <<Torn>>, !.s = k + 1, !.tr = <<>>] : k \in f.s..(Len(f.c) - 1)}
         \cup (IF f.tr # <<>> THEN {[f EXCEPT !.c = f.tr, !.s = Len(f.tr), !.tr = <<>>]} ELSE {})
         \cup (IF ~f.ent THEN {NoFile} ELSE {})
RECURSIVE Lose(_, _)
Lose(fs, n) == IF n = 0 THEN {fs}
               ELSE {[g EXCEPT ![Order[n]] = o] : g \in Lose(fs, n - 1), o \in LoseOpts(fs[Order[n]])}

----------------------------------------------------------------------------
(* Reading the files back at a boot *)

\* disk.rs: scan. The frames of one segment that follow last, up to the first torn or out-of-place
\* one; a frame repeating a position read is skipped. <<good, last, frames taken>>
RECURSIVE Scan(_, _, _, _, _)
Scan(fr, first, last, good, acc) ==
    IF good = Len(fr) THEN <<good, last, acc>>
    ELSE LET f == fr[good + 1]
         IN IF f = Torn \/ (f[1] > last + 1 /\ Bug # "NoGapCheck") \/ (good = 0 /\ f[1] # first)
               \/ (f[1] <= last /\ Bug = "NoDupSkip")
            THEN <<good, last, acc>>
            ELSE IF f[1] <= last THEN Scan(fr, first, last, good + 1, acc)
            ELSE Scan(fr, first, f[1], good + 1, acc \cup {f})

\* disk.rs: DiskStore::open, then DiskRead::read. The files in Order; the first one's name proves
\* the positions before it. After a cut every later segment is removed. read: the files read, which
\* Files::read syncs.
RECURSIVE Found(_, _, _, _, _, _, _, _, _)
Found(fs, i, last, cut, sg, ops, any, acc, rd) ==
    IF i > Len(Order)
    THEN [last |-> last, segs |-> sg, ops |-> ops, any |-> any, ring |-> acc, read |-> rd]
    ELSE LET x == Order[i] IN
    IF ~fs[x].ex THEN Found(fs, i + 1, last, cut, sg, ops, any, acc, rd)
    ELSE IF cut THEN Found(fs, i + 1, last, cut, sg, Append(ops, Op("remove", x, 0, 0)), any, acc, rd)
    ELSE LET r == Scan(fs[x].c, x[1], IF any THEN last ELSE IF Bug = "NoNameProof" THEN 0 ELSE x[1] - 1, 0, {})
             good == r[1]
             torn == good < Len(fs[x].c)
             drop == good = 0 /\ (Gap("ProofRemoved") \/ any)
         IN Found(fs, i + 1, r[2], torn,
                  IF drop THEN sg ELSE Append(sg, [first |-> x[1], inc |-> x[2], n |-> good]),
                  IF drop THEN Append(ops, Op("remove", x, 0, 0))
                  ELSE IF torn /\ Bug # "BootNoTruncate" THEN Append(ops, Op("trunc", x, good, 0)) ELSE ops,
                  TRUE, acc \cup r[3], rd \cup {x})
Read(fs) == Found(fs, 1, 0, FALSE, <<>>, <<>>, FALSE, {}, {})

\* Where a boot would resume if a machine crash kept only what is synced now.
DurableLast(fs) == LET F == Read([x \in Id |-> [fs[x] EXCEPT !.c = SubSeq(@, 1, fs[x].s)]])
                   IN IF F.any THEN F.last ELSE 0

\* node.rs: StreamNode::start (DiskStore::open), then open_stream of the same generation
\* (core.rs: RelayCore::open_read, DiskStore::open_stream, StreamHub::resume_stream). OsFiles
\* locks the directory until the last write of a dropped node has landed: a start before fails,
\* and is tried again (DropNode before 7769068).
\* CutWholeFromZero: before e5bd1cb a stream whose every segment was cut was not found at all.
\* UnsyncedTail: before e5bd1cb Files::read did not sync what it read. RingGap: the ring is refilled
\* without the position before the last (an off-by-one in DiskRead::read or open_origin).
Boot ==
    /\ ~up
    /\ stale = <<>> \/ Gap("DropNode")
    /\ LET F == Read(files)
           fresh == ~F.any \/ (Bug = "CutWholeFromZero" /\ \A i \in 1..Len(F.segs) : F.segs[i].n = 0)
           synced(x) == x \in F.read /\ Bug # "UnsyncedTail"
       IN /\ files' = [x \in Id |->
                \* list syncs the directory, and read each file it reads
                [files[x] EXCEPT !.s = IF synced(x) THEN Len(files[x].c) ELSE @,
                                 !.tr = IF synced(x) THEN <<>> ELSE @,
                                 !.ent = TRUE]]
          /\ cursor' = IF fresh THEN 0 ELSE F.last
          /\ next' = IF fresh THEN 1 ELSE F.last + 1
          /\ ring' = IF fresh THEN {}
                     ELSE IF Bug = "RingGap" THEN {f \in F.ring : f[1] # F.last - 1} ELSE F.ring
          /\ segs' = IF fresh THEN <<>> ELSE F.segs
          /\ unwritten' = F.ops
    /\ up' = TRUE
    /\ UNCHANGED <<crashes, fails, published, pending, flush, phase, pre, failing, seen, stale, lastDurable>>

----------------------------------------------------------------------------
(* The origin *)

\* hub.rs: deliver, from StreamHub::commit_admitted (a batch right after the cursor, nothing else)
\* or at once under DeliverBeforeSync. The follower takes the next position and drops one it has.
Deliver(st, f) ==
    IF st.cursor # f[1] - 1 THEN st
    ELSE [cursor |-> f[1], ring |-> st.ring \cup {f},
          seen |-> IF Len(st.seen) = f[1] - 1 THEN Append(st.seen, f[2]) ELSE st.seen]
Hub == [cursor |-> cursor, ring |-> ring, seen |-> seen]
RECURSIVE Commit(_, _)
Commit(st, bats) == IF bats = <<>> THEN st ELSE Commit(Deliver(st, Head(bats)), Tail(bats))

\* core.rs: RelayCore::publish_admitted; disk.rs: DiskStore::admit. Refused while the last write
\* failed. Rolls to a new segment once the last holds SegFrames.
Publish ==
    /\ up
    /\ published < MaxPub
    /\ ~failing
    /\ LET v == published + 1
           roll == segs = <<>> \/ segs[Len(segs)].n >= SegFrames
           sg == IF roll THEN Append(segs, [first |-> next, inc |-> IF Gap("ClockBack") THEN 0 ELSE crashes, n |-> 0])
                 ELSE segs
           f == <<next, v>>
           st == Deliver(Hub, f)
       IN /\ published' = v
          /\ segs' = [sg EXCEPT ![Len(sg)].n = @ + 1]
          /\ unwritten' = Append(unwritten, Op("append", Name(sg[Len(sg)]), next, v))
          /\ pending' = Append(pending, f)
          /\ next' = next + 1
          /\ IF Bug = "DeliverBeforeSync"
             THEN cursor' = st.cursor /\ ring' = st.ring /\ seen' = st.seen
             ELSE UNCHANGED <<cursor, ring, seen>>
    /\ UNCHANGED <<files, up, crashes, fails, flush, phase, pre, failing, stale, lastDurable>>

\* disk.rs: DiskStore::take and retain. A failed write first, then what was published since, then
\* the k oldest segments retention lets go: never the one being written, nor one this write appends
\* to (RetainUnsynced before it). A removed segment's appends are dropped from the write (fn remove,
\* 294e305), which matters only under RetainUnsynced: there a failed write's Remove of the segment
\* before then runs ahead of every append left.
Retainable ==
    LET ops == flush.ops \o unwritten
        busy == {ops[i].seg : i \in {i \in 1..Len(ops) : ops[i].t = "append"}}
        top == IF segs = <<>> THEN 0 ELSE Len(segs) - (IF Bug = "RetainLast" THEN 0 ELSE 1)
    IN {k \in 0..top : Gap("RetainUnsynced") \/ \A i \in 1..k : Name(segs[i]) \notin busy}
\* RemoveFirst: retention's Removes before the appends.
WriteOps(k) ==
    LET rm == [i \in 1..k |-> Op("remove", Name(segs[i]), 0, 0)]
        gone == {Name(segs[i]) : i \in 1..k}
        ops == SelectSeq(flush.ops \o unwritten, LAMBDA op : op.t # "append" \/ op.seg \notin gone)
    IN IF Bug = "RemoveFirst" THEN rm \o ops ELSE ops \o rm

\* The take handed to Files::apply by the writer task in node.rs. OsFiles::apply_now stops at the
\* first op that fails (a write_all past a full disk, say), maybe after creating the file it appends
\* to; its sync_data and the directory's sync_all may fail after every op (Fail).
Write ==
    /\ up
    /\ phase = "idle"
    /\ \E k \in Retainable :
        LET ops == WriteOps(k)
            bats == flush.bats \o pending
            bad == Missing(files, ops)
            full == IF bad = {} THEN Len(ops) ELSE Least(bad) - 1
        IN /\ ops # <<>> \/ bats # <<>>
           /\ \E upto \in (IF fails < MaxFails THEN 0..full ELSE {full}), touch \in BOOLEAN :
                LET r == Run(files, NoPre, SubSeq(ops, 1, upto))
                    at == ops[upto + 1].seg
                IN /\ touch => upto < full /\ ops[upto + 1].t = "append" /\ ~r[1][at].ex
                   /\ files' = IF touch THEN [r[1] EXCEPT ![at] = NewFile] ELSE r[1]
                   /\ pre' = r[2]
                   /\ fails' = IF upto < full THEN fails + 1 ELSE fails
                   /\ phase' = IF bad = {} /\ upto = full THEN "applied" ELSE "broken"
           /\ segs' = SubSeq(segs, k + 1, Len(segs))
           /\ flush' = [ops |-> ops, bats |-> bats, dir |-> EntChanged(files, ops)]
    /\ unwritten' = <<>>
    /\ pending' = <<>>
    /\ UNCHANGED <<up, crashes, published, next, failing, cursor, ring, seen, stale, lastDurable>>

\* The write's sync_data, then the directory's sync_all: "done" when both pass and apply returns Ok,
\* "synced" when the directory's fails. SyncOnlyLast: only the last file appended to is synced.
Sync ==
    /\ up
    /\ phase = "applied"
    /\ \E ok \in BOOLEAN :
        /\ ok \/ fails < MaxFails
        /\ files' = SyncAppended(files, IF Bug # "SyncOnlyLast" THEN pre
                                        ELSE [x \in Id |-> IF \A y \in Id : pre[y] >= 0 => y = x \/ Before(y, x)
                                                           THEN pre[x] ELSE -1],
                                  ok /\ flush.dir)
        /\ phase' = IF ok THEN "done" ELSE "synced"
        /\ lastDurable' = IF ok /\ flush.bats # <<>> THEN flush.bats[Len(flush.bats)][1] ELSE lastDurable
    /\ UNCHANGED <<up, crashes, fails, published, segs, next, unwritten, pending, flush, pre, failing,
                   cursor, ring, seen, stale>>

\* core.rs: RelayCore::disk_written with Ok; disk.rs: DiskStore::written.
Written ==
    /\ up
    /\ phase = "done"
    /\ LET st == Commit(Hub, flush.bats)
       IN cursor' = st.cursor /\ ring' = st.ring /\ seen' = st.seen
    /\ flush' = NoFlush
    /\ phase' = "idle"
    /\ pre' = NoPre
    /\ failing' = FALSE
    /\ UNCHANGED <<files, up, crashes, fails, published, segs, next, unwritten, pending, stale, lastDurable>>

\* disk_written with Err: the flush is kept and taken again, publishes are refused until one
\* succeeds. OsFiles cuts each appended file back (cut); a store that panics after its appends
\* (world_disk.rs, SimFiles) leaves them, and its own task (18d182f) fails only that write. A
\* "broken" write fails on its own.
\* DropFailedWrite: the flush dropped. PanicEndsWriter: before 18d182f a panic ended the writer
\* task, so nothing was written again until the process restarted. WrittenOnErr: a write whose
\* files were synced reaches the hub though the directory's sync failed and OsFiles cuts it back.
Fail ==
    /\ up
    /\ \/ phase \in {"applied", "synced"} /\ fails < MaxFails /\ fails' = fails + 1
       \/ phase = "broken" /\ UNCHANGED fails
    /\ \E cut \in BOOLEAN :
        /\ files' = IF ~cut THEN files
                    ELSE [x \in Id |-> IF pre[x] >= 0 /\ files[x].ex
                                       THEN [files[x] EXCEPT !.c = SubSeq(@, 1, pre[x]), !.s = Min(@, pre[x])]
                                       ELSE files[x]]
        /\ IF Bug = "PanicEndsWriter" /\ ~cut
           THEN phase' = "dead" /\ UNCHANGED <<flush, failing>>
           ELSE /\ phase' = "idle"
                /\ flush' = IF Bug = "DropFailedWrite" THEN NoFlush ELSE flush
                /\ failing' = TRUE
    /\ IF Bug = "WrittenOnErr" /\ phase = "synced"
       THEN LET st == Commit(Hub, flush.bats) IN cursor' = st.cursor /\ ring' = st.ring /\ seen' = st.seen
       ELSE UNCHANGED <<cursor, ring, seen>>
    /\ pre' = NoPre
    /\ UNCHANGED <<up, crashes, published, segs, next, unwritten, pending, stale, lastDurable>>

----------------------------------------------------------------------------
(* The follower after a boot: a Live subscription takes the positions it lacks from the ring. *)

CatchUp ==
    /\ up
    /\ Len(seen) < cursor
    /\ \E f \in ring : f[1] = Len(seen) + 1 /\ seen' = Append(seen, f[2])
    /\ UNCHANGED <<files, up, crashes, fails, published, segs, next, unwritten, pending, flush, phase,
                   pre, failing, cursor, ring, stale, lastDurable>>

----------------------------------------------------------------------------
(* Crashes *)

Forget ==
    /\ up' = FALSE
    /\ crashes' = crashes + 1
    /\ segs' = <<>>
    /\ next' = 0
    /\ unwritten' = <<>>
    /\ pending' = <<>>
    /\ flush' = NoFlush
    /\ phase' = "idle"
    /\ pre' = NoPre
    /\ failing' = FALSE
    /\ cursor' = 0
    /\ ring' = {}

ProcessCrash ==
    /\ up
    /\ crashes < MaxCrashes
    /\ Forget
    /\ UNCHANGED <<files, fails, published, seen, stale, lastDurable>>

\* A crash inside a write it has just taken: the first j ops have run, and the next, an append, may
\* have run in part (a torn frame). A machine crash then loses what Disk::lose_unsynced loses.
CrashMidWrite ==
    /\ up
    /\ phase = "idle"
    /\ crashes < MaxCrashes
    /\ \E k \in Retainable, machine \in BOOLEAN :
        LET ops == WriteOps(k)
        IN \E j \in 0..Len(ops), torn \in BOOLEAN :
            LET run == Run(files, NoPre, SubSeq(ops, 1, j))[1]
                fs == IF torn THEN ApplyOp(run, [ops[j + 1] EXCEPT !.pos = 0, !.val = 0]) ELSE run
            IN /\ torn => j < Len(ops) /\ ops[j + 1].t = "append"
               /\ files' \in IF machine THEN Lose(fs, Len(Order)) ELSE {fs}
               /\ stale' = IF machine THEN <<>> ELSE stale
    /\ Forget
    /\ UNCHANGED <<fails, published, seen, lastDurable>>

\* The StreamNode is dropped with a write just taken. Inner::stop aborts the writer loop, not the
\* task running files.apply, and spawn_blocking cannot be cancelled: the write goes on.
DropMidWrite ==
    /\ up
    /\ phase = "idle"
    /\ crashes < MaxCrashes
    /\ stale = <<>>
    /\ \E k \in Retainable : WriteOps(k) # <<>> /\ stale' = WriteOps(k)
    /\ Forget
    /\ UNCHANGED <<files, fails, published, seen, lastDurable>>

\* The dropped node's write lands, synced, whenever it gets there: after the next start, maybe.
StaleLand ==
    /\ stale # <<>>
    /\ LET r == Run(files, NoPre, stale)
       IN files' = SyncAppended(r[1], r[2], EntChanged(files, stale))
    /\ stale' = <<>>
    /\ UNCHANGED <<up, crashes, fails, published, segs, next, unwritten, pending, flush, phase, pre,
                   failing, cursor, ring, seen, lastDurable>>

MachineCrash ==
    /\ crashes < MaxCrashes
    /\ Forget
    /\ files' \in Lose(files, Len(Order))
    /\ stale' = <<>>
    /\ UNCHANGED <<fails, published, seen, lastDurable>>

----------------------------------------------------------------------------

Next ==
    \/ Publish \/ Write \/ Sync \/ Written \/ Fail \/ CatchUp
    \/ ProcessCrash \/ CrashMidWrite \/ MachineCrash \/ Boot \/ DropMidWrite \/ StaleLand

Spec == Init /\ [][Next]_vars

\* The writer runs until the node goes, woken by every publish and after every write: a write that can
\* go through does.
Fairness == /\ WF_vars(Write) /\ WF_vars(Sync) /\ WF_vars(Written)
            /\ WF_vars(Fail /\ phase \in {"broken", "synced"}) /\ WF_vars(Boot) /\ WF_vars(StaleLand)
LiveSpec == Spec /\ Fairness

----------------------------------------------------------------------------

\* guide streams.md § History on disk: a subscriber that holds a cursor goes on after it. Checked
\* here: the origin never serves a record at a position the follower saw with another value.
NoRewrite == \A f \in ring : f[1] <= Len(seen) => seen[f[1]] = f[2]

\* "opening the stream again with the same generation resumes it: the next record follows the last
\* durable one". A booted origin is at or after the last record the follower saw.
ResumeAfterSeen == up => cursor >= Len(seen)

\* "the next record follows the last durable one": what is admitted and not delivered yet sits
\* between the cursor and the next position. Boot sets both from one scan, as open_stream and
\* resume_stream take both from DiskRead's last, so this checks Publish, Write, Fail and Written.
NextAfterDurable == up => next = cursor + Len(flush.bats) + Len(pending) + 1

\* node.rs StreamNode::cursor: "The last record subscribers can see. On a stream kept on disk, the
\* last one synced": a machine crash now would resume at or after it (a history cut whole after
\* "the position the first one's name proves").
CursorDurable == up => cursor <= DurableLast(files)

\* "what any subscriber sees is durable", and "a machine crash or power loss also loses nothing
\* durable": checked in every state, as if a machine crash came next.
SeenDurable == Len(seen) <= DurableLast(files)

\* "once that write is synced" the record is durable: a booted origin resumes at or after the last
\* record of every write that returned Ok, delivered or not. While one waits for disk_written
\* ("done"), the cursor is behind it.
DurableKept == up /\ phase # "done" => cursor >= lastDurable

\* The ring a boot refills and the hub extends holds every position from its oldest to the cursor,
\* so a Live follower behind the cursor can always take the next one.
RingContiguous == up => LET ps == {f[1] : f \in ring} IN ps = {} \/ ps = Least(ps)..cursor

\* "what is published while a write is out goes in the next, which starts as soon as that one lands",
\* and a write that fails "is tried again every tenth of a second": in the end nothing waits for the
\* disk (e5bd1cb, 18d182f).
WritesGoThrough == <>[](up /\ ~failing /\ pending = <<>> /\ flush = NoFlush)

=============================================================================
