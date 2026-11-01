# Design — a durable log of keyed records (`axon::files::Log`)

A segmented log in one directory, written by one owner. Many keys share one file, so one write and one fdatasync
cover every key appended since the last write. Each record has an address, its key's epoch and a sequence number,
and the log keeps a fence per key. Status: built (`crates/axon/src/files/log.rs`), with `FileOp::Write` and
zero-filled segments. The design review is in the wave 3 notes (log-design.md, accepted 2026-10-10).

## 1. Scope

In scope:

- One log per owner per node: one for each service on a node (a kv shard set, a workflow history replica, a Durable
  Object store), each in its own directory. A shard, a run or an object is a key in it, not a file.
- Group commit without a timer: one write out at a time, the next takes everything that queued meanwhile.
- Per-key fences by epoch, a replica's prefix rule (`append_at`), checkpoints, trims and removal, reclaim of dead
  segments and copy forward of mostly dead ones.
- Torn tails cut on open, corruption reported, a failed write written again.

Out of scope:

- Replication. The format is ready for it (every frame has an epoch and a sequence number, the fence is stored), but
  the wire, the quorum and repair are not built. spec/paxos-leases.md §1 still holds.
- One journal for every store of a node. Paxos keeps its acceptor log, and streams keep a file per stream (§6).
- A no-fsync mode, an index snapshot, reused segments, checkpoints over `max_record`, writing the next batch while the
  last one syncs, and a shipped log actor (the guide holds the pattern).

## 2. Why a file per owner, not per key or per node

Measured on build-1 (Ryzen 9900X, two Samsung PM9A1 in md RAID1, ext4; no power-loss protection, write cache on),
2026-10-10, a 150-line C program writing 128 B records, about 1.5 s per row, two runs (load 0.4–2.4, then load 25).
p50 per batch:

| One batch of 128 B records | K=1 | K=16 | K=256 |
|---|---|---|---|
| one file, growing, one fdatasync | 4.23–4.30 ms | 4.20–4.34 ms | 4.18–4.36 ms |
| one file, zero-filled first, one fdatasync | 1.29–1.33 ms | 1.30–1.35 ms | 1.24–1.27 ms |
| one file, `fallocate`d, one fdatasync | 1.34 ms | 3.24 ms | 4.32 ms |
| K files open, K fdatasyncs one after another | 4.23–4.29 ms | 23.4–23.7 ms | 343–379 ms |
| K files as `OsFiles::apply` does it (open, fstat, write, sync, close) | | 23.8–24.5 ms | 348–790 ms |
| K files, syncs on 32 threads | | 7.7–7.9 ms | 86–95 ms |

p99: growing file 4.6–8.7 ms, zero-filled 1.4–4.9 ms, 16 files one after another 68–168 ms.

- **A file per key loses**: each extra file costs about 1.4 ms here, and `OsFiles::apply` syncs the files it appended
  to one after another.
- **Zero-filled files sync 3.3 times faster**: a sync of a file that grew also commits its new size through the ext4
  journal. `fallocate` does not help once writes are larger, because unwritten extents must still be converted.
- **One journal per node gains little over one per owner**: 16 files synced on 16 threads took 7.7 ms against 4.2 ms
  for one, so the 2–4 owners on a node sync at the same time for about the cost of two. One shared journal would couple
  their space (Paxos compaction waiting on stream retention), their waits (a lease grant behind an 8 MiB stream batch)
  and their failures. As built, four `files::Log` owners at once kept that for growing segments, but on zero-filled
  ones at 16 records a write or more they made fewer writes a second together than one owner alone
  ([benchmarks § Durable log](../crates/axon/guide/benchmarks.md#durable-log-axon)).

## 3. API

```text
Log::open(files: Arc<dyn Files>, cfg: LogConfig) -> FileFuture<Log>
  LogConfig { name "log", segment_bytes 64 MiB, preallocate true, fill_ahead 256 KiB, max_record 4 MiB,
              max_unwritten_bytes 64 MiB, max_write_bytes 8 MiB, max_bytes 4 GiB, max_keys 1 << 20 }
log.append(key, epoch, body) -> Result<Appended { lsn, at: Address { epoch, seq } }, LogError>
log.append_at(key, at, body) -> Result<Lsn, LogError>          a replica's write: Gap { have } unless at.seq is the next
log.fence(key, epoch) -> Result<(Lsn, Vec<(epoch, length)>), LogError>
log.checkpoint(key, through, state) / trim(key, through) / remove(key) -> Result<Lsn, LogError>
log.take_write(now_ms) -> Option<FileFuture<()>> ; log.written(io::Result<()>) -> Lsn (the new durable())
log.durable() ; failing() ; writing_since() ; key(key) ; keys(from, limit) ; read(key, from, max_bytes) ; pinning(limit)
log.stats() -> LogStats
FileOp::Write(name, offset, bytes)   over bytes inside the file's length, never grows it
```

`Log` has no task, no lock and reads no clock. The owner actor appends in its handlers, takes the next write in
`on_turn_end` and runs it with `cx.submit_future`, under `catch_panic`; its result comes back as a message, and the
owner answers each caller whose LSN `durable()` has reached. `read` returns a future in the same way.

## 4. Format

- **Segments** `<name>.<first lsn, 20 digits>`, each starting with a 64-byte header: magic `AXLG`, version u16 (1),
  flags u16 (1: zero-filled), the log id u64 (drawn when the first segment is made, shared by every segment), the
  first LSN u64, the created ms u64, the zero-filled length u64 (0 for a growing segment), and a CRC-32 of those 40
  bytes. A zero-filled segment is filled ahead of its frames, `fill_ahead` at a time, up to its length: it is made by
  one `Append` of its header, its first frames and zeros `fill_ahead` past them, and a write whose frames pass the
  zeros appends the rest of them and zeros `fill_ahead` past them. Its file may be shorter than its length; past its
  end it reads as zeros. The format is the one a segment filled whole at once had: either build opens the other's.
- **Frames**, little-endian: `len u32 | crc u32 | lsn u64 | kind u8 | key_len u16 | epoch u64 | seq u64 | key | body`.
  `len` counts the bytes after the CRC, and the CRC is `files::crc32` over them: 35 bytes plus the key per frame.
- **Kinds**: 1 record, 2 checkpoint (`seq` is the position it stands for), 3 fence (`epoch` is the fence, `seq` 0),
  4 trim, 5 remove (the key's last position, its body the key's fence). 6–15 are reserved.
- **LSNs** rise by one through a segment; a later segment starts above the last LSN of the one before (a removed
  segment leaves a gap). A frame copied forward gets a new LSN and keeps its kind, key, address and body.
- One write touches one segment: a write that would cross into a new segment stops at the old one's end, and the new
  segment is made by the next write. So only the last segment can hold a torn write.

Every fact a frame carries is a maximum or is placed by its address, so reading the frames in LSN order rebuilds the
same index the log held: a record is live while its address is above the key's checkpoint and trim positions; the
newest checkpoint, the newest trim above it and the newest fence are live; a remove frame is never counted live, and
stays while an older segment still holds frames of its key. A copy and its original (a crash between the copy and the
removal of its segment) read as one, as do two frames with the same address.

## 5. Write path and failures

- At most one write is out. `take_write` takes every frame queued since the last write, up to `max_write_bytes` and
  within one segment, as one `Write` over the zeros of a zero-filled segment, one `Append` to a growing or new one,
  or both where the frames pass the zeros, then the removal of segments with no live frame left whose frames' killers
  are in this write or before it. `Files` syncs a write's appends and `Write`s before a later `Remove`, so a frame
  that ends another is on disk before that one goes.
- A failed write sets `failing()` and keeps `durable()`. The same bytes go again first in the next write: to the same
  offsets when it only wrote over zeros; after a `Truncate` to where it began when it appended, so a retry never leaves
  a second copy. A segment that a failed write appended to but the next write does not (the head moved on) is cut
  back to what was written in that write too. A write that lands clears `failing()`. A panic in `apply` is a failed write
  when run under `catch_panic`. A write that hangs is the owner's decision, from `writing_since()`.
- Refusals change nothing: `TooLarge` (a record past `max_record`, a key past 65 535 bytes), `Behind` (unwritten
  bytes past `max_unwritten_bytes`), `Full` (live bytes past `max_bytes`, records only), `Keys` (past `max_keys`),
  `Fenced { fence }` and `Gap { have }`.
- **Filling ahead.** A segment zero-filled whole when it is made put 64 MiB of zeros in the write that started it: in
  the kv example on the stand-in box that write took 107 to 194 ms, with every key's writes behind it, and the update
  p99.9 at 20 k ops/s was 171 to 178 ms (26 to 33 ms with growing segments). Filled `fill_ahead` at a time, no write
  holds more than its frames, a header and `fill_ahead`, and about one write in `fill_ahead` bytes of frames grows the
  file and syncs its new length. 256 KiB gave kv a lower update p99 and p99.9 than 1 MiB (3.0 against 3.8 to 4.2 ms at
  p99.9), on a disk whose sync of a new length is cheap. Making the next segment ahead of need would keep even that off
  the head's writes, but a segment is named by its first LSN, which is not known until the head is full: it would need a
  rename or LSNs that skip. The cost of both ways:
  [benchmarks § Durable log](../crates/axon/guide/benchmarks.md#durable-log-axon).
- **Fences.** `append` at an epoch above the key's fence raises it; below, `Fenced`. `fence(key, e)` raises it in memory
  at once, and refuses an epoch at or below the fence, so two owners given the same epoch cannot both fence. It
  writes a fence frame and returns each epoch's length; the owner answers once `durable()` reaches its LSN.
  `append_at` accepts only an epoch at or above the fence and the next sequence number of that epoch.
- **Reclaim.** A segment with no live frame is removed by the next write. When the oldest segment is under half live
  and the log's length on disk above half of `max_bytes`, a write copies its live frames to the head, up to half of
  `max_write_bytes`, and the segment goes once nothing in it is live: its `Remove` goes in the write after the copies
  land, so a crash between them leaves both, which read as one. `pinning()` names the keys that hold the oldest
  segments, so the owner can checkpoint them.

## 6. Open

`open` lists the directory, reads each segment once and rebuilds the index. In the last segment the tail is the
first frame that fails a check (its length, CRC, LSN, or zeros). Bytes after it that are not zero and lie within two
`max_write_bytes` of it are a torn write: they are cut by the first write, a `Truncate` to the tail and an `Append` of
the new frames and zeros `fill_ahead` past them, so no part of the torn write can be read as a frame later. Zeros
alone after the tail are no torn write: a zero-filled segment is written over them, a growing one cut first.
Anything else is corruption: a bad frame with data further on, a bad frame in an earlier segment, segments of two logs,
or LSNs that go back. `open` then fails with `InvalidData`, naming the file and offset. The last segment is the last
whose header reads. One file after it whose header is torn (a crash while a new segment was made) is removed by the
first write; a second such file, or one with data further than two `max_write_bytes` in, is corruption.

Corruption that lies within two writes of the tail is read as a torn tail and cut. The page order a disk writes in is
not known, so nothing can tell the two apart there.

## 7. Users

| User | Moves? | Key |
|---|---|---|
| kv recipe / examples kv | yes | shard |
| workflows history replica | yes, its disk half | run |
| edge Durable Object store | yes | object |
| edge registry, cron run table, mdpad catalog and accounts | yes | one or a few |
| mdpad documents | later | doc |
| Paxos acceptor log | no: one file that already groups its commits, rewritten with `Replace` | |
| stream disk store | no, for now: retention per stream assumes whole files per stream | |

## 8. Verification

- Unit tests (`crates/axon/src/files/log/tests.rs`, on `MemoryFiles`): pinned bytes; torn tails at every byte, zeros, a
  bad CRC, an LSN gap, and corruption further on that must fail; fences, `Gap` and `Fenced`; reclaim and copy forward;
  a crash at every op and byte of a write, one that writes over zeros and fills further among them, and a write landed
  in pieces; no write holding more than its frames and `fill_ahead`, after an open too; the index after a reopen equal
  to the index before; random operations against a model, with failed, landed-then-failed and crashed writes.
- `FileOp::Write` in `OsFiles`, `MemoryFiles` and the World's `Disk` agree (`crates/axon-testkit/tests/files.rs`).
- Fuzz target `log_frame`: arbitrary bytes after a valid header never panic `open`, give the same result twice, and
  never read back a frame whose CRC fails.
- Benchmarks, growing against zero-filled: [benchmarks § Durable log](../crates/axon/guide/benchmarks.md#durable-log-axon).
