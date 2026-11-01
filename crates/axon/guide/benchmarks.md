# Benchmarks

Same machine (Apple Silicon laptop, macOS, no thread pinning), release build, 3 runs each,
best-of-3 shown. Old is `../actor`'s `perf_probe` with `ACTOR_PERF_UNCOUNTED=1`; new is
`cargo run --release -p axon --example perf`. Hosted workloads have the same shape in both:
`try_tell` with yield-on-`Full`, 4096-slot mailbox, 256-message turn budget, wait for delivery.

| Workload | Old ns/msg | axon ns/msg | |
|---|---|---|---|
| hosted: 1 producer → 1 actor, 1 worker | 80.2 | 17.9 | 4.5× |
| contended: 4 producers → 1 actor | 112.8 | 22.2 | 5.1× |
| fanout: 1 producer → 64 actors, 4 workers | 368.0 | 185.9 | 2.0× |
| embedded: mailbox + driver, one thread | 29.3 | 10.6 | not like-for-like |
| ask round trip, caller parks | n/a | 3512 | wake latency bound, before [the lone ask's wait](#asks-from-outside-the-workers) |

What this does not establish: the old message type is `u64`, the new one is a 16-byte enum;
embedded chunking differs; nothing was pinned; laptop scheduling noise is visible in fanout
(186–249 ns across runs). Treat ratios as indicative;
[the gate](#the-gate-scriptsbenchsh) judges a change against the baseline committed for its CPU.

Trace context (added later) costs the untraced hosted path about 1–2 ns per message on this machine (one thread-local
read at admission, 16 more bytes per mailbox entry; best of 8 interleaved runs: hosted 21.5 → 22.6 ns, contended
23.1 → 25.1 ns, which is inside the run-to-run noise above); embedded is unchanged.

Where the gain comes from: the consumer takes a whole turn's batch under one lock and releases
capacity with atomics (lock only when a waiter exists), timers/jobs/counters are off the
producer lock, workers swap the whole ready queue out per wake, and no `Arc` clone or clock
read happens per message.

The `tracing` feature ([tracing](tracing.md#the-tracing-crate)) was measured on the AMD EPYC 4484PX box (24 threads,
load 0.4–1.0, `uptime` before and after) with `perf hosted` and `perf traced`. `traced` is `hosted` with every
message starting a trace and recording a handle span. The two builds ran interleaved ten times each, with no
subscriber set. The table gives ns/msg, best and median:

| Workload | feature off | feature on |
|---|---|---|
| hosted, untraced | 15.4 / 15.9 | 15.5 / 16.0 |
| traced, every message | 114.8 / 118.5 | 115.3 / 117.4 |

Where the event sits in `Tracer::record` matters even with no subscriber, although the macro's own check is one atomic
load:

- Emitted inline, it cost the traced path 33 ns a message (117.8–119.6 off, 150.3–153.8 on).
- Moved into a `#[cold]` function behind one `LevelFilter::current()` load, that cost was gone.
- Adding `catch_unwind` around the call inside `record` brought 12 ns of it back (116.7–118.2 off, 129.5–130.9 on).
  Inside the cold function it costs nothing measurable.

Why the inline forms cost this was not looked into.

What this establishes: with the feature compiled in and nobody listening, neither path is measurably slower. It
does not measure a listening subscriber. That cost is the subscriber's own, paid on the recording thread for each
span.

## The gate (`scripts/bench.sh`)

`scripts/bench.sh` runs the harnesses below on a bench box and judges each row against the baseline committed for its
CPU, `scripts/bench/<cpu>.csv`. It refuses a machine whose governor is not `performance` on every CPU or whose
1-minute load stays at 1 or more for 5 minutes. It builds the harnesses once, or takes `BIN=dir` built on another box.
Every job is pinned as in the latest runs on the EPYC 4484PX below. It runs interleaved rounds, each job once a round:
five for the short jobs, three for `quota`, `overhead` and `scale`. The result is one row per metric with its median
and each round's value as the harness printed it.

A row may move by the most of: 3 % of its median; its baseline range, with five rounds or more less the fastest and
slowest, so one disturbed round does not widen it; one and a half steps of its printed precision, so 1.9 ns turning
2.0 is not a change; and, for idle cores at 0, a hundredth of a core. Counts that must stay 0 (leases other nodes
lost, records not delivered, resets) may not move at all. A median worse than the baseline's by more is a REGRESSION,
better by more is `improved`. A row the baseline has and a job that ran no longer prints is MISSING: its output
changed, and the parse with it. Either exits 1; a run that cannot be measured, parsed or compared exits 2. Rows with
no direction are only reported: a pipeline's and a log's max latency (one sample), `syncs/s` and `entries/sync`, tells dropped,
the bytes, reports, fallback and denials of `quota bytes`, quota rows, the subscribers' offered load, a metrics
batch's size. `scripts/bench.sh record` replaces the baseline's rows of the jobs it ran, keeping the others. The
routine set takes about 60 minutes; `full` adds the 10⁷-key quota check, 10 more minutes a round.
`scripts/test-bench.sh`, run by `check`, holds the parse, the comparison and the merge to fixtures.

Rows whose range was over 10 % of their median in the hand-taken baseline at ae6a242 (three rounds) were made
steadier for the gate:

- The short `perf` modes now run for about a second each, not 2 M messages in 25–180 ms: `embedded` 80 M, `hosted`
  60 M, `contended` 8 M per producer, `fanout` 12 M, `traced` 8 M.
- `remote_calls` takes its counts as arguments and the gate passes 2 M requests and 5 M tells.
- `loopback` reports its echo RTT in tenths of a µs over up to 10 000 round trips. Before, it was whole µs over 1 000,
  and whole µs on a 15 µs base move in 7 % steps. The gate sends 2 M frames one way, not 200 000 (30 ms).

The rest have more rounds and a band as wide as their spread. The baseline was recorded on one box (EPYC
4484PX) at 18d182f with these harness arguments:

| Row | ae6a242, 3 rounds | now: rounds, range | band |
|---|---|---|---|
| perf embedded / hosted | 11 % / 38 % | 5, 0.8 % / 4.0 % | 3 % |
| perf contended / fanout | 15 % / 26 % | 5, 6.8 % / 5.0 % | 3 % |
| loopback echo RTT p50 and p99, every payload and codec | up to 22 % | 5, 3.1–6.6 % | 3–3.9 % |
| loopback pseudorandom None one-way | 11 % | 5, 6.1 % | 3 % |
| remote_calls 256 in flight | 26 % | 5, 12 % | 6.8 % |
| remote_calls tell | 19 % | 5, 2.7 % | 3 % |
| pipeline w64 p99 | 10 % | 5, 5.2 % | 3 % |
| quota pull, every key held | 20 % | 3, 4.4 % | 4.4 % |
| scale keyed-paxos renew | 19 % | 3, 8.9 % | 8.9 % |

220 of 246 rows are gated; 117 have the 3 % floor and 30 a band over 10 %. Those catch only large moves:
remote_calls sequential p99 and p99.9 (42 % and 34 %: two rounds in five had a p99 of 29 µs, not 20), quota check at 12
threads (30–75 %, three rounds), overhead embedded (10–12 %), disk memory's wait (0.3 µs, printed to a tenth), and
most `scale` rows of whole nodes (10–45 %, and 228 % for nodes-50-paxos start and agree, which waits on a discovery
poll of 1 s).

What this establishes: a change that moves a row by more than that row's own spread on one quiet bench box. It says
nothing of another CPU, which needs a baseline of its own. A row with a wide band catches only large moves. A band is
taken from one session's rounds, so a box that has changed since (kernel, firmware, another tenant's work) reads as
a change: run main too, and compare the two runs with `scripts/bench.sh compare`.

### build-1 (Ryzen 9 9900X)

`scripts/bench/amd-ryzen-9-9900x-12-core-processor.csv` is the baseline of build-1: a Ryzen 9 9900X (12 cores on two
CCDs, 24 threads), Linux, governor `performance`, nothing else running. It was recorded at ee6dcd82 with the same
pins as on the EPYC: `0-11` is one thread of each of the 12 cores. Before it, `scripts/bench.sh` ran the routine set
at dfab5a65, main at the start of a night of 13 merges, and then at ee6dcd82, main at its end, back to back with a
load under 1 before each. The jobs whose rows `scripts/bench.sh compare` moved most were then run again, interleaved,
on those two builds and four between them: after trim/axon-core, trim/cluster-sims and lane/jobs, the merges that
changed the code those jobs run, and after lane/eager, the main just before lane/ask. `perf hosted`, `traced`,
`route` and `pipeline workers=4 pin=1` ran five rounds of each build, and `quota check 2 1000000` (10⁶ keys only)
three rounds of four of them.

The two `scale nodes` jobs with Paxos leases (`AXON_PAXOS=3`) failed on both trees. The harness started a restarted
node again while its previous node still held the acceptor's lease log, which is locked while its store lives, since
ed4d94d9. The harness now drops that node first. Their rows in the baseline are from the fixed harness at ee6dcd82, so
they have no row at dfab5a65 to compare with.

Of 219 rows, 170 were within their noise, 26 are only reported, `perf asks` is new, and 22 moved: 14 improved and 8
REGRESSION (in bold). Medians from the two gate runs, with the merge each move came at in the interleaved reruns:

| Row | dfab5a65 | ee6dcd82 | change (band) | the merge |
|---|---|---|---|---|
| `perf ask`, ns/msg | 1 351 | 296 | −78 % (3) | lane/ask, as it aimed |
| `perf hosted`, ns/msg | 45.0 | 40.6 | −9.8 % (3) | lane/jobs: 44.7 → 41.8 |
| `perf traced`, ns/msg | 116 | 106 | −9.0 % (3) | trim/axon-core: 115 → 103 |
| `perf start`, ns/msg | 1 746 | 1 630 | −6.7 % (6.4) | lane/ask, by its own run (1 889 → 1 642); not rerun |
| `route round-robin`, ns/msg | 90.8 | 87.0 | −4.2 % (3) | none: within 3 % in the reruns, 92.0 → 90.0 |
| `route least-loaded`, ns/msg | 140 | 150 | **+7.7 %** (3) | lane/ask: 137–140 → 147–154 (first put at trim/axon-core and lane/jobs, see below) |
| `route hash`, ns/msg | 104 | 110 | **+5.2 %** (3) | lane/ask 107.8 → 111.5, as its section says; trim/axon-core +2 % |
| `pipeline` 4 pinned workers, p50 µs | 5 169 | 4 269 | −17 % (3) | none: two modes, 4.1 and 5.1 ms, in every build |
| `pipeline` 4 pinned workers, p99.9 µs | 18 308 | 19 360 | **+5.7 %** (3) | none: 9 to 23 ms in every build |
| `quota check` AcquireDrop, Zipf, 10⁶ keys, 4 threads, M calls/s | 16.7 | 12.7 | **−24 %** (3) | trim/cluster-sims: 17.0 → 12.9 |
| same, 12 threads | 18.5 | 12.3 | **−34 %** (4) | trim/cluster-sims: 18.6 → 12.5 |
| same, 24 threads | 8.66 | 10.4 | +21 % (3) | trim/cluster-sims: 8.8 → 10.2 |
| `quota check` first call for a key, 10⁶ keys, ns | 3 259 | 3 399 | **+4.3 %** (3) | none: 3 415, 3 480, 3 466, 3 545 over the builds, one sample a round |
| `quota check` Take, uniform, 10³ keys, 24 threads | 50.4 | 55.0 | +9.0 % (5.8) | not rerun |
| `quota check` AcquireDrop, hot key, 10³ keys, 4 threads | 5.39 | 5.13 | **−4.8 %** (3.5) | not rerun |
| `overhead` hosted + extension, 1 000 actors, ns/msg | 30.5 | 27.6 | −9.6 % (8.0) | not rerun; the hosted path of `perf hosted` |
| `scale actors` spawn one at a time, /s | 1 252 660 | 1 336 100 | +6.7 % (5.9) | not rerun |
| `scale actors` spawn, 64 starts in flight, /s | 890 028 | 850 876 | **−4.4 %** (4.0) | not rerun |
| `scale nodes 200` resident per node, no links, MB | 0.54 | 0.45 | −17 % (9.3) | not rerun |
| `scale nodes 200` idle, no links / keyed actors placed, cores | 0.059 / 0.572 | 0.056 / 0.540 | −5.1 / −5.6 % (3.4 / 3.1) | not rerun |
| `scale nodes 200` with seeds, idle, keyed actors placed, cores | 0.616 | 0.594 | −3.6 % (3) | not rerun |

What this establishes: on this box, the lone ask is about five times faster (lane/ask, as measured in its section),
the hosted path 6 to 10 % faster, at lane/jobs, and the traced path about 10 %, at trim/axon-core. Two moves were not
aimed at: `route least-loaded` is about 7 % slower, at lane/ask; and acquire-and-drop of a permit on 10⁶ Zipf keys
is a quarter to a third slower on 4 and 12 threads, and a fifth faster on 24, at trim/cluster-sims. Both are found
and fixed, below. What it does not establish: the rows that were not rerun are each one
pair of runs; the pipeline rows are two modes, not a change; anything on the EPYC, whose baseline is older than these
merges.

**Acquire-and-drop.** trim/cluster-sims took the `incarnation: u128` out of the quota node's state, and the compiler
then laid out `Shared` with the `Table`'s shard pointer and length at offset 0x20 of the `Arc` allocation, on the line
of its strong count; they were at 0x160 before. Every `acquire` clones that `Arc` into its `Permit` and every drop
releases it, so each call wrote the line that every other thread's call reads to find its shard. `Take` clones
nothing and kept its speed. The shared `add` that merge made is not the cause. `Table` is now `#[repr(align(128))]`,
off the counts' line whatever its neighbours. `quota check 2 1000000`, the Zipf AcquireDrop rows at 1, 4, 12 and 24
threads in M calls/s, two interleaved rounds of each build: before trim/cluster-sims 4.80–4.83, 17.0–17.1,
18.5–18.6, 8.81–8.83; after it 4.78–4.85, 12.9–13.1, 12.1–12.5, 10.1; with the fix 4.82–4.86, 16.4–16.6, 18.0–20.6,
8.69–8.75. The 24-thread gain goes with it. It is likely that the stall on the counts' line spaced the calls out at
the hot keys' shard locks, but that was not measured.

**Least-loaded.** Interleaved reruns of every merge of the night put this move at lane/ask, not where the first
reruns did. Least-loaded was 134–145 ns/msg up to lane/eager and 147–154 after lane/ask. lane/ask's `mailbox.rs`
alone does not bring it back: with the file from before it, the row was 146–155. lane/ask changed nothing the route
runs. A profile of the producer shows the same instructions per message before and after. Its cycles in parking_lot's
`lock_slow` went from 195 to 332 M over the same window, and nearly all of them came from the lock that `pick` took on
each member to read its queue's length. So the producer met the workers inside that lock more often. Where lane/ask
moved the workers' code is the likely cause, but it was not measured. `pick` now reads a length that the mailbox
keeps beside its lock. The mailbox writes it under the lock wherever the queue changes, and it shares a line with the
tail of `Hot`, which a sender and a turn write in any case. So the pick takes no lock. Two other forms were tried and
were worse. A `try_lock` that counted a held lock as full kept 140–151. A `u32` in the struct's end padding, beside
`phase` and the flags a turn reads, cost `perf hosted` 20 % and `traced` 19 %. `first_block` is now a `u32`, so that
`Mailbox` stays 552 bytes. Three interleaved rounds of `perf hosted`, `traced`, `contended` and `route`, ns/msg, main
→ this change: hosted 42.3–43.1 → 38.2–40.2, traced 106 → 99.5–99.7, contended 99.2–99.4 → 96.4–97.6, route direct
95–100 → 94–98, round-robin 88–91 → 87–91, least-loaded 150–158 → 115, hash 110–114 → 108–111.

`scripts/bench.sh` ran `perf-|pipeline|quota-check` (three rounds a run) on main and on these two fixes, alternated,
two runs each, and `scripts/bench.sh compare` took each fixed run against the main run before it. Rows that moved in
both pairs: least-loaded 151 → 116 and 150 → 117 ns/msg; `perf hosted` 42.1 → 39.1 and 41.6 → 37.5; AcquireDrop Zipf
at 10⁶ keys 12.5 → 17.0 and 12.5 → 16.9 M/s on 4 threads, 12.0 → 19.6 and 12.2 → 18.4 on 12, and 10.1 → 8.8 and
8.87 on 24, its level at dfab5a65; a first call at 24 threads 3 251 → 2 990 and 3 254 → 3 002 ns. Each other row
moved in one pair only, within 8 %. `pipeline` with 4 pinned workers had its p50 in the 5.1 ms mode in 5 of the 6
rounds with the fixes and none of main's 6, at the same req/s; ten more runs alternated gave 4.2–4.3 ms in 4 of 5
with the fixes and 5 of 5 on main, so that is the two modes above, not a change.

The baseline's `perf-route`, `perf-hosted` and `quota-check` rows were then recorded again on this change
(`scripts/bench.sh record`, each job's own rounds): least-loaded 118 ns/msg, hosted 38.3, AcquireDrop Zipf at 10⁶
keys 4.77, 16.8, 19.6 and 8.85 M/s on 1, 4, 12 and 24 threads.

One private enqueue now serves every admission, `tell`, a `send` that waits and a reserved permit alike: each one
reserves `first_block` in a queue with no slots, and a `send` that waits weighs its message once, not twice.
`scripts/bench.sh perf` ran on main and on this change, alternated, four pairs at load 1.7–1.9, and `scripts/bench.sh
compare` took each run against the main run before it. Rows that moved in every pair: `perf hosted` 42.1 → 26.3, 42.5
→ 21.8, 41.7 → 22.6 and 42.2 → 21.8 ns/msg. `contended` went 97.7 → 87.6, 96.3 → 85.7 and 96.4 → 86.8 in three pairs
and 97.3 → 115 in the first. Three other REGRESSIONs came in one pair each and went the other way in the rest: in the
first pair `traced` 110 → 116 and `fanout` 72.4 → 83; in the third `route` direct 102 → 105, which went 104 → 101 and
103 → 99.9 in the second and fourth. Five interleaved rounds pinned to cores 0–11, with the change alone and with the
clean-up that came after it, put the move in this change: hosted 47.6–56.6 → 33.8–50.7 (the change) and 38.9–70.9
(both), contended 98.0–108 → 85.9–97.0 and 86.1–89.6. What this does not establish: why. `hosted` and `contended`
only `tell`, and a `tell` does the same work as before, one lock, one push and the length stored, so the likely cause
is how the smaller admission inlines, and it was not measured. The box was shared with builds, main itself was 10 %
above the baseline's hosted 38.3, and the baseline was not recorded again.

**Linux 7.0, and the merges of 2026-10-10.** build-1 booted Linux 7.0.0-38 on 2026-10-09 at 11:22, after its baseline
was recorded on 6.8.0-146. On 2026-10-10 `scripts/bench.sh` ran the routine set on main at b0bfa15e, with a load under
1 before it. That main has five more merges: lane/testkit (the World's disk), lane/parts (requests and uploads in
parts), lane/log (`files::Log`), lane/epochs (lease epochs) and lane/recipe (a guide recipe). Of 247 rows, 153 were
within their noise, 26 are only reported, 31 were REGRESSION and 23 improved. Two rounds of the Paxos `scale nodes`
jobs failed, and those two jobs are left out of that run (see below). The jobs whose rows moved then ran alternated,
two runs each, on three builds: ee6dcd82, the baseline's own tree; cd0d1e99, main before the five merges; and
b0bfa15e. The short jobs ran three rounds a run, `overhead` and the Paxos jobs two. Medians:

| Row | baseline | ee6dcd82 | cd0d1e99 | b0bfa15e |
|---|---|---|---|---|
| `pipeline` window 64, req/s | 224 004 | 164 388, 167 953 | 166 230, 165 157 | 165 789, 164 353 |
| `pipeline` window 64, p50 µs | 4 304 | 6 036, 5 889 | 5 946, 6 018 | 5 993, 6 028 |
| `pipeline` window 8, req/s | 30 313 | 19 366, 19 750 | 19 477, 19 505 | 19 592, 19 725 |
| `pipeline` 4 pinned workers, req/s | 222 601 | 211 844, 214 741 | 209 255, 212 318 | 211 168, 211 038 |
| `remote_calls` 16 in flight, req/s | 581 342 | 449 889, 448 512 | 443 385, 444 636 | 439 448, 440 307 |
| `perf start`, ns/msg | 1 630 | 1 725, 1 722 | 1 592, 1 657 | 1 565, 1 755 |
| `overhead` hosted 1/128 + labels, ns/msg | 26.5 | 28.7 | 35.6 | 29.8 |
| `scale nodes 50` Paxos, join moving keys settles, s | 0.676 | | 0.962, 0.990 | 1.02, 1.00 |
| `scale nodes 50` Paxos, keyed actors placed, /s | 6 943 | | 7 722, 7 786 | 7 808, 7 825 |
| `scale nodes 200` Paxos, join moving keys settles, s | 1.30 | | 1.56, 1.42 | 1.53, 1.56 |
| `scale keyed` Paxos, keyed spawns, /s | 6 605 | | 6 280, 7 225 | 7 217, 7 207 |
| `disk`, median wait until durable, µs | 11 302 | | 10 798, 10 307 | 10 300, 10 301 |

What this establishes: the five merges moved no gated row. `scripts/bench.sh compare` took each b0bfa15e run against
the cd0d1e99 run before it, and no row moved the same way in both pairs; the p99 and p99.9 rows that moved in one pair
did not move, or went the other way, in the other. The pipeline and the 16 requests in flight lost a quarter to a
third of their rate on the baseline's own tree too, so the box changed, not the code. On this box the pipeline waits
for its writers' `fdatasync`s, on drives without power-loss protection, so its rows follow the disk as much as the
code. The `pipeline` and `remote_calls` rows of the baseline were recorded again on b0bfa15e for this reason
(`scripts/bench.sh record 'pipeline|remote_calls'`, five rounds). Its first pipeline round ran right after the log
benchmark below and was slow (90 497 req/s at window 64); a band leaves out a run's fastest and slowest rounds.
`overhead` hosted is bimodal on the box now, 23 to 37 ns/msg across the rounds of every tree, so it shows no change.
The largest gains against the baseline, `perf hosted` 38.3 → 20.8 ns/msg and `contended` 101 → 91.3, came before
these merges, with the one enqueue measured above. What it does not establish: what in the box's change moved the
pipeline and the 16 requests in flight; and when the Paxos and `disk` rows moved, which were the same before and after
the five merges but were not run on ee6dcd82 (its harness cannot restart an acceptor). Those rows, `overhead` and
`perf start` keep the old baseline. The rows not rerun moved by under 9 % in one run: `quota check` at 12 threads, and
the resident memory of the `scale nodes` jobs with links, by about 4 %.

The Paxos `scale nodes` jobs failed in round 1 of nodes 50 and round 2 of nodes 200: a restarted node's acceptor
found its lease log still locked, though the harness drops the old node before the new boot. The likely cause, not
traced, is a write of the old store still landing on tokio's blocking pool, which holds the lock until it ends. The
harness now waits up to 10 s until a probe can list the directory before it starts the node again. Eleven rounds of each job have passed since, on
cd0d1e99 and b0bfa15e.

**The WAN merges, 2026-10-27.** `scripts/bench.sh` ran `perf-fanout`, `loopback`, `remote_calls`, `fanout`, `disk`,
`scale-streams` and `scale-subscribers` on main at 269b67cf and then on da78a8e6, lane/wan-spec after its ten
merges (a1, a5, a6, a2, a4, b2, b1, c1, c3, c2), back to back, twice. The box had other users' work all night. The
first pair started at 03:52, at a load of 1.2, and the load in its `loopback` and `remote_calls` rounds was 0.9 to 4.8
on main and 1.0 to 15 on the branch. The second pair (`loopback`, `remote_calls`, `scale-streams` only) had 1 to 13 in
both. `scripts/bench.sh compare` took each branch run against the main run before it. The rows that moved, and the
interleaved reruns that settle them (each build in turn, on the gate's cores, at a load of 1.4 to 11):

| Row | pair 1 | pair 2 | interleaved reruns, main → da78a8e6 | the merge |
|---|---|---|---|---|
| `fanout` 10 subscribers, ns per record per subscriber | 2.9 → 3.2 | | 3.0 → 3.5 (6 rounds) | lane/wan-a4: 2.9 → 3.2 (5 rounds) |
| `fanout` 100 subscribers | 1.6 → 2.0 | | 1.6 → 2.0 | lane/wan-a4: 1.6 → 2.0 |
| `fanout` 1 000 subscribers | 1.5 → 1.9 | | 1.6 → 1.9 | lane/wan-a4: 1.5 → 1.8 |
| `fanout` 10 000 subscribers | 1.5 → 2.0 | | 1.7 → 2.1 | lane/wan-a4: 1.6 → 1.9 |
| `remote_calls` sequential request p99, µs | 10.2 → 11.5 | 10.3 → 17.9 | 15.4 → 10.3 (6 rounds): two modes in every build | none |
| `remote_calls` tell sent, M/s | 3.62 → 3.55 (in noise) | 3.26 → 3.10 (in noise) | 3.72 → 3.53 | not found: e3a4447d 3.59 in one rerun, 3.64 against main's 3.67 in the other |
| `remote_calls` tell dropped, k (reported) | 12.5 → 18.0 | 8.0 → 24.3 | 7.7 → 21.4 | lane/wan-b2: 1.5 → 16.8 |
| `loopback` repeated None one-way, M frames/s | 5.68 → 5.32 | 5.60 → 4.97 | 5.84 → 5.87 | none |
| `loopback` pseudorandom Lz4 echo p99, µs | 9.6 → 9.8 (in noise) | 10.0 → 10.3 (in noise) | 10.1 → 9.9 | none |
| `scale streams 50000` published, cores | 3.60 → 3.87 | 3.65 → 4.12 | 3.70 → 3.67 (4 rounds) | lane/wan-a4 4.17, back by da78a8e6 |
| `scale streams 50000` churn, s | 0.37 → 0.41 | 0.37 → 0.42 | 0.35–0.38 → 0.36–0.40 | none |
| `scale streams 50000` relay lost, s | 0.868 → 0.879 | 0.842 → 1.09 | 0.84–0.96 → 0.83–0.89 | none |

Of the rows the pairs flagged, only `fanout` from 10 subscribers up stands. The interleaved reruns put it at the
lane/wan-a4 merge, the push window core: e3a4447d against 966c64e3, the tree just before it, which reads as main. The
row with 1 subscriber, and an origin publish with none, did not move. With a4 each subscriber of the hub holds the
bytes it has in flight, and a batch checks each subscriber's held bytes (its queue and those in flight) and charges
it on each poll. It is likely that this work per subscriber, and the larger entry it reads, make the 0.3 to 0.4 ns,
but that was not measured. It is a cost of the window, at about 2 ns per record per subscriber. It is not fixed in
this change, which holds no library code.

`remote_calls` sequential request p99 has two modes on this box, about 10 µs and about 21 µs, in every build: main
was in the high mode in 3 of 6 rounds, da78a8e6 in 1, and in the low mode main read 10.1–10.4 µs, da78a8e6 9.8–10.4.
The pairs' moves are which mode the rounds fell in; p99.9, and the 16 requests in flight (+7.7 % in one pair, −9.2 %
in the other), follow the load. `tell` sent is 5 % lower at da78a8e6 in the six-build rerun, about lane/wan-b2's own
−3.6 %, and within its band in both pairs, so it is accepted. Which merge moved it is not found. The tells dropped at
the sender's full queue went up at lane/wan-b2, from medians of 1.5 to 13 k a run to 17 to 21 k. b2 put a cluster's
sessions on lanes, and the tells go on `Live`, whose outbox is 8 MiB. The loopback one-way rows followed the load: the
fastest rounds of the first pair are equal, 5.90 and 5.89 M frames/s, and the interleaved reruns read 5.51 to 5.90 on
every build. The Zstd echo p99s that one pair flagged read 10.1 → 10.35 and 10.2 → 9.95 µs in the reruns. A2's Lz4
p99 (+4.5 % in its own run) is box drift: main itself read 9.2 → 9.6 µs against the baseline in the first pair, and
the reruns are equal. `scale streams` cost more publish
cores at the a4 merge (4.06–4.27 against 3.62–4.07), and by da78a8e6 it is back to main's (3.60–4.06): the pairs'
moves are their load. `disk` and `scale-subscribers` did not move against main; main itself read `disk` streams256
55.8 ms against the baseline's 53.8 and `scale-subscribers` 1.08 cores against 1.02. What this does not establish: a
quiet box for any of these rows, which it never was; the rows of the other jobs; and the reason for the `fanout` cost.
No row of the baseline was recorded again.

## Transport (`axon-net`)

TCP loopback, one process, plaintext + shared secret, compression threshold 0. Old is
`actor-transport`'s `loopback_benchmark`; new is `cargo run --release -p axon-net --example loopback`.
One-way throughput in frames/s; RTT is sequential echo, p50 µs. Single run each.

| Payload | Codec | Old frames/s | axon frames/s | | Old RTT | axon RTT |
|---|---|---|---|---|---|---|
| 256 B repeated | none | 766 k | 4 408 k | 5.8× | 47 | 28 |
| 256 B repeated | LZ4 | 846 k | 2 875 k | 3.4× | 45 | 27 |
| 256 B repeated | Zstd | 734 k | 1 098 k | 1.5× | 51 | 28 |
| 256 B random | none | 843 k | 5 365 k | 6.4× | 51 | 28 |
| 4 KiB repeated | none | 668 k | 2 301 k | 3.4× | 31 | 29 |
| 4 KiB repeated | LZ4 | 641 k | 1 607 k | 2.5× | 31 | 27 |
| 4 KiB repeated | Zstd | 433 k | 901 k | 2.1× | 34 | 29 |
| 4 KiB random | none | 671 k | 2 208 k | 3.3× | 31 | 29 |

Wire overhead at 256 B: 1.305× → 1.125× (78 B → 32 B header). Not like-for-like: the old
sender allocates a write receipt per frame, which axon replaced with `flush()`; that is part of
the gain, not noise. Zstd is bound by the codec itself, so its ratio is the smallest.

Where the gain comes from: buffered reads that decode every frame a `read` returned (old: three
syscalls per frame), zero-copy payload slices out of the read buffer, one reused encode buffer
and compression scratch per session, reused Zstd contexts, no per-frame oneshot.

**The write gate on tokio's clock** ([spec/simulation.md](../../../spec/simulation.md#how-each-nondeterminism-source-is-controlled)). A submit writes the
socket itself only after 5 µs without a write. That gate now reads `tokio::time::Instant` rather than std's, so that a
simulated node's segment counts do not depend on CPU speed. `loopback` with 256 B frames, three rounds each, alternating
the build before and after, on the AMD EPYC 4484PX box. Both builds link tokio's `test-util`, as `-p axon-net` does
through its dev-dependencies.

| 256 B, no codec | before | after |
|---|---|---|
| repeated, frames/s | 6.09 / 3.77 / 6.67 M | 6.52 / 6.57 / 6.04 M |
| pseudorandom, frames/s | 6.83 / 5.04 / 6.27 M | 6.68 / 6.50 / 6.18 M |
| echo RTT p50 | 15–16 µs | 14–15 µs |

What this establishes: no change beyond the spread between rounds. Until a runtime pauses its clock, tokio's `now` is
one atomic load and std's `now`. It says nothing about a build in which a clock was paused.

**Slices only from a full read.** A payload sliced out of the read buffer keeps the whole 64 KiB buffer alive, and a
quiet session takes a fresh buffer per read, so each small payload that was kept held 64 KiB: a stream ring of 256 B
records took 256 MiB for its 16 MiB bound. A read that filled less than half the buffer now copies its payloads out;
a fuller one keeps the slices. `loopback` with 256 B frames, five rounds alternating the build before with the one
after, pinned to eight cores of the shared EPYC 4484PX box, plaintext:

| 256 B, no codec, frames/s | zero-copy (before) | copy after a sparse read |
|---|---|---|
| repeated | 6.52 / 4.47 / 5.89 / 6.45 / 4.73 M | 6.54 / 6.71 / 6.36 / 5.75 / 6.50 M |
| pseudorandom | 6.62 / 6.40 / 6.61 / 6.42 / 6.65 M | 6.45 / 6.71 / 6.57 / 6.80 / 6.41 M |

Echo RTT p50 read 14–15 µs in both builds, bar single rounds at 17–23 µs. Copying every payload instead, rejected,
cost about a fifth (6.2–6.6 M before, 5.0–5.2 M after). What this establishes: copying after a sparse read costs
nothing measurable on plaintext loopback. It was not measured over TLS, whose reads fill differently. It does not bound
what a small payload kept out of a full read holds: that still keeps a whole buffer, so the holders in axon that keep
received bytes, stream rings and keyed actors' args, copy what they keep (under Streams).

## Cluster (`axon-cluster`)

Two nodes in one process over TCP loopback, plaintext + shared secret, 256 B bodies, an echo
actor behind a published endpoint: `cargo run --release -p axon-cluster --example remote_calls`.
Apple M5, single run. The old tree has no remote-call benchmark, so there is no baseline here;
the closest reference is the transport's own sequential echo above (28 µs), which puts the cost
of the engine, the registry lookup and the actor hop at about 6 µs per round trip.

| Workload | Result |
|---|---|
| Sequential request → reply | p50 34.5 µs · p99 62–93 µs · p99.9 104–197 µs |
| 16 requests in flight | 285 k req/s |
| 256 requests in flight | 594–616 k req/s |
| `tell` (unacknowledged) | 3.76 M msg/s sent; 0.7 % dropped by the full mailbox and counted as `inbound_rejected` |

A request is two frames, one fewer than the old three, because the ACK is folded into the reply.
The call body is never copied. The frame header carries the endpoint id, schema and correlation id.

On the AMD EPYC 4484PX described under Scheduler (Linux 6.8, pinned to cores 0–11), before is 9ba8e82, where every
frame went from the caller to a writer task that wrote the socket. After, a submit of up to 4 KiB writes the socket itself
when nothing is queued ahead of it, no batch is being written and nothing was written for 5 µs; a faster caller is still
batched by the writer task. Before and after built from the same tree, three interleaved rounds, ranges shown.

| Workload | before | after |
|---|---|---|
| Remote call, sequential, p50 / p99 | 23.8–24.3 / 27.6–37.2 µs | 16.2–16.7 / 19.5–26.2 µs |
| Transport echo (`loopback`), p50 / p99 | 18 / 20–22 µs | 13–14 / 16–17 µs |
| Remote calls, 16 in flight | 376–408 k req/s | 378–387 k req/s |
| Remote calls, 256 in flight | 0.90–1.08 M req/s | 0.90–0.98 M req/s |
| Remote `tell` | 4.17–4.34 M msg/s | 4.05–4.20 M msg/s |
| `loopback` one-way, 256 B, no codec, median of 10: repeated / pseudorandom | 4.8 M / 6.71 M frames/s | 6.4 M / 6.63 M frames/s |

For scale, a bare 256 B TCP ping-pong on the same cores takes 13 µs on tokio's multi-thread runtime and 6.7 µs
between two spinning threads. Before, a remote call made five context switches and about seven futex calls: the
frame went through the writer task on both sides, and a reply leaving an actor woke a tokio worker parked in `epoll`
just to write it. On tokio's current-thread runtime, where no hand-off crosses threads, the echo took 10 µs before
the change as well, so the transport's own work was never the cost.

What this establishes: an idle session, the common case for a request that waits for its answer, loses a third of
its round trip. What it costs: the caller's thread makes the `send` syscall, a few µs, which was the writer task's;
an actor replying on many idle sessions spends that time itself. The `repeated` throughput row is bimodal in both
builds (2.8–6.8 M before), so it shows no regression rather than a gain; `tell` and pseudorandom are 1–3 % lower,
inside the spread of three rounds. Both ends run in one process over loopback, so over a real network each side
pays only its own half.

A profile of `remote_calls` on the same machine at 26ff8e4 showed the session's read loop waiting on a lock:
`tokio::io::split` guards both halves of a stream with one mutex, so the reader and the writer took turns on it. A
plaintext TCP session now splits with `TcpStream::into_split`, which needs none; TLS keeps the shared lock, since both
halves drive one TLS session. Also, a call answered in time no longer locks its link's pending map a third time to
remove an entry the reply already took. All four builds came from the same directory: the lock-free split alone read
394–422 k built in one directory and 381–389 k in another. Rounds were interleaved; cores 0–11.

| Remote calls, 16 in flight | req/s |
|---|---|
| before (4 + 8 rounds) | 373–392 k |
| pending map locked twice, not three times (4) | 383–389 k |
| TCP halves without a lock (4) | 389–404 k |
| both (4 + 8) | 394–419 k |

Sequential p50 is bimodal, 16 or 24 µs. In the four-round set, the three changed builds read 24 µs in 4 of 12 runs and
before in 0 of 4; in the eight-round set of before against both, all 16 runs read 16.1–17.0 µs. So a latency cost is
not shown, and not ruled out. 256 in flight (0.84–1.15 M) and `tell` swing more between rounds than any difference,
and loopback's pseudorandom row read 6.4–7.0 M after against 6.4–6.8 M before, with one run at 3.5 M: no change
shown. TLS sessions keep their lock and gain only the pending-map change. Neither change has been measured over a real
network.

A profile of `remote_calls` at dcc1ec2 put 3.9 % of samples in the vDSO's clock read, 2 % under a call. A call
computed its deadline, then read the clock again to turn it back into a wait, and `Sender::submit` read it a third time
to turn that wait into tokio's deadline. `submit` now takes the deadline, which saves two clock reads per frame. Six
interleaved rounds, both builds from one directory, cores 0–15:

| Workload | before | after |
|---|---|---|
| Remote `tell` | 3.30–4.10 M msg/s, median 4.04 M | 4.23–4.75 M msg/s, median 4.63 M |
| Remote calls, 16 in flight | 399–423 k req/s | 400–418 k req/s |
| Remote calls, 256 in flight | 0.84–1.08 M req/s | 0.88–0.98 M req/s |
| Remote call, sequential, p50 | 16.2–24.5 µs | 16.2–17.0 µs |

A remote `tell` is one task submitting as fast as it can, so a clock read is a visible share of its 240 ns. A request
waits for its answer, and the rows that wait show no change: the 24.5 µs is the bimodal p50 described above.
`loopback`'s one-way loop gave each frame a 30 s wait, a clock read per frame; it now passes none, so its throughput
rows from before this change are not comparable with later runs.

The session's reader also reads the clock for each data frame it delivers, to mark the peer as heard. Marking it once
per socket read instead made no difference above the spread in four interleaved rounds against e2dfd98: streams
publishing took 4.76–4.90 cores against 4.59–4.86, and `remote_calls` moved inside its spread. So it was not kept.

`request_within` (D9) puts the caller's deadline in the request frame, 8 bytes after the trace context, and the far
side reads it on every call that carries one. A call without it gains one branch on each side. `remote_calls` now also
times `request_within` sequentially. Before is d74655c, after is the D9 tree, both built from one directory on the AMD
EPYC 4484PX, cores 0–11, 11 interleaved rounds. Other agents shared the box: load average 2.1–4.5 over 24 threads.

| Workload | before | after |
|---|---|---|
| Remote call, sequential, p50 | 16.2–17.0 µs, 22.5 µs once | 16.4–16.7 µs, 23.0–25.4 µs in 4 runs |
| `request_within`, sequential, p50 | | 16.4–17.2 µs, 21.6 µs once |
| Remote calls, 16 in flight (last 8 rounds) | 394–409 k req/s, and 174 k and 264 k | 401–415 k req/s |
| Remote calls, 256 in flight (last 8 rounds) | 0.80–1.09 M req/s | 0.83–1.07 M req/s |
| Remote `tell` (last 8 rounds) | 4.57–4.93 M msg/s, median 4.76 M | 3.93–4.77 M msg/s and 2.43 M once, median 4.53 M |

The sequential p50 is the bimodal 16 or 24 µs described above, and after landed on 24 µs more often (4 of 11 against 1
of 11). The rows that wait show no change. `request_within` copies its body once into the frame, as a traced call does;
at 256 B that did not show against `request`. The `tell` medians differ by 5 %, but the
ranges overlap and the box was loaded, so neither a cost nor its absence is shown there.

### Requests in parts

Indicative only, and not in the gate: `cargo run --release -p axon-cluster --example uploads -- 5`. Two nodes in one
process over TCP loopback. One task times 256 B requests one after another for 5 s, while another sends 10 MB bodies to
the same actor on the same session, one after another: as one request of one frame (`max_frame` 16 MiB, `outbound_bytes`
and `inbound_bytes` 32 MiB, on every row), or with `upload` in parts of 256 KiB. build-1 (Ryzen 9 9900X), Linux 7.0,
governor `performance`, nothing else running (load under 1 before each run), at b0bfa15e; the ranges of two runs:

| Row | 256 B request p50 | p99 | max | Bodies |
|---|---|---|---|---|
| no bodies | 8.8–8.9 µs | 10.4–10.5 µs | 0.14–0.34 ms | |
| 10 MB as one frame | 841–922 µs | 1.3–1.4 ms | 3.9–5.8 ms | 11 163–11 303 MB/s |
| 10 MB in 256 KiB parts | 267–269 µs | 496–504 µs | 1.5–3.0 ms | 13 354–13 448 MB/s |

With parts the small requests' p50 was 3.1 to 3.5 times lower than with one frame, their p99 2.6 to 2.8 times, and the
bodies went 18 to 20 % faster. A frame is written whole, so a small request waits behind the rest of a 10 MB frame:
the p50 is close to the time one takes at that rate, 0.9 ms. Parts go between other frames, so a small request waits
behind what is in the outbox before it, at most the 4 MiB window: about 0.3 ms at 13 GB/s. On a box shared with
builds (load 61–81 over 24 threads) the same harness gave p50s 22 to 55 times apart. What this does not establish: a real network, where the window and the round trip set what an
upload can send; and memory, which the harness does not measure.

With lanes the bodies can go on a connection of their own. `uploads 3 lanes` (rows `lanes-…`, on cores 0–11) sends the
same 10 MB bodies in parts on `Bulk` and times 256 B requests on `Live` (`lanes-live-p99-bulk`), or a raw endpoint's
round trip on `Control` (`lanes-control-round-bulk`). At da78a8e6, the four rounds of two `scripts/bench.sh` runs that
started at a load of 1.7 to 4.0:

| Row | 256 B request p50 | p99 | max | Bodies |
|---|---|---|---|---|
| `lanes-live-p99-bulk` | 9.1–9.6 µs | 113–126 µs | 1.9–3.4 ms | 6 387–8 663 MB/s |
| `lanes-control-round-bulk` | 9.8–10.6 µs | 28.6–124 µs | 0.9–3.5 ms | 11 547–13 407 MB/s |

A request on `Live` no longer waits behind the bodies' outbox: its p50 is that of no bodies, against 267 µs on the
same lane, and its p99 a quarter of the 501 µs. What it still shares is the processor: the process used about 3.1
cores, for the bodies' copies and their reads. The rows are not in the gate. The median p99 of `lanes-live-p99-bulk` was
133 µs in one run and 192 µs in the other, and the gate takes a row only when two runs agree within its tolerance. In
the rounds at a load of 6 to 14, the p99s went up to 2.2 ms. What this does not establish: a real network, where each
lane is a connection of its own with its own window.

## Quotas (`axon-cluster`)

`cargo run --release -p axon-cluster --example quota -- <check|memory|bytes|pull>` measures
[spec/quotas.md §6](../../../spec/quotas.md). Every node runs in one process, over loopback, each on an `ActorSystem`
with one worker: no message crosses a real network. The machine is the AMD EPYC 4484PX described under Scheduler (12
cores, 24 threads, Linux 6.8), not pinned, and shared with other agents' builds. Each table gives two runs, with the
load average from `uptime` at the start and end of each. Before is b428f3a with this harness.

**The check.** `check` starts one node, which is then every key's only replica. Its limits are never reached, except
`AtLimit`'s 100 a second on one key. The node makes every key's entry on 24 threads, then waits until a pass over
every key admits nothing on the fallback. It then calls for 2 s from 1, 4, 12 and 24 threads, each thread cycling
through 2²⁰ keys drawn before the clock starts: uniform, Zipf with s = 1, or one hot key.

- `Take` is `take(key, &[(req/s, 1)])`.
- `TakeTwo` adds a per-minute weight of 5 in the same call.
- `AcquireDrop` acquires a permit and drops it.

Figures are M calls a second over all threads. On one thread, 10 M/s is 100 ns a call.

| 10³ keys (3 000 entries) | 1 thread | 4 | 12 | 24 |
|---|---|---|---|---|
| Take, uniform: before | 11.6 | 14.5 | 9.2 | 6.5 |
| after, run 1 / run 2 | 10.3 / 10.0 | 34.2 / 32.9 | 41.3 / 41.1 | 56.6 / 55.9 |
| Take, Zipf: before | 11.9 | 14.3 | 8.3 | 5.9 |
| after | 10.3 / 9.9 | 34.2 / 32.7 | 29.9 / 31.7 | 10.0 / 9.8 |
| Take, one hot key: before | 12.9 | 7.2 | 1.8 | 1.1 |
| after | 11.0 / 10.8 | 9.7 / 9.5 | 2.5 / 2.4 | 1.1 / 1.1 |
| AcquireDrop, one hot key: before | 10.2 | 4.6 | 1.8 | 0.56 |
| after | 8.8 / 8.7 | 6.5 / 6.2 | 2.1 / 1.4 | 0.61 / 0.60 |
| AtLimit, one hot key, all refused: before | 13.7 | 7.6 | 2.6 | 1.1 |
| after | 13.1 / 13.0 | 9.3 / 9.4 | 3.0 / 1.9 | 1.1 / 1.1 |

| 10⁶ keys (3 M entries) | 1 thread | 4 | 12 | 24 |
|---|---|---|---|---|
| Take, uniform | 2.42 / 2.40 | 9.7 / 9.6 | 25.2 / 24.2 | 34.6 / 33.3 |
| Take, Zipf | 4.09 / 4.13 | 17.2 / 16.9 | 31.8 / 33.0 | 16.6 / 17.0 |
| TakeTwo, Zipf | 2.79 / 2.89 | 10.3 / 10.5 | 11.6 / 11.5 | 7.8 / 7.9 |
| AcquireDrop, Zipf | 3.91 / 4.01 | 13.4 / 14.2 | 14.4 / 14.4 | 7.6 / 7.7 |

| 10⁷ keys (10 M entries) | 1 thread | 4 | 12 | 24 | admitted |
|---|---|---|---|---|---|
| Take, uniform | 2.08 / 2.07 | 8.6 / 8.3 | 16.4 / 16.5 | 22.6 / 22.6 | 70–72 % |
| Take, Zipf | 3.25 / 3.41 | 13.2 / 12.8 | 17.9 / 18.1 | 15.0 / 15.2 | 53–55 % |

Load averages: 10³ to 10⁷, run 1 from 4.50 to 7.37 and run 2 from 7.37 to 7.10. Before, 0.59 to 8.33. The first call
for a key, which makes its entry, took 5.2–5.7 µs a thread with 24 threads making them.

Three changes, each found by this harness:

- **A check takes its shard's lock and no other.** Before, every check also took one read lock over the replica links.
  24 threads then made fewer checks than 4 did (6.5 M/s against 14.5 M/s). Each shard now keeps its own copy of what a
  check reads of the links, written when a link changes. Test:
  `quota::core::tests::a_check_admits_on_its_grant_while_the_links_are_written`.
- **A replica's rows of a key are a sorted vector.** They were a `BTreeMap` whose first leaf holds eleven rows, so a key
  used by one node cost 1.7 kB at each replica. At 10⁶ keys the replicas could then not keep up. After 10 minutes,
  1.39 M of 3 M entries had rows and 45 % of calls were admitted, the rest refused on the fallback. With the vector,
  every entry had its grant after 97–98 s.
- **The background walks rows once a second and sweeps a sixty-fourth of the entries each 100 ms.** The replica walked
  every row, and the table swept every entry, in a report cycle (100 ms) and a second respectively, on the node's one
  driver thread. A build timing each part of a cycle measured, per 50 cycles, 8.0–8.6 s walking 3 M rows and 0.7–1.0 s
  sweeping 3 M entries. At 10 M entries the sweep took 10.9–11.7 s. Timeouts the walk serves are 15 s and longer, and
  entries are kept 60 s, now up to 6.4 s more, by the clock and not by cycles, so long cycles do not stretch it. At 3 M
  entries the idle node went from 1.02–1.07 cores to 0.26. Entries had their grants after 75 s instead of 97–98 s. The
  check got faster wherever the driver had held a shard's lock or the memory bus: at 10⁶ keys, one thread went from
  1.85–2.01 to 2.40–2.42 M/s (uniform) and from 2.91–3.30 to 4.09–4.13 (Zipf); at 10⁷, from 0.68–0.72 to 2.07–2.08 M/s.

One thread at 10³ keys is 12 ns slower than before. Before, the check took 85–88 ns a call, and after 97–102 ns, in
three rounds that alternated the two builds and six runs. Padding each shard's copy of the links onto a cache line of
its own did not recover it, and cost 15 % at 24 threads (48 M/s against 57 M/s), so it was not kept. Without a profiler
on the box the cause is not established.

Two smaller choices, each measured against a build without it, the three builds alternated in two runs (load average
2.43 to 7.76):

- **Each shard is aligned to 128 B**, so two shards' locks never share a cache line. Without it, uniform keys on 24
  threads reach 43.4 / 42.8 M/s at 10³ keys against 56.4 / 55.9, and 29.3 / 29.9 M/s at 10⁶ against 35.5 / 35.7. One
  thread is the same within noise.
- **The evicted set is read without a lock.** It was behind one mutex, which every new entry took. With the mutex the
  first call for a key took 14.0 / 14.2 µs a thread with 24 threads making entries, against 5.2 / 5.3 µs. Checks on
  entries already made are the same within noise.

**Each shard counts the permits its entries hold**, for `stats().permits`, under the lock the check already holds.
Only an acquire writes the count. The count shares the shard's first cache line with the map's header, which every
check reads, and a store takes the line exclusive, so a check on another core has to pull it back. When every take
stored what its entry added, `+= 0` for a rate, checks lost 17–18 % at 12 and 24 threads. `check 2 1000 1000000` on a
box with nothing else running, not pinned, the build that stored it on every take (before) and the one that stores it
only for permits (after) run back to back in one hold of the box's lock per round, median (range) in M calls a second:

| | threads | before, 7 runs | after, 8 runs |
|---|---|---|---|
| 10³ keys, Take, uniform | 1 | 11.56 (11.38–11.83) | 11.55 (11.06–11.85) |
| | 4 | 33.00 (31.61–34.02) | 37.87 (36.30–38.75) |
| | 12 | 34.38 (34.06–34.75) | 41.96 (40.05–43.35) |
| | 24 | 46.48 (45.83–46.84) | 54.21 (53.19–55.06) |
| 10⁶ keys, Take, uniform | 12 | 21.16 (20.71–21.30) | 24.77 (23.26–25.17) |
| | 24 | 30.89 (30.47–31.01) | 34.63 (34.52–35.09) |
| 10⁶ keys, Take, Zipf | 12 | 26.64 (26.40–29.21) | 31.24 (30.23–31.76) |
| 10⁶ keys, TakeTwo, Zipf | 24 | 7.09 (7.08–7.12) | 7.70 (7.64–7.73) |
| 10⁶ keys, AcquireDrop, Zipf | 12 | 13.37 (13.14–13.89) | 13.14 (13.03–14.22) |
| | 24 | 7.52 (7.32–7.78) | 7.50 (7.49–7.70) |

A build that never writes the count also reached 54.2 M/s at 24 threads, so the rest of the way to 56.6, what the
build before the count reached, is not the count. Moving the count onto a cache line of its own made a hot-key acquire
on 4 threads 14 % faster (6.47 against 5.66 M/s), but that build made 8 % fewer takes a second at 1 and 4 threads
(10.80 against 11.70, 35.49 against 38.55), where the gate's band is 3 %, so the count stays on the shared line.

What it does not establish: what an acquire's store costs the takes of other keys in its shard, since `check` never
runs the two at once; nor where the 24-thread check lost the rest since the build before the count.

**The sweep holds a shard's lock for 32 entries at a time.** Since f6cffd6 swept every shard within 6.4 s by the
clock, each shard's sweep was one `retain` under its lock, about 156 k entries at 10⁷. A thread taking uniform keys
reached that shard within about 64 calls and waited out the whole walk, some ten times a second: a profile of the
first timed row showed the sweep's samples go from 58 to 343 and the timing thread parked for 9 % of it. Each shard
now keeps its ids in a vector, and the sweep walks it from where it stopped, 32 entries a lock hold, the shards taking
turns. `quota check 2 10000000` on the EPYC box (EPYC 4484PX, unpinned as the gate runs it), 9636d3f against this
change, five rounds each alternated under the box's measuring lock, median (range), M calls a second:

| 10⁷ keys | 1 thread | 4 | 12 | 24 |
|---|---|---|---|---|
| Take, uniform: before | 1.77 (1.75–1.82) | 7.69 (7.46–7.88) | 16.58 (16.27–16.66) | 24.38 (24.03–24.63) |
| after | **2.18** (2.12–2.22) | **8.61** (8.34–8.84) | **18.63** (17.64–18.76) | 25.67 (23.66–26.21) |
| Take, Zipf: before | 2.98 (2.70–3.06) | 12.24 (11.79–12.64) | 19.27 (18.82–19.33) | 15.23 (15.03–15.69) |
| after | **3.40** (3.32–3.45) | 12.18 (11.51–13.14) | 19.28 (18.50–19.40) | **18.63** (17.86–18.95) |

The node idle with 10⁷ entries used 1.04–1.17 cores before and 1.03–1.15 after, and 1.90–2.48 M entries had rows
after 10 minutes against 1.95–2.34 M. One thread is back past the 2.04 and 3.35 M/s measured before f6cffd6. Zipf at
4 and 12 threads is the same within its ranges. Uniform at 24 threads overlaps; Zipf at 24 threads gained 22 %, which
this run does not explain. Each entry costs 8 B more for its id in the vector, and a swept entry now costs a hash
lookup rather than a step of the table's own walk; the idle cores do not show it. Test:
`quota::core::tests::a_node_sweeps_a_share_of_every_shard_each_cycle_and_starts_a_walk_again_after_a_pause`.

A later step took each walk's share of the shard's bound, `per_shard`, instead of its vector's capacity. Where a shard
holds far fewer entries than its bound, as at 10⁷ keys under the harness's bound of four times the keys, that did the
whole walk in the first quarter of the 6.4 s, inside the report ticks there: after 10 minutes 0.45 M entries had rows
on the EPYC box and 0.65 M on a second EPYC box, against 2.35 M and 1.60 M on main, and the capacity's share held 2.19 M on
the second. The share is the capacity's again. An eviction now looks the evicted up in a set of their own rather than in `entries`, which does not fit in cache
at 10⁷; a build that pushed a new id out of line split the hot-key rows both ways against one that did not, so the push
is inline. `scripts/bench.sh full quota-check` on the EPYC box, main (90667ab) then this change, three rounds each,
medians: at 10⁷ keys one thread takes 2.06 M/s uniform and 3.39 Zipf against 1.78 and 2.95, Zipf at 24 threads 18.5
against 16.1, 0.80 and 0.62 of calls admitted against 0.74 and 0.54, and 2.23 M entries with rows against 2.24 M. Two
rows reported REGRESSION: `Take Zipf keys=1000, 12 threads` 25.3 → 22.6 (band ±8.7 %; the inline build gave 24.7) and
`first call, 24 threads` 5.17 → 5.70 µs, whose rounds spread 5.13–5.85 before, 5.58–5.91 after and 5.36–5.51 inline.

`scripts/bench.sh full quota` with this change against the baseline reported 17 rows improved and two REGRESSION:
`check AtLimit Hot keys=1000, 1 threads` 13.5 → 12.0 M/s, and `check entries=3000000 idle` 0.27 → 0.29 cores, which
`quota-check` on 9636d3f the same afternoon put within noise of this change (0.27 against 0.29, band ±7.4 %). `quota
check 2 1000` alternated with 9636d3f, five rounds each, three times over, holds both movements at 10³ keys:

- A check denied on one hot key costs about 8 ns more on one thread: medians of 13.5–13.7 M/s before and 12.0–12.3
  after in the three sets; at 4 to 24 threads it is the same within 2 %. It is not the new fields' place in the shard
  (with `repr(C)` keeping the old offsets the row stayed at 12.2), nor eviction inlined into the check (a `#[cold]`
  eviction left 11.9), and an admitted check on the same key is unchanged (12.5–12.6 against 12.3–12.6), so the cause
  is not found.
- Uniform at 4, 12 and 24 threads and Zipf at 4 gained 5–14 % (Uniform, 4 threads: 31.7–34.3 → 36.4–38.3), and
  `AcquireDrop Hot` at 4 threads 18 %. The `repr(C)` build lost all of it, so it comes from where the compiler put the
  shard's fields, not from the sweep.

What this establishes:

- A check costs 100 ns on one thread at 10³ keys, and 240–420 ns at 3 M entries, where it is a cache miss. At
  10 M entries it costs 290–480 ns.
- Uniform keys scale to 54 M/s over 24 threads at 10³ keys and 35 M/s at 10⁶.
- A hot key serialises on its shard's lock. It peaks at 11 M/s on one thread, falls to 1.1 M/s at 24, and falls further
  with a permit's acquire and drop.
- Zipf keys with s = 1 over 10³ keys send 13 % of calls to one key. At 24 threads they reach only 10 M/s for the same
  reason.

What it does not establish:

- A real network, or more than one node for the check.
- Which part of the 10³ single-thread cost is the lock.
- 10⁷ keys in use on one node. After 10 minutes, 2.05 M and 2.23 M of the 10 M entries had rows. The rest were
  admitted on the fallback, which is why 70 % and 53 % were admitted, and 1.04–1.16 cores were still busy. A node
  reports at most 4 096 entries a cycle (§6), 41 k a second at most. An entry in use is reported again every 30 s, and
  a row nobody reported for 120 s has its grant lowered. So a node keeps grants for at most about 41 k × 120 s ≈ 4.9 M
  rate entries in use, and fewer while its cycles run long. At 3 M it did.

**Memory.** `memory` runs each case in a process of its own and reads its resident size before and after. The cases
are node `a` alone using 200 000 keys of a rate, three nodes with `a` using them, and all three using them. From the
three it derives an entry, a replica's key with one row, and each further row. 200 000 keys, not 10⁶: at 10⁶ the
three-node cases waited minutes for every row and were stopped.

| Resident bytes | lone | one of three | all three | entry | key, one row | further row |
|---|---|---|---|---|---|---|
| before (one run) | 2 163 | 5 558 | 6 780 | 466 | 1 697 | 49 |
| after, run 1 | 789 | 1 425 | 3 578 | 471 | 318 | 202 |
| after, run 2 | 789 | 1 432 | 3 577 | 467 | 322 | 202 |

Load 0.08 before; 2.85 to 1.87 after. An entry is 470 B, not the 130 B §6 estimated. Most of it is the 9-bucket ring,
three grants on the heap, and the map's slack. A replica's key with one row costs 320 B, and each further row 200 B.
A further row cost less in the `BTreeMap`, only because the eleven-slot leaf was already paid for. The derivation
assumes the three cases differ only by what they name; allocator slack and each process's baseline blur the per-row
figure by a few tens of bytes.

**Background bytes.** `bytes` starts 50 quota nodes in 3 regions in the one process. Each node takes Zipf keys of 10⁶
under a limit of 100 a second per key, at 0, 1 000 and 10 000 takes a second, in batches every 10 ms. After 5 s it
counts, for 20 s, the wire bytes every node's sessions sent. That count includes the node's answers as a replica and
the cluster's own membership traffic.

| Per node | idle | 1 000 takes/s | 10 000 takes/s |
|---|---|---|---|
| bytes a second sent, run 1 / run 2 | 2 803 / 2 819 | 178 712 / 178 435 | 1 644 771 / 1 633 913 |
| reports a second | 0 / 0 | 240.3 / 240.0 | 59.0 / 59.8 |
| units on the fallback a second | 0 / 0 | 458 / 458 | 324 / 329 |
| refused a second | 0 / 0 | 332 / 332 | 6 926 / 6 888 |

Load averages ran from 1.98 to 10.91 in run 1 and from 10.91 to 11.25 in run 2.

- **Idle.** 2.8 kB/s is the cluster's own traffic. A node that has no entries has no replica links and sends no
  quota heartbeats.
- **Under load.** Each take costs about 165–180 B sent, counting the reports to the key's three replicas and the
  replicas' answers. That is inside §6's estimate of 45 B per replica per changed entry, since grants travel back as
  well.
- **The fallback.** Almost half of the units at 1 000 takes/s went on the fallback: a node's first use of a Zipf key
  it rarely takes, as in the simulation (§5).
- **At 10 000 takes/s** the 50 nodes shared 24 threads with 500 000 takes a second between them. Report cycles
  stretched to about 400 ms, so a node sent fewer, fuller reports.
- **Pushes** (spec/quotas.md §2.3) add to the bytes where nodes contend for a key: a replica pushes each raise a wanting
  row gets, and each lower another row waits for, at most once a report cycle per node, with at most 256 grants set
  since its last message to the node. Indicative only, with 10 s counts on a 16-thread box that other work shared: at
  1 000 takes/s, 200 kB/s a node with pushes in 2 runs of 2, 164 kB/s with pushes turned off, 150 kB/s before pushes.
  With 16 busy loops for the first 5 s of the phase, the nodes came back to 209–249 kB/s, 235–240 reports and 241–244
  refusals a second a node, in 5 runs of 5; where an answer overtaken by a later one counted as a lost report, they
  stayed at 1.81–1.88 MB/s, 63–66 reports and 279–288 refusals a second for the rest of the phase, in 7 runs of 7, and
  the build before pushes came back to 150 kB/s, 240 reports and 242–243 refusals. Why, and what else was tried:
  spec/quotas.md §6. At 10 000 takes/s the counts ran from 200 to 1 070 kB/s with and without, too scattered to compare.

What it does not establish:

- Bytes on a real network, where each node would have its own cores.
- A deployment's key distribution.


**Pull.** `pull` starts four nodes, and `a` uses 10⁶ keys of a rate. Each key has three replicas among the four, so
each node holds rows for about 709 000 keys, 22.5 s after the keys were used. Then `d` shuts down. `a`, `b` and `c`
each gain the keys `d` replicated, and each pulls the rows of every key it now serves from the other two. The harness
waits until each holds a row for every key and none is pulling.

| | run 1 | run 2 |
|---|---|---|
| from `d`'s shutdown until the pulls end | 64.1 s | 63.6 s |

A later form of the harness times two things apart: when no node is pulling any more, and when each node holds a row
for every key. Rows come from both the pulls and `a`'s reports of the keys whose grants it dropped with `d`.

| | run 1 | run 2 |
|---|---|---|
| every key held | 26.1 s | 25.3 s |
| the pulls end | 65.8 s | 64.1 s |

Load average: 2.80 to 1.98 for the first table; 7.76 to 1.96 for the second. Reports bring every key's row back in
about 26 s. The pulls take 64–66 s. A pull asks for one page (4 096 rows) per candidate per report cycle. Each candidate
holds rows for about 709 000 keys, so pacing alone accounts for about 17 s, and more as reports add rows. What takes
the rest is not established. The replica is young for the handed keys while it pulls, so the nodes admit on the key's
other replicas (§2.6). This run did not measure admission during the pull.

The harness then starts `d` again, a new process, and waits until it no longer pulls, which is what `/readyz` waits
for. First the candidates chose the rows of a pull by their own views, and one that did not show `d` yet had nothing
for it: `d` was ready with nothing pulled. Then a candidate answered only once its view showed the puller. Now a pull
carries the puller's candidates and each candidate serves the keys the puller replicates among them
(spec/quotas.md §2.6). Same box, 10⁶ keys, one run each:

| | own view | once the view shows `d` | `d`'s candidates |
|---|---|---|---|
| `d` ready after its start | 0.26 s | 21.2 s | 20.2 s |
| rows `d` holds then | 0 | 750 119 | 750 119 |

`d` replicates about three quarters of the keys, since each key has three replicas among four nodes. Load average
1.03 to 1.70, 1.22 to 1.34, and 5.43 to 6.28 (the box shared with other work) for the last. What it does not
establish: how much of the 20 s is the pull and how much is `a`'s reports, which bring rows too; nor, from one run
each under different load, whether waiting for the views cost anything here, where discovery spreads a record at once.

**Demand while in use.** Raising an entry's demand to the most it uses at once, where a take adds its cost, rather
than adding what `check` admits (spec/quotas.md §2.1), moved `check`'s one-thread take at 10³ keys from 9.30 / 9.36
to 10.97 / 10.58 M/s uniform and `AcquireDrop` from 8.42 / 8.08 to 6.93 / 9.05 M/s, two interleaved runs each at load
9–12 on the same box: at that load no change is established either way. Those takes are never refused, so they time
the path of an entry that does not ask; one that asks adds a `max` to each take.

## Telemetry overhead (`axon-obs`)

`cargo run --release -p axon-obs --example overhead` runs 9 rounds of 1 s trials after a 250 ms
warm-up, interleaves the variants round-robin and reports median and IQR. Apple M5, nothing
pinned. Counters cannot be compiled out, so "timings off" already pays for them; against the commit before
telemetry the embedded figure is unchanged within noise (9.5–10.1 → 9.4–9.9 ns/msg).

| Embedded: admission + dispatch, one thread, no-op handler | ns/msg | IQR | |
|---|---|---|---|
| timings off | 9.05 | 9.00–9.08 | |
| timings 1/128 | 9.29 | 9.25–9.35 | +2.6 % |
| timings 1/128 + message labels | 9.49 | 9.48–9.51 | +4.9 % |
| timings 1/16 + labels | 12.91 | 12.79–13.34 | +43 % |
| timings 1/1 + labels | 84.13 | 83.59–85.24 | +829 % |

A sampled message costs about 60 ns (three clock reads, two uncontended locks), so the overhead
is `60 ns / N` per message whatever the handler does: 5 % of a 9 ns no-op, 0.05 % of a 1 µs handler.

| Other | Result |
|---|---|
| Hosted actor, 1/128 + labels, with vs without the extension sampling 1000 idle actors at 1 Hz | 29.2 vs 29.5 ns/msg, IQRs overlap |
| Transport, 256 B frames, timings off / 1/128 / 1/16 | 244 / 250 / 246 ns/frame, IQR ±5 %: not measurable |

The first run of this harness measured +180 % for 1/128: every admission read the clock so that
any message could be sampled at dequeue. Admission now stamps only the sampled ordinal. The hosted
workload is bistable (throughput depends on how often the worker parks), so it is used only for
the on/off comparison, not as an absolute.

Histograms went from 32 log2 buckets of microseconds to 304 log-linear ones of nanoseconds, eight to each power of two
(a percentile at most 12.5 % high instead of up to 100 %). Recording is still an index computation and one add. Against
6f9430f on the EPYC 4484PX box, shared with other agents (load average 4 to 9), two interleaved rounds of `overhead`,
ns/msg: embedded timings 1/1 + labels, where every message records four histograms, 84.5, 77.5 → 74.9, 74.0; 1/128 +
labels 9.67, 9.79 → 8.50, 8.43; hosted 11.99, 11.56 → 12.21, 12.23; transport timings 1/16 146.8, 151.3 → 172.2,
156.3 ns/frame (timings off moved as much, 149.8, 143.7 → 172.8, 159.5). What this establishes: recording is not slower.
What it does not: embedded with timings off, which records nothing, is also 0.7 ns faster, so part of the embedded gain
is code layout, and transport is noise on that load. The cost is memory: a `Histogram` is 2432 B instead of 256, so an
actor with timings on holds about 9.7 KB of them plus 2.4 KB per message label, and a session 12 KB; with timings off
an actor holds none. On the wire a mostly empty histogram is about 600 B of JSON: the three-node playground's
`/api/overview` carries 310 of them in 288 KB, 23 KB gzipped as the console receives it (not measured before; at two
bytes per empty bucket the 32-bucket layout would have been about 170 KB smaller uncompressed).

The wider histograms cost idle nodes processor time: every node's sample copied the six `AtomicHistogram`s of each of
its links into a 2.4 KB `Histogram` each second, `scale nodes 200` with 20 links a node going from 0.49 cores idle at
fa0bd53 to 0.77. A snapshot is now a `HistogramSnapshot`, the run of buckets between the lowest and highest recorded
into, in one shared allocation (a link's stats went from 15 072 B to under 1 KB,
`a_links_stats_keep_their_histograms_sparse`). Five interleaved rounds of `scale nodes 200` on the EPYC 4484PX box,
shared with other agents under its lock, median (range), against 9636d3f:

| `scale nodes 200` | before | now |
|---|---|---|
| idle, 20 links per node | 0.773 (0.708–0.811) cores | 0.568 (0.556–0.595) cores |
| idle, keyed actors placed | 0.864 (0.826–0.904) cores | 0.652 (0.626–0.682) cores |
| resident per node, 20 links | 2.15 (2.14–2.22) MB | 2.19 (2.14–2.23) MB |
| resident at the end | 1561 (1538–1581) MB | 1073 (1011–1090) MB |
| resident per keyed actor | 1486 (1303–1650) B | 2507 (2448–2771) B |

What this establishes: a quarter less processor time idle, and 490 MB less resident for the 200 nodes. What it does not:
the fa0bd53 figures (0.49 and 0.56 cores), which are about 0.08 cores lower still; a profile puts the rest of the gap in
the sample's other copies and allocations (`Cluster::stats`, `Out::push`). Resident per keyed actor is the growth of the
process's resident memory over placing 20 000 keys, none of which holds a histogram snapshot. The process's resident memory traced
every 0.5 s, three interleaved rounds against 95cf031, grew alike from the leave before the placement to its end: 6.2–7.1
KB a key now against 6.6–7.7 KB (12.5–13.5 KB against 15.9–16.7 KB with Paxos leases). The harness's shorter window
reads more now because before, placing filled memory the allocator kept from the dense copies freed every second. With 50 nodes, a quarter of the copies, it did not move (4538 → 4557 B). Encoding the node's JSON snapshot only when
read was tried and dropped: 0.01 to 0.03 cores for 0.1 MB more a node. The gate on `scale-nodes|scale-metrics`: all ten
idle rows with links improved, by 20 to 34 %, and are recorded. Its seven REGRESSION rows are resident per keyed actor
at 200 nodes (three jobs, as above), and four that their twin jobs, running the same code, did not repeat: `nodes-50`
start and agree at 1.01 s (the bimodal agreement), resident per node with no links +28 % (`nodes-50-paxos` −7 %), and a
join seen, and one moving keys settling, 4 % slower against bands of 3 and 4 %.

Stored rows, `scripts/ctl.sh up` with `AXON_CLICKHOUSE` (the playground's six nodes with their traffic, ClickHouse
25.8 in a container, Apple M5), counted in `samples` over three minutes starting one minute after the start. The
"before" figure is the same run's stored rows plus the growth of `telemetry.samples_skipped`, which is what a batch
carrying every series would have stored.

| 6 nodes, 2046 series, 180 s | rows | rows/s |
|---|---|---|
| every series every second (before) | 366 830 | 2038 |
| a series when it changed, or 30 s after it was last stored (now) | 151 001 | 839 |

59 % fewer rows. Most of what is left is `connection.*` (102 k of the 151 k): byte and frame counters on links that
carry traffic every second. A series that stands still costs one row in 30 s (`listener.*` here, 0.03 rows a second
each). What this does not establish: an idle or a larger cluster (the share saved depends on how many series move,
not on the node count), or ClickHouse bytes on disk, which were not measured.

`MemoryMetrics` in memory: 1160 batches the collector wrote in the first two minutes of the playground (nine nodes,
EPYC 4484PX box), captured and written again into a store under a counting allocator. A batch averages 147 samples,
1.6 labels each, and 35 histograms with 21 non-zero buckets over a run of 35. Kept as received, a batch held 176.8 KB:
35 × 2.4 KB of histogram and a 544 B B-tree node per labelled sample. Kept with labels in a slice and a histogram as
its run of buckets it asks the allocator for 42.7 KB. Writing costs the same: best of 7 per batch, three rounds
alternating on one core, 35.9, 37.5, 35.1 µs before and 36.5, 36.2, 36.1 µs after. That capture and its harness were
one-off; the check that stays is `cargo run --release -p axon-obs --example scale metrics 256`, which fills a
256 MiB store with batches of that average shape, then as much again three times, and compares the growth of the
process's resident memory with the store's count of its bytes. Counting only the bytes each allocation asks for, RSS
grew 1.525 times the count (41.2 KB a batch counted); counting each allocation as glibc lays it out (a word of header,
16 B steps, 32 B at least) it grows 1.005 times the count (62.6 KB a batch), steady through the evictions, and a
write takes 36 µs. What this does not establish: another allocator, a longer run, whose batches carry fewer unchanged
series, or the cost of a history query, which was not measured.

## Streams (`axon-stream`)

`cargo run --release -p axon-stream --example fanout` runs one hub on one thread with batches of
16 × 64 B records, publishing each batch and then polling it on every subscriber. Median of 7, Apple M5.
Payloads are shared, so fanout cost is queue bookkeeping, not copying.

| Live fanout | ns per record per subscriber | deliveries/s |
|---|---|---|
| 1 subscriber | 10.1 | 99 M |
| 10 | 2.7 | 365 M |
| 100 | 2.1 | 479 M |
| 1 000 | 2.5 | 406 M |
| 10 000 | 3.1 | 321 M |

| Replay-to-live join | Result |
|---|---|
| 1 M records from `MemoryStore`, 1024-record pages, one subscription | 13.4 ns/record, 74 M records/s |

Namespace limits ([streams § Namespace limits](streams.md#namespace-limits)) put a lookup on each subscribe and
publish, in a map that is empty on a node without them. The same harness publishes 16 × 64 B batches through an
origin's `RelayCore`, which keeps the ring, with no subscribers and no limits: pinned to one core of the EPYC 4484PX
box (load average 1.2 to 1.3 from other work), three rounds alternating the build before the limits with the one after:

| Build | origin publish | fanout, 1 subscriber | fanout, 100 subscribers |
|---|---|---|---|
| before | 17.0–17.1 ns/record | 10.4–11.0 ns | 1.8–1.9 ns |
| after | 17.0 ns/record | 10.6–11.7 ns | 1.9–2.1 ns |

What this establishes: a node without limits publishes at the same cost; the fanout rows, whose publish and poll the change
does not touch, vary by up to 10 % between rounds of one build. It does not measure a limited namespace, whose publish also takes
from `Quotas` (under Quotas) or looks at its ring budget.

A relay copies each received batch out of the push in one block, so that its ring does not keep the transport's read
buffer (under Transport). The same harness decodes a push of one 16-record batch, median of 7, pinned to one core of
the EPYC 4484PX box, three rounds alternating the build before (records sliced out of the push) with the one after:

| Push decode | sliced (before) | copied per batch |
|---|---|---|
| 64 B records | 15.9 ns/record | 18.2–18.4 ns/record |
| 4 KiB records | 17.4–18.2 ns/record | 78.4–78.8 ns/record |

What this establishes: the copy costs a few ns per small record and about 60 ns per 4 KiB one, some 15 µs per MiB
pushed, against a round trip per push. It is not a relay's end-to-end throughput, which no harness here measures.

Compacted streams put a check of the stream's state on every publish and moved the record block of a push into a helper
shared with snapshots. Same harness, pinned to one core of the EPYC 4484PX box (load 1.2 to 1.9), two rounds alternating
main (1890db2) with the change, ns per record:

| Row | before | after |
|---|---|---|
| fanout, 1 subscriber | 11.5–11.8 | 11.8–12.0 |
| fanout, 100 subscribers | 2.1 | 1.9 |
| fanout, 10 000 subscribers | 2.4–2.5 | 2.0 |
| replay join | 21.8 | 21.4 |
| push decode, 64 B records | 18.1–18.3 | 13.1–13.2 |
| push decode, 4 KiB records | 77.7–77.8 | 73.6–74.9 |

What this establishes: a stream without compaction publishes, fans out and decodes no slower. The faster rows are
not claimed as a gain of the change: nothing in it removes work there, and code layout moves them as much.

The harness's `publish` rows put the same 16-record batches of 64-byte keyed records, cycling over 1024 keys, into a
stream with no subscriber, with and without compaction. Same box and core, two rounds, ns per record:

| Row | ns per record |
|---|---|
| publish, replay required | 8.7–9.2 |
| publish, compacted over 1024 keys | 95.6–99.8 |

What this establishes: keeping the latest record per key costs about 90 ns per record at the origin (each batch builds a
map of its changes, then looks up and inserts each key), so a compacted origin admits some 10 M records/s on one core
instead of 110 M. Only the origin pays it; relays pass records on as for any stream. It is one key count and one record
size, not measured at others.

The review fixes that followed hold a branch's state aside until the branch stops replaying, which put a check on every
poll. Unguarded, it cost 0.1 to 0.2 ns per record per subscriber from 10 to 10 000 subscribers; checked only when a
state is held, the fanout rows match the build before them (two rounds alternating, ns per record per subscriber):

| Row | before | after |
|---|---|---|
| fanout, 1 subscriber | 10.5–10.9 | 10.4 |
| fanout, 100 subscribers | 1.7 | 1.6 |
| fanout, 10 000 subscribers | 1.8 | 1.8–1.9 |

Joining namespace limits with compacted streams had the hub look a publish's stream up three times: to check it, for
its compacted state, and to deliver the batch. It now looks it up once and hands the stream on, and an origin charges
the ring for a state only when the stream has one. `fanout` pinned to core 1 of the EPYC 4484PX box, ten rounds
alternating main (9636d3f), the change (660684a) and the build before the join (b56a4d1, which has no origin row),
median (range), ns per record:

| Row | 9636d3f | 660684a | b56a4d1 |
|---|---|---|---|
| fanout, 1 subscriber | 12.0 (11.7–12.6) | 11.5 (11.4–12.2) | 11.7 (11.6–12.1) |
| fanout, 10 subscribers | 2.6 (2.6–2.8) | 2.5 (2.5–2.6) | 2.7 (2.5–2.8) |
| fanout, 100 subscribers | 1.8 (1.8–1.9) | 1.8 (1.8–1.9) | 1.9 (1.8–2.0) |
| fanout, 1 000 subscribers | 1.9 (1.8–1.9) | 1.8 (1.8–1.9) | 2.0 (1.9–2.0) |
| fanout, 10 000 subscribers | 2.1 | 2.0 (2.0–2.1) | 2.1 (2.0–2.1) |
| publish, replay required | 10.4 (10.3–10.8) | 10.3 (10.2–10.5) | 10.2 (10.1–10.5) |
| publish, compacted over 1024 keys | 110.8 (108.6–113.4) | 112.9 (107.5–114.2) | 110.4 (107.4–111.7) |
| origin publish | 19.3 (19.2–20.3) | 18.8 (18.7–19.4) | |
| replay join | 21.8 (21.7–22.1) | 21.9 (21.8–22.3) | 21.8 (21.7–21.9) |
| push decode, 64 B records | 13.9 (13.7–14.4) | 14.4 (14.1–14.8) | 14.0 (13.8–14.5) |
| push decode, 4 KiB records | 75.2 (74.9–75.9) | 76.1 (75.7–77.0) | 74.9 (74.8–75.5) |

Instructions per publish (`perf stat -e instructions:u` over each row alone, 875 000 publishes, two runs each, equal
to 0.001 %):

| Row | b56a4d1 | 9636d3f | 660684a |
|---|---|---|---|
| publish, replay required | 1 669 | 1 750 | 1 586 |
| publish, compacted over 1024 keys | 29 635 | 30 083 | 29 921 |
| origin publish | | 3 282 | 3 021 |

What this establishes: a publish with replay required takes 165 fewer instructions, 83 fewer than before the join, and
an origin publish 261 fewer; the origin row is 0.5 ns per record faster, and the gate calls fanout to 1 subscriber
improved (11.9 → 11.4, recorded). Replay required is 0.1 ns faster, inside its range, and 0.1 ns slower than b56a4d1
with fewer instructions: most of that row is the harness's own clones and drops of the batch, so the lookups were not
what held it above 10.1.

An earlier version of the change took the compacted state out from behind its trait and made `apply` generic over its
callback. That inlined `apply` into the caller's crate, where `BTreeMap::insert` was no longer inlined: 109.3 against
124.8 ns in one run on the build box, with 0.7 % more instructions. The trait was back; a later cleanup (0854165) removed
it again and made `apply` generic again, and the gate read the compacted row 111.8 (110–114) → 121.6 (120–122.2) in
four rounds interleaved with 95cf031. `apply` now takes a `&mut dyn FnMut` and is `#[inline(never)]` on the one type
left: six runs alternating with main (5811003) read 98.8–101.3 against 109.0–110.4 ns, with replay required 9.0–9.4
against 9.3–9.5 and the origin row 16.6–17.2 against 16.3–18.4, and the gate records it at 112.6 ns (recorded). The
compacted row still reads 2 %
slower with fewer instructions, and the push decode rows, whose code the change does not touch, 1 to 4 %: `apply`,
`decode_push` and `get_records` are the same length in both builds, each 32 bytes off where it sat in a 64-byte line.
Built with every function aligned to 64 bytes (`-C llvm-args=-align-all-functions=6`), ten rounds more, the compacted
row is 113.0 (110.4–113.3) before and 113.5 (111.7–117.2) after; the decode rows stay 3 % and 2 % apart, which alignment
does not explain and the change's code cannot, as it runs none of theirs. The gate calls every row within noise but the
one improved. None of this measures a limited namespace, whose origin publish also reads its ring budget, or a stream on
disk, whose batch is looked up again when its write lands.

Between nodes each child has a window of pushes in flight, up to `max_push_window` (64 MiB) a round trip, and one
stream up to half its branch (16 MiB at the defaults, 4 MiB when these numbers were taken). In the World at 100 ms
each way (`crates/axon-stream/tests/world_relay.rs`, records of 4 KiB, no bandwidth limit), a child reads 30 MiB of
32 streams 0.82 s after its first record, and 20 MiB of one stream in 1.02 s, with 5 pushes in flight; one push per
round trip took 6.32 s and 4.08 s. That is the World, not a WAN: the kernel's send buffer and the link bound a real one.

Over loopback the window first cost processor time: each publish woke a push of its own while others were in flight,
where one push per round trip had taken in everything published during it. A push under half of `max_push_bytes` that
took all there was now holds the next until an answer comes (Nagle's rule, [push-window §2.4](../../../spec/push-window.md#24-the-window)).
`scale streams 50000` on the Ryzen, three interleaved rounds each, cores 0–15, others' jobs on the box at a load of
2.5 to 6 in every run:

| `scale streams 50000` | one push per round trip (4585e21d) | a window (e56b37cc) | a window, Nagle's rule (f62af308) |
|---|---|---|---|
| 200 k records/s published | 4.15–4.20 cores | 4.71–4.83 cores | 3.92–4.24 cores |

With the rule, publishing costs what it did with one push in flight, within the spread. That shows a trickle no longer
costs a call per publish over loopback. What a window gains across round trips is in
[Streams across a WAN](#streams-across-a-wan).

### Streams across a WAN

`scripts/relay-wan.sh` on build-1 (harness `crates/axon-stream/examples/relay_wan.rs`, [push-window
§12](../../../spec/push-window.md#12-bench-d2)). An origin and a child, each a process in a network namespace, are
joined by a bridge. `netem` on each namespace's veth adds half the round trip and holds the link to 1 Gbit/s, so no
cell can go above 125 MB/s (119.6 MB/s of TCP payload). The origin keeps a backlog in each of its streams, 1 or 64: it
publishes 4 KiB records in batches of 16 while its branches to the child hold under three quarters of their bound. The
child follows each stream `Live`. A cell measures 10 s after 3 s of warm-up. Meanwhile each node sends a 256 B request
to the other every 50 ms and renews the Paxos lease of a key (both nodes are acceptors). `tcp` is one TCP connection
between the same namespaces, with 1 MiB writes. "Defaults" are what a new namespace gets: `tcp_wmem` max 4 MiB,
`tcp_rmem` max 6 MiB, cubic and no qdisc on `eth0`. "§8" are [wan.md § 8](../../../spec/wan.md#8-host-tcp-settings)'s
settings: max 64 MiB and 128 MiB, BBR, `fq` and `tcp_slow_start_after_idle` 0. Both are set inside the namespaces
only.

"Today" is main at 269b67cf, one push per child in flight, with the harness taken back to that API. "After" is the
push window and lanes (lane/wan-spec at da78a8e6) with the harness. Both ran three times on 2026-10-26, back to back:
today 22:27–22:53, after 23:04–23:33. The 1-minute load was 0–4 during the cells, except the first after cells at 0 ms
(3.5–10.6, others' jobs). Linux 7.0, governor `performance`. The table gives the child's MB/s, as the median of three
runs:

| RTT | Settings | `tcp` | Today, 1 stream | Today, 64 | After, 1 stream | After, 64 |
|---|---|---|---|---|---|---|
| 0 ms | defaults | 119.6 | 116.7 | 115.8 | 119.3 | 119.3 |
| 0 ms | §8 | 119.5 | 116.7 | 115.7 | 119.3 | 119.3 |
| 92 ms | defaults | 31.3 | 9.63 | 9.63 | 33.6 | 30.7 |
| 92 ms | §8 | 114.5 | 7.57 | 7.57 | 40.1 (39.6–40.9) | 113.3 |
| 190 ms | defaults | 15.3 | 4.72 | 4.72 | 16.4 | 15.2 |
| 190 ms | §8 | 112.7 | 3.54 | 3.54 | 19.3 (18.6–19.6) | 108.9 |
| 219 ms | defaults | 13.2 | 4.13 | 4.13 | 15.6 (13.5–15.6) | 13.0 |
| 219 ms | §8 | 114.3 | 3.15 | 3.15 (3.05–3.15) | **17.0 (16.9–17.3)** | **111.7 (110.4–112.2)** |
| 219 ms | §8, the namespace on `Bulk` | | | | 17.0 (16.7–17.3) | 111.8 (111.3–112.2) |

The merge gate is the two bold cells, and both pass. 64 streams moved 111.7 MB/s (106.5 MiB/s) against a target of
60 MiB/s. 1 stream moved 17.0 MB/s against 13 MB/s. In every cell resets, gaps, resends, capped pushes, failed
requests and failed renewals were 0. No node was isolated, and the key stayed where it was. Each node used at most
0.04 cores.

What limits each cell:

- **Today: one push in flight.** One push of at most 960 KiB goes per round trip, for all of a child's streams, so 64
  streams move what 1 does: 4.13 MB/s at 219 ms. §8's settings make it slower (3.15). Each push leaves after an idle
  round trip, and `fq` paces it at the rate BBR has measured, which is the pushes' own. The origin's request to the
  child then waits behind it too: p50 293 ms at 219 ms with §8, 219 ms with the defaults.
- **After, 1 stream: half its branch.** A stream has at most half its 8 MiB branch in flight (push-window §2.5).
  The origin held 3.9 MiB in flight against a window of 7.2 MiB. 4 MiB per 219 ms is 19.2 MB/s, and the cell read 17.0
  (89 %; at 92 ms 40.1 of 45.6, 88 %). The connection was not the limit: `tcp` read 114 there. The default is 32 MiB
  since; this cell then read 68.8 MB/s, at a higher load (push-window §2.5, **Default**).
- **After, 64 streams with §8: the link.** 111.7 of `tcp`'s 114.3 at 219 ms. The window was 55 MiB, with 52 MiB in
  flight. The round trip the window measures went from 219 to 279 ms: the window is 2 × rate × round trip, so at the
  link's rate up to about a round trip of pushes waits in `netem`'s queue. That queue is deep: its limit is 10⁶ packets.
- **After with the defaults: the connection.** A new namespace's 6 MiB receive buffer holds one connection to about
  13 MB/s at 219 ms, and the 64-stream cells read what `tcp` reads at every round trip. This is not the 4 MiB send
  buffer that push-window §12 expected (19 MB/s). The 1-stream cell read above `tcp` at 190 and 219 ms (16.4 against
  15.3, 15.6 against 13.2). This is not explained here.

Requests and renewals during the cells. The origin's request goes the same way as the pushes; the child's goes against
them. Medians of three runs, in ms:

| Cell (after, §8) | Origin → child p50 / p99 | Child → origin p99 | Renewal p99, origin / child |
|---|---|---|---|
| 0 ms, 1 stream (today) | 11.9 / 16.1 (4.1 / 8.5) | 4.4 (1.1) | 2.2 / 1.7 (5.4 / 1.1) |
| 219 ms, 1 stream on `Live` | 235.6 / 301.4 | 219.4 | 438.1 / 219.1 |
| 219 ms, 1 stream on `Bulk` | 219.0 / 219.5 | 219.4 | 438.1 / 219.1 |
| 219 ms, 64 streams on `Live` | 437.6 / 524.5 | 411.6 | 371.8 / 372.2 |
| 219 ms, 64 streams on `Bulk` | 370.4 / 372.2 | 382.8 | 371.4 / 371.2 |

With 1 stream on `Bulk`, a request on `Live` crosses in one round trip, as each lane has its own connection. On `Live`
the request waits behind the pushes ahead of it in that connection, which `fq` sends at BBR's paced rate: 17 ms
more than a round trip at the p50, 82 ms at the p99. With 64 streams
the link itself is full. A request then waits in the queue described above, on any lane, about 150 ms at 219 ms. `Bulk`
takes away only the wait in its own connection (p99 524 → 372 ms). The window's floor of 2 MiB per child puts up to
17 ms of queue at a 1 Gbit/s link at 0 ms: the request's p50 is 11.9 ms after, 4.1 ms today. A renewal takes one round
trip or two, as it did today: 438 ms at the origin in every 219 ms cell with 1 stream.

What this establishes: on one box, with `netem`, 1 Gbit/s and no loss, the window meets both targets at 219 ms with
§8's settings. Over the defaults the connection's receive buffer, not axon, sets the rate. A child follows 1 stream at
about 88 % of half its branch per round trip. What it does not establish: a real WAN, with loss, reordering and a
buffer of its own size (the loss cells were not run). It does not cover more than one child, a link above 1 Gbit/s
(where 64 MiB per 219 ms, 306 MB/s, would bind 64 streams), or record sizes other than 4 KiB. The queueing delay above
is that of a deep FIFO, and a shallower link drops instead. `scripts/relay-wan.sh table <results>` prints a run's
medians and ranges. These runs are in `target/relay-wan/` of the tree that ran them: no CSV baseline holds them, as
`bench.sh` does not run them (they need root and namespaces).

### History on disk

Keeping an origin's history on disk ([streams § History on disk](streams.md#history-on-disk)) split the hub's publish
into a check and a commit, which every publish now goes through. `fanout`, pinned to one core of the EPYC 4484PX box
(load 1.0 to 2.0), two rounds alternating main (f7d5073) with the change, ns per record:

| Row | before | after |
|---|---|---|
| fanout, 1 subscriber | 11.7–11.8 | 11.9–12.0 |
| fanout, 100 subscribers | 1.8–1.9 | 1.9 |
| fanout, 10 000 subscribers | 2.1–2.2 | 2.1 |
| publish, replay required | 10.2 | 10.3–10.4 |
| publish, compacted over 1024 keys | 114.7–115.9 | 112.4–114.9 |
| replay join | 22.0 | 22.0 |

`cargo run --release -p axon-stream --all-features --example disk` publishes 2 M records of 64 B, in batches of 16, to
one origin `StreamNode` as fast as it admits them, until the last is durable (median of 5), then publishes 200 batches
one at a time and waits for each to be durable. Once with the stream in memory, once kept on disk with the shipped
`DiskConfig` (then a write every `sync_every`, 5 ms) through `OsFiles` on the box's NVMe ext4, on cores 4 to 7, three
runs (load 1.5):

| Stream | records/s, published and durable | median wait until durable |
|---|---|---|
| in memory | 53.8–54.7 M | 0.3 µs |
| on disk | 4.65–4.92 M | 6.21 ms |

What this establishes: a stream that is not kept on disk publishes and fans out as before; the rows moved by less than
their spread. On disk, a record waits for the next write and its fsync, `sync_every` and about 1.2 ms here, and one origin
writes some 4.8 M small records/s, about 330 MB/s of frames, a tenth of its rate in memory: each publish frames its
batch and computes its CRC under the node's lock, and what bounds it was not profiled. It says nothing of another disk, of larger records, of many streams on one node, or of a disk
shared with other writers.

**A write as soon as the last lands, and its files synced together.** The writer then slept `sync_every` between
writes, so a record waited for the next tick and then the write; and `OsFiles` synced the files of a write one after
another, so a write to 256 streams waited for 256 syncs in a row. Now a publish wakes the writer, each write starts as
soon as the last lands and takes everything published meanwhile (`sync_every` is gone), and `OsFiles` syncs up to 32
files of a write at once. The same harness, now with a third row: 256 streams on disk, `streams256`, its throughput
publishing to each stream in turn and its wait from the first of 256 batches, one to each stream, until all are
durable (median of 50). On build-1 (Ryzen 9900X, two Samsung PM9A1 without power-loss protection in an md RAID1 under
ext4, where one fdatasync takes about 4.2 ms), cores 4 to 7, two runs each, load 1.3 to 3.4 from other work on the box:
indicative only. The gate's `disk` job on the merged tree (265d29a3, quiet box, 5 rounds) agrees: 5.88 M records/s and
4.26 ms on disk, 3.61 M records/s and 53.8 ms for streams256; those are this CPU's baseline rows now.

| Row | before | write when the last lands | and files synced together |
|---|---|---|---|
| disk, records/s published and durable | 5.52–5.54 M | 5.98 M | 5.67–5.81 M |
| disk, median wait until durable | 10.30–10.31 ms | 4.20 ms | 4.21 ms |
| streams256, records/s published and durable | 0.45 M | 0.37 M | 3.40–3.58 M |
| streams256, median wait until durable | 353 ms | 347 ms | 52–54 ms |

What this establishes: on this disk a record published to an idle origin now waits for one fdatasync and nothing else
(4.2 ms; a growing file's fdatasync measured alone here took 4.2–4.3 ms), against 10.3 ms with the tick before it; the
writer change is all of that. With 256 streams each write syncs 256 files, and syncing them 32 at a time cuts the write
from 347 ms to about 53 ms, 6.6 times, and lifts throughput 7.8 times. The memory row did not move (33.8–34.0 M). It
does not establish the numbers on the bench box, whose drives sync in tens of µs and where starting a write's threads
costs a larger share, nor anything about throughput of one stream, which moved within what this box's load explains.

## Against other frameworks

Two outside suites on the AMD Ryzen 7 3800X described under Scale, single runs of 5 samples, medians. "Before" is
commit 2183e01. Run-to-run spread is about 10%, and up to 2× for `tell` and `ask`, which depend on whether the worker
is awake when the message lands. fastact was the fastest of the other six (actix, kameo, ractor, xtra, coerce) on
nearly every row, so it is the one shown.

| Savina suite, ms, lower is better | 1 worker: before | now | fastact | 8 workers: before | now | fastact |
|---|---|---|---|---|---|---|
| ping-pong, 40 k round trips | 28.8 | 15.1 | 8.9 | 277 | 25.8 | 21.2 |
| counting, 1 M messages to one actor | 63.8 | 59.6 | 44.9 | 81.2 | 40.8 | 55.2 |
| thread-ring, 100 actors, 100 k hops | 25.1 | 13.1 | 10.1 | 749 | 69.9 | 36.0 |
| fork-join-throughput, 60 actors × 10 k | 40.6 | 40.0 | 19.7 | 710 | 136 | 97.6 |
| fork-join-create, 40 k actors, one message each | 639 | 329 | 39.1 | 602 | 227 | 92.6 |
| chameneos, 100 actors, 200 k meetings | 87.0 | 65.6 | 86.1 | 211 | 200 | 258 |
| big, 120 actors × 20 k | 542 | 413 | 401 | 316 | 282 | 193 |

| Throughput suite, ns per message, lower is better | before | now | fastact |
|---|---|---|---|
| tell: 1 producer → 1 actor | 86 | 96–139 | 201 |
| contended: 4 producers → 1 actor | 181 | 174 | 315 |
| fanout: 1 producer → 64 actors, 4 workers | 491 | 317 | 315 |
| parallel: 8 producers → 8 actors, 8 workers | 73 | 66 | 112 |
| scale10k: 4 producers → 10 k actors | 25 | 21 | 27 |
| scale100k: 4 producers → 100 k actors | 73 | 35–47 | 36 |
| ask round trip | 15 400 | 1 200–4 400 | 17 000 |
| spawn 50 k and send one message to each, per actor | 18 000 | 4 000 | 2 300 |

What changed: an idle worker spins briefly before it sleeps, so a sender rarely pays for a thread wake; busy time is
counted from sleeps, so a working worker never reads the clock; the first message reserves one block and a turn swaps
the queue out where it used to copy it; a turn takes the jobs lock and reads the clock only when there are jobs or
timers; an exit runs a hook where it used to wake a tokio task per actor; `start()` returns at once and messages
queue behind `on_start`, so a spawn no longer makes a round trip to the worker that starts the actor (4 500 → 4 000 ns;
chameneos, which starts its actors before the run is timed, did not move).

What is still behind, and why. One hop between two workers costs about 0.9 µs here, all of it the dozen cache lines
the mailbox, the run queue and the cell move between cores; frameworks on tokio run a woken task on the waking thread
and never cross. One hop on one worker is about 130 ns, of which some twenty locked instructions (two mutexes and the
capacity counters per side) are most. Both want a lock-free mailbox and run queue. A spawn is the cell allocation, the
registry insert and one wake of a worker; the frameworks ahead skip the wake. The ask rows here predate
[the lone ask's wait](#asks-from-outside-the-workers), which takes the caller's wake out of a quick round trip.

## Scheduler (`axon`)

Before is 2078975, where each actor ran on one worker for life. After, an actor woken by a handler runs next on the
handler's worker when nothing waits there, a worker with nothing to do takes half of a stuck or deep queue, and the
mailbox keeps its capacity counters under the lock it already takes. AMD EPYC 4484PX (12 cores in two 6-core
complexes, 24 threads, Linux 6.8, performance governor), every process pinned to cores 0–11 with `taskset` so that
no two threads share a core. Before and after were built from the same tree and run interleaved, three rounds of 7
samples; the table gives the median of the three medians. fastact ran once.

| Savina suite, ms | 1 worker: before | after | fastact | 8 workers: before | after | fastact |
|---|---|---|---|---|---|---|
| ping-pong | 7.3 | 6.3 | 5.0 | 19.4 | 7.0 | 10.2 |
| counting | 36.5 | 34.0 | 25.5 | 28.0 | 34.4 | 37.9 |
| thread-ring | 7.8 | 7.2 | 6.2 | 69.2 | 7.3 | 14.2 |
| fork-join-throughput | 22.7 | 23.5 | 10.6 | 99.9 | 115.5 | 16.3 |
| fork-join-create | 165 | 172 | 32.2 | 169 | 168 | 59.3 |
| chameneos | 39.4 | 41.0 | 56.0 | 174 | 139 | 78.9 |
| big | 256 | 273 | 248 | 396 | 174 | 153 |

| Throughput suite, ns per message | before | after | fastact |
|---|---|---|---|
| tell: 1 producer → 1 actor | 40 | 33 | 104 |
| contended: 4 producers → 1 actor | 45 | 46 | 143 |
| fanout: 1 producer → 64 actors, 4 workers | 88 | 100 | 70 |
| parallel: 8 producers → 8 actors, 8 workers | 96 | 107 | 91 |
| scale10k / scale100k | 17.5 / 26 | 17.2 / 26 | 8.4 / 34 |
| ask round trip | 2 540 | 1 810 | 6 310 |
| spawn, per actor | 3 070 | 3 230 | 1 160 |

`cargo run --release -p axon --example perf -- <mode>`, same pinning, median of three interleaved runs: embedded 14.8
→ 12.9 ns/msg, hosted 22.0 → 22.5, contended 38.7 → 34.9, fanout 71.6 → 79.3, ask 2 780 → 2 470.

What this establishes: a message between actors on a pool no longer crosses cores when it need not, which is most of
ping-pong, thread-ring and big at 8 workers (2.3–9.5×), and an actor behind a handler that blocks is taken by another
worker (`a_handler_that_blocks_does_not_hold_up_the_actors_that_share_its_worker`, which waits out the whole block
before). What it costs: an actor woken by a long handler waits for that handler to end rather than running beside it
on its own worker, the same trade tokio's LIFO slot makes, so an 8-worker pipeline fed from one long handler
(counting) runs at its 1-worker speed; and senders outside the pool are 10–15 % slower (fanout, parallel,
fork-join-throughput), which profiles put in the wake and the mailbox lock, not in one place. So that a sleeping
worker notices a blocked handler, a worker that goes from idle to busy with no sleeper watching wakes one: a fence per
such change, which nine interleaved rounds of tell, contended and ask put within noise. What it does not establish:
`parallel` and `contended` put more threads than cores on the box and move by ±15 % between rounds of the same build,
so their rows are within noise; on one core complex (`taskset -c 0-5`) fanout is 62 → 67. Nothing here ran on the 3800X of the rows above.

A worker that is spinning now also takes from the queue of one that is asleep, since waking that one takes longer.
`perf start` spawns 40 k actors from outside onto 8 workers and awaits each start. Each worker's turn comes round
every eighth spawn; once that is long enough for it to fall asleep, a start waits about 100 µs for the wake, which
keeps the next worker asleep too. Same machine and pinning, twelve interleaved runs against 3c0c745:

| `perf start`, ns per actor | before | now |
|---|---|---|
| Runs at the usual speed | 8 of 12, 2 519–2 734 | 12 of 12, 2 476–2 850 |
| Runs caught asleep | 5 317, 14 015, 21 134, 21 908 | none |

Through `ActorSystem` and `ActorHandle::ready` on tokio every run was caught (a probe not kept here: 40 k in 926–933
ms before, 165–226 now), and a start still waits about 100 µs when every worker sleeps and none is spinning to take
it. On 1 worker, and in `perf` `hosted`, `contended` and `ask`, which run on one, nothing can be taken, and nothing
moved beyond their usual spread. `fanout` (4 workers) is 80–84 → 80–82 ns/msg, and the Savina suite, whose axon
adapter does not wait for `on_start`, is unchanged on 1 and 8 workers (fork-join-create 138–163 → 141–158 ms).
`a_worker_that_is_looking_takes_what_lands_on_a_sleeping_one` fails before.

The threadless runtime (`RuntimeConfig::threadless`, for the whole-cluster simulation) adds a branch where an actor is
placed, a waker slot the queue checks only when it already wakes a worker, and a third arm to the clock's source.
Against 0e65f1a, same machine and pinning, load average 2–3, three interleaved rounds of `perf`, median (range) in
ns/msg: embedded 12.4 (12.4–13.3) → 13.2 (13.0–13.3), hosted 18.7 (17.1–32.3) → 29.9 (17.8–33.4), contended 32.2
(30.8–33.7) → 33.2 (32.5–34.7), fanout 84.0 (82.7–88.6) → 82.9 (76.0–102.3), ask 2 680 (2 628–2 740) → 2 676
(2 633–2 695), start 2 133 (2 046–2 150) → 2 193 (2 108–2 293). Two interleaved rounds of `overhead`: embedded timings
off 7.56, 7.56 → 7.73, 7.83; hosted 11.70, 11.76 → 11.66, 12.09; transport 142.1, 140.9 → 143.8, 143.0 ns/frame.

What this establishes: the ask round trip, contended, fanout, hosted and transport are unmoved; hosted lands at 17–19
or 30–33 in either build, depending on whether the worker parks, so its median says which mode two of three runs hit
and nothing more. What it does not establish: embedded is 0.2–0.8 ns slower in every run of both harnesses (3–6 %),
and it stays on an idle box (load 0.1–1.6, three interleaved rounds after the review fixes: 7.55, 7.60, 8.61 → 7.72,
7.77, 7.73), about 0.17 ns or 2 %. With timings off that path reads no clock, so it is not the clock's arm doing work;
likely code layout, not chased further, and accepted for S2. `start` is within its spread.
A threadless system's own speed is not measured here: it is for tests on paused time, not for production.

Trace and span ids, restart jitter and placement now draw from `axon::trace::random_u64`
([spec/simulation.md](../../../spec/simulation.md#how-each-nondeterminism-source-is-controlled)): the same per-thread xorshift the trace ids had, which a simulation
seeds per node. Against 8231492 on the EPYC 4484PX box, which other work shared (load average 3 to 10), three
interleaved rounds, median (range) in ns/msg: `perf` embedded 12.8 (12.7–12.9) → 12.8 (12.7–13.3), hosted 16.8
(16.6–16.8) → 16.7 (16.6–16.7), contended 37.5 (36.5–82.3) → 29.9 (29.8–52.9), fanout 95.4 (60.5–204.2) → 150.4
(139.4–224.7), ask 3 555 (3 239–3 555) → 2 856 (2 850–2 906), start 1 815 (1 691–1 840) → 1 850 (1 823–1 913); `overhead`
(5 rounds of 1 s) embedded timings off 8.08, 8.90, 8.71 → 8.10, 8.03, 8.95, hosted 12.50, 12.37, 12.29 → 12.51, 12.56,
12.53, transport 151.9, 150.8, 149.9 → 150.7, 149.6, 147.5 ns/frame. What this establishes: embedded, hosted and
transport are unmoved. What it does not: neither harness samples traces or restarts an actor, so no path they time
draws from the generator, and contended, fanout and ask moved both ways on a loaded box; they are noise here, not a
change.

A stash (`cx.stash`, `cx.unstash_all`) adds, on the path of an actor that never stashes, one relaxed load of a flag
beside `paused` per message a batch hands out, and a check of an empty `Option` a few times a turn; the driver's
own turn count went (the mailbox's copy of it serves), so the driver stays within its 216 bytes. Against 58bde5e on the EPYC
4484PX box, which another lane's `check.sh` shared (load average 2.5 to 13), `taskset -c 0-11`, five interleaved
rounds of `perf`, median (range) in ns/msg: embedded 13.2 (13.0–13.6) → 13.4 (13.2–13.7), hosted 16.7 (16.5–27.9)
→ 25.7 (22.9–50.9), contended 33.9 (30.3–45.3) → 37.1 (31.3–65.3), fanout 79.4 (63.8–171.6) → 80.7 (57.9–175.9),
ask 2 857 (2 767–2 968) → 2 964 (2 805–3 003), start 2 260 (2 128–2 666) → 2 533 (2 101–2 624). Hosted again, twelve
interleaved rounds at load 12: 25.4 (17.0–58.0) → 24.7 (16.4–57.4). Four rounds of `overhead`, embedded timings
off: 8.49, 7.69, 8.91, 7.82 → 8.88, 8.94, 8.09, 7.83; 1/128 + labels 8.67, 8.51, 9.29, 8.46 → 8.87, 9.53, 8.84, 8.93;
transport 158.3, 219.1 → 166.1, 148.0 ns/frame. What this establishes: nothing moved beyond the spread of either
build; a build without the per-message load landed in the same spread (`overhead` embedded 7.72, 8.97). What it does
not: the box was loaded throughout, so a change under about 5 % would not show, and the first hosted rounds, where
before hit its fast mode three times in five and after never did, did not repeat in the twelve that followed. A
replay itself is not timed: it hands out one stashed message per step of the turn's budget, as a batch would.

A mailbox without a lock per message was built and measured, and lost. Senders took room with one compare-and-swap on a
word packing slots, bytes and a waiters bit, and a slot with one on the tail of a chain of reused blocks; the owner read
written slots in order and allocated nothing once warm (loom models, Miri and the whole suite passed; commit 2cd78a3,
not merged). Against c1b7541 on the EPYC 4484PX box (load average 0.1 to 1.6), pinned as the rows above, five
interleaved rounds, median in ns/msg unless named:

| Row | lock | no lock |
|---|---|---|
| `perf` hosted / contended / embedded | 16.6 / 29.5 / 12.4 | 31.6 / 70.6 / 14.9 |
| `perf` hop cross-worker / same-worker | 220 / 71.1 | 232 / 82.1 |
| `perf` fanout / ask / start | 71.8 / 2 753 / 2 276 | 65.6 / 3 128 / 2 128 |
| `perf` topic publish / route round-robin | 52.4 / 86.4 | 46.0 / 94.5 |
| `overhead` hosted 1/128 + labels / embedded timings off | 11.3 / 7.67 | 30.3 / 10.3 |
| `pipeline` w64 / w8 / w4pin, req/s | 5.98 M / 869 k / 8.55 M | 5.07 M / 779 k / 8.95 M |
| `scale` one message to each, /s | 1.85 M | 1.63 M |

`remote_calls`, transport and spawn moved 3 % or less. What this establishes: on one producer or four into one actor,
two contended atomic words per message cost more than the one lock they replaced, and fanout (−9 %) and topic publish
(−12 %) are the only clear wins. What it does not: why, since `perf record` was not allowed on the build box; the
likely costs are the second compare-and-swap, the owner's pass that counts written slots before taking them, and the
owner's head sharing a cache line with the senders' tail.

### Asks from outside the workers

An ask from a thread outside the workers used to sleep at its first poll and be woken by the reply: on Linux a thread
wake is about 1.5 µs, most of a round trip to a quick handler. Now an ask alone on its actor and on its thread, on the
system clock, first waits up to 2 µs on its thread for the reply ([actors](actors.md#the-three-ways-to-send)). Three
smaller changes came with it: a send with room takes it in one locked section, with no reservation; an ask on the
system clock reads it for its deadline only once its deadline's wake is armed, so a reply that is there at the first
poll reads none; and the clock thread is notified only by a wake earlier than the one it sleeps until.

`perf ask` makes 2 M asks one at a time from the main thread to an actor on one worker. `perf asks` runs 1 000 tokio
tasks of 100 asks each on a tokio runtime of one thread, over 16 actors on 4 workers, so most of its asks have others
outstanding on their thread and go straight to sleep. Against ca7715f on the Ryzen 9 9900X build box, which other
lanes' builds shared (load average 11 to 48), `taskset -c 0-11`, five interleaved rounds, median (range) in ns/msg:

| `perf` | before | after |
|---|---|---|
| ask | 2 030 (1 384–3 144) | 324 (298–334) |
| asks | 451 (426–1 068) | 414 (321–809) |
| embedded | 14.4 (13.6–52.1) | 17.8 (14.0–54.3) |
| hosted | 51.3 (44.5–275) | 46.8 (44.6–207) |
| contended | 127 (102–197) | 99.9 (98.6–131) |
| fanout | 176 (71.7–271) | 185 (73.0–274) |
| start | 2 121 (1 692–6 901) | 2 072 (1 605–3 410) |

Embedded alone on one core (`taskset -c 4`), four interleaved rounds: 13.7, 14.0, 13.7, 13.7 → 13.7 in each. A first
build lost 3 ns a message there with the same instructions retired (IPC 2.80 → 2.37): the reply capability's drop, which
now checks whether it has replied before it takes the lock, was inlined into the drop of every message that may hold
one. Moved out of line, the loss is gone.

The merge was then measured on a quiet build-1 (load under 1, governor `performance`): `scripts/bench.sh perf` on main
at 2b0f618c and on the merge at d7c63173, back to back, five interleaved rounds each, judged by `scripts/bench.sh
compare`. Median in ns/msg:

| `perf` | main | merged | verdict |
|---|---|---|---|
| ask | 1 373 | 288 | improved, −79 % |
| asks | | 307 | new |
| embedded | 13.2 | 13.2 | within noise |
| hosted | 41.5 | 42.7 | within noise |
| traced | 106 | 106 | within noise |
| contended | 97.3 | 98.4 | within noise |
| fanout | 70.8 | 70.3 | within noise |
| start | 1 889 | 1 642 | improved |
| route direct | 102 | 96.3 | improved, −5.7 % |
| route round-robin, least-loaded | 92.3, 159 | 89.3, 153 | improved, −3.3 % and −3.8 % |
| route hash | 108 | 112 | REGRESSION, +4.1 % |
| topic direct, publish | 70.7, 70.9 | 69.8, 69.9 | within noise |

`route hash` is slower in each of five back-to-back pairs, by 2.4 to 4.1 % (3 to 4 ns), where main's own reruns stayed
within 1.5 %. In the same runs `route direct`, a tell to the same four actors, was 4 to 7 % faster. The merged tree
with main's `mailbox.rs` put both back near main (hash +0.7 and −0.4 %, direct −1.0 and −3.4 %), so both come from the
send's locked section moving into `admit`, which `try_tell` and the first poll of a send share. That section does the
same work as before, and profiles of the two builds show the same functions in the same shares. Neither `#[inline]` on
`admit` nor `#[inline(never)]` on `try_tell` moved the hash row, so it is taken as an effect of where the code was
placed, not of more work, and accepted for the gains on the other three router rows. `remote_calls` and the stream
`fanout` were within noise over three pairs: the first pair had remote `tell` sent −6.8 % and `request_within` p99.9
+16 % from two rounds with long tails, and the two reruns were within 1 % (tell) and 3 % (p99.9). These are build-1
numbers, a Ryzen 9 9900X, taken before it had a baseline of its own ([below](#build-1-ryzen-9-9900x)). The EPYC
bench box's baseline still reports `perf-asks` as `new`.

What this establishes: a lone ask from outside the workers to a quick handler is about five times faster, because its
reply comes inside the wait and its thread is never put to sleep. What it does not establish: anything on another CPU,
or a cause for `route hash` beyond the code placement above. The wait costs its
thread up to 2 µs of CPU for each ask whose reply comes later, and such an ask pays the wake as well. `asks` is held
back by what the wait does not touch: with more tokio threads, its time goes to the lock on tokio's queue for tasks
woken from outside (870–1 500 ns at 8 threads). A list of deadlines per mailbox in place of the clock thread's wakes
was tried and measured the same (`asks` 371–386 against 375–379 ns), so it was not kept. Fanout over 12 workers is
[the section below](#turns-on-the-asking-thread).

### Turns on the asking thread

Three changes, measured with the six-framework harness kept outside this repository (13 scenarios at 1, 4 and 12
workers, each a process of its own, 7 samples, the median per row). An ask from outside the workers, a plain thread or a
tokio task, alone on its actor and its thread, to an idle actor of the shared pool runs that actor's turn on the asking
thread: the message and the answer cross no core, where on a worker they cross two. Two such turns in a row longer than
0.5 µs send the actor back to the workers for its next 250 asks. A named pool's actors and those of workers pinned to
cores are never run there. A worker that takes half of a busy worker's queue now counts the theft, so the other workers
look once more before they take from it again, however deep it is: behind a turn that tells 64 idle actors, each is
moved once, not once per message. `STEAL_DEPTH` went from 8 to 32. A deadline on the system clock is kept in a grain of
1 ms with the others due in it, and a cancelled deadline's slot is used again.

Against main at 909f833d on the EPYC 4484PX box (load under 1, governor `performance`), the harness's axon binary built
on each, run in turns, median ns per operation:

| Scenario | workers | main | after |
|---|---|---|---|
| `ask_latency`: one ask at a time from a tokio task | 1 / 12 | 373 / 515 | 156 / 152 |
| `ask_latency` p99 | 1 / 12 | 421 / 2 134 | 200 / 180 |
| `ask_concurrent`: 1 000 asking tasks on one tokio thread | 1 / 12 | 386 / 719 | 128 / 127 |
| `fanout_1to64`: one actor tells 64 actors in turn | 4 / 12 | 60 / 201 | 40 / 39 |
| `contended_4to1` | 4 / 12 | 51 / 58 | 48 / 52 |
| `tell_outside`, `tell_1to1`, `mesh`, `ping_pong`, `cpu_work`, `latency_100k` | | | within 1 ns or 2 % |
| `spawn` | 12 | 1 462 | 1 336 |

A first build kept "this send may run the turn here" in a thread-local flag of its own, which every wake from outside
a pool read. `perf route` least-loaded, whose tells wake an idle actor more often than the other routings, went from
112 to 124.5 ns/msg over ten interleaved rounds (ranges 107–116 and 120–127). The flag is now a value of the
thread-local that wake already reads, the worker's pool: in ten rounds of the three in turn, main 113.5, that build
116.7, and the same build with turns on asking threads switched off 115.2, their ranges overlapping.

The first message now reserves 4 slots, not 8. A spawn is mostly memory: in a DWARF profile of the harness's `spawn` on
one worker, 49 % of the spawning handler's samples are page faults on new memory and 39 % `malloc`, and the two
registries' inserts 6 %. Of the 1.5 KB an actor touches, the 8 slots of 48 bytes (a `u64` message) were 384, the mailbox
552, the driver 200, the cell 88 and its exit signal 72. With 4 slots, five interleaved rounds of 3 samples: 1 062 → 978
ns per actor and 1 569 → 1 377 bytes on 1 worker, 1 246 → 1 159 ns on 12. A single slot measured the same as 4.

The ask rows come from the turn run on the asking thread, a tokio task's too. A build that ran it only on threads
outside tokio measured as main, five interleaved rounds: `ask_latency` 535 / 369 ns against main's 592 / 379 and 158 /
156 with it, `ask_concurrent` 338 / 689 against 407 / 689 and 127 / 117. A threadless runtime already runs each turn on
its tokio thread, so tokio threads were kept. A build that removed each deadline grain once empty woke the clock thread
on every ask, through the grain left last, as main did with each deadline: the last empty grain is now kept, the
others removed. In the full run the site shows, the merged tree measured `ask_latency` 160 / 158 (p99 191 / 190),
`ask_concurrent` 128 / 123 and `fanout_1to64` 37 / 42 at 4 / 12 workers.

The bench gate on the same box, `scripts/bench.sh perf-` on main and then on the lane, judged by `bench.sh compare`,
median ns/msg:

| `perf` | main | lane | verdict |
|---|---|---|---|
| ask | 302 | 107 | improved, −64.7 % |
| asks | 302 | 145 | improved, −51.8 % |
| hosted | 15.4 | 13.8 | improved, −10.4 % |
| contended | 31.6 | 30.5 | improved, −3.5 % |
| embedded, traced, fanout, start, topic | | | within noise |
| route direct, least-loaded, hash | 81.6, 110, 96 | 88.6, 122, 102 | REGRESSION, +8.6, +10.8, +6.1 % |

The three router rows come from where the code is placed: `bench.sh` builds `perf` together with the pipeline and log
harnesses, and the lane's changes move the code that they share. Built alone, the same way, and run in turns, 8 rounds of each: least-loaded 118.0
on main and 119.7 on the lane, direct 98.2 and 93.6, hash 99.9 and 100.7, round-robin 88.1 and 88.2. An earlier
round of the same, with the lane's turns on asking threads switched off as a third build, measured least-loaded 118.1,
119.1 and 119.8. Tells, which these rows send, run none of the changed code but the theft count and the wake's look for an asking
thread.

What this establishes: on this CPU a quick lone ask from outside the workers is two to three times faster, and one
actor that tells many idle ones no longer loses its turns to workers that each take one message and go back to sleep.
What it does not: anything on another CPU, or under a load that preempts the asking thread. A turn run on an asking
thread uses that thread's time, which other tasks on it wait for: the 0.5 µs rule bounds it, at the cost of a clock
read on one turn in 16 and on each actor's first. A deadline may end up to 1 ms late.

What is still behind. A spawn costs about twice actix's on one worker (978 against 529 ns), and does not scale with
spawning actors: one spawner per worker, as the actix harness runs it, stays near 1 µs at 12 workers, where actix's
reach 94 ns. Both registries are one lock each and every new page faults under the process's one memory map; sharding
the registries and fewer, smaller allocations per actor are the work, not done here. `ask_concurrent` trails actix 5
to 7 times at 4 and 12 workers, whose askers run on the Arbiter of the actor they ask and so never cross a thread.
Giving axon's askers a tokio runtime of one thread per worker made it worse, 722 and 1 428 ns: each answer from a
worker then wakes a task from outside tokio, through the lock on its queue, as in `perf asks` above.

### Routers

`perf route` sends from 1 producer to 4 actors on 4 workers through an `ActorSystem`: a direct `ActorHandle::tell`
to each in turn, then the same through a `Router` for each routing, twice in one process, the first round waking
the workers. Same tell-and-yield loop and 4096-slot mailboxes as the rows above. AMD EPYC 4484PX (24 threads), not
pinned, load average 0.5–0.9, three processes, the second round of each, ns/msg:

| Path | ns/msg (3 runs) | over direct |
|---|---|---|
| direct `handle.tell` | 83.0, 83.9, 84.6 | |
| `Routing::RoundRobin` | 84.3, 85.1, 86.4 | +1–2 |
| `Routing::LeastLoaded` | 129.2, 130.7, 130.9 | +46 |
| `router.member(&i).tell` | 96.9, 97.5, 100.2 | +14–16 |

Then `pick` learned to pass over a member that is stopping or draining, one atomic phase load per member it looks
at. Rerun on the same box, load 0.2, same three processes: direct 82.9, 85.3, 83.7; round-robin 84.2, 88.3, 85.9
(+1–3, one load); least-loaded 136.0, 135.6, 137.2 (+52, a load per member on top of its lock); a key 96.4, 95.2,
98.8 (untouched).

Before and after on the same box and load, `perf hosted` 18.4, 18.9, 20.7 → 15.3, 23.8, 21.7 and `perf fanout` 92.2,
91.7, 69.7 → 78.6, 83.3, 79.9: unmoved, as the router is not on their path; the one change there is a mailbox method
only a router calls.

What this establishes: round-robin costs a read lock and a counter over a direct tell. Least-loaded took each
member's mailbox lock to read its length, the same lock the workers take their batches under, so with 4 members it
cost half again as much; it pays where members' handlers differ in cost, which this harness does not have. A key
costs a hash of the key, a score per member and a handle clone. What it does not establish: one producer only, so
the read lock and the round-robin counter are uncontended; how least-loaded and the key scale past 4 members (both
are linear in them); nothing was pinned.

Since then `pick` reads a length that each mailbox keeps beside its lock, and takes no lock
([build-1](#build-1-ryzen-9-9900x)). On build-1, least-loaded went from 150–158 to 115 ns/msg, against 94–98 for a
direct tell. With 4 members it now costs about a fifth more than a direct tell, not half again as much.

### Topics

`perf topic` sends from 1 producer to 64 actors on 4 workers through an `ActorSystem`: a `tell` to each actor in turn,
then one `topic.publish` to the same 64 subscribers, in rounds of 1024 messages to each that fit every 4096-slot
mailbox, waiting for each round to be handled, so nothing is missed. Twice in one process, the first round waking the
workers. AMD EPYC 4484PX (24 threads), shared with other work, load average 1.0–1.3 before and after, not pinned,
three processes, the second round of each, ns per delivery:

| Path | ns per delivery (3 runs) |
|---|---|
| a `tell` to each of the 64 | 52.8, 52.6, 87.6 |
| `topic.publish` | 56.0, 55.7, 56.7 |

`perf fanout` does not go through an `ActorSystem`, so the topic is not on its path: before (main 58bde5e) and after,
ten interleaved runs each, median (range) 80.4 (56.9–113.4) and 85.0 (78.8–102.2) ns/msg.

What this establishes: a publish costs about what telling each subscriber yourself costs, some 3 ns more per delivery
for a clone of the 16-byte message and a match per subscriber and one read lock per publish, and it varied less in
these runs. What it does not establish: publishers on
several threads at once, which share the topic's read lock; misses, each of which takes the dead letters' lock; large
messages, whose clone is the user's; nothing was pinned.

## Durable pipeline (`axon`)

`cargo run --release -p axon --example pipeline -- key=value ...` runs a whole service shape in one process: clients
keep `window` requests in flight; each request goes to the shard that owns its key, which checks it against the key's
holdings and passes it on; one log writer per shard appends a turn's entries to a preallocated file, `fdatasync`s it
and acknowledges each client once per sync; a feed sends 100 000 reference updates a second to every shard, and a
shard re-checks every key holding an updated item. Shards and writers are `Execution::Dedicated`, clients shared on the
default 8 workers, 1 024-message turns. Latency is from a client's send to its ack. AMD EPYC 4484PX, Samsung PM9A3
(power-loss protected) under ext4, Linux 6.8, single runs of 10 s after 2 s of warm-up; defaults are 4 shards, 16
clients, 100 000 keys, 500 items, 4 holdings a key.

| Run | req/s | p50 µs | p99 µs | p99.9 µs | syncs/s | entries/sync |
|---|---|---|---|---|---|---|
| window 64, no sync, cores 0–11 | 5.81 M | 134 | 620 | 1 213 | 224 837 | 25.8 |
| window 64, cores 0–11 | 6.06 M | 139 | 484 | 1 162 | 82 981 | 73.1 |
| window 64, no feed, cores 0–11 | 6.12 M | 137 | 482 | 1 106 | 90 321 | 67.8 |
| window 256, cores 0–11 | 6.93 M | 532 | 1 459 | 2 033 | 89 936 | 77.0 |
| 8 shards, 32 clients, cores 0–11 | 4.84 M | 315 | 1 737 | 2 678 | 139 265 | 34.7 |
| window 8, cores 0–11 | 874 k | 84 | 1 047 | 1 988 | 57 603 | 15.2 |
| window 8, no sync, cores 0–11 | 981 k | 69 | 799 | 1 494 | 76 654 | 12.8 |
| window 8, `LOOK_AGAIN` 100 µs, cores 0–11 | 815 k | 88 | 1 127 | 2 141 | 54 048 | 15.1 |
| window 8, cores 0–23 | 1.59 M | 76 | 174 | 219 | 74 750 | 21.3 |
| window 64, cores 0–23 | 6.41 M | 150 | 302 | 370 | 93 410 | 68.6 |

What this establishes: on this drive a sync costs tens of microseconds, so group commit keeps it off the throughput
(syncing or not, about 6 M requests a second through four writers). The tail is set by threads, not by the runtime or
the disk: the pipeline runs some 18 busy threads (8 workers, 4 shards, 4 writers, the feed, tokio), and on 12 cores
the p99 at window 8 is a millisecond whether the log syncs or not and whatever the idle watcher's period; given 24
hardware threads it is 174 µs. More shards on the same cores (8 shards) is slower for the same reason. What it does not
establish: there is no network, encoding or allocation of real payloads; single runs, so the rows carry run-to-run
noise; and the only thread placement tried is `taskset` on the whole process.

Pools (`RuntimeConfig::pools`, `Execution::Pool`), same box and harness, cores 0–11, two interleaved rounds of 8 s,
the mean of the two shown (rounds within 8 % of each other except `max`). Before is the layout above: 8 shared workers,
shards and writers dedicated. `workers=4` only shrinks the shared pool. `pin=1` pins the 4 shared workers to cores 0–3,
puts the shards on a pool of 4 workers pinned to 4–7 and the writers on one pinned to 8–11, both without `balance`;
`hot=1` also sets those two pools' `spin` to `Duration::MAX`.

| Layout | window 8: req/s | p50 µs | p99 µs | p99.9 µs | window 64: req/s | p50 µs | p99 µs | p99.9 µs | max µs |
|---|---|---|---|---|---|---|---|---|---|
| before | 832 k | 87 | 1 095 | 2 119 | 5.90 M | 138 | 585 | 2 040 | 5 600–6 000 |
| `workers=4` | 1.77 M | 68 | 158 | 220 | 6.99 M | 134 | 318 | 619 | 4 800–7 200 |
| `workers=4 pin=1` | 1.58 M | 78 | 160 | 189 | 8.70 M | 108 | 234 | 269 | 390–450 |
| `workers=4 pin=1 hot=1` | 1.59 M | 77 | 162 | 195 | 8.31 M | 114 | 239 | 272 | 3 700–3 900 |

What this establishes: at light load (window 8) the whole gain is fewer threads than cores, which the shared pool's
`workers` already gave; pinning then costs 11 % of throughput and 10 µs of p50, since every hand-off from shard to
writer now crosses cores, and buys a little at p99.9. Under load (window 64) pinned pools that keep each actor on its
worker are 24 % more throughput than the right-sized shared pool, cut p99.9 from 619 to 269 µs and the worst request
from several milliseconds to under half of one. Never sleeping bought nothing here, and cost throughput and the
worst case: a pool's workers wake within the 20 µs spin at these rates anyway. What it does not establish: the feed
thread and tokio are not pinned and share the 12 cores; one core layout was tried; nothing ran across NUMA nodes.

## Durable log (`axon`)

`cargo run --release -p axon --example log -- secs=5 owners=1,4` appends records of 128 bytes, each to the next of
1 024 keys, then takes one write of them and waits for it: a write and a sync of `files::Log` on `OsFiles`, as one
owner turn gives it. Latency is per write, from `take_write` to `written`. With 4 owners, each runs its own log in a
directory of its own on the same disk, on a thread of its own, all at once; records/s and writes/s are the sum of the
owners' rates, the percentiles over all their writes. build-1 (Ryzen 9 9900X), two Samsung PM9A1 (no power-loss
protection) in RAID 1 under ext4, Linux 7.0, governor `performance`, nothing else running, at b0bfa15e; four rounds of
5 s a row, one after another, and the range of the four. Indicative, not part of the gate.

| Owners | Segments | records a write | records/s | writes/s | p50 ms | p99 ms | p99.9 ms |
|---|---|---|---|---|---|---|---|
| 1 | growing | 1 | 179–230 | 179–230 | 4.7–5.8 | 5.8–7.9 | 9.4–11.5 |
| 1 | growing | 16 | 2 808–4 432 | 176–277 | 3.4–5.8 | 4.7–13.6 | 9.4–13.6 |
| 1 | growing | 256 | 43 643–62 536 | 170–244 | 4.7–5.8 | 4.7–9.4 | 7.3–14.7 |
| 1 | zero-filled | 1 | 363–721 | 363–721 | 1.4–2.9 | 1.7–6.8 | 5.2–10.5 |
| 1 | zero-filled | 16 | 5 974–11 380 | 373–711 | 1.4–2.6 | 1.7–2.9 | 5.2–6.8 |
| 1 | zero-filled | 256 | 85 502–163 301 | 334–638 | 1.3–2.6 | 1.6–5.8 | 5.8–92.3 |
| 4 | growing | 1 | 360–467 | 360–467 | 9.4–11.5 | 12.6–18.9 | 29.4–50.3 |
| 4 | growing | 16 | 5 698–9 019 | 356–564 | 7.3–11.5 | 9.4–23.1 | 10.5–27.3 |
| 4 | growing | 256 | 64 549–97 745 | 252–382 | 10.5–15.7 | 14.7–23.1 | 23.1–50.3 |
| 4 | zero-filled | 1 | 564–1 375 | 564–1 375 | 3.1–5.8 | 4.7–21.0 | 7.9–336 |
| 4 | zero-filled | 16 | 8 029–8 400 | 502–525 | 7.9 | 12.6–13.6 | 302 |
| 4 | zero-filled | 256 | 116 663–124 090 | 456–485 | 7.9 | 11.5–12.6 | 302 |

Each range has two levels. The first round after the disk had rested for some minutes had the lower latencies (one
owner: p50 4.7 ms growing, 1.4 ms zero-filled), and the three rounds that followed it within a minute had the higher
ones (5.8 and 2.6 ms). The same happened in two earlier sessions of two rounds that day. Why the disk is slower after
a minute of such writes was not measured.

What this establishes: on this disk the sync is the cost, whatever the write holds: a write of 256 records takes as
long as one of 1, so records a second grow with the records each write takes. With one owner, a segment zero-filled
when it is made (`LogConfig::preallocate`, the default) syncs 2.2 to 3.3 times faster at p50 than one that grows,
since a write over it does not change the file's length; 3.3 is the ratio the design measured (spec/durable-log.md).
Four owners at once on growing segments write 1.5 to 2 times as many times a second as one, and each write waits 2 to
2.7 times as long: about two syncs' cost for four, as the design expected. On zero-filled segments, four owners lose most of
that gain: at 16 and 256 records a write, all four together made 456 to 525 writes a second at a p50 of 7.9 ms in
every round, fewer than one owner alone in its first round (638 to 711). The p99.9 of 302 ms is each owner's first
write, which made and zero-filled its first 64 MiB segment while the others did the same; a segment is now filled
`fill_ahead` at a time (below). What it does not
establish: why four zero-filled logs that write 16 records or more wait about as long as four growing ones; actors (each owner is a thread
that waits for its own writes); a disk with power-loss protection (on a PM9A3 a sync is tens of microseconds, see
Durable pipeline above); and more than one write out at a time.

### Across a segment roll

The gate runs the example with one owner: `log`, the rows above for 5 s each, and `log-roll`, records of 4 KiB, 64 a
write, for 10 s on zero-filled segments, so that a 64 MiB segment fills about every 0.25 s. Each prints its slowest
write as `max`, reported but not judged. A zero-filled segment was zero-filled whole in the write that made it, and it
is now filled `fill_ahead` ahead of its frames. On the EPYC 4484PX (two Samsung PM9A3, with power-loss protection,
ext4, Linux 6.8, governor `performance`), main's `log.rs` (before) and the new one with `fill_ahead` at 1 MiB (after)
were built from one tree and run in turn, twice each, five rounds a run, the box doing nothing else (load 0.9 to 1.2,
the owner itself). The medians of the two runs, in µs:

| Job, records a write | p50 | p99 | p99.9 | max | records/s |
|---|---|---|---|---|---|
| `log`, zero-filled, 1 | 40 → 40 | 65–73 → 65 | 73 → 73 | 109 051 → 1 966 | 24 388–24 639 → 24 671–24 955 |
| `log`, zero-filled, 16 | 57 → 57 | 65–73 → 73 | 81 → 1 179 | 117 440 → 1 703–1 835 | 211 964–213 580 → 219 695–220 021 |
| `log`, zero-filled, 256 | 81 → 90 | 98–114 → 1 310 | 122–147 → 1 572 | 117 440 → 1 966–2 359 | 676 538–683 088 → 732 851–734 066 |
| `log-roll`, 64 of 4 KiB | 245 → 245 | 327 → 1 572 | 109 051 → 1 835 | 109 051–117 440 → 2 097–2 359 | 58 183–58 311 → 66 186–66 448 |

The rows of growing segments did not move, except the p99.9 at 1 a write by one histogram step in one run (147 to
163). The same build, with `fill=` set and two rounds of 5 s a size, the range in µs:

| `fill_ahead` | 16 a write: p99, p99.9, max | 256 a write: p50, p99, p99.9, max | `log-roll`: p99, p99.9, max |
|---|---|---|---|
| 64 KiB | 196–212, 212–229, 393–425 | 196, 262–294, 458–491, 491–524 | 720, 720, 851 |
| 256 KiB | 360, 393–425, 589–655 | 90, 491–524, 720, 720–851 | 786, 851, 1 048 |
| 1 MiB | 65–73, 1 179, 1 572 | 90, 1 310, 1 572, 1 703–1 835 | 1 572–1 703, 1 835, 2 097–2 359 |
| 64 MiB, whole | 65–73, 73–81, 117 440 | 90, 98, 122–131, 109 051 | 327, 109 051, 109 051–117 440 |

What this establishes: on this disk a write that zero-fills costs about 150 µs and 1.6 µs a KiB of zeros, so the write
that made a 64 MiB segment took 109 to 117 ms, and every write queued behind it waited as long; with `fill_ahead` at
1 MiB no write took more than 2.4 ms. The zeros are the same bytes either way. Filling ahead puts them in one write in
`fill_ahead` of records, not one in 64 MiB, and that write shows where it is more often than one in 100 or one in
1 000. Records a second rose 3 to 14 %: zeros cost less a byte in small writes than in one of 64 MiB. A smaller
`fill_ahead` lowers the tail, but more writes grow the file: at 64 KiB, the p50 at 256 a write is a growing segment's
(196 µs, growing 163 µs).

The p50 at 256 a write is not the change's: the same build of main measured 81 µs at 11:38 and 11:45, 90 µs at 13:02
and 13:13 and 106 µs at 13:31 that day, five rounds out of five each time, and the new builds run between them were
within one histogram step of it. The disk got slower over the afternoon's writes (a segment roll on main took 109 to
117 ms, then 151 ms), as the PM9A1 above did after a minute of writes.

### Which `fill_ahead`

The kv example (axon-examples, `examples/kv`) led to this change: its update p99.9 at 20 k ops/s was 171 to 178 ms on
zero-filled segments, 26 to 33 ms on growing ones, on a box other work shared. It was run again on the EPYC 4484PX
with nothing else on it: three durable nodes (`scripts/run.sh up durable 3`, `--workers 4`, `taskset -c 0-11`), each
round a new cluster, go-ycsb workload A on `taskset -c 12-23` (64 threads, 200 000 records of 10 by 100 bytes, loaded,
then 90 s at 20 000 ops/s), governor `performance`. The four builds ran in turn, three rounds, the order reversed in the
second: main as kv uses it, this change at 1 MiB and at 256 KiB (an environment variable read into kv's `wal_config`
in a copy on the box), and main with `preallocate: false`. Every run held 20 000 ops/s. In ms, the range of the three
rounds:

| Build | update p50 | p99 | p99.9 | max | read p50 | p99 | p99.9 | max |
|---|---|---|---|---|---|---|---|---|
| main | 0.54–0.58 | 2.51–2.87 | 146–156 | 202–207 | 0.26–0.28 | 1.50–1.54 | 19.4–20.2 | 198–206 |
| `fill_ahead` 1 MiB | 0.53–0.78 | 1.82–2.55 | 3.80–4.16 | 37–45 | 0.25–0.38 | 0.89–1.17 | 1.78–2.28 | 36–42 |
| `fill_ahead` 256 KiB | 0.55–0.57 | 1.57–1.73 | 3.02–3.15 | 46–47 | 0.26 | 0.90–0.93 | 1.67–1.74 | 42–45 |
| main, growing segments | 0.77–1.09 | 2.21–2.35 | 3.65–3.88 | 48–54 | 0.26–0.43 | 0.97–1.46 | 2.00–2.27 | 47–53 |

The `log` rows again on the same day, each size a run of five rounds in turn with main, the medians in µs. The first
three runs had the disk as it was at 13:05; the last three, in the reverse order, as it was at 13:30 (the three between,
while it slowed, are left out):

| Build | 16 a write: p99, p99.9, max | 256 a write: p50, p99, p99.9, max | `log-roll`: p50, p99, p99.9, max |
|---|---|---|---|
| main | 73, 73, 117 440 | 90, 98, 122, 117 440 | 245, 327, 109 051, 117 440 |
| 1 MiB | 73, 1 179, 1 835 | 90, 1 310, 1 572, 2 359 | 262, 1 572, 1 835, 2 359 |
| 256 KiB | 393, 425, 720 | 90, 491, 720, 786 | 524, 786, 851, 1 048 |
| main, later | 73, 81, 150 994 | 106, 131, 147, 150 994 | 393, 491, 150 994, 150 994 |
| 1 MiB, later | 65, 1 703, 2 359 | 106, 1 966, 2 359, 2 621 | 393, 2 359, 2 621, 2 883 |
| 256 KiB, later | 425, 589, 786 | 114, 720, 851, 917 | 851, 1 179, 1 179, 1 441 |

On this disk an `fdatasync` after a write that grows the file took 102 µs at p50 for 4 KiB and 138 µs for 40 KiB,
after one over zeros already synced 26 and 50 µs (3 000 of each, ext4). The PM9A3 reports no volatile write cache
(`nvme id-ctrl`: vwc 0x6, bit 0 clear; the kernel's `write_cache` is write through): it has power-loss protection,
and a sync waits for no flush.

What this establishes: on this box both sizes end the kv stall; the update tail is then about that of growing segments
at 1 MiB and below it at 256 KiB. 256 KiB gave the lower update p99 and p99.9 in every round (1.57 to 1.73 and 3.02 to
3.15 ms, against 1.82 to 2.55 and 3.80 to 4.16), and in the `log` rows the lower p99.9 and max at 16 and 256 a write and
at a roll: each write that fills ahead carries 256 KiB of zeros, not 1 MiB. That is the default, and the gate's baseline
holds the 256 KiB rows of the first run. Its cost: four times as many writes grow the file, so the p99 at 16 a write is
one that grows it (393 µs, against 73), and a write of 256 KiB or more grows it every time (`log-roll` p50 524 µs,
against 262). Records a second moved less than 1.5 %. The maximum of 37 to 54 ms in kv is the same with growing
segments, so it is not the zeros. What it does not establish: a disk without power-loss protection, where each write
that grows a file pays for a sync of its length (2.2 to 3.3 times one that does not, the PM9A1 table above), and 256 KiB
makes four times as many of them as 1 MiB; several owners; kv on three boxes.

## Scale (`axon-obs`)

`cargo run --release -p axon-obs --example scale -- actors|keyed|nodes|streams|subscribers` runs everything in one process over
loopback, single runs on an unloaded AMD Ryzen 7 3800X (8 cores, 16 threads, 62 GB, Linux 6.8). `nodes` runs under `ulimit -n 500000`
with one worker thread per node. "Cores" is processor time over wall time for the whole process. "Before" is the
commit before the scale work (a510235).

| One node, 1 M idle actors, 64 B messages, capacity 1024 | before | now |
|---|---|---|
| Spawn, one after another (was 121 k/s while `start` waited for `on_start`) | 43 k/s | 455 k/s |
| Spawn, 64 starts in flight | n/a | 498 k/s |
| Resident per idle actor | 7.1 KB | 2.9 KB |
| Resident per actor that has handled a message (the first one reserves 32 slots) | 7.7 KB | 6.5 KB |
| Virtual per idle actor | 107 KB | 2.9 KB |
| One message to each (the first one now allocates the queue) | 1.18 M/s | 0.57 M/s |
| Idle, telemetry at 1 Hz with `scan_limit` above the actor count | 0.46 cores | 0.23 cores |

Capacity 64 gives the same figures now; the mailbox allocates as it fills.

A mailbox held its timing histograms inline, 1136 of its 1872 bytes, though timings are off unless
`timing_sample_every` is set; now they are boxed when on. `scale actors 1000000` on the EPYC 4484PX described under
Scheduler, unpinned, three runs of each taken in turn against 5fee4fe:

| One node, 1 M idle actors | before | now |
|---|---|---|
| Resident per idle actor | 2883 B | 1747 B |
| Resident per actor that has handled a message | 6509 B | 5259 B |
| Spawn, one after another | 530–559 k/s | 701–702 k/s |
| Spawn, 64 starts in flight | 460–467 k/s | 464–477 k/s |
| One message to each | 681–763 k/s | 693–733 k/s |

A profile of the spawning thread put 80 % of it in `malloc`, most of that the kernel faulting in fresh pages, so
fewer bytes are a faster spawn. Keyed spawns did not move (`AXON_PAXOS=3 scale keyed 100000`, 120.4–121.7 k/s
before, 120.4–121.4 now).

The exit an actor publishes and the state of its ending were inline too, 176 bytes of every actor for what only an
ending one writes; now they are boxed. Same machine and harness, against 8f6bc48:

| One node, 1 M idle actors | before | now |
|---|---|---|
| Resident per idle actor | 1747 B | 1571 B |
| Resident per actor that has handled a message | 5259 B | 5078 B |
| Spawn, one after another | 691–735 k/s | 773–790 k/s |
| Spawn, 64 starts in flight | 463–478 k/s | 465–473 k/s |
| One message to each | 665–727 k/s | 723–728 k/s |

The mailbox was 640 bytes then and the driver of an actor without state 216, and `const` assertions keep them there.

A spawn made ten allocations, four of them wrappers: the scheduler's wake was a closure behind an `Arc`, wrapped again
in a `Waker`; the system kept the address twice, once in a probe closure; and every spawn built a default restart
policy it never used. Now the cell is the waker, the mailbox is what the system keeps, the policy is built only for a
supervised spawn, and the exit hook's list has room for the one hook it holds. A spawn now makes five: the cell, the
mailbox, the driver, the hook and its list. Same machine and harness, three runs of each taken in turn against 86df3aa,
twice:

| One node, 1 M idle actors | before | now |
|---|---|---|
| Resident per idle actor | 1571 B | 1349 B |
| Resident per actor that has handled a message | 5078 B | 4835 B |
| Spawn, one after another | 830–846 k/s | 908–941 k/s |
| Spawn, 64 starts in flight | 448–491 k/s | 470–490 k/s |
| One message to each | 707–753 k/s | 735–795 k/s |

Savina fork-join-create, where each actor is spawned, handles one message and ends, five rounds of 5 samples pinned to
cores 0–11: 88–132 ms (median 120) before and 81–118 (111) now on 1 worker, 121–151 (146) and 87–137 (120) on 8. Its
profile is a third futex calls, most of them glibc's arena lock, taken when a worker frees what the spawning thread
allocated; fewer allocations take it less often.

The first message reserved 32 slots, 3.3 KB with 64-byte messages: past glibc's per-thread cache (1 KB), and pages
the kernel had to fault in for a queue that mostly held one message. It reserved 8 then, 832 bytes (4 since
[turns on the asking thread](#turns-on-the-asking-thread)), and the queue doubles from there, so a long first burst
regrows it a few times. It is no longer cut to 4 KB for large messages nor raised to 4 slots, so `with_first_block`
gets what it asks for. Same machine and harness, against 1dc1d73: `scale
actors 1000000` six runs each and `perf start` three, taken in turn:

| One node, 1 M idle actors | before | now |
|---|---|---|
| Resident per actor that has handled a message | 4835 B | 2339 B |
| One message to each | 707–785 k/s | 1.76–1.98 M/s |
| `perf start` (spawn, await the start, one message), ns per actor | 2463–2581 | 2061–2152 |
| Spawn, one after another | 907–970 k/s | 861–895 k/s |

Spawning reads a few percent lower in all six runs, though it never touches the block. `perf` hosted, contended, fanout
and ask, and the Savina suite, moved within their run-to-run spread.

The system learnt of each exit through a closure it boxed for every actor, in a list allocated to hold it. Now the
system itself is told, through a weak reference each actor keeps in place of the list, and finds the actor by the
exit's incarnation. A spawn makes three allocations: the cell, the mailbox and the driver. Same machine and harness,
against e2233cd, `scale actors 1000000` six runs each and `perf start` three, taken in turn:

| One node, 1 M idle actors | before | now |
|---|---|---|
| Resident per idle actor | 1349 B | 1253 B |
| Resident per actor that has handled a message | 2339 B | 2234 B |
| Spawn, one after another | 956–975 k/s | 1001–1017 k/s |
| Spawn, 64 starts in flight | 473–496 k/s | 475–536 k/s |
| One message to each | 1.93–1.98 M/s | 1.98–2.05 M/s |

`perf start` and Savina fork-join-create on 8 workers moved within their spread; fork-join-create on 1 worker read
90–109 ms against 113–117.

Every mailbox held the state of the jobs, sources and sinks its actor started, 96 bytes, though most actors start
none. Now the first job allocates it, and the mailbox is 552 bytes. Same machine and harness, against 64a6491,
`scale actors 1000000` three runs each, taken in turn:

| One node, 1 M idle actors | before | now |
|---|---|---|
| Resident per idle actor | 1253 B | 1173 B |
| Resident per actor that has handled a message | 2234 B | 2147 B |
| Spawn, one after another | 971–994 k/s | 973–1003 k/s |
| One message to each | 1.78–1.95 M/s | 1.94–2.03 M/s |

`perf` embedded, hosted, fanout and start, and the Savina suite, moved within their run-to-run spread. Two changes
measured against 64a6491 and not kept: a mutex that spins before it parks took chameneos on 8 workers from 105–169 ms
to 107–116, but `perf` hosted from 22 to 80 ns a message and contended from 35 to 130; and the driver in the cell's
allocation saved 15 bytes an actor and read slower in every spawn run and in ping-pong.

Every spawn took the system's lock twice, once to turn a closed system away and once to register, and every exit
twice, once to unregister and once to add to the retired totals. So a thread spawning waited on workers ending actors.
Now each takes it once: the early refusal reads a flag, and the retired totals have a lock of their own. Same machine
and harness, against a7532f5, `scale actors 1000000` three runs each, taken in turn:

| One node, 1 M idle actors | before | now |
|---|---|---|
| Spawn, 64 starts in flight | 476–484 k/s | 695–704 k/s |
| Spawn, one after another | 993–1006 k/s | 997–1010 k/s |
| One message to each | 1.96–2.08 M/s | 2.07–2.11 M/s |

Savina fork-join-create on 8 workers, with the harness built on mimalloc so that glibc's arena lock does not hide the
system's, four rounds: 62–73 ms before, 60–66 now. The `perf` modes and the rest of the Savina suite, on the system
allocator, moved within their run-to-run spread. At a7532f5, fork-join-create on 1 worker read 82–99 ms on the system
allocator and 24–35 on mimalloc: most of it is the arena lock above.

At a7532f5, Savina fork-join-throughput on 8 workers, 110 ms, is bound by its one sending thread: every `tell` finds
its receiver idle and queues it, and moves about five cache lines from the worker that last ran it (the message's
slot, the mailbox lock, the cell's scheduled flag, the run queue's lock and its length). On cores 0–5, which share an L3, it reads 38 ms on 5
workers and 13 on 1; on cores 0–11, across both, 57 and 26. Ordering the mailbox's fields so that what a sender reads
is away from what a turn writes moved it within its spread (107–116 ms before, 109–113 after), and was not kept.

| Keyed spawns on one node, 256 callers in flight | before | now |
|---|---|---|
| No ownership lease cost | 29 k/s | 178 k/s |
| Lease that takes 1 ms | 472 /s | 50 k/s |
| Lease that takes 5 ms | 164 /s | 18.8 k/s |

| Cluster in one process, `MemoryDiscovery` | 200 nodes, 20 links each | 200 nodes, full mesh | 400 nodes, 20 links each |
|---|---|---|---|
| Start and agree on the view | 1.05 s | 1.06 s | 0.13 s |
| Resident per node, no links / with links | 0.38 / 1.52 MB | 0.36 / 10.1 MB | 0.42 / 1.76 MB |
| Idle, no links | 0.13 cores | 0.13 cores | 0.41 cores |
| Idle, with links | 0.93 cores | 5.5 cores | 1.87 cores |
| Links opened in: before / now | 0.19 / 0.19 s | 54 / 1.35 s | 0.29 / 0.30 s |
| Metric rows per second with links: before / now | 213 k / 166 k | 1.02 M / 166 k | 416 k / 331 k |
| A join / a leave seen by every node | 0.9 / 1.0 s | 0.13 / 1.0 s | 0.18 / 1.0 s |
| 50 k keyed actors placed from one node | 89 k/s | n/a | 60 k/s |
| Resident per keyed actor | 8.7 KB | n/a | 9.4 KB |
| A join that moves its share of 50 k keys: processor time over idle | 2.2 s | n/a | 3.1 s |

The same run with `AXON_CONSUL` set: discovery by blocking query and one lease per keyed actor, against one Consul 1.20
server in a container on the same machine (`http_max_conns_per_client` raised, because here every node shares one
address). It ends with a tenth of the nodes restarted one after another under their own names.

| Cluster in one process, Consul | 50 nodes, 20 links each | 200 nodes, 20 links each |
|---|---|---|
| Start and agree on the view | 1.2 s | 2.1 s |
| A join / a leave seen by every node | 0.03 / 1.0 s | 0.24 / 1.0 s |
| Keyed actors placed from one node, one lease each | 13.9 k/s | 14.7 k/s |
| A join that moves 97 keyed actors settles in | 1.6 s | 1.9 s |
| One node restarted, seen by every node | 0.67 s | 1.8 s |
| Consul, idle / during the restarts | 0.03 / 0.14 cores | 0.13 / 1.3–2.4 cores |

A leave takes a second because a watch that has just answered is re-armed one `poll_interval` later. During the
restarts Consul's cost is 200 watchers each reading the whole member list again on every change, twice per
registration. The first attempt failed with `429 Too Many Requests`: 256 keyed spawns in flight opened 256 connections
from one client, which is what `ConsulConfig::max_in_flight` now prevents.

The same run with `AXON_SEEDS=3`: discovery through three `SeedServer`s in the same process, every node holding a
session to each, no leases. The baseline beside it is the same build on `MemoryDiscovery`.

| Cluster in one process, 20 links each | 200 nodes, memory | 200 nodes, 3 seeds | 1000 nodes, memory | 1000 nodes, 3 seeds |
|---|---|---|---|---|
| Start and agree on the view | 1.0 s | 6.0 s | 1.3 s | 6.4 s |
| Resident per node, no links | 0.4 MB | 0.8 MB | 0.9 MB | 2.35 MB |
| Idle, no links | 0.14 cores | 0.19 cores | 2.0 cores | 1.6 cores |
| A join / a leave seen by every node | up to 1 s / 1.0 s | 0.03 / 0.09 s | up to 1 s / 1.0 s | 0.39 / 0.61 s |
| A join that moves its share of 20 k keys settles in | 1.3 s | 0.6 s | 1.5 s | 0.8 s |
| One node restarted, until every node sees the new boot | 1.0 s | 0.14 s | 1.1 s | 1.4 s |

A start on seeds is the seeds' 5 s warm-up plus about a second. No registration was refused and no handshake failed.
A seed takes 64 handshakes at a time and the rest of the first 1000 dials wait their turn. When it turned them away
instead, 72 to 235 per seed were, and one start in three on the EPYC below had a node give up with `attached to too
few seeds`; three starts there that waited took 6.2 s, as before, and turned none away. At 1000 nodes the times are
bound by the machine: every node does a few ms of work per change, and all of them share 16 threads here. The
per-address handshake cap was lifted for these runs, because every node dials from one address.

The same run with `AXON_PAXOS=3`: one lease per keyed actor from `PaxosLeases`, the first three nodes serving as
acceptors, a 6 s lease renewed every 2 s. These runs are on another machine, an otherwise idle AMD EPYC 4484PX (12
cores, 24 threads, 93 GB, Linux 6.8), so the baseline beside them is the same build there without leases. Each run
was repeated three times; the range is shown.

| Cluster in one process, 20 links each, 20 k keys | 50 nodes, no leases | 50 nodes, Paxos | 200 nodes, no leases | 200 nodes, Paxos |
|---|---|---|---|---|
| Keyed actors placed from one node | 248–251 k/s | 108–109 k/s | 174–182 k/s | 61.8–62.6 k/s |
| Resident per keyed actor, the acceptors' share included | 7.3 KB | 10.1 KB | 7.7–8.4 KB | 11.9–12.0 KB |
| Idle, keyed actors placed | 0.17–0.18 cores | 0.23–0.24 cores | 0.53–0.58 cores | 0.67–0.68 cores |
| A join that moves its share of the keys settles in | 1.45 s | 1.35–1.51 s | 1.42–1.44 s | 1.21–1.43 s |
| A tenth of the nodes restarted one by one, the acceptors first: each | 1.0 s | 4.2 s | 1.0 s | 1.8 s |
| Leases the other nodes lost | 0 | 0 | 0 | 0 |

A placement is two round trips to a majority of the acceptors, and every lease is renewed in batches. The restarts
take longer only for the acceptors: each answers nothing for its 6.2 s warm-up, and the harness waits that out before
stopping the next, which is what a rolling deploy has to do. The other nodes restart in a second, as without leases.
Consul's 14.7 k/s above was measured on the Ryzen, so the two are not compared here.

These runs found two faults. A rebalance releasing keys while their renewal was out failed the whole node's renewal,
and three nodes of 200 went past their grace and stopped their actors during the restarts
(`a_release_during_a_renewal_does_not_fail_it`). And the first placements after start sent all 200 nodes dialling
the three acceptors at once, and each turned away the dials above its 64 handshakes in flight: an acquire failed
with `no majority` in two runs of three. Now those dials wait their turn. Three runs of each on this machine, at the
default cap: the listener that turned dials away failed one run, the one that queues them none, and placement went
at 62.2–62.3 k/s against 62.9–63.8 k/s for the runs that finished. The runs do not establish anything about
acceptors on their own machines: here a round trip is loopback.

What the lease cores cost, before (3156261) and now, on the same machine, three runs of each taken in turn:

| Paxos leases, EPYC 4484PX | before | now |
|---|---|---|
| `scale keyed 100000`: keyed spawns from one node of three, all acceptors | 74.1–75.1 k/s | 121.3–122.0 k/s |
| `scale nodes 50`: keyed actors placed / resident per keyed actor | 76.9–77.5 k/s / 12.3 KB | 108.1–108.9 k/s / 10.1 KB |
| `scale nodes 200`: keyed actors placed / resident per keyed actor | 54.5–56.4 k/s / 14.9–15.0 KB | 61.8–62.6 k/s / 11.9–12.0 KB |

Placement kept every core busy both times (21.4–21.9 cores), so the gain is less work per key. Most of it is the
proposer keeping one request in flight to each acceptor instead of four: on its own that took `nodes 50` from
77–78 k/s to 100–102 k/s and 12.3 KB to 10.3 KB, because the keys that wait for the answer leave together in fewer,
fuller requests. The rest is keys shared as one `Arc<str>` between the proposer's maps and its requests, the set of
acceptors that answered kept as bits, the proposer's name sent once per request instead of once per key, and one
map walk per answer on the acceptor. Renewing the 100 k leases of `scale keyed` takes 69–74 ms now; the build with
one request in flight and none of the rest took 107–115 ms. The old build's timed renewal failed in every run: it
ran beside the cluster's own, and the second of two renewals asked at once dropped the first caller's answer
(a renewal asked while one is out is now refused: `paxos/mod.rs::a_renewal_asked_for_while_one_is_out_is_refused`). All of this is over loopback: across a network, one request in
flight per acceptor carries at most 512 keys per round trip, far above these rates at a millisecond, and nothing
here measures acceptors on machines of their own.

What the acceptors' lease log costs ([paxos-leases.md § 4.1](../../../spec/paxos-leases.md#41-the-lease-log)), on the
EPYC 4484PX, idle, governor `performance`: before (89671e1), with the log alone, and with the two lanes of requests
per acceptor and the answers held until the log is synced, three runs of each taken in turn:

| Paxos leases, EPYC 4484PX | before | with the log | with the lanes and held answers |
|---|---|---|---|
| `scale keyed 100000`: keyed spawns from one node of three, all acceptors | 115.0–116.4 k/s | 99.5–100.6 k/s | 85.0–86.0 k/s |
| the same, renewing the 100 k leases, median of 5 | 70.3–77.5 ms | 70.6–73.0 ms | 91.4–92.8 ms |

Spawns are 26 % slower, renewals 20 to 30 %. The log alone takes 13 % of the spawns: each acceptor answers a grant only
once its log is synced, and here the three acceptors sync to one NVMe drive. One fsync covers every grant that came
while the last write was out, so the cost is a wait per write, not per key, and renewals, which write nothing, kept
their time. The rest came with the change that sends each acceptor's requests in two lanes and holds an answer until
its frame is synced: a further 15 % of the spawns, and the renewals' whole loss. What in that change costs it was not
profiled. It added, per item, a lookup of the key's round before it is sent and a tag kept until its request is
answered, per request to the node's own acceptor a channel and a timer, and the log was rewritten twice in a run of
100 k grants that released nothing (at 1 MiB and at 4 MiB; a log of grants alone is no longer rewritten). So the numbers establish the cost of the change as a whole
on one drive shared by three acceptors, and not which part to cut, nor the cost with a drive per acceptor. The
committed baseline rows are this run's (85.3 k/s, 92.1 ms). `a_burst_of_paxos_acquires_is_granted` (`ownership.rs`,
release) took 1.367–1.372 s before the log and 1.364–1.370 s with it, five runs each on 44fea76 and its child: the
acceptors' 1.2 s warm-up and then the burst, which does not tell the two apart. The `scale nodes` rows above were not
run again.

Each keyed start asks which node owns its key, and each lookup worked out the kind's candidates afresh: every
member's advertised kinds read and parsed, then kind, key and node id hashed for every candidate. Since f8acecd a start
also looks again under the lock every keyed start on the node takes, and `scale nodes 200` fell from 174–182 k/s to
about 102 k/s: a profile put 47 % of the processor time in the lookup. Now the candidates of a kind and placement are
worked out once per view and kept in it, read without a lock, kind and key are hashed once for all the candidates,
and the second lookup runs only when the view moved since the first. Before (9636d3f) and now, on the
EPYC 4484PX, five runs of each taken in turn:

| `scale`, 20 k keys from one node, median (range) | before | now |
|---|---|---|
| `nodes 50`, keyed actors placed | 191 k/s (190–192) | 271 k/s (268–273) |
| `nodes 200`, keyed actors placed | 102 k/s (99–104) | 231 k/s (224–244) |
| `nodes 50` under `PaxosLeases`, placed | 93.4 k/s (93.1–94.7) | 112 k/s (110–113) |
| `nodes 200` under `PaxosLeases`, placed | 48.4 k/s (48.1–49.1) | 67.8 k/s (67.4–68.1) |
| `nodes 200`, idle with the keys placed | 0.83 cores (0.77–0.86) | 0.87 cores (0.83–0.89) |
| `keyed 100000`: one node, `MemoryLeases`, 1 ms lease | 58.3 k/s (57.6–60.0) | 53.8 k/s (53.6–54.3) |

Placement kept every core busy both times (22.1 and 22.4 cores), so the gain is less work per key. Every node still
picks the same owner: the score of a key on a node is pinned (`every_version_scores_a_key_alike`), and hashing in two
steps gives the same bits as in one. The join that moves keys looks slower (1.30–1.38 s before, 1.46–1.49 s now at
200 nodes), but that row measures when the placement ends against the 1 s discovery poll: the join comes as much
earlier as the placement got shorter, and waits that much longer for the last node's poll. A build whose lookups were
slower than before placed at 58 k/s and settled in 1.20 s. All of this is one process on loopback, where 200 nodes
share 24 threads; on their own machines placement is bound by round trips, not by this lookup.

`scale keyed` is slower, and not from work: it runs 12 % fewer instructions (11.4 G against 12.9 G, `perf stat`, six
runs each) and 6 % less processor time. With 256 starts in flight and a 1 ms lease a start takes 4.4 ms before and
4.8 ms now, most of it waiting for the timer to fire the lease's sleep, which the runtime checks when a worker goes
idle or between tasks. Without the lease sleep (`scale keyed 100000 0`) the same builds run 163–172 k/s before and
171–183 k/s now; with 2 ms, 44.0–44.3 k/s before and 40.8–42.3 k/s now, the same 0.35 ms more per start. Builds that
put back each part of the old work, the candidates worked out on every lookup or the second lookup under the lock on
every start, stay at 53–54 k/s, and so does one whose spawn path does exactly what the old one did written the new
way; the build with the new placement and the old spawn path runs 57–59 k/s. So the row moves with how that function
is compiled against the timer, not with what a start costs; nothing here says how a start waiting on a real lease
behaves across a network.
The gate's `renew 100 k leases` row under `PaxosLeases` moved both ways across two runs of this change (−14 % and
+12 % against the baseline): renewals take no owner lookup, and the row's spread is wider than its band.

A join that moves a quarter of a million keyed actors: the device-twins example (`scale 1000000 12 5000`), now in
AR1011/axon-examples, whose first commit pins axon 3356677; these runs predate the move. Three twin nodes with 1 M
twins under `PaxosLeases`, then a fourth joins. Before, every move asked its new owner at once; now at most a quarter of
`max_pending` (256 here) are asked at a time. Same EPYC 4484PX, runs taken in turn:

| device-twins, 1 M twins, a fourth node joins | before (3 runs) | now (4 runs) |
|---|---|---|
| Run, 12 M reports | 123–131 s | 114–119 s |
| Reports per second, median second | 74–94 k | 107–116 k |
| Moves that failed, of about 233 k | 28–230 k | 33–69 k |

The pacing speeds up the run, and `scale nodes 50` with Paxos is unchanged (a join that moves 385 keys settles in
1.55–1.56 s before, 1.50–1.56 s now). It does not make the moves succeed: in a run that logged them, 100 of 114
failed moves were the new owner's lease acquire answered `no majority`, the rest `Full`. A key whose move failed
starts again on its next report, so none were lost. The unit test that shows the bound doing its job is
`a_join_that_moves_more_keys_than_calls_in_flight_moves_every_one`.

A link is two session ends. An idle session end costs about 65 µs of processor time a second (the 1 s keepalive)
and about 23 KB, which is what `idle_link_timeout` gives back. A sample holds about 60 rows for a node plus about 25
for each of its busiest `sample_connections` connections, of which a batch stores the ones that changed
([telemetry](#telemetry-overhead-axon-obs)). `MemoryDiscovery` has no `watch`, so joins and
leaves are seen at the next 1 s poll here; with Consul or seeds a watch reports them as they happen.

`streams`: one origin, three relays and six leaves in one process, ten cluster nodes sharing the machine's 16 threads.
Streams are named like `md:binancef.sym00042/usd.1m`, each with a 64-record ring; every leaf subscribes to every
second stream with one task per subscription. Records are 128 B, published round-robin one at a time, so a stream sees
2–4 records a second and nothing batches per stream. Each stream ends up on 1.5 relays.

| Stream tree in one process | 50 k streams | 100 k streams |
|---|---|---|
| Open the streams on the origin | 0.10 s | 0.18 s |
| Subscriptions, a branch held for each all the way to the origin | 150 k in 6.0 s at 3.1 cores | 300 k in 12.3 s at 3.1 cores |
| Interests sent for that / streams named in them | 8 k / 225 k | 14.7 k / 450 k |
| Resident, before any record / after 40 (50 k) or 20 (100 k) records a stream | 1.2 GB / 3.5 GB | 2.4 GB / 4.7 GB |
| Idle, every stream pulled | 0.005 cores, 4.5 interests/s | 0.005 cores, 4.5 interests/s |
| 200 k records/s published: 300 k relay and 600 k leaf deliveries a second | 8.6 cores, none lost, no resets | 9.0 cores, none lost, no resets |
| 4250 subscriptions a second added or dropped, half each | 1.23 cores, 2260 interests/s naming 5330 streams/s | 1.22 cores, 2240 interests/s |
| A relay killed: a branch for every subscription again, on the two left | 2.5 s, no resets | 4.5 s, no resets |
| 200 k records/s for 3 s right after that | all delivered, the last 2.1 s after the end | all delivered, the last 3.0 s after |

Before this work a child sent its whole wanted set every 2 s (112 k streams named a second at 50 k streams, idle), and
each push and reconcile walked every stream a node held. Cost now follows records and changes: 8.6 cores for 1.1 M
record-hops plus 600 k subscriber wake-ups a second is about 5 µs a hop, spread over syscalls, allocation, key
comparison and hand-off of each node's one lock, with no single hot spot in the profile. A subscription change costs
about 290 µs of processor time across the three tiers, most of it the kernel sending small interests and pushes over
loopback TCP. After the relay is killed four of the six leaves share one relay (a parent is chosen per child, not per
stream), which cannot keep up with 200 k records/s through its one lock and catches up once the burst ends. With 5 k
streams and every leaf on every stream the same process saturates at 170 k published records/s (1 M leaf deliveries/s,
10.6 cores) and falls behind without losing or resetting anything. This harness found two bugs the simulations had
not, both fixed and now tested. A heartbeat moved the child's revision but not the parent's, so a full resync followed
every heartbeat. And a push looked at only the first 1024 ready branches, so a branch with records behind that many
drained ones waited for the next wake, which showed up as deliveries arriving two minutes late after the relay loss.

A later profile of the 50 k publishing phase did show one: SipHash of `StreamKey`, 11.6 % of samples, since a record
looks its stream up in the hub, the relay's fed and watcher maps and the recovery log on every node it passes, each
hashing both names again. A key now hashes itself once when made, with a key drawn per process so a peer naming
streams still cannot choose collisions, and those maps take that hash as it is; its names are `Box<str>` behind
`namespace()` and `name()`, 40 B a key instead of 48. Same harness, both builds from one directory, four interleaved
rounds on 16 threads of the EPYC 4484PX:

| `scale streams 50000` | before | cached hash |
|---|---|---|
| 200 k records/s published | 4.97–5.12 cores | 4.76–4.98 cores |
| Subscriptions all up | 2.60–2.69 s | 2.33–2.66 s |
| Resident after subscriptions / after the relay loss | 1236–1242 / 3548–3574 MB | 1202–1205 / 3504–3527 MB |

SipHash fell to 0.6 % of samples, a key decoded from each push item, and the lookups that take its hash show 0.8 %. The
cached hash used less in each of the four paired rounds, by 0.01 to 0.36 cores and 4 % at the medians, but on one
machine over loopback, and the subscription times overlap. The table above was measured on the Ryzen, so the two are
not compared.

What is left of key handling is the ordered maps (a relay's feeds, a child's branches) comparing names, about 5 % of
samples in `memcmp`. Ordering keys by a hash of their names first, the same in every process, took that away and
still lost: four rounds each, publishing took 5.24–5.34 cores against 4.58–4.86. The profile does not place the loss
cleanly, since inlining moved between the builds; the push handler grew from 6.1 % to 8.3 % of samples. Cache misses
are the likely reason, not shown: this harness publishes in name order, so keys that were neighbours in every map no
longer are, and a publisher in some other order may not see the loss. Subscriptions came up faster (2.02–2.26 s
against 2.23–2.56 s) and a lost relay was replaced sooner (1.21–1.32 s against 1.40–1.46 s), but publishing is the
path that runs all the time, so the order stays by name.

A profile at e2dfd98 found `RelayCore::stats` copying the parent's node name into a new `String` for every stream a relay
pulls, while it held the relay's lock. The harness polls `stats` every 20 ms while it waits for subscriptions to come
up and for a lost relay to be replaced, and telemetry samples it too. Now the name is copied once per parent. Five
interleaved rounds against e2dfd98, cores 0–15; counting streams without a parent in the same walk, instead of a
second one, came after these rounds:

| `scale streams 50000` | before | after |
|---|---|---|
| Subscriptions all up | 2.36–2.69 s, median 2.48 | 2.08–2.60 s, median 2.20 |
| A relay lost, every subscription fed again | 1.40–1.47 s | 1.35–1.40 s |

Failover was sooner in all five paired rounds, and subscriptions came up sooner in four of the five. Those phases time
the harness's own polling as well, so this shows what `stats` cost under load, not a faster protocol. Publishing does
not poll `stats`, and its 4.66–4.83 cores against 4.72–4.97 is not claimed.

The same profile had `malloc` and `free` at 5 % between them, `malloc` mostly under the push handler (1.1 %) and the
pump that builds pushes (0.7 %). Both allocated for keys on every item, though inlining hides how much: every item
a relay pushes cloned its `StreamKey`, two `Box<str>` copies, and every item it took in built one from two `String`s
that the decoder copied out of the frame through a `Vec`. A key's names are now `Arc<str>`, so a clone is two reference counts, and
the decoder copies each name straight from the frame into its `Arc`. Five interleaved rounds against f373f61, cores
0–15:

| `scale streams 50000` | before | `Arc<str>` names |
|---|---|---|
| 200 k records/s published | 4.57–4.87 cores, median 4.80 | 4.29–4.58 cores, median 4.52 |
| Subscriptions all up | 2.69–2.80 s | 2.32–2.47 s |
| A relay lost, every subscription fed again | 1.37–1.44 s | 1.03–1.07 s |
| Resident after subscriptions / after the relay loss | 1199–1207 / 3495–3525 MB | 1095–1122 / 3390–3432 MB |

Each phase was faster in all five paired rounds. The lower resident sizes are likely the maps that hold one stream's
key now sharing its names, not shown. The subscription and failover phases also time the harness's `stats` polling,
as above.

Decoding still allocates a namespace per item, though the items of one push nearly always share it. Reusing the
previous item's namespace when the bytes match was tried and not kept: over five rounds against the above, publishing
took 4.28–4.60 cores (median 4.40) against 4.40–4.64 (median 4.48) and was lower in four, while failover was slower in
all five, 1.06–1.11 s against 1.04–1.07. Neither difference is clear of the spread.

`subscribers`: many local subscribers on one node, as a websocket gateway holds them. 20 k subscriptions, half on each
of two streams, one task each, publishing one 128 B record per batch from a task on the runtime, so every publish
wakes 10 k tasks. "ns per record" is the whole process's processor time over records delivered; two 10 s runs of
each, before and after alternating, which agreed within 10 %. "Before" is 3743c40, where every `Subscription::next`
took the node's one core lock to poll its batch out of the hub, so 10 k woken tasks queued on it; a profile of a
websocket gateway showed that lock's slow path costing about as much as the socket writes. Now the publish that
wakes a subscription moves the batch into that subscription's own inbox while it already holds the lock, and the
subscriber takes only its inbox.

| 20 k subscribers, 2 streams, asked for | before | now |
|---|---|---|
| 50 batches/s (0.5 M deliveries/s) | 10.6 µs/record, 5.3 cores | 2.0 µs/record, 1.0 cores |
| 100 batches/s (1 M/s) | 5.2 µs/record, 5.2 cores, last 42 ms late | 2.1 µs/record, 2.1 cores |
| 200 batches/s (2 M/s) | 1.2 k–3.3 k subscribers reset | publisher held to 180/s: 1.8 M/s, 1.8 µs/record, 3.3 cores, no resets |
| 400 and 800 batches/s | 18 k–20 k of 20 k reset | publisher held to 183–189/s, everything delivered, no resets |

The ceiling moved from the subscribers to the publisher. Before, a publisher went on at any rate and the subscribers
that could not keep up were reset by their branch limits; now the publisher fills every woken inbox itself, so one
stream with 10 k local subscribers publishes at most about 190 batches/s on this machine (1.9 M deliveries/s) and
nobody falls behind. Waking each subscriber as its inbox is filled measured faster than waking them all once the lock
is free (726 against 982 ns/record on an M5), because the first ones run while the rest are filled. The hub still
does work per subscriber per publish, so a stream's cost grows with its subscribers. An inbox takes a fill only when
empty, so it holds at most what the hub had queued, and a subscriber that stops reading is still reset by its branch
limits.

A keyed actor is named `kind/key` now, which costs each keyed start a string and an entry in the system's name
table. `scale keyed 100000` (one node, `MemoryLeases`, 1 ms lease) on the EPYC 4484PX, three runs each against
fa66963, load under 1.5: 54.4–55.2 k/s before, 53.4–55.0 k/s now. What it establishes: the name is lost in the
run-to-run spread of a keyed start. What it does not: calls to a running key, which never reach the name.

## Whole-cluster simulation (`axon-testkit`)

What a seed of the World costs, which sets how many seeds a sweep can afford
([spec/simulation.md](../../../spec/simulation.md)). The scenario is
`axon-testkit/tests/world.rs::three_nodes_on_memory_discovery_reach_a_steady_view_and_answer_a_keyed_call`: three nodes,
`MemoryDiscovery` polled every 20 ms, default heartbeats, the view settles, 2 s more, then keyed calls until one lands
on b and one answered call from c. That is 2.07 s of virtual time in 2 071 quanta for every seed. "Steady 60 s" is the
same test with 60 s in place of the 2 s. `SEEDS=0..20`, `--test-threads=1`, two rounds each on the AMD EPYC 4484PX box
(load 0.4–1.5), the time the test harness reports divided by 20.

| Run | debug | release |
|---|---|---|
| proving scenario, per seed | 27.5 ms | 2.0–2.5 ms |
| steady 60 s, per seed | 718–733 ms | 49 ms |
| steady, per virtual second of 3 idle nodes | 12.0–12.2 ms | 0.81 ms |
| 1 000 crash and restart cycles of one of 2 nodes, whole test | 6.27–6.33 s | 0.46 s |

What this establishes: stepping 3 idle nodes runs about 1 200 times faster than real time in release and 80 times in
debug. That is the floor of any scenario: once the view has settled on `MemoryDiscovery`, the nodes send no byte over
the World's wires (a 60 s trace holds only the three boot lines), so the steady rows measure the stepping alone. What it
does not establish: the cost of a cluster that talks, and so whether the spec's 2 000 seeds per World test a night fit.
Paxos, seed servers, streams and the faults of S5 add traffic, and the cost grows with the bytes and wakes per quantum,
not with virtual time alone. Each scenario's seed count is set from its own run time. Nothing here is a statement about
axon's speed: virtual time says nothing about it.
