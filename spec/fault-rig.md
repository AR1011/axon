# Real-fault rig

Keyed accounts on `PaxosLeases`, streams relayed between the same nodes and the rest of axon's surface run as five
real processes, under a seeded schedule of real faults; afterwards a checker decides from the logs alone whether
anything the leases, the fencing epoch, the streams and the other workloads promise was broken, and whether every
key and stream was served again in a stated time. Code: `examples/rig` (the Rust workloads), `examples/rig/ops`
(`fault.sh`, `nemesis.py`, `check.py`, `run.sh`, `setup.sh`), `scripts/rig.sh` (the laptop's driver).

The rig covers what the World ([simulation.md](simulation.md)) cannot: real kernels and TCP (retransmission,
keepalive against a silent machine, reordering inside a connection), real threads and the blocking pool, real clocks
(`SIGSTOP` against `CLOCK_MONOTONIC`, tick drift, wall steps), real process limits (`SIGKILL` mid-write, cgroup
throttling, OOM, systemd restarts), real disks under the application's journal and axon's stream history, and hours
at scale. It does not replay an interleaving: a seed gives the same fault schedule, not the same run. A finding
becomes a World test where the World can express its faults, and a `scripts/rig.sh replay` command otherwise.

Not covered: a machine crash (on one box it takes the store too; `powercut` covers its disk half), the websocket
gateway and `MetricsService` under faults, floods at volume (`peer` sends malformed input at low volume), and the
wire-version window (`axon_net::VERSIONS` is one version, so `roll` covers schemas side by side only).

## Layout

```
laptop: scripts/rig.sh  ── ssh b1: ~/rig/run.sh <mode> <seed>   (setsid nohup; the run never depends on the laptop)
b1 (control + node): rig store :7900 · nemesis.py · check.py · probe.py · clock reference
b1..b5 (nodes):      unit rig-node: rig node → cluster :7400, seed server :7001, Health :7401, acceptor on b1..b3
                     (its lease log in LOG_DIR/leases),
                     journal on /mnt/rig-journal and disk streams on /mnt/rig-streams (ext4 on dm over a loop file)
```

- **The fencing store** (`src/store.rs`, on b1) is the World's `Ledger` oracle made real: it refuses a write from an
  epoch older than the newest owner it has seen, and logs every request and verdict to `store.jsonl`. Its clock is
  the run's timeline. Nothing faults it: network faults match ports 7400 and 7001 only, never 7900 (store), 7401
  (probes) or 22, clock faults never touch b1, cgroup and disk faults target the node's unit and its own dm devices.
  A store that fails voids the run rather than failing it.
- **The node** runs under a systemd unit (its own cgroup, so `set-property --runtime` and the freezer reach it alone),
  with seed discovery on every node, `PaxosLeases` acceptors b1..b3 (`ACCEPTORS=5` for all five), lease
  `LEASE_SECS` (`T`), plaintext with a shared secret behind an allowlist of the five addresses (`TLS=1` for mutual
  TLS with the CA rotated mid-run; `SEEDS=n` for seed servers on b1..bn only).
- **Shell for mechanisms, Python 3.9 stdlib for the schedule and the checker, Rust for the workload.** No new crate
  dependency.
- **One box** (`RIG_BOX=<ip>`, `setup.sh --netns`): the five nodes are network namespaces `b1..b5` at 10.77.0.1..5
  on bridge `rig0`, units `rig-node@bN` on CPUs 1-7, the store, nemesis and checker on CPU 0, `~/rig/in.sh` in place
  of ssh, and every per-node name (unit, dm device, mount, env, fault state) suffixed by `ops/node.sh`. The box shares
  one clock and one kernel, so it runs the 57 classes not tagged `five-box-only` or `global`; its timings are rerun on
  five boxes before they are written up. With `RIG_BOX` unset every name is the plain one and plans are the same bytes.
  Each namespace gets [wan.md § 8](wan.md#8-host-tcp-settings)'s TCP settings (bbr, no slow start after idle,
  `tcp_wmem` and `tcp_rmem` to 64 and 128 MiB), which Linux keeps per namespace; `doctor` prints them per node.

Runs land in `~/rig/runs/<run>/` on b1 and `target/rig/<run>/` on the laptop, never committed; `<run>` is
`<mode>-<yyyymmdd-hhmm>-s<seed>`. `meta.json` records seed, mode, commit, binary and plan sha256, plan version, nodes,
`one_box`.

## Running it

```bash
export RIG_BOX=<ip>                 # or RIG_NODES="ip1 .. ip5" (b1 first) and RIG_BUILD=<a box with Rust>
scripts/rig.sh setup                # tools, namespaces or boxes, journals, units; then doctor (every node: missing [])
scripts/rig.sh build [plant <name>] # build rig and rig-v2 on the build box (RIG_SRC=<dir>), install on every node
scripts/rig.sh doctor faults b2     # every class on b2's stand-in, seen by its own probe; the other nodes clean
scripts/rig.sh doctor combos b2     # pairs stacked on b2 and b3, off in both orders; every node clean after each
scripts/rig.sh smoke [seed] [min]   # waits, fetches, exits 1 unless PASS
scripts/rig.sh heal                 # fault.sh heal on every node, prints each check
```

The header of `scripts/rig.sh` lists every command (`soak`, `night`, `replay`, `chaos|pairs|scenarios`,
`coverage`, `status`, `fetch`, `gate`, `down`, `teardown`). A run starts on b1 and runs there alone (`~/rig/run.lock`);
`rig.sh` only starts it and polls for its bundle, so a dropped ssh costs nothing. On a shared box, hold
`flock ~/rig-box.lock` around a whole sequence and release it only after `heal` prints every node clean.

| Mode | What runs | Time |
|---|---|---|
| `smoke [seed] [min]` | ledger load (500 keys, 5 s per node), `T` = 2 s, 1 min steady; every class once (`ONLY=a,b` those, in rounds for `min`), and from plan version 2 a mix of one pair cell per relation, two scenarios and a flap (`SMOKE_MIX=0` leaves it out); 1 min settle | 10 to 30 min |
| `soak [min]` | serve load alone (10 000 keys at 1 s per node, `SOAK_KEYS`), the last node held to 4 cores for the second half | 30 min |
| `night [seed] [min]` | a soak of `min/5`, then 2000 keys at 20 s per node, `T` = 6 s: 5 min steady, `min` (150) of mostly single faults far apart (a shorter night is a prefix of a longer one), 3 min settle | 3 h 25 min |
| `chaos\|pairs\|scenarios [seed] [min]` | a smoke's phases around a schedule (§ Schedules); `TIMING=night` a night's; `TIMING=wan`, scenarios only, a night's at `T` = 10 s on the WAN profile (*Across regions* below) | `min` + 3 min; wan about 80 min |
| `replay <run> <i> [n]` | that run's plan version and parameters on a fresh cluster: episode `i` with the episodes whose aftermath reached it, `n` (5) times `B + 15 s` apart; prints how many repetitions some violation named | 5 + 1.5 n min |

Knobs, in any mode: `WORKLOADS=` (below), `RIG_N=n` (the first n nodes), `PLAN_VERSION=v`, `TLS=1`, `SEEDS=n`,
`ACCEPTORS=5`, `FIREHOSE_MBS=` (the `firehose` workload's rate), `FIREHOSE_BRANCH_MB=` (every node's branch
bytes, 32), `RIG_LOCK=<file>` (a run starts only while it holds
that flock on the box, which it shares with benchmarks).

**Reading a verdict.** `summary.txt` names the run, the episodes run of planned per class, every fault command that
exited non-zero, forced heals, end clock offsets, the verdict and the first violation (invariant, time, episode,
window). `check.txt` holds each invariant's PASS or FAIL with what it judged, the first violation with its context
(the key's store records around it, the ops' client and journal lines, the nodes' `start`/`stop` and `stats`, every
episode overlapping it), then the reports: recovery per class against its bound (`OVER` is a report, not a failure),
latency during faults, blast radius, explained refusals, resources per incarnation, refusal kinds, fault effects
witnessed per class. Every violation is in `violations.jsonl`. Verdicts, in order:

| Verdict | Exit | Means |
|---|---|---|
| `UNREADABLE` | 2 | the logs cannot be judged (no `send` read while the store accepted appends, a `node.jsonl` with no `up`) |
| `VOID` | 3 | `store-void`: the store's log does not hold together, so nothing judged on it counts |
| `FAIL` | 1 | a violation outside an assumption probe's episode (probe violations are reported apart) |
| `UNJUDGED` | 2 | below a floor (`FLOORS`, applied only where the workloads in `FLOOR_WORKLOAD` ran), or too little steady time before the first episode |
| `PASS` | 0 | none of the above; `run.sh` writes `ABORTED` when a run could not finish |

`scripts/rig.sh gate <run>` compares a night's recovery per class with `examples/rig/baseline-night.json` (night
2609, five boxes, `T` = 6 s) and exits 1 on a REGRESSION: a class with 5 or more episodes in both whose p95 or max
took over twice the baseline's plus 1 s.

## Workloads

`WORKLOADS` lists what each node runs, each a `start(&Ctx) -> Tick` in `src/<w>.rs`, one arm of `main.rs`'s match,
writing `LOG_DIR/<w>.jsonl` and bumping the tick that `heartbeat` watches. The default is `accounts,streams` under
`LOAD=ledger` and `accounts` under `LOAD=serve`. The node's settings are the environment variables at the top of
`src/main.rs`, which `run.sh` writes to `~/rig/env`.

| Workload | What it runs |
|---|---|
| `accounts` | kind `acct`, keys `k0..`, through the store and a journal per node, and a client task per key on every node |
| `streams` | `STREAMS` (4) streams per node, 20 records a second each, followed by every other node through the next node as relay; even followers `Live`, odd ones `After` their cursor and dropping it every 10 to 30 s |
| `quota` | before `streams`: `Quotas` rates `q0`, `q1`, `r0` and permits `p0`, `p1` across the nodes, and the stream node's limits on namespace `lim` |
| `disk` | after `streams`: streams `disk-<node>/s0..1` kept on the streams disk (`DiskConfig`, 8 MiB retention), resumed after each crash |
| `compacted` | after `streams`: one `Compacted` stream per node (32 keys at most), followed by `SnapshotThenLive` rounds |
| `eager` | kind `eag`, `.eager(..)`, `EAGER_KEYS` (100) keys b1 starts once a run and nothing calls after |
| `watch` | `watch_key` on `WATCH_KEYS` (20) accounts |
| `echo` | a named actor called every 500 ms with `request_within(1 s)`, `resolve_all` every 10 s, `spawn_on` of an idle-3 s kind every 2 s; the `authz` fault's file as its `Authorizer` |
| `idle` | kind `idl` with `.idle(2 s)`, called about every 2 s so calls race drains |
| `blob` | 64 KiB to 4 MiB echoes to every other node once a second, checked byte for byte |
| `local` | a 100 ms timer, a topic with plain and supervised flaky subscribers, a round-robin router |
| `bulk` | while the `bulk` fault's file exists, b1 uploads 10 MB bodies on `Lane::Bulk` to every other node, two at once per peer, each answered with its CRC ([lanes.md § The rig (D3)](lanes.md#the-rig-d3)) |
| `firehose` | b1 publishes `FIREHOSE_MBS` (13) MB/s of 4 KiB records on one stream in namespace `firehose` (on Bulk), followed `Live` by b2 and b3; each logs bytes, lag and its last cursor per second, a `gap` line for records skipped and a line for each `Ready`, `OriginLost` and `Replacement`, every node its push windows ([push-window.md § The rig (D3)](push-window.md#the-rig-d3)) |

**The accounts' commit point.** An account loads its records from the store in `on_start`; an op is one store
append (`submit_io`, `pause_until`), then one journal line and `fdatasync`, then the reply. The store's `OK` is the
commit; nothing is answered from memory before it. A retry with the same op id is answered from `done`, which keeps
an op for `KEEP` = 300 s: a try begins its turn at most 125 s after the op's record (120 s of client retries, a 5 s
`request_within` whose deadline travels), 138 s under 10 % drift, so a forgotten op's late try would need a frame
held 127 s, and `applied-twice` catches one anyway. Clients use op ids `<node>.<boot ms>.<n>`, 60 % deposits and
40 % withdrawals, write lines unbuffered, and give up after 120 s. Transfers between accounts are not modelled: the
rig checks conservation per key and in total.

Every node also serves `/healthz`, `/readyz` and `/metrics`, which `probe.py` on b1 polls with a 0.8 s timeout from
outside the process, and logs `stats`, `res` and `dead` every 10 s to `node.jsonl`. It polls `LOG_DIR/ctl` for a
`hook`. `SIGTERM` is a clean leave ending in `system.shutdown().await`, logged as `down`.

**Logs.** Every line is JSON with `t`, wall µs on its writer's clock, and its event in `r` (the nemesis's in `ev`).
The writers define the fields; the files are `store.jsonl`, `nemesis.jsonl`, `plan.jsonl`, `meta.json`,
`probes.jsonl` and `tls.jsonl` on b1, and per node `client.jsonl`, `node.jsonl`, `<workload>.jsonl`, `peer.jsonl`,
the journals (`journal-<run>.jsonl`, archives of a reset beside them), `unit.jsonl` and `kernel.jsonl` (the unit's
and the kernel's journal since the run began; one kernel file per box on one box).

## Fault classes

Every class has `on`, `off`, `check` and `heal` in `fault.sh` (or is composed of other classes by `nemesis.py`'s
`steps()`), a row in `TAGS` and the newest `PLANS` version (weight and duration range in seconds), a probe in
`doctor faults`, and evidence in `check.py`'s `EFFECT`. Tags: `network`, `disk`, `clock`, `restart` (the process
ends), `global` (changes the box), `five-box-only`, `probe` (outside what axon states: reported apart, never in a
combination), `wan` (in `nemesis.WAN`, outside the plan versions: only `TIMING=wan`'s scenarios draw it). Plan version 1 froze the first 22 classes (bold). Disk classes take `-d journal|streams` (`readerr`,
`diskcorrupt` and `inodes` only `streams`). Evidence: *silent* (nothing logged from 200 ms after the on to the off),
*restarted* (an `up` in the fault), *cut* (no op crossed the cut), *delayed* (ops to the node took half the delay),
*redialed* (`net.dials`, `net.dial_failures` or `became_unreachable` moved), *journal_err* (failed journal writes
where the node wrote records), *Disk(..)* (a streams publish refused so); none means it counts unwitnessed.

| Class | Tags | On / off | Drawn | axon promises | Evidence |
|---|---|---|---|---|---|
| **kill** | restart | SIGKILL the main process, held down / start | hold 0-30 | a crash: keys free `T` after the last renew | silent, restarted |
| **pause** | | SIGSTOP / SIGCONT | 1-20 | fences checked before each turn; a handler mid-pause finishes late and only the store refuses it | silent |
| **freeze** | | cgroup freezer / thaw | 1-20 | as `pause` | silent |
| **leave** | restart | `systemctl stop` (SIGTERM) / start | 5-30 | a clean leave hands keys over at once | silent, restarted |
| **isolate** | network | drop the rig ports in and out | 5-40 | the node stops its keyed actors within 7 s; `/readyz` 503 | cut |
| **partition** | network | the smaller side drops the other, 1\|4 or 2\|3 | 5-40 | the majority side takes the keys after the leases lapse | cut |
| **oneway** | network | A drops output to B | 5-40 | as `partition` | cut |
| **bridge** | network | {a,b} and {d,e} drop each other, c reaches all | 10-40 | majorities overlap in c | cut |
| **netem** | network | delay, jitter, loss, reorder, duplicate on band 4 (round trip under 1 s) | 10-60 | TCP hides it; a Paxos round waits `T/6` | delayed |
| **rate** | network | `netem rate` 0.5-10 mbit | 10-60 | a slow receiver holds credit, not memory | |
| **cpu** | | `CPUQuota` 2-50 % | 5-60 | a starved node isolates itself like a cut one; fences hold | |
| **oom** | restart | `MemoryMax` 60-90 % of anon: killed until off | 10-40 | a crash | restarted |
| **memhigh** | | `MemoryHigh` 70-90 % of anon | 10-60 | as `cpu` | |
| **io** | disk | `IOWriteIOPSMax` 5-50 | 10-60 | slow work is on jobs; handlers stay free | fsync p99 |
| **slowdisk** | disk | dm `delay` 20-500 ms | 10-60 | as `io` | fsync p99 |
| **diskerr** | disk, restart | dm `flakey` error_writes; reset at off | 5-30 | a failed `fdatasync` is no reply, a failed actor, a rebuild | journal_err, Disk(Failed) |
| **diskdrop** | disk, restart | dm `flakey` drop_writes; reset at off | 5-30 | none: a disk that lies, which the checker must see | journal lines gone |
| **diskfull** | disk | a filler of the space left | 10-60 | as `diskerr`, without a reset | journal_err `No space`, Disk(Full) |
| **step** | clock, five-box-only | `date -s` ±0.5 to +300 s | 10-60 | exclusivity rests on drift, never on clock values | |
| **drift** | clock, five-box-only | `adjtimex --tick` ≤ 0.4 % | 30-120 | `PaxosConfig::drift`, 1 % | |
| **drift-far** | clock, five-box-only, probe | tick 1-10 % | 30-120 | outside the bound: measures the margin | |
| **skew-restart** | clock, five-box-only, probe, restart | b2 and b3 stepped back `2T`, killed 2 s; clocks back after `T + 5 s` | 20-30 | an older epoch needs restarts plus skew over `T`, reported as `clock behind` | restarted |
| authz | | the refused peers in `$state/authz`, read by `echo` | 5-40 | `set_authorizer` checks every later call; what was admitted finishes | `authz` lines within 3 s |
| peer | | `rig peer` at the node's port: secret, garbage, truncated, oversized, stall, fuzz, impostor, stale | 5-30 | refused by the handshake or framing, no real session lost; a stale incarnation handed no calls | handshake counters moved |
| powercut | disk, restart | freeze, drop writes, SIGKILL, unmount / fsck, mount, start | 5-30 | what was synced survives | silent, restarted |
| diskhang | disk | `dmsetup suspend` | 5-20 | publishes past `max_unwritten_bytes` refused `Disk(Behind)` | fsync max; Disk(Behind) |
| rofs | disk, restart | ext4 error (`trigger_fs_error`) / reset | 5-30 | as `diskerr` | journal_err, Disk(Failed) |
| flaky | disk, restart | dm `flakey` up 1-5 s, down 1-3 s / reset | 10-60 | a failed write is `Disk(Failed)`, judged where the table was down 2 s before the off | journal_err, Disk(Failed) |
| splitdelay | disk | dm `delay` reads 0-100, writes 50-500 ms | 10-60 | as `slowdisk` | fsync p99 |
| readerr | disk, restart | pages dropped, error_reads, SIGKILL / reset | 5-30 | a start that cannot read its segments fails and retries | restarted, `Input/output error` |
| diskdead | disk, restart | dm `error` / reset | 5-30 | as `diskerr` | journal_err, Disk(Failed) |
| diskcorrupt | disk, restart | byte 32 of every write set / reset | 5-30 | none: a start cuts a stream at its first bad crc | `truncated` at the next start |
| inodes | disk | empty files to `ENOSPC` | 10-60 | a segment that cannot be made is `Disk(Full)` | Disk(Full) if seen |
| roll | restart | each node in turn to the other build (`exe` link), `2T + 1` s apart | 30-60 | schemas side by side; keys go to nodes serving the most callers' schemas | restarted, upgraded |
| cfgdrift | probe, restart | `env.override`: `LEASE_SECS` 2T or T/2, `HEARTBEAT_MS` 250 or 2000 | 30-120 | none: settings that differ are outside the contract | restarted, drifted |
| scale | restart | stop, held / start | 60-300 | a leave of its keys and a join | silent, restarted |
| cold-restart | restart | every node killed or left / started together or 1 s or `T` apart | 10-40 | nothing owned until warming ends, then epochs still grow | restarted |
| acceptors-restart | restart | b1..b3 killed at once | 5-20 | warm up and serve again | restarted |
| acceptors-rolling | probe, restart | b1..b3 killed in turn, faster than warming | 5-20 | none: the README asks for one after `warming()` | restarted |
| leave-minority | restart | partition, then a leave inside the minority | 5-30 | a clean leave ends in bounded time with no one to hand to | cut, leaver restarted |
| leave-slowdisk | disk, restart | slowdisk 50-500 ms, then a leave | 5-30 | as `leave-minority` | restarted |
| crashloop | restart | SIGKILL every 300, 800 or 2000 ms | 10-40 | a restart storm leaks nothing into the peers | restarted |
| killterm | restart | 1 or 5 SIGTERMs, SIGKILL 1-3000 ms later | 5-20 | a shutdown cut anywhere is a crash | restarted |
| tasksmax | probe | `TasksMax` = tasks + 0, 4, 16 | 10-40 | none: a thread the runtime cannot start | |
| nofile | | soft `RLIMIT_NOFILE` = fds + 0, 4, 16 | 10-40 | `EMFILE` accepts and dials retried | |
| threadstarve | | clock, tokio and worker threads pinned to one core beside a `SCHED_FIFO` hog | 5-40 | as `cpu`: fences go by the clock, not the timer | |
| reclaim | | `memory.reclaim` every 200 ms | 10-60 | as `memhigh` | |
| hook | | `<hook> <ms>` in `LOG_DIR/ctl`: panic, panic_start, panic_job, block, jobsleep (`src/hooks.rs`) | 5-30 | a panic restarts the actor, not the node; a block slows only its key | `hook` line |
| coredump | five-box-only, global, restart | `core_pattern`, SIGABRT | 5-20 | as `kill` | restarted |
| swap | five-box-only, global | 2 GiB swap, `MemoryHigh` 30-60 % | 10-60 | as `memhigh` | |
| thp | five-box-only, global | THP always or never | 30-120 | latency only | |
| takeover-step | clock, five-box-only, restart | kill one node, step another −2..+5 s | 10-30 | as `step` and `kill` | restarted |
| drift-edge | clock, five-box-only | two nodes ±0.49 % | 30-120 | the edge of the 1 % bound | |
| step-huge | clock, five-box-only, probe | step ±3600 or +86400 s | 10-30 | epochs `max(now, seen + 1)` | |
| leap | clock, five-box-only, probe | a leap second inserted at midnight | 20-30 | as `step` | |
| reject | network | reject with tcp-reset or icmp | 5-40 | a refused dial retried with backoff | cut, redialed |
| syndrop | network | drop SYNs; with reset 1 abort sessions up | 5-40 | sessions up keep working | redialed (reset 1) |
| halfopen | network, restart | drop FIN and RST, SIGKILL | 10-40 | peers learn by heartbeat, not by a close | restarted |
| portcut | network | drop 7400 or 7001 only | 5-40 | membership and the data path fail apart | cut (7400) |
| graylink | network | drop 30-90 % of packets to a peer | 10-60 | no key flaps faster than its lease | |
| flap | network | a rule added and removed every 200-2000 ms | 10-60 | membership does not flap with it | |
| mtu | network | drop packets over 576, 1200 or 1400 B | 10-60 | small frames pass while large ones stall | |
| asym | network | delay 200-900 ms one way to one peer | 10-60 | as `netem` | delayed, to that peer |
| dup | network | `netem duplicate` 30-100 % | 10-60 | TCP drops duplicates | |
| corrupt | network | `netem corrupt` 1-20 % | 10-60 | a corrupt segment is lost, never delivered | |
| conntrack | network | a stateful firewall that lost its table | 5-30 | stalled sessions are given up and redialed | redialed |
| backlog | network, restart | `somaxconn` 1, then SIGKILL | 10-40 | dials retry and the cluster forms | restarted |
| ports | network | `ip_local_port_range` of 4 or 16, sessions aborted (never b1) | 10-40 | a dial with no port retried; no port leak | redialed |
| geo | network, wan | a `prio` qdisc with a `netem` delay per destination region (b1 and b4 eu, b2 sin, b3 and b5 us; 46, 95, 109.5 and 0.12 ms one way), `rate=` Mbit/s and `loss=` % in each band, `to` some peers only; under `TIMING=wan`'s backdrop it changes the bands' rate and loss only | scenario | [wan.md § 12](wan.md#12-the-rig-work-item-d3) | |
| bulk | wan | the `bulk` workload's file (`$state/bulk`): b1 uploads while it exists | scenario | Bulk does not hold Live up | |

A fault whose on was done under 1 s before its off was sent is counted unwitnessed. A contradicted fault fails
`fault-effect`; a run must witness `FLOORS`' count (or what its plan can) or it is UNJUDGED.

**Composing.** `fault.sh -i <id> on|off <class>` names each fault so they stack on a node and each off undoes only
its own: rules carry `rig:<id>`; a resource one fault holds at a time (`SOLE`: the qdisc, a cgroup limit, a disk's
table or filler, the tick, a sysctl) is claimed in `/run/rig-fault/own/`, a second claim exits 3 and a stranger's off
prints `noop .. held by fault <id>`; a held kill or leave writes `hold/<id>` and the unit starts at the last release;
clock faults keep `clock.d/<id>` and the last off restores realtime from the saved raw clock; `toggle <ms> <class>`
puts a fault on and off in a transient unit. A stop of a unit not running prints `noop inactive`. One `fault.sh` runs
per node at a time.

**Reset** (at the off of `diskerr`, `diskdrop`, `powercut`, `rofs`, `flaky`, `readerr`, `diskdead`, `diskcorrupt`,
and by `heal` of an errored disk): stop the unit, unmount, `e2fsck -fy`, mount; failing that, archive what reads to
`runs/<run>/<disk>-archive/<n>/` and `mkfs`; log `<disk>-reset fsck|mkfs`. The checker treats a reset as a kill.

**`check`** is clean when the rig chains, qdiscs, cgroup limits and ledgers are empty, every dm device is resumed,
`linear` and mounted without a filler, sysctls and the tick are back, timesyncd and (while `~/rig/up` exists) the unit
are active, and from b1 the offset is under 50 ms. **`heal`** resumes a suspended device first, then undoes all of
it. After every episode the nemesis runs `check` on every node (a forced `heal` if dirty, which fails `nemesis`) and
samples clock offsets; `run.sh` heals every node on exit.

## The nemesis and its schedules

`nemesis.py plan <seed> <mode> [n] [min]` writes JSON lines of episodes `{i, at, dur, faults: [{class, nodes, args,
at?, trigger?}]}`, drawn from `random.Random(seed)` in a fixed order: the same seed, mode, node count and plan
version give the same bytes. `PLANS` is versioned: version 1 is frozen and `nemesis.py selftest` holds its digests,
so an old night replays byte for byte; a new class or draw goes only into the newest version, which `meta.json`
records and `replay` reads. On n nodes or one box the plan keeps only the episodes it can run, each with its gap, so
it is a subsequence of the full plan.

**Legality** (`legal`, `timeline_legal`): no probe beside another fault; per node no two faults on one `SOLE`
resource, no stop beside a restart, no `STOPS` class twice, no two restarts unless both are in `LEDGER`, nothing beside
a `roll` but one fault off the rolled node, no restart of a clock stepped back; checked at every moment a fault goes
on, a role counting on all of its `among`, a trigger over its whole window. `fault.sh` refuses the first of these
itself (exit 3). A schedule draws only from `PLANS`, never a probe, and only classes that fit `RIG_N` (`NEEDS`).

**The timeline.** A fault's `at` is its offset in its episode; its off is due `dur` after its on was due. Events due
together run in plan order, an off before an on; each step runs on its own thread and waits only for earlier steps on
its nodes. Each on and off logs `plan_ms` and `slip_ms`. A **trigger** `{log, when: [patterns], within, delay}` puts
a fault on when a store or node log shows the sequence (fields matched by value, list or `$name` bound on first
match; an accepted own carries `prev`, its previous owner), or at `within`. A **role** in `nodes` (`@owner:k7`,
`@trigger`) resolves when the fault goes on, to `among[0]` if it names no node in `among`; `ROLES` maps the names.

**Edge durations** `edges(T)`: the renew interval, the grace G = 7/8 (0.99 T − 0.2), T, W = 1.01 T + 0.2, 2T, 3T, and
1 to 10 s, each also 2 % and 10 % short and long; an `EDGE` class takes one with probability 0.3 in chaos and pairs.

| Schedule | Episodes |
|---|---|
| night | 85 % single faults by weight, 15 % two or three; per-class durations; quiet gap `B + 15 s` ± 5 s so each recovery is measured alone |
| smoke | every class once in seeded order, 6 s each, quiet 12 s; the mix (above) from version 2 |
| `chaos` | clusters of arrivals every 2T on average for 10T to 50T, at most three on, one in five of a `ROLE` class on a key's owner; one episode in ten correlated (one class on three or more nodes); three in ten cascade within `B`; a rest of 2 (B + 20) s at least every 10 min |
| `pairs` | every feasible cell (an unordered class pair in relation `same`, `apart`, `stagger` or `cascade`, placeable legally in 50 draws) in a fixed order, `SHARD=k/m` every m-th from the k-th, two disjoint cells per episode, `B + 5 s` apart |
| `scenarios` | named sequences from `library(T)` swept over their parameter, `SCENARIOS=a,b` to pick: ownership moves (kill after takeover, double hop), stream origin and relay kills at open, heal, pause and freeze at expiry, acceptor minority, majority down, cold start, rolling acceptors, partition switch, flaps, grey and stacked faults, whole-cluster netem, cpu and rate, correlated classes; two clock scenarios on five boxes |

**Across regions** (`TIMING=wan`, [wan.md § 12](wan.md#12-the-rig-work-item-d3)). `scenarios` only, at `T` = 10 s,
with `TIMINGS=wan` in every node's env: `src/main.rs` sets the WAN profile of wan.md § 9 (keepalive 1 s, idle 8 s,
connect 3.5 s, heartbeat 500 ms and grace 2 s, operation timeout 7 s, the seed timings, the relay timings) and caps
each Bulk session at 16 MB/s. `WORKLOADS` is `accounts,streams,eager,echo,bulk,firehose` unless set. It plans only these, all of them unless `SCENARIOS` picks. `geo`'s delays are on from before
the nodes start until they stop (`nemesis.py backdrop`, wan.md § 12); each scenario's `geo` spans its fault from 30 s
before to 30 s after (A2 is b2, sin), and one with `rate=` or `loss=` changes the bands for that span:

| Scenario | Faults inside `geo` |
|---|---|
| `geo-quiet` | none: 20 min of `geo` |
| `geo-partition-short` | `partition` A2 from the rest for `G − 1` s |
| `geo-partition-long` | `partition` A2 for `3T` |
| `geo-syndrop` | `syndrop` on A3 to N2 for 30 s, sessions kept, with a 10 s `isolate` of N2 in it |
| `geo-loss` | `geo loss=2` on A2's links both ways, all 5 min |
| `wan-bulk` | `bulk` for 9 min |
| `rate-bulk` | `bulk`, b1's bands at `rate=150` (over the 16 MB/s cap) |
| `rate-bulk-uncapped` | `bulk`, b1's bands at `rate=100` (under the cap): a probe |

`geo` without loss and `bulk` are the night's backdrop, not episodes: the checker keeps them apart (`ambient`), so a
quiet window under them is quiet, except in `rate-bulk-uncapped`, whose episode they stay.

`nemesis.py coverage <run>..` (`scripts/rig.sh coverage`) derives the cells runs actually covered from their nemesis
logs and prints them against the feasible ones per relation; runs must share plan version, lease, box and nodes.

## Invariants

`T` is `LEASE_SECS`; `B` = 5T, the recovery bound after an episode; `D` the bound during one (`3T + 2 s` for `kill`
and `oom`, 3 s for `leave`, `3T + 5 s` otherwise). Episodes that overlap, or begin before the last heal ended, are
judged as one. *Exempt* means the node was stopped, starved, restarted, healed or had its clock moved in the window;
*quiet* means no episode active or ended in the last 30 s, and not a phase's first 60 s. Store invariants (marked S) rest
on the store's fence alone and fail the run even inside a probe; the others hold back there, and invariants that
read a node's clock leave out nodes with a clock fault. Every invariant has a plant in `check.py --selftest`.

| Invariant | Holds when |
|---|---|
| `fence-order` S | a key's accepted records have contiguous `s` from 0, epochs never go down, no append below an earlier accepted own, and `f` equals `e` |
| `epoch-two-nodes` | no epoch owned or written by two nodes (`CONFLICT` on an own) |
| `epoch-two-writers` | no `CONFLICT` on an append |
| `turn-after-takeover` | no refused request began its turn after the newer owner's own (§ Refusals) |
| `ack-lost` S | every ack has an accepted record of its op |
| `ack-mismatch` S | the ack's outcome, balance, node and epoch are the record's: it names the writer, also from `done` |
| `applied-twice` S | no op id in two accepted records |
| `phantom` S | every accepted op has a `send` with its key and delta |
| `chain` S | per key `b` follows the previous `a`, `a = b + d` (or `b` for an overdraft, given exactly when `b + d < 0`), `a ≥ 0`; balances sum to the accepted deltas |
| `journal-missing` S | every acked op has a journal line on its writer, waived for a `diskdrop`, a logged `journal_err`, a writer killed between the store's OK and its `fdatasync` (up to the ack where its journal disk or its threads were starved), a `mkfs` whose archive lacks it |
| `journal-divergent` | every journal line equals the store's record of its op, unless a `diskdrop` left it |
| `liveness-after` | every op in flight at an episode's end, or sent before the next, is acked by max(its start, the end) + `B`, or `B` after the next episode's end where that began first and caught it in flight |
| `liveness-during` | the callers in the part holding a majority of acceptors and of seeds, neither impaired nor reaching a stranded node, have every op acked within `D`, unless sent near an on or off or on a key a cut-off node keeps or wrote |
| `io-lease-lost` | a disk-only episode moves none of the node's `leases_lost` or `isolations` |
| `mass-drop` | in a quiet window no node stops more than max(10, 1 %) of its actors other than `Drained` |
| `lease-lost-quiet` | in a quiet window no `leases_lost` moves |
| `latency-quiet` | in a quiet window every node's client p50, and p99 over 20 ops, is under 1 s; an op still in flight when an episode began is not judged |
| `clock-behind` | a `skew-restart` that made the store refuse an own shows `clock behind` on some acceptor (always a probe) |
| `zombie` | no actor runs `T + 17 s` past a newer epoch of its key granted or started elsewhere (from the off of a stop, restart or `memhigh` on its node) |
| `turn-after-acquire` | no accepted append's turn began after a newer epoch's actor started on another node |
| `blast-radius` | through a fault on one node, ops away from it keep p99 under 1 s and none past 5 s (from `3T + 5 s` after the on for classes that stop or cut it) |
| `causality` | no second of a node's clock has a `send` after its record or an `ack` before it by over 2 ms (§ The clock map), away from a clock fault |
| `growth` | no incarnation keeps growing in memory or fds, and threads stay under a ceiling (§ Resource growth) |
| `stream-dup`, `stream-reorder`, `stream-gap` | no follower logged one: every record once, in order, or an announced reset (no clock: fail inside a probe too) |
| `stream-liveness` | a follower's silence on a stream whose origin is up ends by max(start, episode end) + `B` |
| `stream-publish-err` | no origin's publish failed |
| `stream-reset-kind` | no end for `Gap`, `InvalidRecovery`, `Revoked`, no refused or broken subscribe, `SlowConsumer` or `GlobalCapacity` only where the path was starved or the follower restarted in the `B` before |
| `stream-quiet` | no reset, failed subscribe or `origin_lost` in a quiet window, unless the origin's disk lied in the 120 s before (or until it publishes past where it was) or refuses every publish |
| `stream-generation` | each origin opens each stream above its last generation |
| `disk-durable` | no follower logged `rewritten` (a restart went back over a record it had), unless the origin's streams disk lied in between |
| `disk-resume` | each start opens each disk stream in its last generation after at least its last position, and every publish is the next position |
| `disk-error` | a refused disk publish is `Disk(..)`: `Full` or `Failed` only under a fault of that disk (or `nofile`), `Behind` only near a fault of the node; each cleared within `B` of the faults' off, or of a stop of the node or a fault of its disk begun by then |
| `disk-retention` | each stream's bytes stay within retention plus a segment and what waits to be written |
| `give-up` | no client gave up on an op |
| `log-complete` | every node has logs and a unit log, and its `acks` counter exceeds its ack lines by at most 16 |
| `heartbeat` | `stats` and `streams` lines at most 15 s apart and every workload's tick moving, unless exempt |
| `client-task` | every key is called within 2.5 `EVERY_MS` + 10 s of the `up` and then every 1.5 `EVERY_MS` + 125 s |
| `unexpected-restart` | every `up` but a phase's first is explained by a restart fault, a reset or a heal; no panic or SIGKILL in the unit log outside them |
| `shutdown-bounded` | every `down` without an error within 25 s; no stop timeout or nonzero exit outside a disk fault |
| `panic-contained` | a panic hook restarts actors, not the node, and its records resume within `B` + 30 s |
| `peer-leak` | after restart faults alone, the untouched nodes hold at most 8 more fds and no more sessions than before |
| `sockets` | no node holds over 8 connections to one peer (a session each way per lane, and a redial) or a `CLOSE-WAIT` in two samples in a row, or over six sessions per peer in two `res` lines in a row, outside a `stale` or `impostor` peer probe or a `halfopen` |
| `kernel` | no kernel message of `KERNEL` (conntrack full, TCP out of memory, SYN flooding, hung task, ext4 or I/O error, OOM kill) outside the classes that explain it |
| `peer-slots` | a `peer` episode alone leaves `became_unreachable` where it was |
| `peer-impostor` | a wrong `secret` is refused as `auth` and counted; a `stale` impostor receives no frame |
| `nemesis` | each planned fault on and off once with rc 0 and its arguments, nothing unplanned, each step within 2 s (`SLOW_STEP` for slow ones), no forced or failed heal, every disk reset done, no class the plan version or box lacks |
| `fault-effect` | no fault's evidence contradicts it |
| `store-void` | the store's log holds together: appends for owned epochs, stale refusals with a newer owner, no silence over 1 s with ops in flight, a clean `store.out` |
| `quota-rate-over`, `lim-rate` | admissions in any 1 s of 125 ms buckets, on the store's clock, are at most 1.26 times the limit plus each fault's excess of [quotas.md § 5](quotas.md) while it holds and 10 s after |
| `quota-permit-over`, `lim-open` | permits held, and `lim` streams open, at once are at most the limit plus the same excess, and an incarnation's first use's fallback share |
| `quota-permit-leak` | an idle node's table holds at most 3 more than its holders; a quiet drain gets every permit |
| `quota-retry` | every refusal says when to retry (a bucket for rates, a heartbeat for permits); the under-limit `r0` is never refused at its limit and waits at most 1.5 s in a quiet window |
| `quota-under` | a quiet second offered twice a hot limit admits at least half of it |
| `quota-pulling` | a started replica logs `qready` within 30 s unless another fault holds |
| `lim-subs` | no node holds more than 4 subscriptions to `lim` |
| `lim-err` | no `lim` operation fails other than at a limit |
| `compact-origin` | an origin's compacted records are numbered 1, 2, .., its state within its limit, `StateFull` only at the limit with a new key |
| `compact-fold` | each snapshot plus fold a follower logged equals the fold of the origin's records through its cursor |
| `eager-restart` | every eager key runs `B + 5 s` after b1 started them and `B + 2 s` after each episode |
| `watch-owner` | an owner `watch_key` shows is the store's owner of that epoch |
| `watch-epoch` | a watch never shows an epoch below one it showed, unless exempt |
| `echo-deadline` | a named call runs by its send plus its deadline plus 100 ms (200 under `cpu`, its quota's period) and the clock slop, where no network, stop or clock fault touched either node, nor `memhigh`, `reclaim`, `swap` or `threadstarve` the one running it (a page-in or a hog has no bound) |
| `authz-leak` | no call from a refused peer runs past its deadline plus 100 ms after the refusal |
| `resolve-all` | in a quiet window `resolve_all` finds every other node up for 15 s, and fails on none |
| `spawn-idle` | every `spawn_on` actor stops within its idle and 10 s |
| `echo-liveness` | calls between two nodes are answered again within `B` of the episodes around the silence |
| `idle-race` | in a quiet window an op within 1 s of its key's idle stop is acked within 5 s |
| `readyz-isolated` | an isolated node answers `/readyz` 503 `isolated` from 7 s after the cut (12.5 s at `TIMING=wan`) until the heal (a timeout is no answer) |
| `probe-quiet` | in a quiet window every probe of every node answers 200 |
| `metrics-monotonic` | no counter of `/metrics` goes down within an incarnation, but the per-connection and per-name series |
| `tls-rotated` | every node takes each CA pass within 10 s, unless exempt |
| `handshake-quiet` | in a quiet window no handshake failure is counted |
| `blob-corrupt` | every echo came back byte for byte |
| `blob-quiet` | a quiet `blobs` window has calls answered and none failed |
| `local-lost` | no topic subscriber's gaps or dups grow, unless exempt and 5 s after |
| `supervision-bound` | the 4 flaky families stay alive, each failure restarted |
| `timer-step` | an unexempt `local` window holds 80 to 120 ticks, none over 1 s late, whatever the wall clock did |
| `local-quiet` | in a quiet window no router ask fails and the publisher, router, subscribers and timer all move |
| `dead-quiet` | in a quiet window no dead letter but those the flaky subscribers plan |
| `isolation-quiet` | at `TIMING=wan`, in a quiet window and through a geo scenario, no node's `isolations` moves unless a fault cut it from every peer for over 8.5 s (idle − keepalive − heartbeat interval + grace, the soonest it can isolate); through faults shorter than `lease_grace` no `leases_lost` moves on a node that kept a majority of the acceptors |
| `live-under-bulk` | while `bulk` runs, away from other episodes, each node's `echo` p99 to each peer over the episode's minutes (10 calls or more; not per minute, whose 30 calls' p99 is the slowest) is under the pair's round trip plus 250 ms, and no `leases_lost`, `isolations` or `became_unreachable` moves |
| `firehose-gap` | no `firehose` follower's batch skips records of its generation within one subscription, fault or none |
| `stream-throughput` | in a quiet 10 s under `geo` (the backdrop) and no `bulk`, each `firehose` follower receives 90 % of the bytes b1 published, under 2 s behind |
| `stream-window` | in every `window` line, each child's bytes in flight are at most its largest window in the last 10 s, or twice a delivery rate times a round trip of two lines in a row there (a push works its window out between lines), plus one push, the window at most 64 MiB, 64 pushes in flight, the node's in flight at most 256 MiB plus a push per child; `resent` moves only within `B` of a fault on either node; lines after their phase, as the nodes stop, are not judged |

**Refusals.** A refused write alone is no bug: a pause mid-handler finishes that handler late, and only the store
catches it. What axon promises is that no turn *begins* after the old fence lapsed. For a refusal of epoch `e` at
store time `S` with `lag` `L` (µs on the node's monotonic clock from the turn's start to the request), the turn began
no earlier than `S − L·(1 + ε) − δ` (`ε` 1 % or a drift's rate, `δ` the node's slowest store round trip then, at
least 5 ms); it is a violation when that is past the newer own plus `slack` (10 ms, +100 ms under `cpu`, +1 s under
`memhigh`), and explained when the node resumed from `pause`, `freeze` or `skew-restart` within 50 ms before it. A
refused write that no fault explains with a turn 200 ms after a resume is the planted bug the selftest holds.

**The clock map.** The nemesis samples offsets only at each settle. The ops bracket each second of a node's clock too
(a `send` precedes its record, an `ack` follows it); where a bracket is 100 ms wide or less and the sample falls
outside it, lines are placed by its middle. A second the clock stepped back over has no bracket: there a client line is placed
by write order, before the step on the offset of the second after the top it stepped back from plus the step, after it on that offset. On one box every bracket holds 0, so the map proves itself only on five.

**Resource growth.** `growth` judges each incarnation apart, after a 6 min warmup and only with 15 min more: cut into
quarters by time, a resource grows when the last quarter's p50 exceeds the first's by 25 % plus its slack (64 MiB
for `rss_kb` and `anon_kb`, 32 for `fds`) and still rises in the last quarter by a sixth of that. Threads are held to
a ceiling instead, the node's fewest plus 512 (tokio's blocking pool keeps its high-water mark) plus 16. A node with
no incarnation judged is named above the verdict. A leak of a few MiB an hour needs the soak to show.

**Recovery bounds.** Per class, the slowest key's time from an episode's end to its next accepted write, against
`B_CLASS` plus the callers' own pace; `recovery.json` holds them for `gate`.

## Extending the rig

Each addition is entries in tables that exist, each on its own line so two lanes merge, plus the test that fails
without it. `PLANS[1]`, the `V1` digests and `synth()`'s clean baseline never change; after a night on the newest
version, open the next (`PLANS[3] = dict(PLANS[2])`).

**A fault class.** `fault.sh`: `on`/`off` arms, `claim`/`release` for a `SOLE` resource, `hold`/`unhold` if it stops
the unit, rules tagged `$(tag)`, what `check` sees and `heal` undoes. `nemesis.py`: `TAGS`, the newest `PLANS` row,
`SOLE`, its draw in `draw_fault`, its arguments in `commands()`, a probe in `kit()`, a pair in `combos()` if it
stacks. `check.py`: an `EFFECT` entry, and `SLOW_STEP`, `DURING`, `RESUMES` or `STREAM_STARVED` where they apply
(`HALTS`, `RESTARTS`, `IO`, `CLOCK`, `PROBES` come from the tags). Its row above. Evidence: `nemesis.py selftest`,
`doctor faults` and `doctor combos`, and `ONLY=<class> scripts/rig.sh smoke`.

**A workload.** `src/<w>.rs` with `start(&Ctx) -> Tick`, an arm in `main.rs`, its own `<w>.jsonl`. In `check.py` a
check in `CHECKS` that counts `self.judged[inv]` and calls `self.flag(inv, t, ..)`, floors in `FLOORS` and
`FLOOR_WORKLOAD`, and its clean lines in `SYNTH_EXTRAS`. Its row above. `WORKLOADS=accounts,streams,<w>` runs it.

**An invariant.** Its name in `INVARIANTS`, its check, and a plant: `@plant(want=[..], probe=[..], rc=..)` on
`p_<name>(files, info)` that breaks the synthetic run so exactly those invariants fail (`want=[]` holds an exemption
in place). Mutate the check and watch the plant fail; `check.py --selftest 0` fails an invariant with no plant. Its
row above.

**A real-binary plant** is `examples/rig/plants/<name>.sh`, which checks its anchor occurs once and edits the source
in the build box's copy (`scripts/rig.sh build plant <name>`; `meta.json` names it). A smoke on it must fail its
invariant: `auth` (`peer-impostor`, `ONLY=peer smoke 17`), `blob` (`blob-corrupt`), `eager` (`eager-restart` after a
kill), `local` (`supervision-bound`), `nosync` (`disk-durable`, `ONLY=powercut` with `disk`), `panic-escape`
(`panic-contained`, `ONLY=hook`), `quota-double` (`quota-rate-over`, `quota-permit-over`), `snapshot`
(`compact-fold`), `tls` (`handshake-quiet` with `TLS=1` and a restart class), `v2-codec` (`phantom`, `ONLY=roll`),
`watch` (`watch-epoch`); at `TIMING=wan` with `WORKLOADS=accounts,streams,eager,echo,bulk,firehose`,
`isolate-early` (`isolation-quiet`, `SCENARIOS=geo-partition-short`), `bulk-on-live` (`live-under-bulk`, `rate-bulk`),
`window-floor` (`stream-throughput`, `geo-quiet`), `no-window` (`stream-window`, `geo-partition-short`). `fence` (a fence held twice its lease) is not found by any rig fault, since none starves the
timer alone; the World's `starve` and `hold_actors` find it.

**A scenario, role or trigger.** A scenario is an entry in `library(T)` built from `fault()` (the selftest checks
each for timeline legality at T = 2 and 6); a role is an entry in `ROLES`; a trigger on a new log line needs only `t`
and `r`. `EDGE`, `ROLE` and `LEDGER` are sets a class joins.

## Open findings

Each is axon's, not the rig's, and reproduces on one box with every workload
(`WORKLOADS=accounts,quota,streams,disk,compacted,eager,watch,idle,echo,blob,local`):

| Finding | Invariant | Reproduce |
|---|---|---|
| A `Live` follower is sent its generation again from record 1 after the origin resumes that generation from disk | `stream-dup` | `SMOKE_MIX=0 smoke 51`, episode 1 (`acceptors-rolling`); its replay hit 1 of 3 |
| A `Live` follower skips the last durable record when the origin resumes from disk | `stream-gap` | `chaos 62 5`, episode 4 (`splitdelay` then a kill of the origin); its replay hit 0 of 3 |
| A `Live` follower skips records after `OriginLost`, with no `Ready` or reset, when its relay restarted or it lost the origin during a cut | `stream-gap` | `chaos 52 8`; `ONLY=roll,cfgdrift,scale,cold-restart,acceptors-restart,acceptors-rolling,leave-minority,leave-slowdisk smoke 7`, episode 3, some runs |
| A late first use of a permit key can exceed [quotas.md § 5](quotas.md)'s new-key bound: 16 of 12 against 15 when every node took its fallback share at once and one took a second before releasing | `quota-permit-over` | `pairs 53 8`, before any fault |
| An isolated node answered `/readyz` 200 through an `isolate` (once) | `readyz-isolated` (probe) | `chaos 47 8` |

## Findings fixed

| Finding | Seen | Test that now holds it |
|---|---|---|
| After a partition healed, a caller waited up to the redial backoff's 30 s cap to reach an owner; `ClusterConfig::backoff` now caps it at 2 s. Every World test had shortened the cap, hence `scripts/check.sh timing` | night 2609, `liveness-after` | `world_faults.rs::calls_resume_within_the_redial_cap_once_a_long_cut_heals`, the shipped timings in `crates/axon-stream/tests/world.rs` |
| A node that took a paused owner's keys handed them back only at its next discovery resync: a stale view now asks for a snapshot on each heartbeat | smoke 501, `liveness-after` | `world_leases.rs::every_op_answers_within_two_leases_of_a_paused_owners_resume`, `tests/cluster/discovery.rs::a_stale_view_is_fresh_again_once_its_watched_provider_answers_and_a_hung_one_holds_no_heartbeat_back` |
| A key started elsewhere while its owner restarted stayed where no caller looked | smoke 31, `liveness-after` | `world_leases.rs::a_key_started_elsewhere_while_its_owner_restarted_goes_back_to_it` |
| Eager keys lost for good while their node stayed a member: a key whose actor saw its lease lapse before the renew loop did was released and forgotten; a key handed back at a heal, whose new owner had answered and then could not take the lease in time, was let go by both nodes; and keys still to start again were dropped when their node left | chaos-20261001-1931-s1001, `SMOKE_MIX=0 smoke 61` (after a `bridge`), `eager-restart` | `world_eager.rs::an_eager_key_fenced_by_its_own_turn_on_a_node_that_stays_a_member_starts_again`, `::an_eager_key_handed_back_to_a_node_whose_start_then_fails_starts_again`, `::an_eager_key_lost_just_before_its_node_leaves_runs_on_a_survivor` |
| Eager keys were not started again after every acceptor restarted at once: the restarted acceptors listed no key as anyone's. Each acceptor now logs who holds each key (`PaxosConfig::store`, the node's `LEASE_DIR`), and a node where a kind turns eager restarts every lapsed lease nobody released ([paxos-leases.md § 4.1](paxos-leases.md#41-the-lease-log)) | `WORKLOADS=accounts,eager ONLY=acceptors-restart smoke 43` (all 100 keys down), `eager-restart` | `world_eager.rs::every_eager_key_runs_again_after_every_acceptor_restarts_at_once`, `::every_eager_key_runs_again_after_a_cold_restart`, `::the_eager_keys_of_a_node_that_never_returns_from_a_cold_restart_run_on_the_others`, the restarts of every acceptor in `paxos/sim.rs` |
| A clean leave behind a slow disk let the actors' drain spend all of `shutdown_timeout`; actors now drain until a quarter is left | `leave-slowdisk`, `shutdown-bounded` | `crates/axon/src/system.rs` `coordinate` |
