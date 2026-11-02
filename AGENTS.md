# axon for coding agents

The user-facing entry is [crates/axon/README.md](crates/axon/README.md): the API on one page, the rules that prevent
most mistakes, and which guide in [crates/axon/guide](crates/axon/guide) to read for what. Read it first. The guides
hold complete programs that the test suite runs; copy from them rather than guessing signatures. Why it is built this
way: [spec/](spec/00-overview-and-requirements.md).

## Working in this repository

```bash
scripts/check.sh
```

Format, clippy with warnings denied, every workspace test and doctest, doc links, the I/O-free cores, the fault matrix and the built UI.
About three minutes from a cold target dir on a 24-thread dev box, where the test executables run in two lanes (one
`cargo test` below six cores, as on CI). CI runs the same script with `SEEDS=4` (4 seeds per World test, to fit
GitHub's runners), so before a merge run it and `scripts/check.sh sim` in full on a box (`box.sh` below). Iterate on
one file with `cargo test -p <crate> --all-features --test <file>`, and before handing a change over run
`scripts/check.sh quick $(git diff --name-only main)`: clippy and the tests of the crates it touches and of those that
depend on them, the World tests on a few seeds. Leave the full check and sim to where changes are merged.

| Command | Runs | Needs |
|---|---|---|
| `scripts/check.sh live` | the `#[ignore]`d ClickHouse and Consul tests | `docker compose up -d` (ClickHouse on 18123, Consul on 18500) |
| `scripts/check.sh repeat [n]` | the workspace tests n times (default 5), then every failure with its output. CI runs it nightly (`flakes.yml`) on two cores, where timing assumptions break | on Linux, `taskset -c 1,2` for two cores without core 0, so a hardcoded core id fails |
| `scripts/box.sh sync <dir>` then `run <dir> <cmd>` | heavy work on the dev box `BOX=user@host` names, or else the one line in `~/.config/axon/box` (where agents' commands get it, with no prefix): copies the tree by checksum (never `rsync -a`, whose kept mtimes make cargo skip edits) to `~/axon/<dir>` (`BOX_ROOT` another name) and runs there with a target dir of its own (`<dir>-target`, which the tree's `target/` links to: sccache keys on a `CARGO_TARGET_DIR`, so trees would share no dependency hits), sccache for dependencies, incremental builds of the workspace's crates and line-table debug info. With a number in the box's `~/.config/box-slots`, at most that many runs go at once (`scripts/slot.sh`), and the rest wait | ssh to the box |
| `scripts/check.sh smoke` | a playground from `scripts/ctl.sh` that passes `doctor`, then is stopped | `jq` |
| `scripts/check.sh loom` | the loom models (release, `target/loom`) of the mailbox and a turn's start, the idle workers' sleep and wake, a remote reply's state and a session's write order, then clippy on them. Mutation-check a new model: weaken the ordering it guards and watch it fail | |
| `scripts/check.sh sim [test]` | the World tests (`tests/world*.rs`) in the sim build (`--cfg tokio_unstable`, profile `sim`: release without LTO, `target/sim`), in two processes whose digests must match; prints the seeds each `check_seeds` test ran and, per failing seed, the `SIM_SEED=n scripts/check.sh sim <test>` that replays it. `SEEDS=a..b` runs a range | |
| `scripts/seeds.sh a..b user@host..` | `check.sh sim` on every box at once, one `SEEDS` shard each, through `box.sh`; then per box the seeds run and the failing seeds | ssh to the boxes |
| `scripts/rig.sh setup`, `doctor [faults\|combos <node>]`, `build [plant <name>]`; `smoke [seed] [min]`, `chaos\|pairs\|scenarios [seed] [min]`, `soak [min]`, `night [seed] [min]`, `replay <run> <i> [n]`; `status`, `fetch <run>`, `coverage <run>..`, `gate <run>`, `heal`, `down`, `teardown` | the real-fault rig ([spec/fault-rig.md](spec/fault-rig.md)): keyed accounts on `PaxosLeases`, streams and the other workloads on five nodes under a seeded schedule of real faults, checked from their logs by `examples/rig/ops/check.py`. A run starts on b1 and runs there alone; `target/rig/<run>/summary.txt` names its seed and verdict. `RIG_N=2` runs on the first two nodes; `RIG_BOX=<ip>` puts all five nodes on one box, each in a network namespace, without the clock classes. `scripts/check.sh rig` runs the planner's and the checker's selftests | `RIG_NODES="ip1 .. ip5"` (b1 the control box) or `RIG_BOX`, `RIG_BUILD` a box with Rust, ssh as ubuntu with passwordless sudo, `jq` |
| `scripts/bench.sh [full] [pattern]`, `record [full] [pattern]`, `build`, `compare <base> <now>` | the performance gate ([benchmarks § The gate](crates/axon/guide/benchmarks.md#the-gate-scriptsbenchsh)): every harness row, each median against the baseline committed for the CPU (`scripts/bench/<cpu>.csv`); exits 1 on a REGRESSION or MISSING row. About 60 min; `record` replaces the baseline's rows of the jobs it ran; `BIN=dir` runs binaries built elsewhere (`build` on a build box). `scripts/test-bench.sh` (in `check`) holds it to fixtures | a bench box: Linux, governor `performance`, load under 1, `python3`; on a shared one, its lock |
| `scripts/check.sh faults` | checks that every test [spec/faults.md](spec/faults.md) cites for a feature and fault exists, then prints the gaps per feature. `check` runs it too | |
| `SIM_SEED=n cargo test -p axon-stream --test interests` (or `-p axon-cluster --lib random_faults`, `-p axon-cluster --lib paxos::sim`, `-p axon-cluster --lib quota::sim`) | one seed of a simulation; `SEEDS=n` runs more of the seed, paxos and quota sims | |
| `scripts/check.sh tla`; `java -cp tla2tools.jar tlc2.TLC -workers auto -config <cfg> <Model>` in `spec/tla` | `tla`: every model's base config passes (`RelayGiveUp`, `SnapshotThenLive`, `DiskResume`, `Handover`, `SeedMembership`, `QuotaGrant`, `SkewAtWait.cfg`, `SkewLogFloor.cfg` and `PaxosLeasesListed.cfg` for `PaxosLeases`, `HandoverReach.cfg`, `SeedMembership-Odd.cfg` and `-Wan.cfg`, `PushWindow`, `PushWindowOneStream.cfg` and `PushWindowThreeFaults.cfg`) and every `mutations/*.cfg` ends with an invariant or property violated, 152 configs, the longest `SeedMembership.cfg` at about 26 min on a shared 24-thread box; each model's spec section says what it checks. `PaxosLeases.cfg` and `Skew.cfg` do not finish: the last long run, before `LogFloor`, found no violation to depth 31 and 20 (1.06 B and 1.80 B distinct states, 13.7 h on a 24-thread EPYC 4484PX it shared with benchmarks); with it, `PaxosLeases.cfg` none to depth 24 in 45 min (165.8 M distinct states, 8 workers). Run them after changing the protocol | Java, [tla2tools.jar](https://github.com/tlaplus/tlaplus/releases) |
| `cargo +nightly fuzz run <target>` | `frame_decode`, `handshake`, `stream_wire`, `stream_core`, `log_frame` | nightly |

Held by the build, not by prose: no `unsafe` outside `crates/axon-net/src/tcp.rs`; no `unwrap` in library code (`expect`
with the reason, or handle it); no unbounded channels, and no std `Instant`, `SystemTime`, `RandomState::new`, thread
start or `HashMap`/`HashSet` iteration outside an `#[expect]` that says why, so real time, entropy and hash order stay
out of the World (`clippy.toml`, and `iter_over_hash_type` in the `src` of axon, axon-net, axon-cluster and
axon-stream; the `examples/*` apps keep only the channel rules, in `examples/clippy.toml`); `Instant::now` and `tokio`
never appear in `crates/axon-stream/src/core.rs` or `crates/axon-cluster/src/seed/core.rs`, which take `now` as an
argument so the simulations can drive them; a test or example that sets a timing field of a shipped `*Config` or
`*Timings` (or of a `Backoff`) has a `//` reason on that line or right above it (`scripts/check.sh timing`), so a
shortened timing is a choice and the shipped ones keep a test of their own (`crates/axon-stream/tests/world.rs`); every
relative link in the docs resolves.

**Shell.** The Bash tool runs zsh, and a worktree agent's commands pass a guard that refuses what it cannot follow. One
plain command per call: no `VAR=` prefix, no `$VAR` in a path, no `cd X && cat > f <<EOF`, no `sh -c '..'` inside
`box.sh run`. Multi-line shell goes in a scratchpad file run as `bash /abs/file` (a box job under `nohup setsid` too),
and a file's content goes through Write, after a Read when it exists. Never wait with a foreground `sleep`: use
`run_in_background` or Monitor. Quote globs and `====`. The boxes are GNU and this Mac BSD: `tail -n N`, the Edit tool
over `sed -i`. Over ssh, `pkill -f tlc2` matches its own shell and kills it: `pkill -f '[t]lc2'`. A subagent returns
its report as text; it cannot write report files.

**Live.** `scripts/ctl.sh` starts your own playground cluster on a free port and drives it: `up`, `doctor`, `api <route>`,
`shot <page> [--wait css] [--steps f.mjs]`, `down`. It prints JSON and `scripts/ctl.sh help` lists it all. Stop what
you start. The console's feature map (routes, selectors, what the playground runs) is
[.claude/skills/verify-axon/features](.claude/skills/verify-axon/features/README.md).

### Feature map

| Feature | Code | Tests | Guide |
|---|---|---|---|
| actors: spawn, tell/send/ask, supervision, watches, links, routers, topics, timers, jobs, sources, sinks, stash, drain, shutdown, diagnostics and uptime, pools and their cores, a store's files | `crates/axon/src/{system,context,driver,runtime,supervision,mailbox,reply,clock,lifecycle,router,topic,jobs,sink,files}.rs` | `crates/axon/tests/{system,hosted,embedded,faults,router,topic}.rs`, `crates/axon-testkit/tests/files.rs`, `crates/axon-testkit/tests/world_faults.rs`, loom `src/mailbox/model.rs` | [actors](crates/axon/guide/actors.md) |
| durable log: the records of many keys in one directory, fences, checkpoints, group commit, reclaim, open after a crash; `FileOp::Write`; the recipe of a node's log actor and keyed actors kept in it | `crates/axon/src/files/log.rs`, `FileOp::Write` in `crates/axon/src/files.rs` and the World's `Disk` | `crates/axon/src/files/log/tests.rs`, `crates/axon-testkit/tests/{files,world}.rs`, a keyed actor's state in its node's log `crates/axon-cluster/tests/world_log.rs`, fuzz `log_frame`, `--example log` | [actors § A log of keyed records](crates/axon/guide/actors.md#a-log-of-keyed-records), [cluster § A key's state on disk](crates/axon/guide/cluster.md#a-keys-state-on-disk), [spec/durable-log.md](spec/durable-log.md), [benchmarks § Durable log](crates/axon/guide/benchmarks.md#durable-log-axon) |
| dead letters, trace context, the `tracing` bridge | `crates/axon/src/{dead,trace}.rs` | `crates/axon/tests/{trace,system}.rs`, `crates/axon-cluster/tests/cluster/{trace,remote}.rs` | [tracing](crates/axon/guide/tracing.md) |
| transport: handshake, framing, compression, TLS and its rotation, lanes and the Bulk rate cap, `TCP_NOTSENT_LOWAT` and the host's TCP limits | `crates/axon-net/src/{handshake,frame,session,queued,net,config,tcp}.rs`, `Lane` in `crates/axon/src/lane.rs`, `Cluster::set_tls` in `crates/axon-cluster/src/engine.rs` | `crates/axon-net/tests/{session,frame,faults}.rs`, loom `src/queued/model.rs`, rotation in `crates/axon-cluster/tests/{cluster/tls,seed}.rs`, the engine's lanes in `crates/axon-cluster/tests/{world_lanes,cluster/lanes}.rs`, fuzz `frame_decode`, `handshake` | [cluster § Security](crates/axon/guide/cluster.md), [cluster § Lanes](crates/axon/guide/cluster.md#lanes), [spec/lanes.md](spec/lanes.md) |
| membership, node sets (`Nodes`), remote calls and the lane each takes, requests and replies in parts, remote spawn, keyed actors, their watches and kept references (`KeyedRefs`), leases | `crates/axon-cluster/src/{cluster,engine,engine/parts,membership,remote,spawn,placement,ownership,wire}.rs`, `src/paxos/` | `crates/axon-cluster/tests/cluster/` (in parts: `cluster/{parts,uploads}.rs`, cut by faults in `world_faults.rs`), `crates/axon-cluster/tests/{ownership,world,world_leases,world_faults,world_eager,world_reach,world_lanes}.rs`, `src/{engine,placement,remote}.rs`, `src/paxos/{core/tests,sim}.rs`, shipped timings in `crates/axon-stream/tests/world.rs`, the timings across regions in `crates/axon-stream/tests/world_wan.rs`, TLC `spec/tla/{PaxosLeases,Handover}.tla` | [cluster](crates/axon/guide/cluster.md), [spec/paxos-leases.md](spec/paxos-leases.md), [spec/lanes.md](spec/lanes.md) |
| rolling upgrades: wire version window, schemas side by side, schema-aware placement | `crates/axon-net/src/handshake.rs`, `crates/axon-cluster/src/{cluster,engine,spawn,placement,discovery}.rs` | `crates/axon-net/tests/session.rs`, `crates/axon-cluster/tests/upgrade.rs`, `crates/axon-stream/tests/world_upgrade.rs` | [cluster § Rolling upgrades](crates/axon/guide/cluster.md#rolling-upgrades), [spec/rolling-upgrades.md](spec/rolling-upgrades.md) |
| discovery: memory, static, Consul, seed servers | `crates/axon-cluster/src/{discovery,consul}.rs`, `src/seed/` | `tests/{seed,consul,world_faults,cluster/discovery}.rs`, `src/seed/sim.rs`, shipped timings in `crates/axon-stream/tests/world.rs`, the timings across regions in `crates/axon-stream/tests/world_wan.rs`, TLC `spec/tla/SeedMembership.tla` | [cluster § Discovery](crates/axon/guide/cluster.md), [spec/seed-discovery.md](spec/seed-discovery.md) |
| quotas: rates and permits per key, replicas per region, handover | `crates/axon-cluster/src/quota/` | `src/quota/{core/tests,model,sim}.rs`, `crates/axon-cluster/tests/{world_quota,cluster/quota}.rs`, readiness in `crates/axon-obs/tests/health.rs`, TLC `spec/tla/QuotaGrant.tla` | [cluster § Quotas](crates/axon/guide/cluster.md#quotas), [spec/quotas.md](spec/quotas.md), `--example quota` in [benchmarks](crates/axon/guide/benchmarks.md#quotas-axon-cluster) |
| streams: hub, relay core and its push window, node, interests, recovery, compacted state, wire, a lane per namespace | `crates/axon-stream/src/{hub,core,node,recovery,wire}.rs` | `tests/{hub,sim,interests,relay,world,world_relay,world_lanes,world_limits,world_wan,gateway}.rs`, `src/core/tests.rs` (the push window), `src/wire/tests.rs` pinned bytes, fuzz `stream_wire`, `stream_core`, TLC `spec/tla/{RelayGiveUp,SnapshotThenLive,PushWindow}.tla` | [streams](crates/axon/guide/streams.md) |
| streams: history on disk at the origin, segments, retention, resume after a crash, waiting until a record is durable, one node a directory | `crates/axon-stream/src/disk.rs`, `take_disk_writes`/`disk_written`/`reached` in `core.rs`, the writer task and `wait_cursor` in `node.rs` | `src/disk/tests.rs`, `src/node/tests.rs`, `tests/world_disk.rs`, TLC `spec/tla/DiskResume.tla` | [streams § History on disk](crates/axon/guide/streams.md#history-on-disk) |
| telemetry, metrics collection, the event timeline and each node's event log, ClickHouse | `crates/axon-obs/src/{telemetry,metrics,provider,clickhouse}.rs`, `crates/axon/src/{events,histogram}.rs` | `tests/{telemetry,metrics,faults,descriptors}.rs`, `crates/axon/tests/embedded.rs`, `crates/axon-cluster/tests/{cluster/keyed,cluster/leases,world_eager}.rs`, `crates/axon-stream/tests/{relay,sim}.rs` | [observability](crates/axon/guide/observability.md), [telemetry](crates/axon/guide/telemetry.md) |
| Prometheus `/metrics` on the Health port | `crates/axon-obs/src/{prometheus,health}.rs`, `exposition` in `crates/axon-obs/src/telemetry.rs` | `crates/axon-obs/tests/{prometheus,faults,descriptors}.rs`, live with `scripts/ctl.sh` | [observability § Prometheus](crates/axon/guide/observability.md#prometheus) |
| dashboard host and console UI, health probes | `crates/axon-obs/src/dashboard/`, `crates/axon-obs/src/health.rs`, `crates/axon-obs/ui` | `tests/{dashboard,health,faults,descriptors}.rs`, `ui/tests/{dashboard,login}.spec.ts` (`npm run e2e`), live with `scripts/ctl.sh` | [dashboard](crates/axon/guide/dashboard.md), [observability § Health probes](crates/axon/guide/observability.md#health-probes) |
| seeded network simulator, the World, its per-node `Disk`, every end of a crash inside a write (`crashes`) | `crates/axon-testkit/src/{lib,world,crash}.rs` | `crates/axon-testkit/tests/{sim,world,world_faults,files}.rs`, five nodes in three regions in `crates/axon-stream/tests/world_wan.rs` | [cluster § Simulating a cluster](crates/axon/guide/cluster.md#simulating-a-cluster), [spec/simulation.md](spec/simulation.md) |

A change to a row updates that row's tests and guide in the same commit. The guides ship in the `axon-actors` crate, render as `axon::guide`,
and are doctests of `axon-obs` (`crates/axon-obs/src/lib.rs`), so a stale guide fails `check`. Fragments that cannot compile are fenced `rust,ignore`.

### Workflows

**Bug.** Reproduce first: a failing test in the row's test file, a `SIM_SEED=n` that fails, or the symptom on a
`scripts/ctl.sh` run. Then the cause, the fix, `scripts/check.sh`. The test stays. When the bug is a class (a leak on
one exit path, a counter that can go backwards), extend the fault loop (`crates/axon/tests/faults.rs`,
`crates/axon-net/tests/faults.rs`) or the fuzz oracle so the class is covered, not the instance.

**Feature.** Write the acceptance test and the guide paragraph before the code, copying the shape of the nearest existing
test and guide program. Implement, then `scripts/check.sh`. A wire change bumps `SCHEMA` and the pinned bytes in that
crate's wire tests (in `wire.rs`, or in `wire/tests.rs` beside it). New public API goes on the API page in
`crates/axon/README.md`.

**Performance.** Run `scripts/bench.sh` before and after on the same bench box (a pattern narrows it to the harnesses
the change touches, `scripts/bench.sh perf`): each run is judged against the committed baseline, and
`scripts/bench.sh compare <before>/results.csv <after>/results.csv` judges the two. Record the rows that moved in
`crates/axon/guide/benchmarks.md` with what they do and do not establish. No claim without both numbers, and no merge
with a REGRESSION that is not explained there.

Done means: `scripts/check.sh` passes, you can name the test that failed before the change, and the guide matches. A
change the console shows has also been seen there, on a `scripts/ctl.sh` run, with the screenshot or JSON to show for it.

### Conventions

- A test earns its place by failing when the behaviour breaks. Assert what must still hold afterwards: nothing hangs,
  counters return to zero, nothing is silently lost.
- Few comments; say why, not what. Match the surrounding code. No new dependency for what a few lines do.
- Smallest change that solves it. Delete before adding, and fold one-caller wrappers back in.
- A new requirement goes in as if it had been there from the start. Its types, guides, examples and spec change with it,
  instead of a special case bolted on.
- Evidence is the real thing: the value read back, the test run, the page seen. A subagent's summary is not evidence;
  its diff and a rerun are.
- A lesson that comes up twice becomes a check (`scripts/check.sh`, a lint, a test, a script), not another line here.
- The simulations (`crates/axon-stream/tests/{sim,interests}.rs`, `crates/axon-cluster/src/{seed,paxos,quota}/sim.rs`) are
  seeded and must stay reproducible: maps that are iterated in `RelayCore`, `StreamHub` and the seed and lease cores
  are `BTreeMap`s, and a same-seed-twice test in each sim fails if that slips. `SIM_DEBUG=1` dumps the seed and quota sims.
- Seed discovery: design and measurements in `spec/seed-discovery.md`; the state machines in
  `crates/axon-cluster/src/seed/core.rs` have no I/O and are tested by `seed/sim.rs`. Seed servers in one test need
  distinct node ids.
- After changing `crates/axon-obs/ui`, run `npm run build` there and commit `dist`; the binary embeds it and `check`
  fails when `src` is newer than `dist`.
