---
name: blast-radius
description: Find what a change could break outside its diff, then prove the one fact its safety rests on by running real code. Use for "blast radius of X", "what could this break", before merging a small diff you don't fully trust, and for any change to a wire format, an atomic ordering, a public API, a metric name or the dashboard's JSON.
---

# Blast radius

`grep` for callers takes a second and is not the job. The job is the breakage grep does not show. A writeup that sounds
right is worth nothing on its own, so find the one or two facts the change is safe because of, and prove them.

## How sure are you

Take every safety fact as far down this list as is cheap, and say where each one stopped.

1. You said so. Worthless alone.
2. You pointed at the line: a real `file:line`, or the dependency's own source at the version in `Cargo.lock`.
3. You walked the failure step by step and showed it cannot happen.
4. You ran it: a test, a `SIM_SEED`, a loom model or a fuzz input that calls the real code and fails loudly if you are wrong.
5. You reproduced it in the running system (the **verify-axon** skill).

Any fact that stops before 4 is marked unproven in the writeup.

## Steps

1. Read the change: the diff, the symbols it adds, changes and removes, and what now behaves differently that the diff
   does not spell out. `git log -p` for the commits behind it.
2. Find the one fact it is safe because of, for example "this only runs after the mailbox is closed, so nothing can
   still enqueue". If that fact holds, most of the risky cases clear at once. Spend your time here.
3. Look where grep stops. In this repo that means the following.
   - **Bytes on the wire.** `WireProtocol::ID` and `SCHEMA`, the pinned bytes in each crate's `wire.rs` or `wire/tests.rs`, frames that
     `axon-net` decodes (fuzz `frame_decode`, `handshake`), and stream records (`stream_wire`). An old node talking to a
     new one is a real case.
   - **Orderings and concurrency.** Atomics, the scheduler, drain and shutdown. Check the loom models
     (`scripts/check.sh loom`) and whether one of them still covers the path. Weaken the ordering and watch the model fail.
   - **Determinism.** The relay, hub and seed cores must stay reproducible. Iterated maps are `BTreeMap`s, and nothing in
     `core.rs` reads a clock or touches tokio. Run the same seed twice.
   - **JSON the UI reads.** `crates/axon-obs/src/dashboard/` and `telemetry.rs` feed `ui/src/api.ts`. A renamed
     field compiles on both sides and breaks the console.
   - **Names people store.** Metric names and labels in `crates/axon/guide/telemetry.md` end up in ClickHouse and in dashboards
     outside this repo. Keyed-actor `kind` strings and published actor names are looked up by string.
   - **Every exit path.** Stop, drain, restart, shutdown and the error return of each. Counters must return to zero and
     nothing may hang. The fault loops in `crates/axon/tests/faults.rs` and `crates/axon-net/tests/faults.rs` are
     where a class of leak gets covered.
   - **Public API and guides.** The API page in `crates/axon/README.md` and the guides under `crates/axon/guide/`, which are doctests.
4. Be honest about each risk: how it breaks, the `file:line`, how likely it is and how bad. Keep confirmed risks apart
   from the ones you checked and cleared. A search that finds nothing is still an answer. Never invent a caller.
5. Prove the one fact. Write the smallest test or script that runs the real code, run it, and paste the result. Keep it
   in the row's test file if it guards against a real regression.
6. For a wide change, give two independent subagents the same question without your conclusion, and compare. Where they
   agree, trust it more. Where they differ, look again.

## What to hand back

- **What it does.** What changed, including the part that is not obvious.
- **The fact it is safe because of.** The fact, how far down the list it got, and the proof. Or "unproven".
- **Risks.** Only the real ones. For each one, how it breaks, where, how likely and how bad, and the check that
  catches it.
- **Cleared.** What you checked, and why it is fine.
- **Before merging.** The cheapest test or repro that would catch the real bug, and the command that runs it.
