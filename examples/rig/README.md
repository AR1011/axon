# rig

The workloads of the real-fault rig ([spec/fault-rig.md](../../spec/fault-rig.md)), one process per machine: keyed
accounts on `PaxosLeases` writing to a fencing store that refuses an epoch older than the newest owner it has seen,
relayed streams, and the other workloads of [spec § Workloads](../../spec/fault-rig.md#workloads), chosen by
`WORKLOADS`. Everything goes to JSON-lines logs that the rig's checker reads afterwards.

| Process | Runs |
|---|---|
| `rig store [bind] [log]` | the fencing store, in memory, every request and verdict in `store.jsonl`. One, on a machine no fault reaches |
| `rig node` | a seed server, a cluster node and its workloads; `SIGTERM` is a clean leave. Its settings are environment variables, listed at the top of `src/main.rs`; on the rig, `run.sh` writes them |
| `rig peer <addr> <mode>` | a malformed or confused peer, for the `peer` fault class (`src/peer.rs`) |

## Run it

On one Linux machine, on loopback: a store and three nodes, 200 keys, `b3` killed at 30 s and back at 40 s, `b2`
stopped with `SIGSTOP` for 8 s at 60 s. It prints PASS when every node has acks, none gave up, the store has owns from
all three nodes and some key changed epoch, and every node received stream records, subscribed after a cursor, with
no duplicate, reorder or gap and no stream silent at the end.

```bash
examples/rig/ops/local.sh 90      # builds with cargo build --release -p rig unless RIG_BIN is set
```

```text
b1: 8135 sent, 8134 acked, 0 gave up; 1 up, 129 starts, 129 stops, 0 stale
b1 streams: 4 opened, 11284 received, 15 after a cursor, max lag 627 ms, 8 resumes, 2 resets, 2 origin lost; 0 dups, 0 reorders, 0 gaps
b2: 7558 sent, 7558 acked, 0 gave up; 1 up, 160 starts, 160 stops, 0 stale
b2 streams: 4 opened, 11920 received, 12 after a cursor, max lag 8018 ms, 12 resumes, 2 resets, 2 origin lost; 0 dups, 0 reorders, 0 gaps
b3: 7056 sent, 7055 acked, 0 gave up; 2 up, 177 starts, 105 stops, 0 stale
b3 streams: 8 opened, 13364 received, 10 after a cursor, max lag 5032 ms, 8 resumes, 0 resets, 0 origin lost; 0 dups, 0 reorders, 0 gaps
store: {('A', 'ok'): 22747, ('O', 'ok'): 466}; owns from ['b1', 'b2', 'b3']; 133 of 200 keys changed epoch
PASS
```

## The rig on one box

The full rig (the store, five nodes under systemd, the nemesis, the checker, every fault class that stays on one node)
also runs on one Linux box, each node in a network namespace of its own ([spec § Layout](../../spec/fault-rig.md#layout)).
From the laptop, with ssh as ubuntu and passwordless sudo on the box:

```bash
export RIG_BOX=<ip>               # RIG_BUILD defaults to it
scripts/rig.sh setup              # namespaces b1..b5 at 10.77.0.1..5, a journal and unit instance each; then doctor
scripts/rig.sh build              # RIG_SRC=<dir> builds in ~/<dir> there instead of ~/rig-src
scripts/rig.sh doctor faults b2   # each class on b2, seen; every other node clean meanwhile
scripts/rig.sh doctor combos b2   # faults stacked on b2 and b3, off in both orders; every node clean after each
scripts/rig.sh smoke              # every class but the clock ones, which would move the whole box
scripts/rig.sh teardown           # namespaces, units, journals and the bridge gone
```

To add a fault class, a workload or an invariant, follow [spec § Extending the rig](../../spec/fault-rig.md#extending-the-rig).
`examples/rig/plants/` holds the source mutations that `scripts/rig.sh build plant <name>` builds in.

## Left out

- Transfers between accounts: the rig checks conservation per key and in total with deposits and withdrawals; the
  ledger example in the AR1011/axon-examples repository checks transfers on one machine.
- Durability of the store: it is the oracle, kept in memory for one run.
