# Examples

The fault rig lives here, a workspace member, so `scripts/check.sh` builds and lints it.
Eleven more, from a chat server to a matching engine, live in the AR1011/axon-examples repository.

| Example | Shows |
|---|---|
| [rig](rig/README.md) | Keyed accounts on Paxos leases, one process per machine, under the real faults of the [fault rig](../spec/fault-rig.md): a fencing store that refuses old epochs, a journal on each node, a client per key, and JSON-lines logs a checker reads afterwards |
