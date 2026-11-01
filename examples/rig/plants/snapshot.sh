#!/usr/bin/env bash
# A follower's snapshot of a compacted stream loses its first key (src/compacted.rs): compact-fold fails.
. "$(dirname "$0")/lib.sh"
plant examples/rig/src/compacted.rs 'apply(&mut table, snap.records());' \
    $'apply(&mut table, snap.records());\n                        table.pop_first();'
