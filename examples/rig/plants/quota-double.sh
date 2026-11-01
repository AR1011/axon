#!/usr/bin/env bash
# The quota lane's plant: every replica grants as if each limit were twice its size (spec/fault-rig.md § Workloads).
# One replica alone that doubled would be hidden, by design: a node admits on the smallest grant of a majority (core.rs).
. "$(dirname "$0")/lib.sh"
f=crates/axon-cluster/src/quota/core.rs
plant $f 'let plan = targets(key.max, &asks);' 'let plan = targets(key.max * 2, &asks);'
plant $f '(key.max.saturating_sub(total), total <= key.max)' '((key.max * 2).saturating_sub(total), total <= key.max * 2)'
