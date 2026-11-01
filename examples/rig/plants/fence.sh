#!/usr/bin/env bash
# R5's plant: a renewed key's fence held for twice its lease (spec/fault-rig.md § Extending the rig).
. "$(dirname "$0")/lib.sh"
plant crates/axon-cluster/src/spawn.rs 'hold_for(lease.until.saturating_duration_since(now))' \
    'hold_for(lease.until.saturating_duration_since(now) * 2)'
