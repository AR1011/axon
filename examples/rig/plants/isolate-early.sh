#!/usr/bin/env bash
# A session closes idle after two keepalives, not after idle_timeout (spec/wan.md § 12): in a geo run a node cut for
# under the soonest an unplanted one can isolate then does (isolation-quiet).
. "$(dirname "$0")/lib.sh"
plant crates/axon-net/src/session.rs \
    'let mut heard = Instant::now();' \
    'let (mut heard, idle) = (Instant::now(), keepalive * 2);'
