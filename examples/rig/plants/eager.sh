#!/usr/bin/env bash
# The eager kind registered without `.eager` (src/eager.rs): a killed node's keys stay down, and eager-restart fails.
. "$(dirname "$0")/lib.sh"
plant examples/rig/src/eager.rs '.eager(Bytes::new(), Placement::default())'
