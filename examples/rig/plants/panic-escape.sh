#!/usr/bin/env bash
# panic-contained's plant: a hook's panic aborts the process instead of unwinding into its supervisor
# (spec/fault-rig.md § Fault classes). A smoke with ONLY=hook flags panic-contained and unexpected-restart.
. "$(dirname "$0")/lib.sh"
plant examples/rig/src/hooks.rs 'panic!("rig hook {hook} on {key}");' 'std::process::abort();'
