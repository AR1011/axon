#!/usr/bin/env bash
# The local workload's plant: the flaky families give up after 2 restarts a minute, where they fail 12 times
# (check.py supervision-bound).
. "$(dirname "$0")/lib.sh"
plant examples/rig/src/local.rs 'RestartPolicy { max_restarts: 100,' 'RestartPolicy { max_restarts: 2,'
