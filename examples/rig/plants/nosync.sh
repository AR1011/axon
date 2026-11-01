#!/usr/bin/env bash
# The disk lane's plant: appends never synced, so a power cut loses records followers had (spec/fault-rig.md § Fault
# classes). It is in OsFiles, so the lease acceptors' logs lose them too. A smoke with ONLY=powercut and the disk
# workload fails disk-durable.
. "$(dirname "$0")/lib.sh"
plant crates/axon/src/files.rs 'appended.file.sync_data()?;'
