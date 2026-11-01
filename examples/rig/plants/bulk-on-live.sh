#!/usr/bin/env bash
# The bulk workload's uploads on Live (spec/lanes.md § The rig (D3)): echo calls wait behind them (live-under-bulk).
. "$(dirname "$0")/lib.sh"
plant examples/rig/src/bulk.rs 'const LANE: Lane = Lane::Bulk; // rig-plant: bulk-on-live' 'const LANE: Lane = Lane::Live;'
