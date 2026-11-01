#!/usr/bin/env bash
# Every push window held at its floor, 2 MiB a round trip (spec/push-window.md § The rig (D3)): 11 MB/s at 190 ms, under
# firehose's 13 (stream-throughput).
. "$(dirname "$0")/lib.sh"
plant crates/axon-stream/src/core.rs 'usize::try_from(want).unwrap_or(usize::MAX).clamp(floor, cfg.max_push_window)' \
    'usize::try_from(want).map_or(floor, |_| floor)'
