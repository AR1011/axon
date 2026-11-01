#!/usr/bin/env bash
# next_push ignores the window, as PushWindow-NoWindow does (spec/push-window.md § 7): stream-window.
. "$(dirname "$0")/lib.sh"
plant crates/axon-stream/src/core.rs 'budget = budget.min(entry.window.saturating_sub(entry.inflight));'
