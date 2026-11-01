#!/usr/bin/env bash
# A watch shows the epoch before an owner's right after it, on every fourth epoch (src/watch.rs): watch-epoch fails.
. "$(dirname "$0")/lib.sh"
a='log.line("own", json!({"k": key, "n": &*node, "e": epoch}));'
plant examples/rig/src/watch.rs "$a" "$a"$'\n                            if let Some(e) = epoch.filter(|e| e % 4 == 0) { log.line("own", json!({"k": key, "n": &*node, "e": e - 1})); }'
