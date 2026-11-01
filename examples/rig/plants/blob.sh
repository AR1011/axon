#!/usr/bin/env bash
# The blob workload's plant: an echo of more than 2 MiB comes back with its first byte flipped (check.py blob-corrupt).
. "$(dirname "$0")/lib.sh"
plant examples/rig/src/blob.rs 'let _ = reply.send(body); // rig-plant: blob' \
    'let _ = reply.send(if body.len() > 2 << 20 { let mut b = body.to_vec(); b[0] ^= 1; b.into() } else { body });'
