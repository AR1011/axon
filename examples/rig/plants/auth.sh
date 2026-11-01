#!/usr/bin/env bash
# The peer class's plant: a responder takes an initiator's proof unchecked, so a wrong secret is let in (peer-impostor).
. "$(dirname "$0")/lib.sh"
plant crates/axon-net/src/handshake.rs 'let proven = hmac::verify(' 'let proven = !first || hmac::verify('
