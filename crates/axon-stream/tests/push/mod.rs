//! A child's side of pushes, for the simulations that drive cores by hand.
use std::time::Instant;

use axon::Lane;
use axon_stream::*;
use bytes::Bytes;

/// The push in `body`, whose kind byte the node strips.
pub fn decode(body: &Bytes) -> (u64, u64, Push) {
    wire::decode_push(&mut body.slice(1..)).expect("a push decodes")
}

/// The child's answer to a push of `from`.
pub fn answer<A: Authorizer>(child: &mut RelayCore<A>, from: &PeerId, body: &Bytes, now: Instant) -> PushAnswer {
    let (channel, seq, push) = decode(body);
    child.handle_push(from, Lane::Live, from.incarnation, channel, seq, push, now)
}

/// Every push `parent` has for `child`, each answered at once.
pub fn deliver<A: Authorizer, B: Authorizer>(
    parent: &mut RelayCore<A>,
    p: &PeerId,
    child: &mut RelayCore<B>,
    c: &PeerId,
    now: Instant,
) {
    while let Some((id, body)) = parent.next_push(&c.node, Lane::Live, now, None) {
        let answer = answer(child, p, &body, now);
        parent.push_answered(c, Lane::Live, id, answer, now);
    }
}
