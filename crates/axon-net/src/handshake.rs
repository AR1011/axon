//! Binary hello exchange plus a mutual HMAC-SHA256 challenge–response over the whole transcript.
//! The initiator proves first; the responder proves nothing to a peer that has not.
//!
//! Hello: `"AXON" | lowest version u16 | highest version u16 | body_len u16 | nonce [32] | incarnation u128 |
//! max_frame u32 | accept u8 | node_id (u16 len) | namespace (u16 len) | lane u8`. The initiator names the lane and the
//! responder echoes it, with that lane's `max_frame`. A later version only appends to the body, and both hellos are sent
//! before either side checks the versions, so each can name both ranges when they do not meet.
//! Version 1 had one version where `lowest` is now, so a range starts at 2 and a 1 there is a version-1 peer.

use std::ops::RangeInclusive;
use std::panic::{AssertUnwindSafe, catch_unwind};

use bytes::{Buf, BufMut};
use ring::hmac;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

use crate::config::random;
use crate::frame::Codecs;
use crate::{Config, Error, Identity, Lane, Negotiation, PeerContext};

const MAGIC: &[u8; 4] = b"AXON";
/// The wire versions this build speaks. Its sessions run the highest one both ends speak.
pub const VERSIONS: RangeInclusive<u16> = 7..=7;
const HEAD: usize = 10;
const FIXED: usize = 32 + 16 + 4 + 1;
/// Room for what a later version appends.
const MAX_BODY: usize = 1024;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Role {
    /// Dials, and names the session's lane.
    Initiator(Lane),
    /// Accepts, on the lane the initiator names.
    Responder,
}

pub(crate) struct Link {
    pub role: Role,
    pub secure: bool,
    pub certificates: Vec<tokio_rustls::rustls::pki_types::CertificateDer<'static>>,
    pub remote: Option<std::net::SocketAddr>,
    /// The TCP socket under the stream, for kernel statistics.
    pub fd: Option<i32>,
}

pub(crate) struct Hello {
    pub versions: (u16, u16),
    pub identity: Identity,
    pub namespace: String,
    pub max_frame: usize,
    pub accept: Codecs,
    /// `None` in a hello from before version 7, or one naming no lane this build knows.
    pub lane: Option<Lane>,
}

fn encode(cfg: &Config, lane: Lane) -> Vec<u8> {
    let (node, ns) = (cfg.identity.node_id.as_bytes(), cfg.namespace.as_bytes());
    let mut out = Vec::with_capacity(HEAD + MAX_BODY);
    out.put_slice(MAGIC);
    out.put_u16(*cfg.versions.start());
    out.put_u16(*cfg.versions.end());
    out.put_u16((FIXED + 5 + node.len() + ns.len()) as u16);
    out.put_slice(&random::<32>());
    out.put_u128(cfg.identity.incarnation);
    out.put_u32(cfg.lanes.get(lane).limits.max_frame as u32);
    out.put_u8(cfg.accept.bits());
    for s in [node, ns] {
        out.put_u16(s.len() as u16);
        out.put_slice(s);
    }
    out.put_u8(lane.byte());
    out
}

fn parse(versions: (u16, u16), mut body: &[u8]) -> Option<Hello> {
    body.advance(32);
    let (incarnation, max_frame, accept) = (body.get_u128(), body.get_u32() as usize, Codecs::from_bits(body.get_u8()));
    let mut string = || {
        let len = (body.remaining() >= 2).then(|| body.get_u16() as usize).filter(|n| *n <= body.remaining())?;
        let s = std::str::from_utf8(&body[..len]).ok().filter(|s| axon::valid_text(s, axon::MAX_NAME))?.to_owned();
        body.advance(len);
        Some(s)
    };
    let (node_id, namespace) = (string()?, string()?);
    (max_frame > 0).then_some(Hello {
        versions,
        identity: Identity { node_id, incarnation },
        namespace,
        max_frame,
        accept,
        lane: body.first().copied().and_then(Lane::from_byte),
    })
}

async fn read_hello<S: AsyncRead + Unpin>(
    io: &mut S,
    ours: (u16, u16),
    transcript: &mut Vec<u8>,
) -> Result<Hello, Error> {
    let mut head = [0; HEAD];
    io.read_exact(&mut head).await?;
    let u16_at = |i: usize| u16::from_be_bytes([head[i], head[i + 1]]);
    let (versions, len) = ((u16_at(4), u16_at(6)), u16_at(8) as usize);
    if &head[..4] != MAGIC {
        return Err(Error::Negotiation(Negotiation::Magic));
    }
    if versions.0 == 1 {
        return Err(Error::Negotiation(Negotiation::Version { ours, theirs: (1, 1) }));
    }
    if len > MAX_BODY {
        return Err(Error::Negotiation(Negotiation::Oversized));
    }
    if len < FIXED {
        return Err(Error::Negotiation(Negotiation::Malformed));
    }
    let start = transcript.len() + HEAD;
    transcript.extend_from_slice(&head);
    transcript.resize(start + len, 0);
    io.read_exact(&mut transcript[start..]).await?;
    parse(versions, &transcript[start..]).ok_or(Error::Negotiation(Negotiation::Malformed))
}

/// The highest version both ranges hold.
fn common(ours: (u16, u16), theirs: (u16, u16)) -> Result<u16, Error> {
    let version = ours.1.min(theirs.1);
    (version >= ours.0.max(theirs.0))
        .then_some(version)
        .ok_or(Error::Negotiation(Negotiation::Version { ours, theirs }))
}

async fn send<S: AsyncWrite + Unpin>(io: &mut S, bytes: &[u8]) -> Result<(), Error> {
    io.write_all(bytes).await?;
    Ok(io.flush().await?)
}

fn challenge(initiator: bool, transcript: &[u8]) -> Vec<u8> {
    let label: &[u8] = if initiator { b"axon/1 initiator" } else { b"axon/1 responder" };
    [label, transcript].concat()
}

/// The peer's hello, the version the session runs and its lane. The caller bounds it.
pub(crate) async fn run<S>(io: &mut S, link: &Link, cfg: &Config) -> Result<(Hello, u16, Lane), Error>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    // The authenticator is the application's code: a panic in it refuses this peer instead of unwinding through the
    // dial or accept task that runs the handshake.
    let secret = catch_unwind(AssertUnwindSafe(|| cfg.auth.secret())).map_err(|_| Error::Auth)?;
    let key = hmac::Key::new(hmac::HMAC_SHA256, secret);
    let ours = (*cfg.versions.start(), *cfg.versions.end());
    let mut transcript = Vec::with_capacity(2 * (HEAD + MAX_BODY));
    let (peer, lane) = match link.role {
        Role::Initiator(lane) => {
            let mine = encode(cfg, lane);
            send(io, &mine).await?;
            transcript.extend_from_slice(&mine);
            (read_hello(io, ours, &mut transcript).await?, lane)
        }
        Role::Responder => {
            let peer = read_hello(io, ours, &mut transcript).await?;
            // A hello that names no lane is still answered, so an older peer learns both version ranges.
            let lane = peer.lane.unwrap_or(Lane::Control);
            let mine = encode(cfg, lane);
            transcript.extend_from_slice(&mine);
            send(io, &mine).await?;
            (peer, lane)
        }
    };
    let version = common(ours, peer.versions)?;
    if peer.lane != Some(lane) {
        return Err(Error::Negotiation(Negotiation::Malformed));
    }
    let ctx = PeerContext {
        identity: &peer.identity,
        namespace: &peer.namespace,
        remote: link.remote,
        secure: link.secure,
        certificates: &link.certificates,
    };
    let first = matches!(link.role, Role::Initiator(_));
    let mine = hmac::sign(&key, &challenge(first, &transcript));
    if first {
        send(io, mine.as_ref()).await?;
    }
    let mut tag = [0; 32];
    io.read_exact(&mut tag).await.map_err(|_| Error::Auth)?;
    let proven = hmac::verify(&key, &challenge(!first, &transcript), &tag).is_ok();
    let authorized = || catch_unwind(AssertUnwindSafe(|| cfg.auth.authorize(&ctx))).unwrap_or(false);
    if !(proven && peer.namespace == cfg.namespace && authorized()) {
        return Err(Error::Auth);
    }
    if !first {
        send(io, mine.as_ref()).await?;
    }
    Ok((peer, version, lane))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::SharedSecret;

    fn config(node: &str) -> Config {
        let mut cfg = Config::new(node, SharedSecret::new("a-cluster-wide-secret").expect("valid"));
        cfg.allow_plaintext = true;
        cfg
    }

    #[test]
    fn the_v7_hello_is_these_bytes() {
        let mut hello = encode(&config("a"), Lane::Control);
        hello[HEAD..HEAD + 48].fill(0);
        let mut want = vec![0x41, 0x58, 0x4F, 0x4E, 0x00, 0x07, 0x00, 0x07, 0x00, 0x42];
        want.extend([0; 48]);
        want.extend([0x00, 0x10, 0x00, 0x00, 0x07, 0x00, 0x01, 0x61, 0x00, 0x07]);
        want.extend(b"default");
        want.push(0x00);
        assert_eq!(hello, want);
        let bulk = encode(&config("a"), Lane::Bulk);
        assert_eq!((&bulk[HEAD + 48..HEAD + 52], bulk.last()), (&[0x00, 0x80, 0x00, 0x00][..], Some(&2)));
    }

    #[test]
    fn a_hello_parses_with_what_a_later_version_appends_to_its_body() {
        let mut hello = encode(&config("a"), Lane::Live);
        let len = u16::from_be_bytes([hello[8], hello[9]]) + 3;
        hello[8..HEAD].copy_from_slice(&len.to_be_bytes());
        hello.extend_from_slice(&[7, 7, 7]);
        let parsed = parse((2, 2), &hello[HEAD..]).expect("the appended bytes are skipped");
        assert_eq!((parsed.identity.node_id.as_str(), parsed.namespace.as_str()), ("a", "default"));
        assert_eq!(parsed.lane, Some(Lane::Live), "the lane comes before what a later version appends");
    }

    /// Runs `role` against a peer that sends `theirs`: the result, and the hello this side sent.
    async fn against(role: Role, theirs: Vec<u8>) -> (Result<Lane, Error>, Vec<u8>) {
        let (mut ours, mut peer) = tokio::io::duplex(4096);
        let link = Link { role, secure: false, certificates: Vec::new(), remote: None, fd: None };
        let cfg = config("b");
        let run = async { run(&mut ours, &link, &cfg).await.map(|(_, _, lane)| lane) };
        let talk = async {
            peer.write_all(&theirs).await.expect("written");
            let mut head = [0; HEAD];
            peer.read_exact(&mut head).await.expect("a hello");
            let mut body = vec![0; u16::from_be_bytes([head[8], head[9]]) as usize];
            peer.read_exact(&mut body).await.expect("its body");
            [&head[..], &body].concat()
        };
        tokio::join!(run, talk)
    }

    /// Node `a`'s hello on Control, with `byte` in place of the lane.
    fn with_lane(byte: Option<u8>) -> Vec<u8> {
        let mut hello = encode(&config("a"), Lane::Control);
        hello.pop();
        hello.extend(byte);
        let len = (hello.len() - HEAD) as u16;
        hello[8..HEAD].copy_from_slice(&len.to_be_bytes());
        hello
    }

    #[tokio::test]
    async fn a_hello_with_an_unknown_lane_is_malformed() {
        let malformed = |r: &Result<Lane, Error>| matches!(r, Err(Error::Negotiation(Negotiation::Malformed)));
        for byte in [None, Some(3), Some(255)] {
            let (result, sent) = against(Role::Responder, with_lane(byte)).await;
            assert!(malformed(&result), "lane {byte:?}: {result:?}");
            assert_eq!(sent.last(), Some(&Lane::Control.byte()), "answered, so the peer learns both ranges");
        }
        let (result, _) = against(Role::Initiator(Lane::Live), with_lane(Some(Lane::Bulk.byte()))).await;
        assert!(malformed(&result), "an echo of another lane: {result:?}");
        let mut older = with_lane(None);
        older[4..8].copy_from_slice(&[0, 6, 0, 6]);
        let (result, _) = against(Role::Responder, older).await;
        assert!(matches!(result, Err(Error::Negotiation(Negotiation::Version { theirs: (6, 6), .. }))), "{result:?}");
    }
}
