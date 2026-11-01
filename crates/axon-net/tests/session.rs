#![expect(clippy::disallowed_methods, reason = "these tests run on real threads and real time")]
use std::collections::HashMap;
use std::io;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use axon_net::rustls::pki_types::{PrivatePkcs8KeyDer, ServerName};
use axon_net::rustls::server::WebPkiClientVerifier;
use axon_net::rustls::{ClientConfig, ServerConfig};
use axon_net::*;
use bytes::Bytes;
use common::{Identity, config, identity, one_way, trust};
use tokio::sync::mpsc;
use tokio::time::Instant;

mod common;

const SECOND: Duration = Duration::from_secs(1);
const MS: Duration = Duration::from_millis(1);

fn frame(id: u64, payload: impl Into<Bytes>) -> Frame {
    Frame { kind: 1, schema: 1, protocol: 42, id, payload: payload.into(), ..Frame::default() }
}

async fn pair(a: &Config, b: &Config) -> (Result<Session, Error>, Result<Session, Error>) {
    pair_on(Lane::Control, a, b).await
}

async fn pair_on(lane: Lane, a: &Config, b: &Config) -> (Result<Session, Error>, Result<Session, Error>) {
    let (x, y) = tokio::io::duplex(1 << 16);
    tokio::join!(establish(x, Role::Initiator(lane), a), establish(y, Role::Responder, b))
}

async fn connected(a: &Config, b: &Config) -> (Session, Session) {
    connected_on(Lane::Control, a, b).await
}

async fn connected_on(lane: Lane, a: &Config, b: &Config) -> (Session, Session) {
    let (a, b) = pair_on(lane, a, b).await;
    (a.unwrap(), b.unwrap())
}

#[tokio::test]
async fn frames_arrive_in_order_with_each_codec_and_stats_add_up() {
    for codec in [Compression::None, Compression::Lz4, Compression::Zstd] {
        let mut a = config("a");
        a.lanes.control.compression = codec;
        let (a, mut b) = connected(&a, &config("b")).await;
        assert_eq!((a.peer().identity.node_id.as_str(), a.peer().compression), ("b", codec));
        let sender = a.sender.clone();
        let sending = tokio::spawn(async move {
            for i in 0..500 {
                sender.submit(frame(i, vec![i as u8; 1500]), None).await.unwrap();
            }
            sender.flush().await.unwrap();
        });
        for i in 0..500 {
            let got = b.receiver.recv().await.unwrap();
            assert_eq!((got.id, got.kind, got.protocol, &got.payload[..]), (i, 1, 42, &vec![i as u8; 1500][..]));
        }
        sending.await.unwrap();
        let (tx, rx) = (a.sender.stats(), b.sender.stats());
        assert_eq!(
            (tx.tx_frames, rx.rx_frames, tx.tx_payload_bytes, rx.rx_payload_bytes),
            (500, 500, 750_000, 750_000)
        );
        assert_eq!(tx.tx_wire_bytes, rx.rx_wire_bytes);
        assert_eq!(tx.tx_compressed_frames, if codec == Compression::None { 0 } else { 500 });
        assert!(tx.tx_batches < 500 && (codec == Compression::None || tx.tx_wire_bytes < 100_000));
        assert_eq!((tx.outbound_queued_frames, tx.outbound_reserved_bytes, rx.inbound_reserved_bytes), (0, 0, 0));
    }
}

#[tokio::test]
async fn a_packed_frame_crosses_a_session_without_a_codec_and_a_bad_one_is_handed_back() {
    let mut small = config("b");
    small.lanes.control.limits.max_frame = 16 << 10;
    let (a, mut b) = connected(&config("a"), &small).await;
    assert_eq!(a.peer().compression, Compression::None);
    let payload = b"abcd".repeat(2000);
    let packed =
        Frame { payload: axon_net::frame::pack(&payload, 1).unwrap(), packed: Compression::Zstd, ..frame(7, "") };
    a.sender.submit(packed.clone(), None).await.unwrap();
    a.sender.flush().await.unwrap();
    let got = b.receiver.recv().await.unwrap();
    assert_eq!((got.id, got.packed, &got.payload[..]), (7, Compression::None, &payload[..]));
    let (tx, rx) = (a.sender.stats(), b.sender.stats());
    assert_eq!((tx.tx_payload_bytes, rx.rx_payload_bytes, tx.tx_compressed_frames), (8000, 8000, 1));
    assert!(tx.tx_wire_bytes < 200);

    // Small on the wire, too large once decoded for this peer: refused here, the session lives.
    let big = Frame { payload: axon_net::frame::pack(&vec![0; (16 << 10) + 1], 1).unwrap(), ..packed.clone() };
    assert_eq!(a.sender.try_submit(big).unwrap_err().reason, SubmitReason::Oversized);
    let short = Frame { payload: Bytes::from_static(b"ab"), ..packed.clone() };
    assert_eq!(a.sender.try_submit(short).unwrap_err().reason, SubmitReason::Oversized);
    a.sender.submit(packed, None).await.unwrap();
    assert_eq!(b.receiver.recv().await.unwrap().payload.len(), 8000);
}

struct DenyAll;

impl Authenticator for DenyAll {
    fn authorize(&self, _: &PeerContext<'_>) -> bool {
        false
    }
}

#[tokio::test]
async fn authentication_is_mutual_and_default_deny() {
    let mut wrong = config("b");
    wrong.auth = Arc::new(SharedSecret::new("a-different-secret").unwrap());
    let mut other_ns = config("b");
    other_ns.namespace = "staging".into();
    let mut deny = config("b");
    deny.auth = Arc::new(DenyAll);
    for bad in [wrong, other_ns, deny] {
        let (a, b) = pair(&config("a"), &bad).await;
        assert!(matches!((a.err(), b.err()), (Some(Error::Auth), Some(Error::Auth))));
        let (b, a) = pair(&bad, &config("a")).await;
        assert!(matches!((a.err(), b.err()), (Some(Error::Auth), Some(Error::Auth))));
    }
    assert!(SharedSecret::new("short").is_err());

    let mut plain = config("a");
    plain.allow_plaintext = false;
    let mut unnamed = config("a");
    unnamed.namespace = String::new();
    let any = "127.0.0.1:0".parse().unwrap();
    for bad in [plain, unnamed] {
        assert!(matches!(pair(&bad, &config("b")).await.0, Err(Error::Config(_))));
        assert!(matches!(connect(any, Lane::Control, None, &bad).await, Err(Error::Config(_))));
        assert!(matches!(Listener::bind(any, None, bad, AcceptLimits::default()).await, Err(Error::Config(_))));
    }
}

#[tokio::test]
async fn garbage_and_silence_fail_the_handshake_without_detail() {
    use tokio::io::AsyncWriteExt;
    let (mut x, y) = tokio::io::duplex(1 << 16);
    x.write_all(b"GET / HTTP/1.1\r\n\r\n").await.unwrap();
    let got = establish(y, Role::Responder, &config("b")).await;
    assert!(matches!(got, Err(Error::Negotiation(Negotiation::Magic))));

    let (_silent, y) = tokio::io::duplex(1 << 16);
    let mut cfg = config("b");
    // A silent peer times out in 50 ms, not the shipped 5 s.
    (cfg.handshake_timeout, cfg.connect_timeout) = (Duration::from_millis(50), Duration::from_millis(50));
    assert!(matches!(establish(y, Role::Responder, &cfg).await, Err(Error::Timeout(Timeout::Handshake))));
}

/// Each side speaks a range of wire versions, and a session runs the highest both speak. Only ranges that do not meet
/// are refused, by both sides, naming both, and a listener counts that apart from other failed negotiations.
#[tokio::test]
async fn a_session_runs_the_highest_version_both_speak_and_ranges_that_do_not_meet_are_refused_naming_both() {
    let speaking = |node: &str, versions: std::ops::RangeInclusive<u16>| {
        let mut cfg = config(node);
        cfg.versions = versions;
        cfg
    };
    assert_eq!(config("a").versions, VERSIONS);
    for (x, y, runs) in [(2..=3, 2..=3, 3), (2..=2, 2..=3, 2), (2..=3, 3..=4, 3), (2..=5, 3..=3, 3)] {
        for (x, y) in [(x.clone(), y.clone()), (y, x)] {
            let (a, b) = connected(&speaking("a", x.clone()), &speaking("b", y.clone())).await;
            assert_eq!((a.peer().version, b.peer().version), (runs, runs), "{x:?} with {y:?}");
        }
    }
    let (a, b) = pair(&speaking("a", 2..=2), &speaking("b", 3..=4)).await;
    let (a, b) = (a.err(), b.err());
    let version = |ours, theirs| Some(Error::Negotiation(Negotiation::Version { ours, theirs }));
    assert_eq!(
        (format!("{a:?}"), format!("{b:?}")),
        (format!("{:?}", version((2, 2), (3, 4))), format!("{:?}", version((3, 4), (2, 2))))
    );
    let said = a.unwrap().to_string();
    assert!(said.contains("2..=2") && said.contains("3..=4"), "{said}");

    let any = "127.0.0.1:0".parse().unwrap();
    let listener = Listener::bind(any, None, speaking("server", 3..=4), AcceptLimits::default()).await.unwrap();
    let refused = connect(listener.local_addr(), Lane::Control, None, &speaking("client", 2..=2)).await.err();
    assert!(matches!(refused, Some(Error::Negotiation(Negotiation::Version { .. }))), "{refused:?}");
    handshake_failures(&listener).await;
    assert_eq!((HANDSHAKE_FAILURE_REASONS[4], listener.stats().handshake_failures), ("version", [0, 0, 0, 0, 1]));

    // Version 1's hello, `"AXON" | version u16 | body_len u16 | body`, is refused as version 1, not as a malformed one.
    let mut v1 = b"AXON\x00\x01\x00\x40".to_vec();
    v1.resize(8 + 0x40, 7);
    let (mut x, y) = tokio::io::duplex(1 << 16);
    tokio::io::AsyncWriteExt::write_all(&mut x, &v1).await.unwrap();
    let low_high = (*VERSIONS.start(), *VERSIONS.end());
    let refused = establish(y, Role::Responder, &config("b")).await.err();
    let v1_refused =
        matches!(refused, Some(Error::Negotiation(Negotiation::Version { ours, theirs: (1, 1) })) if ours == low_high);
    assert!(v1_refused, "{refused:?}");

    #[expect(clippy::reversed_empty_ranges, reason = "an empty range is the case under test")]
    for bad in [1..=2, 3..=2] {
        assert!(matches!(pair(&speaking("a", bad), &config("b")).await.0, Err(Error::Config(_))));
    }
}

/// Each lane negotiates its own codec and frame size, from each end's entry for it, and has its own outbox.
#[tokio::test]
async fn compression_is_negotiated_and_limits_only_shrink() {
    let mut a = config("a");
    a.lanes.live.compression = Compression::Zstd;
    a.lanes.bulk.compression = Compression::Lz4;
    a.lanes.live.limits.outbound_frames = 2;
    let mut b = config("b");
    b.accept = Codecs::NONE.with(Compression::Lz4);
    b.lanes.live.limits.max_frame = 4096;
    let ((live, mut far_live), (bulk, _far_bulk)) =
        (connected_on(Lane::Live, &a, &b).await, connected_on(Lane::Bulk, &a, &b).await);
    let peer = |s: &Session| (s.peer().compression, s.peer().max_frame);
    assert_eq!((peer(&live), peer(&far_live)), ((Compression::None, 4096), (Compression::None, 4096)));
    assert_eq!(peer(&bulk), (Compression::Lz4, 8 << 20), "Bulk's frame is 8 MiB at both ends by default");

    let rejected = live.sender.try_submit(frame(1, vec![0; 4097])).unwrap_err();
    assert_eq!((rejected.reason, rejected.frame.payload.len()), (SubmitReason::Oversized, 4097));
    live.sender.try_submit(frame(2, vec![0; 4096])).unwrap();
    assert_eq!(far_live.receiver.recv().await.unwrap().id, 2);
    assert_eq!(live.sender.stats().tx_compressed_frames, 0);
    bulk.sender.try_submit(frame(3, vec![0; 64 << 10])).unwrap();
    bulk.sender.flush().await.unwrap();
    assert_eq!(bulk.sender.stats().tx_compressed_frames, 1);

    // Live's outbox fills while its peer reads nothing; Bulk's still takes frames.
    let full = loop {
        if let Err(e) = live.sender.try_submit(frame(4, vec![0; 4096])) {
            break e.reason;
        }
    };
    assert_eq!(full, SubmitReason::Full);
    bulk.sender.try_submit(frame(5, "")).unwrap();
}

#[tokio::test(flavor = "multi_thread")]
async fn a_frame_to_an_idle_session_is_written_by_the_caller_even_off_the_runtime() {
    let (a, mut b) = connected(&config("a"), &config("b")).await;
    // Past the keepalive each side sends as the session comes up, and its answer.
    while a.sender.stats().pongs == 0 || b.sender.stats().pongs == 0 {
        tokio::time::sleep(Duration::from_millis(1)).await;
    }
    tokio::time::sleep(Duration::from_millis(1)).await;
    let (sender, batches) = (a.sender.clone(), a.sender.stats().tx_batches);
    let stats = std::thread::spawn(move || {
        sender.try_submit(frame(0, "now")).unwrap();
        sender.stats()
    });
    let stats = stats.join().unwrap();
    assert_eq!((stats.tx_frames, stats.tx_batches - batches, stats.outbound_reserved_bytes), (1, 1, 0));
    assert_eq!(b.receiver.recv().await.unwrap().id, 0);
}

#[tokio::test]
async fn a_full_outbox_rejects_definitely_and_credit_returns_when_frames_drop() {
    let mut a = config("a");
    a.lanes.control.limits.outbound_frames = 4;
    let mut b = config("b");
    b.lanes.control.limits.inbound_frames = 1;
    let (x, y) = tokio::io::duplex(1024);
    let (a, b) = tokio::join!(establish(x, Role::Initiator(Lane::Control), &a), establish(y, Role::Responder, &b));
    let (a, mut b) = (a.unwrap(), b.unwrap());

    let mut admitted = 0;
    let full = loop {
        match a.sender.try_submit(frame(admitted, vec![7; 1024])) {
            Ok(()) => admitted += 1,
            Err(e) => break e,
        }
        tokio::task::yield_now().await;
    };
    assert_eq!((full.reason, full.frame.id), (SubmitReason::Full, admitted));
    let waited = a.sender.submit(full.frame, Some(Instant::now() + Duration::from_millis(30))).await.unwrap_err();
    assert_eq!(waited.reason, SubmitReason::Deadline);
    assert_eq!(a.sender.stats().tx_rejected, [1, 0, 1, 0]);

    let pending = tokio::spawn({
        let sender = a.sender.clone();
        async move { sender.submit(waited.frame, None).await }
    });
    for i in 0..=admitted {
        assert_eq!(b.receiver.recv().await.unwrap().id, i);
    }
    pending.await.unwrap().unwrap();
}

#[tokio::test]
async fn shutdown_flushes_then_the_peer_sees_a_clean_close() {
    let (a, mut b) = connected(&config("a"), &config("b")).await;
    // A deadline already passed still admits a frame that fits: it only bounds a wait for room.
    for i in 0..100 {
        a.sender.submit(frame(i, "bye"), Some(Instant::now())).await.unwrap();
    }
    a.sender.shutdown(Instant::now() + SECOND).await.unwrap();
    assert_eq!(a.sender.try_submit(frame(0, "late")).unwrap_err().reason, SubmitReason::Closed);
    for i in 0..100 {
        assert_eq!(b.receiver.recv().await.unwrap().id, i);
    }
    assert!(matches!(b.receiver.recv().await, Err(Error::PeerClosed)));
    assert!(matches!(a.sender.closed().await, Error::Closed));
    assert_eq!(b.sender.stats().closed, Some("peer_closed"));
}

#[tokio::test]
async fn dropping_the_receiver_closes_both_directions() {
    let (a, b) = connected(&config("a"), &config("b")).await;
    drop(a.receiver);
    assert!(matches!(a.sender.closed().await, Error::Closed));
    assert!(matches!(b.sender.closed().await, Error::PeerClosed));
}

#[tokio::test(start_paused = true)]
async fn keepalive_holds_an_idle_session_open_and_measures_rtt() {
    let (a, b) = connected(&config("a"), &config("b")).await;
    tokio::time::sleep(Duration::from_secs(300)).await;
    let stats = a.sender.stats();
    assert!(a.sender.is_active() && b.sender.is_active());
    assert!(stats.pings >= 29 && stats.pongs >= 29 && stats.tx_frames == 0);
}

/// A stream whose bytes arrive `delay` after they were written, each way.
fn delayed(delay: Duration) -> (tokio::io::DuplexStream, tokio::io::DuplexStream) {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    async fn carry(
        mut from: tokio::io::ReadHalf<tokio::io::DuplexStream>,
        mut to: impl AsyncWriteExt + Unpin + Send + 'static,
        delay: Duration,
    ) {
        let (tx, mut rx) = mpsc::channel::<(Instant, Vec<u8>)>(1024);
        tokio::spawn(async move {
            while let Some((at, bytes)) = rx.recv().await {
                tokio::time::sleep_until(at).await;
                if to.write_all(&bytes).await.is_err() {
                    return;
                }
            }
        });
        let mut buf = vec![0; 1 << 16];
        while let Ok(n @ 1..) = from.read(&mut buf).await {
            if tx.send((Instant::now() + delay, buf[..n].to_vec())).await.is_err() {
                return;
            }
        }
    }
    let ((a, a_far), (b, b_far)) = (tokio::io::duplex(1 << 16), tokio::io::duplex(1 << 16));
    let ((a_rx, a_tx), (b_rx, b_tx)) = (tokio::io::split(a_far), tokio::io::split(b_far));
    tokio::spawn(carry(a_rx, b_tx, delay));
    tokio::spawn(carry(b_rx, a_tx, delay));
    (a, b)
}

/// A session pings as it comes up, so the round trip is known after one, not after a `keepalive_interval`.
#[tokio::test(start_paused = true)]
async fn the_round_trip_is_known_one_round_trip_after_a_dial() {
    let (x, y) = delayed(Duration::from_millis(100));
    let (a, b) = (config("a"), config("b"));
    let (a, b) = tokio::join!(establish(x, Role::Initiator(Lane::Control), &a), establish(y, Role::Responder, &b));
    let (a, b) = (a.unwrap(), b.unwrap());
    assert_eq!(a.sender.min_rtt(), Duration::ZERO);
    tokio::time::sleep(Duration::from_millis(300)).await;
    for end in [&a, &b] {
        let rtt = end.sender.min_rtt();
        assert!((Duration::from_millis(200)..Duration::from_millis(210)).contains(&rtt), "{rtt:?}");
    }
}

/// A silent peer's session ends as idle whichever half runs first: a writer that saw the reader go and called it
/// `Closed` made the cluster wait out a handshake for a peer that had only stopped answering. Many sessions on several
/// workers, each with a caller waiting on its receiver as the cluster's does, so a writer can run before its reader has
/// said why it stopped.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn a_peer_that_stops_reading_is_timed_out_as_idle() {
    let mut a = config("a");
    // Short, so that two thousand sessions time out in well under a second.
    (a.keepalive_interval, a.idle_timeout) = (Duration::from_millis(50), Duration::from_millis(150));
    let mut b = config("b");
    b.lanes.control.limits.inbound_frames = 1;
    let (mut ends, mut peers) = (Vec::new(), Vec::new());
    for _ in 0..2000 {
        let (a, b) = connected(&a, &b).await;
        // b's consumer never receives: its reader blocks on delivery and stops answering pings. A starved runtime can
        // time the session out before the last submit; its reason is still asserted below.
        for i in 0..3 {
            drop(a.sender.submit(frame(i, "x"), None).await);
        }
        let (mut receiver, sender) = (a.receiver, a.sender);
        ends.push(tokio::spawn(async move { (receiver.recv().await.err(), sender.stats().closed) }));
        peers.push(b);
    }
    for end in ends {
        let end = end.await.unwrap();
        assert!(matches!(end, (Some(Error::Timeout(Timeout::Idle)), Some("timeout_idle"))), "{end:?}");
    }
}

#[tokio::test(start_paused = true)]
async fn a_slow_consumer_is_not_a_silent_peer() {
    let mut a = config("a");
    // a's pings would refresh b's clock and hide the fault, and a's idle timer would time b out as in the test above.
    (a.keepalive_interval, a.idle_timeout) = (Duration::from_secs(1000), Duration::from_secs(2000));
    let mut b = config("b");
    b.lanes.control.limits.inbound_frames = 1;
    let (a, mut b) = connected(&a, &b).await;
    for i in 0..3 {
        a.sender.submit(frame(i, "x"), None).await.unwrap();
    }
    // b's reader waits on its consumer three times as long as b's idle timeout.
    tokio::time::sleep(Duration::from_secs(90)).await;
    for i in 0..3 {
        assert_eq!(b.receiver.recv().await.unwrap().id, i);
    }
    tokio::time::sleep(Duration::from_secs(60)).await;
    assert!(b.sender.is_active());
}

#[tokio::test]
async fn listener_accepts_tcp_and_redial_backs_off_a_dead_address() {
    let any = "127.0.0.1:0".parse().unwrap();
    let mut listener = Listener::bind(any, None, config("server"), AcceptLimits::default()).await.unwrap();
    let addr = listener.local_addr();

    let mut intruder = config("intruder");
    intruder.auth = Arc::new(SharedSecret::new("not-the-cluster-secret").unwrap());
    assert!(matches!(connect(addr, Lane::Control, None, &intruder).await, Err(Error::Auth)));

    let client = connect(addr, Lane::Control, None, &config("client")).await.unwrap();
    let mut server = listener.accept().await.unwrap();
    assert_eq!((server.peer().identity.node_id.as_str(), server.peer().remote.is_some()), ("client", true));
    client.sender.submit(frame(9, "over tcp"), None).await.unwrap();
    assert_eq!(&server.receiver.recv().await.unwrap().payload[..], b"over tcp");
    #[cfg(any(target_os = "linux", target_vendor = "apple"))]
    for session in [&client, &server] {
        let tcp = session.sender.stats().tcp.expect("the kernel reports on a TCP session");
        assert!(tcp.mss > 0 && tcp.cwnd_bytes > 0, "{tcp:?}");
    }
    let stats = listener.stats();
    assert_eq!((stats.accepted, stats.handshake_failures), (1, [1, 0, 0, 0, 0]));
    let timings = client.sender.stats().timings;
    assert_eq!(
        (timings.sample_every, timings.queue.count(), timings.encode.count(), timings.write.count()),
        (128, 1, 1, 1)
    );
    assert_eq!(server.sender.stats().timings.decode.count(), 1);

    drop(listener);
    let mut redial = Redial::new(addr, Lane::Control, None, config("client"), Backoff::default());
    assert!(matches!(redial.connect().await, Err(DialError::Failed(Error::Io(_)))));
    assert!(
        matches!(redial.connect().await, Err(DialError::Backoff { retry_in }) if retry_in <= Duration::from_millis(120))
    );
    tokio::time::sleep(Duration::from_millis(130)).await;
    assert!(matches!(redial.connect().await, Err(DialError::Failed(_))));
    assert_eq!(redial.failures(), 2);
}

/// At the defaults: a peer dials a session on each of three lanes.
#[tokio::test]
async fn handshake_slots_are_limited_per_ip() {
    let limits = AcceptLimits::default();
    let per_ip = limits.max_handshakes_per_ip;
    assert_eq!((limits.max_connections, limits.max_handshakes, per_ip), (3072, 64, 16));
    let mut listener = Listener::bind("127.0.0.1:0".parse().unwrap(), None, config("server"), limits).await.unwrap();
    let addr = listener.local_addr();
    let idle: Vec<_> = futures(per_ip + 1, || tokio::net::TcpStream::connect(addr)).await;
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert_eq!(listener.stats().rejected_per_ip, 1);
    drop(idle);
    tokio::time::sleep(Duration::from_millis(50)).await;
    let _client = connect(addr, Lane::Control, None, &config("client")).await.unwrap();
    assert!(listener.accept().await.is_some());
}

#[tokio::test]
async fn handshakes_over_the_cap_wait_their_turn() {
    let (server, client) = one_way(&identity("node-a"), "node-a");
    for (server, client) in [(None, None), (Some(server), Some(client))] {
        let limits = AcceptLimits { max_handshakes: 1, max_handshakes_per_ip: 64, ..Default::default() };
        let any = "127.0.0.1:0".parse().unwrap();
        let mut listener = Listener::bind(any, server, config("server"), limits).await.unwrap();
        let addr = listener.local_addr();
        let dials: Vec<_> = (0..32)
            .map(|i| {
                let client = client.clone();
                tokio::spawn(
                    async move { connect(addr, Lane::Control, client.as_ref(), &config(&format!("c{i}"))).await },
                )
            })
            .collect();
        for dial in dials {
            dial.await.unwrap().unwrap();
            assert!(listener.accept().await.is_some());
        }
    }
}

#[tokio::test]
async fn a_silent_dial_holds_a_handshake_slot_only_for_its_own_budget() {
    let mut server = config("server");
    // The budget under test.
    (server.handshake_timeout, server.connect_timeout) = (Duration::from_millis(500), Duration::from_millis(500));
    let limits = AcceptLimits { max_handshakes: 1, ..Default::default() };
    let mut listener = Listener::bind("127.0.0.1:0".parse().unwrap(), None, server, limits).await.unwrap();
    let addr = listener.local_addr();
    let mut silent = Vec::new();
    for _ in 0..2 {
        silent.push(tokio::net::TcpStream::connect(addr).await.unwrap());
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    // Served when the second silent dial's budget, counted from its accept, runs out: at 600 ms, not 1000.
    let mut client = config("client");
    // Longer than the wait the test measures, so the dial is served, not timed out.
    (client.handshake_timeout, client.connect_timeout) = (Duration::from_millis(700), Duration::from_millis(700));
    connect(addr, Lane::Control, None, &client).await.unwrap();
    assert!(listener.accept().await.is_some());
    let stats = listener.stats();
    assert_eq!((stats.rejected_busy, stats.handshake_failures), (0, [0, 0, 2, 0, 0]));
}

async fn futures<F: Future>(n: usize, mut make: impl FnMut() -> F) -> Vec<F::Output> {
    let mut out = Vec::new();
    for _ in 0..n {
        out.push(make().await);
    }
    out
}

/// Waits up to a second for the listener to count a failed handshake, which it does after the dialler has seen it fail.
async fn handshake_failures(listener: &Listener) {
    let deadline = Instant::now() + SECOND;
    while listener.stats().handshake_failures.iter().sum::<u64>() == 0 && Instant::now() < deadline {
        tokio::time::sleep(Duration::from_millis(1)).await;
    }
}

/// Valid only from the first day of `from` to the first of `to`: how a good certificate looks to a clock that far off.
fn dated(name: &str, from: i32, to: i32) -> Identity {
    let mut params = rcgen::CertificateParams::new(vec![name.to_owned()]).unwrap();
    (params.not_before, params.not_after) = (rcgen::date_time_ymd(from, 1, 1), rcgen::date_time_ymd(to, 1, 1));
    let key = rcgen::KeyPair::generate().unwrap();
    let cert = params.self_signed(&key).unwrap();
    Identity { cert: cert.der().clone(), key: PrivatePkcs8KeyDer::from(key.serialize_der()) }
}

#[tokio::test]
async fn a_certificate_outside_its_validity_is_refused_at_the_handshake_and_the_listener_serves_on() {
    for bad in [dated("node-a", 2000, 2001), dated("node-a", 2990, 2999)] {
        let (server, client) = one_way(&bad, "node-a");
        let any = "127.0.0.1:0".parse().unwrap();
        let mut listener = Listener::bind(any, Some(server), config("node-a"), AcceptLimits::default()).await.unwrap();
        let addr = listener.local_addr();
        let refused = connect(addr, Lane::Control, Some(&client), &config("node-b")).await.err();
        assert!(matches!(refused, Some(Error::Io(_))), "{refused:?}");

        let (server, client) = one_way(&identity("node-a"), "node-a");
        listener.tls().unwrap().set(server);
        connect(addr, Lane::Control, Some(&client), &config("node-b")).await.unwrap();
        assert!(listener.accept().await.is_some());
        handshake_failures(&listener).await;
        assert_eq!(listener.stats().handshake_failures, [0, 0, 0, 1, 0]);
    }
}

#[tokio::test]
async fn a_redial_reaches_the_process_that_replaced_a_gone_one_as_a_newer_incarnation() {
    let any = "127.0.0.1:0".parse().unwrap();
    let mut listener = Listener::bind(any, None, config("server"), AcceptLimits::default()).await.unwrap();
    let addr = listener.local_addr();
    // A fixed 50 ms redial, so the new process is reached within the test.
    let backoff = Backoff { initial: Duration::from_millis(50), max: Duration::from_millis(50), jitter: 0.0 };
    let mut redial = Redial::new(addr, Lane::Control, None, config("client"), backoff);
    let first = redial.connect().await.unwrap();
    let served = listener.accept().await.unwrap();
    // The process goes, its sessions with it.
    listener.close().await;
    drop(served);
    first.sender.closed().await;
    assert!(matches!(redial.connect().await, Err(DialError::Failed(_))));
    tokio::time::sleep(Duration::from_millis(60)).await;

    let mut listener = Listener::bind(addr, None, config("server"), AcceptLimits::default()).await.unwrap();
    let second = redial.connect().await.unwrap();
    assert!(listener.accept().await.is_some());
    let (was, is) = (first.peer().identity.incarnation, second.peer().identity.incarnation);
    assert!(is > was, "the new process is a newer incarnation: {is:x} after {was:x}");
    assert_eq!(redial.failures(), 0);
}

/// node-a serves and node-b dials, each trusting only the other's certificate.
fn mtls() -> (Arc<ServerConfig>, ClientTls) {
    let (server, client) = (identity("node-a"), identity("node-b"));
    let server_tls = ServerConfig::builder()
        .with_client_cert_verifier(WebPkiClientVerifier::builder(trust(&client.cert)).build().unwrap())
        .with_single_cert(vec![server.cert.clone()], server.key.clone_key().into())
        .unwrap();
    let client_tls = ClientTls {
        name: ServerName::try_from("node-a").unwrap(),
        config: Arc::new(
            ClientConfig::builder()
                .with_root_certificates(trust(&server.cert))
                .with_client_auth_cert(vec![client.cert.clone()], client.key.clone_key().into())
                .unwrap(),
        ),
    };
    (Arc::new(server_tls), client_tls)
}

#[tokio::test]
async fn mtls_binds_the_node_id_to_the_certificate() {
    let (server_tls, client_tls) = mtls();
    let any = "127.0.0.1:0".parse().unwrap();
    let cfg = Config::new("node-a", CertificateIdentity);
    let mut listener = Listener::bind(any, Some(server_tls), cfg, AcceptLimits::default()).await.unwrap();
    let addr = listener.local_addr();

    let impostor = Config::new("node-c", CertificateIdentity);
    assert!(matches!(connect(addr, Lane::Control, Some(&client_tls), &impostor).await, Err(Error::Auth)));
    assert!(matches!(
        connect(addr, Lane::Control, None, &Config::new("node-b", CertificateIdentity)).await,
        Err(Error::Config(_))
    ));

    let session =
        connect(addr, Lane::Control, Some(&client_tls), &Config::new("node-b", CertificateIdentity)).await.unwrap();
    let mut accepted = listener.accept().await.unwrap();
    assert!(session.peer().secure && accepted.peer().secure);
    assert_eq!(accepted.peer().identity.node_id, "node-b");
    session.sender.submit(frame(1, "over mtls"), None).await.unwrap();
    assert_eq!(&accepted.receiver.recv().await.unwrap().payload[..], b"over mtls");
}

type Accepts = mpsc::Sender<io::Result<(Box<dyn Io>, SocketAddr)>>;

/// Streams in memory; every dial comes from 10.0.0.2.
#[derive(Default)]
struct Duplex {
    listeners: Mutex<HashMap<SocketAddr, Accepts>>,
}

impl Duplex {
    const CLIENT: IpAddr = IpAddr::V4(Ipv4Addr::new(10, 0, 0, 2));

    fn listener(&self, at: SocketAddr) -> Option<Accepts> {
        self.listeners.lock().unwrap().get(&at).cloned()
    }
}

impl Network for Duplex {
    fn connect(&self, to: SocketAddr, _: Lane) -> Dial {
        let listener = self.listener(to);
        let from = SocketAddr::new(Self::CLIENT, 40_000);
        Box::pin(async move {
            let (near, far) = tokio::io::duplex(1 << 16);
            let listener = listener.ok_or(io::ErrorKind::ConnectionRefused)?;
            listener.send(Ok((Box::new(far), from))).await.map_err(|_| io::ErrorKind::ConnectionRefused)?;
            Ok(Box::new(near) as Box<dyn Io>)
        })
    }

    fn bind(&self, at: SocketAddr) -> io::Result<(SocketAddr, Incoming)> {
        let (tx, rx) = mpsc::channel(8);
        self.listeners.lock().unwrap().insert(at, tx);
        Ok((at, rx))
    }
}

#[tokio::test]
async fn sessions_run_over_a_custom_network() {
    let network = Arc::new(Duplex::default());
    let on = |mut cfg: Config| {
        cfg.network = Some(network.clone());
        cfg
    };

    // No interface here has 10.0.0.1: only the network can bind it.
    let at: SocketAddr = "10.0.0.1:7000".parse().unwrap();
    let limits = AcceptLimits { max_handshakes_per_ip: 2, ..Default::default() };
    let mut listener = Listener::bind(at, None, on(config("server")), limits).await.unwrap();
    assert_eq!(listener.local_addr(), at);
    let client = connect(at, Lane::Control, None, &on(config("client"))).await.unwrap();
    let mut server = listener.accept().await.unwrap();
    assert_eq!(server.peer().remote.map(|r| r.ip()), Some(Duplex::CLIENT));
    client.sender.submit(frame(1, "in memory"), None).await.unwrap();
    assert_eq!(&server.receiver.recv().await.unwrap().payload[..], b"in memory");
    // No socket, so no fd to ask the kernel about.
    assert!(client.sender.stats().tcp.is_none() && server.sender.stats().tcp.is_none());

    // Three silent dials from one address: the third is turned away.
    let mut silent = Vec::new();
    for _ in 0..3 {
        silent.push(network.connect(at, Lane::Control).await.unwrap());
    }
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert_eq!(listener.stats().rejected_per_ip, 1);
    drop(silent);

    // An accept error (EMFILE) is counted, and the listener goes on.
    network.listener(at).unwrap().send(Err(io::Error::from_raw_os_error(24))).await.unwrap();
    let _again = connect(at, Lane::Control, None, &on(config("client"))).await.unwrap();
    assert!(listener.accept().await.is_some());
    let stats = listener.stats();
    assert_eq!((stats.accepted, stats.accept_errors, stats.handshake_failures), (2, 1, [0, 0, 0, 2, 0]));

    // Closing drops the listener's end, so the address refuses dials.
    listener.close().await;
    assert!(matches!(connect(at, Lane::Control, None, &on(config("client"))).await, Err(Error::Io(_))));

    // TLS on the same network. CertificateIdentity passes only if each side saw the other's certificate.
    let (server_tls, client_tls) = mtls();
    let cfg = on(Config::new("node-a", CertificateIdentity));
    let mut listener = Listener::bind(at, Some(server_tls), cfg, AcceptLimits::default()).await.unwrap();
    let impostor = on(Config::new("node-c", CertificateIdentity));
    assert!(matches!(connect(at, Lane::Control, Some(&client_tls), &impostor).await, Err(Error::Auth)));
    let session =
        connect(at, Lane::Control, Some(&client_tls), &on(Config::new("node-b", CertificateIdentity))).await.unwrap();
    let mut accepted = listener.accept().await.unwrap();
    assert!(session.peer().secure && accepted.peer().secure);
    assert_eq!(accepted.peer().identity.node_id, "node-b");
    session.sender.submit(frame(2, "tls in memory"), None).await.unwrap();
    assert_eq!(&accepted.receiver.recv().await.unwrap().payload[..], b"tls in memory");
    assert!(session.sender.stats().tcp.is_none() && accepted.sender.stats().tcp.is_none());
}

/// Dials never answer; a bind hands out port 7001 for :0 and then closes at once.
struct Silent;

impl Network for Silent {
    fn connect(&self, _: SocketAddr, _: Lane) -> Dial {
        Box::pin(std::future::pending())
    }

    fn bind(&self, at: SocketAddr) -> io::Result<(SocketAddr, Incoming)> {
        Ok((SocketAddr::new(at.ip(), 7001), mpsc::channel(1).1))
    }
}

#[tokio::test]
async fn a_silent_network_times_out_dials_and_ends_listeners() {
    let mut cfg = config("client");
    cfg.network = Some(Arc::new(Silent));
    // Short, so the silent dials time out quickly.
    (cfg.handshake_timeout, cfg.connect_timeout) = (Duration::from_millis(100), Duration::from_millis(100));

    // A crashed machine never answers: the dial ends at the caller's deadline.
    let dialled =
        tokio::time::timeout(SECOND, connect("10.0.0.1:7000".parse().unwrap(), Lane::Control, None, &cfg)).await;
    assert!(matches!(dialled, Ok(Err(Error::Timeout(Timeout::Connect)))));
    // And at connect_timeout when that comes first: a machine that answers no SYN is gone.
    // Short, as above; the handshake's own budget is long.
    (cfg.handshake_timeout, cfg.connect_timeout) = (Duration::from_secs(60), Duration::from_millis(100));
    let dialled =
        tokio::time::timeout(SECOND, connect("10.0.0.1:7000".parse().unwrap(), Lane::Control, None, &cfg)).await;
    assert!(matches!(dialled, Ok(Err(Error::Timeout(Timeout::Connect)))));

    // The listener reports the address the network gave, and ends when the network closes its channel.
    let mut listener = Listener::bind("10.0.0.1:0".parse().unwrap(), None, cfg, AcceptLimits::default()).await.unwrap();
    assert_eq!(listener.local_addr(), "10.0.0.1:7001".parse().unwrap());
    assert!(tokio::time::timeout(SECOND, listener.accept()).await.expect("accept loop must end").is_none());
}

/// Each dial is answered after the delay, on a stream whose far end never says a word.
struct Slow(Duration, Mutex<Vec<tokio::io::DuplexStream>>);

impl Network for Slow {
    fn connect(&self, _: SocketAddr, _: Lane) -> Dial {
        let (near, far) = tokio::io::duplex(1 << 16);
        self.1.lock().unwrap().push(far);
        let delay = self.0;
        Box::pin(async move {
            tokio::time::sleep(delay).await;
            Ok(Box::new(near) as Box<dyn Io>)
        })
    }

    fn bind(&self, _: SocketAddr) -> io::Result<(SocketAddr, Incoming)> {
        Err(io::ErrorKind::Unsupported.into())
    }
}

#[tokio::test(start_paused = true)]
async fn a_slow_connect_and_a_stalled_handshake_share_one_handshake_timeout() {
    let mut cfg = config("client");
    cfg.network = Some(Arc::new(Slow(Duration::from_millis(600), Mutex::default())));
    // A round second, so the bound reads plainly; the clock is paused.
    (cfg.handshake_timeout, cfg.connect_timeout) = (SECOND, SECOND);
    let started = Instant::now();
    let dialled = connect("10.0.0.1:7000".parse().unwrap(), Lane::Control, None, &cfg).await.err();
    assert!(matches!(dialled, Some(Error::Timeout(Timeout::Handshake))), "{dialled:?}");
    let took = started.elapsed();
    assert!(took < SECOND + Duration::from_millis(10), "the connect's 600 ms came out of the same budget: {took:?}");
}

/// The connect has its own bound, apart from the keepalive: a SYN the kernel sends again at 1 s still connects when
/// `connect_timeout` is longer. The handshake after it keeps the rest of `handshake_timeout`.
#[tokio::test(start_paused = true)]
async fn a_dial_waits_connect_timeout_for_its_connect_and_handshake_timeout_for_the_rest() {
    let at: SocketAddr = "10.0.0.1:7000".parse().unwrap();
    let mut cfg = config("client");
    // A keepalive shorter than the connect, as ClusterConfig::new sets them, and round numbers on the paused clock.
    (cfg.keepalive_interval, cfg.connect_timeout, cfg.handshake_timeout) = (SECOND, 2 * SECOND, 5 * SECOND);
    cfg.network = Some(Arc::new(Silent));
    let started = Instant::now();
    let dialled = connect(at, Lane::Control, None, &cfg).await.err();
    assert!(matches!(dialled, Some(Error::Timeout(Timeout::Connect))), "{dialled:?}");
    let took = started.elapsed();
    assert!((2 * SECOND..2 * SECOND + MS).contains(&took), "a hanging connect fails at connect_timeout: {took:?}");

    cfg.network = Some(Arc::new(Slow(Duration::from_millis(1500), Mutex::default())));
    let started = Instant::now();
    let dialled = connect(at, Lane::Control, None, &cfg).await.err();
    assert!(matches!(dialled, Some(Error::Timeout(Timeout::Handshake))), "{dialled:?}");
    let took = started.elapsed();
    assert!((5 * SECOND..5 * SECOND + MS).contains(&took), "the handshake keeps the rest of its budget: {took:?}");

    // Past the handshake's budget it could never be waited for.
    cfg.connect_timeout = 6 * SECOND;
    assert!(matches!(connect(at, Lane::Control, None, &cfg).await, Err(Error::Config(_))));
}

#[tokio::test]
async fn the_hello_names_the_lane_and_the_responder_echoes_it() {
    for lane in Lane::ALL {
        let (a, b) = connected_on(lane, &config("a"), &config("b")).await;
        let max_frame = if lane == Lane::Bulk { 8 << 20 } else { 1 << 20 };
        assert_eq!((a.peer().lane, b.peer().lane), (lane, lane));
        assert_eq!((a.peer().max_frame, b.peer().max_frame), (max_frame, max_frame), "{lane:?}");
    }
    let any = "127.0.0.1:0".parse().unwrap();
    let mut listener = Listener::bind(any, None, config("server"), AcceptLimits::default()).await.unwrap();
    let client = connect(listener.local_addr(), Lane::Live, None, &config("client")).await.unwrap();
    let server = listener.accept().await.unwrap();
    assert_eq!((client.peer().lane, server.peer().lane), (Lane::Live, Lane::Live));
}

/// `a`'s Bulk lane capped at `per_sec`.
fn capped(per_sec: u64) -> Config {
    let mut a = config("a");
    a.lanes.bulk.max_bytes_per_sec = Some(per_sec);
    a
}

/// Reads `n` frames off `b`, checking their order, and returns when the last came.
fn read_in_order(mut b: Session, n: u64) -> tokio::task::JoinHandle<Instant> {
    tokio::spawn(async move {
        for i in 0..n {
            assert_eq!(b.receiver.recv().await.unwrap().id, i, "frames in submission order");
        }
        Instant::now()
    })
}

/// 64 frames of 64 KiB from `a` to `b`: how long until the last arrived, in order.
async fn burst(a: &Session, b: Session) -> Duration {
    let started = Instant::now();
    let arrived = read_in_order(b, 64);
    for i in 0..64 {
        a.sender.submit(frame(i, vec![7; 64 << 10]), None).await.unwrap();
    }
    arrived.await.unwrap() - started
}

#[tokio::test(start_paused = true)]
async fn a_capped_session_writes_no_faster_than_its_rate_and_keeps_order() {
    const RATE: u64 = 1 << 20;
    const FRAME: usize = 64 << 10;
    let (a, b) = connected_on(Lane::Bulk, &capped(RATE), &config("b")).await;
    let took = burst(&a, b).await;
    // The bucket starts full, one write batch deep, and the last frame it takes may overdraw it by up to a frame.
    let wire = 64 * (FRAME + axon_net::frame::HEADER_LEN);
    let depth = config("a").lanes.bulk.limits.write_batch_bytes;
    let least = Duration::from_secs_f64((wire - depth - FRAME) as f64 / RATE as f64);
    let most = Duration::from_secs_f64(wire as f64 / RATE as f64) + 100 * MS;
    assert!((least..most).contains(&took), "{wire} bytes at {RATE}/s in {took:?}, not within {least:?}..{most:?}");
    let paced = a.sender.stats().paced;
    assert!(paced > took / 2, "paced {paced:?} of {took:?}");
    let (uncapped, b) = connected_on(Lane::Bulk, &config("a"), &config("b")).await;
    assert!(burst(&uncapped, b).await < 10 * MS, "only the capped lane waits");
    assert_eq!(uncapped.sender.stats().paced, Duration::ZERO);
}

#[tokio::test(start_paused = true)]
async fn a_capped_session_still_answers_pings_on_time() {
    let quick = |mut cfg: Config| {
        // Keepalives the test can count within a backlog of 16 s.
        (cfg.keepalive_interval, cfg.idle_timeout) = (SECOND, 3 * SECOND);
        cfg
    };
    let (a, b) = connected_on(Lane::Bulk, &quick(capped(64 << 10)), &quick(config("b"))).await;
    let far = b.sender.clone();
    let arrived = read_in_order(b, 16);
    for i in 0..16 {
        a.sender.submit(frame(i, vec![7; 64 << 10]), None).await.unwrap();
    }
    tokio::time::sleep(8 * SECOND).await;
    for (side, stats) in [("a", a.sender.stats()), ("b", far.stats())] {
        assert!(stats.pongs >= 6, "{side}: {} pongs in 8 s", stats.pongs);
        assert!(stats.rtt_ns < 50_000_000, "{side}: a keepalive took {} ms behind the cap", stats.rtt_ns / 1_000_000);
    }
    assert!(a.sender.stats().outbound_queued_frames > 0, "still behind the cap");
    assert!(a.sender.is_active() && far.is_active(), "behind the cap and up");
    arrived.await.unwrap();
}

/// What the session's first TCP sample read back.
#[cfg(target_os = "linux")]
async fn lowat(s: &Session) -> Option<u32> {
    for _ in 0..100 {
        if let Some(tcp) = s.sender.stats().tcp {
            return tcp.notsent_lowat;
        }
        tokio::time::sleep(10 * MS).await;
    }
    panic!("no TCP sample")
}

#[cfg(target_os = "linux")]
#[tokio::test]
async fn tcp_notsent_lowat_is_set_on_dialled_and_accepted_sockets() {
    let mut server = config("server");
    server.lanes.bulk.notsent_lowat = Some(64 << 10);
    server.lanes.control.notsent_lowat = None;
    let any = "127.0.0.1:0".parse().unwrap();
    let mut listener = Listener::bind(any, None, server, AcceptLimits::default()).await.unwrap();
    // The server's Control is left at the kernel's default.
    for (lane, dialled, accepted) in [
        (Lane::Control, Some(128 << 10), None),
        (Lane::Live, Some(128 << 10), Some(128 << 10)),
        (Lane::Bulk, Some(128 << 10), Some(64 << 10)),
    ] {
        let client = connect(listener.local_addr(), lane, None, &config("client")).await.unwrap();
        let server = listener.accept().await.unwrap();
        assert_eq!((lowat(&client).await, lowat(&server).await), (dialled, accepted), "{lane:?}");
    }
}
