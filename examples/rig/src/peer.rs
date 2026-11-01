//! `rig peer <addr> <mode> [claim_id]` — a low-volume probe that sends one malformed or confused peer input at a
//! time at this node's own cluster port, to check the node shrugs it off. Correctness, not load: one attempt then a
//! pause, run from the node's own (allowlisted) address against its cluster port. fault.sh's `peer` class runs it in a
//! transient unit; check.py reads the lines it appends.
//!
//! env: SECRET the cluster secret; RIG_PEER_LOG where to append a line per attempt (default stderr).
//!
//! Modes:
//!   secret     the real handshake with the wrong secret — must be refused (auth failure on the node).
//!   impostor   the real handshake as `claim_id` with a fresh incarnation — may connect; it must receive no calls.
//!   stale      the same as an old incarnation (1) — the node keeps the live peer; the impostor gets nothing.
//!   garbage    random bytes at the port.
//!   truncated  a hello that stops mid-body, then closes.
//!   oversized  a hello whose body length is past the maximum.
//!   stall      a half-open handshake that holds its slot until the node's handshake timeout ends it.
//!   fuzz       one of a few hello-decoder boundaries (our own, not the fuzz corpus).

use std::io::{Read, Write};
use std::net::{SocketAddr, TcpStream};
use std::time::Duration;

use axon::trace::{random_u64, unix_micros};
use axon_net::{Config, SharedSecret};
use serde_json::json;

use crate::account::Log;
use crate::{Result, env};

const MODES: [&str; 8] = ["secret", "impostor", "stale", "garbage", "truncated", "oversized", "stall", "fuzz"];

pub fn run(addr: &str, mode: &str, claim: &str) -> Result<()> {
    let addr: SocketAddr = addr.parse()?;
    if !MODES.contains(&mode) {
        return Err(format!("unknown mode {mode}").into());
    }
    let secret = env("SECRET", "rig-shared-secret");
    let log = match std::env::var("RIG_PEER_LOG") {
        Ok(p) => Some(Log::create(std::path::Path::new(&p))?),
        Err(_) => None,
    };
    // The handshake modes need an async runtime; the raw ones do not, so one current-thread runtime serves both.
    let rt = tokio::runtime::Builder::new_current_thread().enable_all().build()?;
    loop {
        let started = unix_micros();
        let (outcome, frames, err) = match mode {
            "secret" => handshake(&rt, addr, claim, &format!("wrong-{:020}", random_u64()), None),
            "impostor" => handshake(&rt, addr, claim, &secret, None),
            "stale" => handshake(&rt, addr, claim, &secret, Some(1)),
            _ => match raw(addr, mode) {
                Ok(()) => ("sent", 0, None),
                Err(e) => ("error", 0, Some(e.to_string())),
            },
        };
        let line = json!({"mode": mode, "outcome": outcome, "frames": frames, "err": err, "started": started});
        match &log {
            Some(l) => l.line("peer", line),
            None => eprintln!("{line}"),
        }
        std::thread::sleep(Duration::from_millis(500));
    }
}

/// The real handshake, at `Config::new`'s fresh incarnation unless given one, then a short read: `connected` with the
/// count of application frames the node sent the impostor (it should send none), `refused` when the node rejected it.
fn handshake(
    rt: &tokio::runtime::Runtime,
    addr: SocketAddr,
    claim: &str,
    secret: &str,
    incarnation: Option<u128>,
) -> (&'static str, u64, Option<String>) {
    let mut cfg = match SharedSecret::new(secret.as_bytes()) {
        Ok(s) => Config::new(claim, s),
        Err(e) => return ("error", 0, Some(e.to_string())),
    };
    cfg.allow_plaintext = true;
    if let Some(inc) = incarnation {
        cfg.identity.incarnation = inc;
    }
    rt.block_on(async {
        match axon_net::connect(addr, axon_net::Lane::Control, None, &cfg).await {
            // No listener, a reset, no TCP connect: nothing the node's handshake decided, so check.py does not count it.
            Err(e @ (axon_net::Error::Io(_) | axon_net::Error::Timeout(axon_net::Timeout::Connect))) => {
                ("error", 0, Some(e.to_string()))
            }
            // The bounded label: check.py judges only the `auth` ones against the node's auth failures.
            Err(e) => ("refused", 0, Some(e.label().to_owned())),
            Ok(mut session) => {
                // Hold the session briefly and count any application frames the node routes to the impostor.
                let mut frames = 0;
                let watch = async {
                    while session.receiver.recv().await.is_ok() {
                        frames += 1;
                    }
                };
                let _ = tokio::time::timeout(Duration::from_secs(2), watch).await;
                ("connected", frames, None)
            }
        }
    })
}

/// Raw bytes at the port: the node must reject them without restarting or dropping a real session.
fn raw(addr: SocketAddr, mode: &str) -> std::io::Result<()> {
    let mut s = TcpStream::connect_timeout(&addr, Duration::from_secs(3))?;
    s.set_write_timeout(Some(Duration::from_secs(2)))?;
    s.set_read_timeout(Some(Duration::from_millis(500)))?;
    match mode {
        "garbage" => {
            let mut buf = [0u8; 256];
            for chunk in buf.chunks_mut(8) {
                chunk.copy_from_slice(&random_u64().to_le_bytes());
            }
            s.write_all(&buf)?;
        }
        // A well-formed prefix that stops mid-body: magic, versions 2..=2, a body length, then too few body bytes.
        "truncated" => s.write_all(&[b'A', b'X', b'O', b'N', 0, 2, 0, 2, 0, 53, 0, 0, 0, 0])?,
        // A body length past MAX_BODY (1024): the node reads it and rejects the hello as oversized.
        "oversized" => s.write_all(&[b'A', b'X', b'O', b'N', 0, 2, 0, 2, 0xff, 0xff])?,
        // Half-open: the magic only, held past the node's handshake timeout (the shipped 5 s), so the node ends it.
        "stall" => {
            s.write_all(b"AX")?;
            std::thread::sleep(Duration::from_secs(6));
        }
        // One per connection: the node drops a connection at its first bad hello, so a second would never be read.
        "fuzz" => s.write_all(FUZZ[random_u64() as usize % FUZZ.len()])?,
        _ => unreachable!(),
    }
    let mut sink = [0u8; 64];
    let _ = s.read(&mut sink);
    Ok(())
}

/// Hello-decoder boundaries of our own (the fuzz corpus is not committed).
const FUZZ: [&[u8]; 4] = [
    &[0; 32],                                     // all zeros: a bad magic
    &[b'A', b'X', b'O', b'N', 0, 1, 0, 1, 0, 53], // a version-1 peer
    // A whole hello of a version-3 peer (nonce, incarnation, max_frame 64 KiB, id b9, namespace x): the node answers,
    // then finds no version both speak.
    b"AXON\0\x03\0\x03\0\x3c\
      \0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\
      \0\x01\0\0\0\0\x02b9\0\x01x",
    &[b'A', b'X', b'O', b'N', 0, 2, 0, 2, 0, 10], // a body length below the fixed minimum
];
