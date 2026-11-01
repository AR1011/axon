#![no_main]
//! `cargo +nightly fuzz run handshake`. Oracle: a peer that does not know the secret never gets a session,
//! whatever it sends, in either role.
use std::sync::OnceLock;
use std::time::Duration;

use axon_net::{Config, Lane, Role, SharedSecret, establish};
use libfuzzer_sys::fuzz_target;
use tokio::io::AsyncWriteExt;
use tokio::runtime::{Builder, Runtime};

static RUNTIME: OnceLock<Runtime> = OnceLock::new();

fuzz_target!(|data: &[u8]| {
    let Some((role, bytes)) = data.split_first() else { return };
    // The responder, or an initiator on each lane, whose hello the fuzzer's must echo.
    let role = match role % 4 {
        0 => Role::Responder,
        n => Role::Initiator(Lane::ALL[n as usize - 1]),
    };
    let runtime = RUNTIME.get_or_init(|| Builder::new_current_thread().enable_time().build().unwrap());
    runtime.block_on(async {
        let mut cfg = Config::new("fuzzed", SharedSecret::new("a-secret-the-fuzzer-cannot-know").unwrap());
        cfg.allow_plaintext = true;
        (cfg.handshake_timeout, cfg.connect_timeout) = (Duration::from_millis(200), Duration::from_millis(200));
        let (ours, mut theirs) = tokio::io::duplex(1 << 16);
        theirs.write_all(bytes).await.unwrap();
        theirs.shutdown().await.unwrap();
        assert!(establish(ours, role, &cfg).await.is_err());
    });
});
