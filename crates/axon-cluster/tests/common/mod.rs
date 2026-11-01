#![allow(dead_code, reason = "each cluster test file uses part of it")]
use std::time::Duration;

use axon_cluster::ClusterConfig;
use axon_net::{Backoff, SharedSecret};

/// A plaintext transport, sharing every test node's secret.
pub fn transport(id: &str) -> axon_net::Config {
    let mut transport = axon_net::Config::new(id, SharedSecret::new(*b"0123456789abcdef0123").unwrap());
    transport.allow_plaintext = true;
    transport
}

pub fn short(cfg: &mut ClusterConfig) {
    // Short, so these tests run in seconds, or a World seed in moments of virtual time; axon-stream's world.rs runs
    // the shipped timings.
    cfg.poll_interval = Duration::from_millis(40);
    cfg.backoff = Backoff { initial: Duration::from_millis(20), max: Duration::from_millis(100), jitter: 0.1 };
    cfg.request_timeout = Duration::from_secs(2);
}
