#![allow(dead_code, reason = "each test file uses part of it")]
use std::net::SocketAddr;
use std::sync::Arc;

use axon::files::{FileOp, Files};
use axon_net::{Network, SharedSecret};
use axon_testkit::world::{Wire, World, WorldConfig};
use bytes::Bytes;

pub const SECRET: &str = "at-least-16-bytes-long";

/// Nodes and the World's wires, no cluster. The n-th added is 10.0.0.n.
pub fn wires(seed: u64, wire: Wire, names: &[&str]) -> World<Arc<dyn Network>> {
    wires_in(WorldConfig { seed, wire, ..WorldConfig::default() }, names)
}

pub fn wires_in(cfg: WorldConfig, names: &[&str]) -> World<Arc<dyn Network>> {
    let mut w = World::new(cfg);
    for name in names {
        w.add(name, |host| async move { host.transport(SharedSecret::new(SECRET).unwrap()).network.unwrap() });
    }
    w
}

pub fn at(n: u8) -> SocketAddr {
    SocketAddr::from(([10, 0, 0, n], 7000))
}

pub const NAMES: [&str; 2] = ["a", "b"];

/// One to three writes of one to six ops on two names, the bytes of each append or replace counting on from the last.
pub fn writes(seed: u64) -> Vec<Vec<FileOp>> {
    random_writes(seed, false)
}

/// As [`writes`], with a `Write` over the file's bytes now and then. The crash points tell a `Write` apart only as an op
/// that lands whole, so the World's crash ends are checked against them without it.
pub fn writes_over(seed: u64) -> Vec<Vec<FileOp>> {
    random_writes(seed, true)
}

fn random_writes(seed: u64, over: bool) -> Vec<Vec<FileOp>> {
    let mut lens = [None::<u64>; NAMES.len()];
    let mut state = seed;
    let mut pick = |n: u64| {
        state = state.wrapping_mul(6_364_136_223_846_793_005).wrapping_add(1_442_695_040_888_963_407);
        (state >> 33) % n
    };
    let mut byte = 0u8;
    let mut bytes = |n: u64| -> Bytes {
        (0..n)
            .map(|_| {
                byte = byte.wrapping_add(1);
                byte
            })
            .collect()
    };
    (0..1 + pick(3))
        .map(|_| {
            (0..1 + pick(6))
                .map(|_| {
                    let i = pick(2) as usize;
                    let name = NAMES[i].to_owned();
                    match (pick(if over { 6 } else { 5 }), lens[i]) {
                        (0, len) => {
                            let to = pick(6);
                            lens[i] = len.map(|l| l.min(to));
                            FileOp::Truncate(name, to)
                        }
                        (1, _) => {
                            lens[i] = None;
                            FileOp::Remove(name)
                        }
                        (2, _) => {
                            let data = bytes(pick(4));
                            lens[i] = Some(data.len() as u64);
                            FileOp::Replace(name, data)
                        }
                        (5, Some(len)) if len > 0 => {
                            let offset = pick(len);
                            FileOp::Write(name, offset, bytes(1 + pick(len - offset)))
                        }
                        (_, len) => {
                            let data = bytes(pick(4));
                            lens[i] = Some(len.unwrap_or(0) + data.len() as u64);
                            FileOp::Append(name, data)
                        }
                    }
                })
                .collect()
        })
        .collect()
}

/// A future over files in memory, or over the World's disk outside a write: ready at once.
pub fn now<T>(future: impl Future<Output = std::io::Result<T>>) -> T {
    match std::pin::pin!(future).poll(&mut std::task::Context::from_waker(std::task::Waker::noop())) {
        std::task::Poll::Ready(out) => out.unwrap(),
        std::task::Poll::Pending => panic!("the files waited"),
    }
}

/// Each name the files hold, and its bytes.
pub fn contents(files: &dyn Files) -> Vec<(String, Vec<u8>)> {
    now(files.list()).into_iter().map(|(name, _)| (name.clone(), now(files.read(&name)))).collect()
}
