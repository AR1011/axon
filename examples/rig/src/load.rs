//! The clients: one task per key, each calling its key about every `every` through a `KeyedRef`, retrying an op
//! with the same id every 50 ms until it is answered or 120 s pass. Each try carries its deadline, `request_timeout`
//! after it began, lookup included, and runs only before it (`account::KEEP`).

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering::Relaxed};
use std::time::{Duration, Instant};

use axon::trace::random_u64;
use axon_cluster::{Cluster, Placement};
use bytes::Bytes;
use serde_json::json;

use crate::Tick;
use crate::account::{Api, Op, Shared, sample};

/// Op ids are `<node>.<boot>.<n>`, one count for every client of the incarnation, of any kind.
static N: AtomicU64 = AtomicU64::new(0);

const RETRY: Duration = Duration::from_millis(50);
pub const GIVE_UP: Duration = Duration::from_secs(120);

/// The clients of keys `<prefix>0..` of `kind`, a kind of accounts. Returns the tick: every try bumps it, so it moves
/// while the clients run, answered or not.
pub fn clients(
    cluster: &Cluster,
    sh: Arc<Shared>,
    kind: &str,
    prefix: &str,
    keys: usize,
    every: Duration,
    boot_ms: u64,
) -> Tick {
    let (within, tick) = (cluster.request_timeout(), Tick::default());
    for k in 0..keys {
        let key = format!("{prefix}{k}");
        let keyed = cluster.keyed::<Api>(kind, &key, Bytes::new(), &Placement::default());
        let (sh, tick) = (sh.clone(), tick.clone());
        tokio::spawn(async move {
            // A random phase, then every `every` ± 50 %.
            tokio::time::sleep(every.mul_f64(unit())).await;
            loop {
                tokio::time::sleep(every.mul_f64(0.5 + unit())).await;
                let id = format!("{}.{boot_ms}.{}", sh.node, N.fetch_add(1, Relaxed));
                let d = (1 + random_u64() % 100) as i64;
                let d = if random_u64() % 10 < 6 { d } else { -d };
                if !sh.serve {
                    sh.client_log.line("send", json!({"op": id, "k": key, "d": d}));
                }
                let (started, mut tries) = (Instant::now(), 0);
                loop {
                    tries += 1;
                    tick.fetch_add(1, Relaxed);
                    let err = match keyed.request_within(Op { id: id.clone(), delta: d }, within).await {
                        Ok(a) => {
                            let ms = started.elapsed().as_millis() as u64;
                            sh.acks.fetch_add(1, Relaxed);
                            if sh.serve {
                                sample(&sh.latency, ms);
                            } else {
                                sh.client_log.line(
                                    "ack",
                                    json!({"op": id, "k": key, "o": a.outcome, "a": a.after, "n": a.node,
                                           "e": a.epoch, "tries": tries, "ms": ms}),
                                );
                            }
                            break;
                        }
                        Err(e) => e,
                    };
                    sh.errors.fetch_add(1, Relaxed);
                    if started.elapsed() > GIVE_UP {
                        sh.give_ups.fetch_add(1, Relaxed);
                        if !sh.serve {
                            let err = err.to_string();
                            sh.client_log.line("give_up", json!({"op": id, "k": key, "tries": tries, "err": err}));
                        }
                        break;
                    }
                    tokio::time::sleep(RETRY).await;
                }
            }
        });
    }
    tick
}

pub fn unit() -> f64 {
    (random_u64() >> 11) as f64 / (1u64 << 53) as f64
}
