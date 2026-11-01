//! The `eager` workload: accounts of an eager kind, `eag`, keys `e0..`. b1 starts each key once, at the first boot of
//! the run; nothing calls them after that, so every later start is the cluster's own: a gone node's keys restarting on
//! a survivor, or on the node itself when it comes back. Each start owns its key at the store (`O`), so the store's
//! fences judge them, and logs `start` and `stop` to node.jsonl; check.py's `eager-restart` wants every key running
//! again within a bound of its node's loss.

use std::sync::atomic::Ordering::Relaxed;
use std::time::Duration;

use axon::RestartPolicy;
use axon_cluster::{Placement, Unaddressed};
use bytes::Bytes;
use serde_json::json;

use crate::account::{AcctApi, Log, factory};

pub const KIND: &str = "eag";

pub async fn start(cx: &crate::Ctx<'_>) -> Result<crate::Tick, Box<dyn std::error::Error>> {
    cx.cluster
        .kind::<AcctApi>(KIND)
        .policy(RestartPolicy { max_restarts: 100, ..RestartPolicy::default() })
        .eager(Bytes::new(), Placement::default())
        .register(factory(cx.sh.clone()))
        .await?;
    let keys: usize = crate::env("EAGER_KEYS", "100").parse()?;
    let log = Log::create(&cx.log_dir.join("eager.jsonl"))?;
    // Once a run, marked when all are started: b1 starting them again after that would be a caller, and a b1 killed
    // before then starts them all at its next boot.
    let marker = cx.log_dir.join("eager-started");
    let first = cx.sh.node == "b1" && !marker.exists();
    let (cluster, tick, nodes) = (cx.cluster.downgrade(), crate::Tick::default(), cx.nodes);
    let t = tick.clone();
    tokio::spawn(async move {
        if first {
            // Keys are placed among the nodes serving the kind when they start, and b1 boots first.
            if let Some(c) = cluster.upgrade() {
                let all = c.wait_view(|v| v.members.len() >= nodes);
                let _ = tokio::time::timeout(Duration::from_secs(120), all).await;
            }
            tokio::time::sleep(Duration::from_secs(5)).await;
            for k in 0..keys {
                let key = format!("e{k}");
                for _ in 0..60 {
                    let Some(c) = cluster.upgrade() else { return };
                    match c.get_or_spawn::<Unaddressed>(KIND, &key, Bytes::new(), &Placement::default()).await {
                        Ok(_) => break,
                        Err(e) => log.line("spawn_err", json!({"k": key, "err": e.to_string()})),
                    }
                    tokio::time::sleep(Duration::from_secs(1)).await;
                }
            }
            log.line("spawned", json!({"keys": keys}));
            if let Err(e) = std::fs::write(&marker, "") {
                log.line("marker_err", json!({"err": e.to_string()}));
            }
        }
        let mut every = tokio::time::interval(Duration::from_secs(1));
        for n in 0u64.. {
            every.tick().await;
            let Some(c) = cluster.upgrade() else { return };
            if n % 10 == 0 {
                let hosted = c.hosted(usize::MAX).keys.iter().filter(|h| h.kind == KIND).count();
                log.line("hosted", json!({"n": hosted}));
            }
            t.fetch_add(1, Relaxed);
        }
    });
    Ok(tick)
}
