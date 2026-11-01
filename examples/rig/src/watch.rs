//! The `watch` workload: this node watches keys `k0..` of the accounts kind (`WATCH_KEYS`, 20) and logs every owner it
//! is shown to watch.jsonl, `own` with its node and epoch or `stopped`, and `end` when the watch ends, then watches
//! again. check.py's `watch-epoch` wants no epoch older than one already shown, and `watch-owner` each shown owner to
//! be the node that owned that epoch at the store, so never one epoch on two nodes.

use std::sync::Arc;
use std::sync::atomic::Ordering::Relaxed;
use std::time::Duration;

use axon_cluster::{Owner, Placement};
use serde_json::json;

use crate::account::{KIND, Log};

pub fn start(cx: &crate::Ctx<'_>) -> Result<crate::Tick, Box<dyn std::error::Error>> {
    let keys: usize = crate::env("WATCH_KEYS", "20").parse()?;
    let log = Arc::new(Log::create(&cx.log_dir.join("watch.jsonl"))?);
    let tick = crate::Tick::default();
    for k in 0..keys {
        let (cluster, log, tick, key) = (cx.cluster.downgrade(), log.clone(), tick.clone(), format!("k{k}"));
        tokio::spawn(async move {
            while let Some(c) = cluster.upgrade() {
                let mut watch = c.watch_key(KIND, &key, &Placement::default());
                drop(c);
                loop {
                    match tokio::time::timeout(Duration::from_secs(1), watch.next()).await {
                        // Ticks while it waits, so a watch loop that wedges stalls the tick.
                        Err(_) => _ = tick.fetch_add(1, Relaxed),
                        Ok(Some(Owner::Running { node, epoch })) => {
                            log.line("own", json!({"k": key, "n": &*node, "e": epoch}));
                        }
                        Ok(Some(Owner::Stopped)) => log.line("stopped", json!({"k": key})),
                        Ok(None) => {
                            log.line("end", json!({"k": key}));
                            break;
                        }
                    }
                }
                tokio::time::sleep(Duration::from_secs(1)).await;
            }
        });
    }
    Ok(tick)
}
