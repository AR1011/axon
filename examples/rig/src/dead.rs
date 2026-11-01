//! The node's dead letters, settled since the last line, by reason, label and target: a `dead {by, total}` line in
//! `node.jsonl` every 10 s, so check.py tells the ones a workload plans (the local workload's failing subscribers) from
//! the ones nothing explains.

use std::collections::BTreeMap;
use std::sync::atomic::{AtomicU64, Ordering::Relaxed};

use axon::ActorSystem;
use serde_json::json;

use crate::account::Shared;

/// The last settled position read; one reader per process.
static AFTER: AtomicU64 = AtomicU64::new(0);

pub fn log(system: &ActorSystem, sh: &Shared) {
    let mut by = BTreeMap::<String, u64>::new();
    loop {
        let letters = system.dead_letters().settled(AFTER.load(Relaxed), 1000);
        let Some(last) = letters.last() else { break };
        AFTER.store(last.seq, Relaxed);
        for l in &letters {
            let target = l.target.as_deref().unwrap_or("-");
            *by.entry(format!("{} {} {target}", l.reason, l.label)).or_default() += l.count;
        }
    }
    sh.node_log.line("dead", json!({"by": by, "total": system.dead_letters().discarded()}));
}
