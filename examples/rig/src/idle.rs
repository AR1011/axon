//! The `idle` workload: accounts of kind `idl`, keys `i0..`, each ending after `IDLE` without a message, and clients
//! (src/load.rs) on every node calling each so that it hears about one call every `IDLE`, so calls land on keys
//! draining, just drained or starting again. Their records go to the store like `acct`'s and every store invariant
//! judges them; check.py's `idle-race` wants each op sent near an idle stop of its key answered in time.

use std::time::Duration;

use axon::RestartPolicy;

use crate::account::{Api, factory};

pub const KIND: &str = "idl";
const IDLE: Duration = Duration::from_secs(2);

pub async fn start(cx: &crate::Ctx<'_>) -> Result<crate::Tick, Box<dyn std::error::Error>> {
    // Called through `Api` as `acct` is, so a rolling upgrade serves both schemas here too.
    let kind = cx.cluster.kind::<Api>(KIND);
    #[cfg(feature = "v2")]
    let kind = kind.also::<crate::account::AcctApi>();
    kind.idle(IDLE)
        .policy(RestartPolicy { max_restarts: 100, ..RestartPolicy::default() })
        .register(factory(cx.sh.clone()))
        .await?;
    let keys = crate::env("IDLE_KEYS", "20").parse()?;
    // Each node every `IDLE` times the nodes ± 50 %: a key's gaps between calls are then about `IDLE` on average, so
    // about a third of them outlast it and end in a drain. At `IDLE` a node, five callers would never let one drain.
    let every = IDLE * cx.nodes as u32;
    Ok(crate::load::clients(cx.cluster, cx.sh.clone(), KIND, "i", keys, every, cx.boot_ms))
}
