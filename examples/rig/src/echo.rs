//! The `echo` workload: named calls with deadlines, lookups, remote spawns, and the authorizer the `authz` fault sets.
//!
//! Each node publishes an actor as `echo-<node>` and as `echo`. Every 500 ms it calls the next other node in turn
//! through a `NamedRef` with `request_within(WITHIN)`, logging `ok` or `err`; the far actor logs `ran` with the
//! caller's send time and deadline unless its reply is abandoned. Every 10 s it asks `resolve_all("echo")` and logs
//! what it found; every 2 s it spawns a `tmp` actor on the next other node with `spawn_on` and calls it once; `tmp`
//! actors end after `TMP_IDLE` without a message, logging `tmp_up` and `tmp_stop`. All to echo.jsonl.
//!
//! The authorizer: every 250 ms the node reads the `authz` file fault.sh writes (`AUTHZ`, which run.sh sets to the
//! node's fault state; `/run/rig-fault/authz` without it), node ids to refuse, and on a change sets an authorizer
//! refusing those peers' calls to `echo*` and spawns of `tmp`, then logs `authz` with them. check.py holds it to that:
//! no `ran` from a refused peer after the line.

use std::collections::BTreeSet;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering::Relaxed};
use std::time::Duration;

use axon::{Actor, Context, StopReason};
use axon_cluster::{Action, ActorProtocol, Authorizer, CallError, Cluster, Rejection, Reply, WireProtocol};
use axon_net::Peer;
use bytes::Bytes;
use serde_json::json;
use tokio::time::MissedTickBehavior;

use crate::account::Log;
use crate::store::now_us;

const WITHIN: Duration = Duration::from_secs(1);
const TMP: &str = "tmp";
const TMP_IDLE: Duration = Duration::from_secs(3);

pub struct Ping {
    from: String,
    id: u64,
    sent_us: u64,
    within_ms: u64,
}

pub struct EchoApi;

impl WireProtocol for EchoApi {
    type Message = Ping;
    type Reply = String;
    const ID: u64 = 0x5164;
    const SCHEMA: u16 = 1;

    fn encode(p: Ping) -> Result<Bytes, CallError> {
        Ok(Bytes::from(format!("{} {} {} {}", p.from, p.id, p.sent_us, p.within_ms)))
    }

    fn decode_reply(body: Bytes) -> Result<String, CallError> {
        String::from_utf8(body.to_vec()).map_err(|_| CallError::Codec("bad reply".into()))
    }
}

impl ActorProtocol for EchoApi {
    type Mailbox = (Ping, Option<Reply<String>>);

    fn encode_reply(node: String) -> Result<Bytes, CallError> {
        Ok(Bytes::from(node))
    }

    fn decode(body: Bytes, reply: Option<Reply<String>>) -> Result<Self::Mailbox, Rejection> {
        let s = std::str::from_utf8(&body).map_err(|_| Rejection::InvalidMessage)?;
        let [from, id, sent, within] = s.split(' ').collect::<Vec<_>>()[..] else {
            return Err(Rejection::InvalidMessage);
        };
        let n = |x: &str| x.parse().map_err(|_| Rejection::InvalidMessage);
        Ok((Ping { from: from.into(), id: n(id)?, sent_us: n(sent)?, within_ms: n(within)? }, reply))
    }
}

struct Echo {
    me: String,
    log: Arc<Log>,
    /// A `tmp` actor's number on this node.
    tmp: Option<u64>,
}

impl Actor for Echo {
    type Message = (Ping, Option<Reply<String>>);
    type Error = String;

    fn on_start(&mut self, _: &mut Context<'_, Self>) -> Result<(), String> {
        if let Some(id) = self.tmp {
            self.log.line("tmp_up", json!({"id": id}));
        }
        Ok(())
    }

    fn on_message(&mut self, (p, reply): Self::Message, _: &mut Context<'_, Self>) -> Result<(), String> {
        let Some(reply) = reply else { return Ok(()) };
        // What a handler with effects does first (src/account.rs): a caller past its deadline gets nothing run.
        if reply.is_abandoned() {
            return Ok(());
        }
        let line = json!({"from": p.from, "id": p.id, "sent": p.sent_us, "within_ms": p.within_ms, "tmp": self.tmp});
        self.log.line("ran", line);
        let _ = reply.send(self.me.clone());
        Ok(())
    }

    fn on_stop(&mut self, reason: &StopReason) {
        if let Some(id) = self.tmp {
            self.log.line("tmp_stop", json!({"id": id, "why": format!("{reason:?}")}));
        }
    }
}

struct Refuse(BTreeSet<String>);

impl Authorizer for Refuse {
    fn authorize(&self, peer: &Peer, action: Action<'_>) -> bool {
        let guarded = match action {
            Action::Call { name } => name.starts_with("echo"),
            Action::Spawn { kind } => kind == TMP,
            Action::Watch { .. } => false,
        };
        !guarded || !self.0.contains(&peer.identity.node_id)
    }
}

pub async fn start(cx: &crate::Ctx<'_>) -> Result<crate::Tick, Box<dyn std::error::Error>> {
    let (cluster, system, me) = (cx.cluster, cx.system, cx.sh.node.clone());
    let log = Arc::new(Log::create(&cx.log_dir.join("echo.jsonl"))?);
    // Before the names and the kind are served: a restart under the fault refuses from its first call.
    let (path, mut last) = (PathBuf::from(crate::env("AUTHZ", "/run/rig-fault/authz")), BTreeSet::new());
    authz(cluster, &log, &path, &mut last);
    let handle = system.spawn(Echo { me: me.clone(), log: log.clone(), tmp: None }).start()?;
    cluster.publish::<EchoApi>(&handle, &format!("echo-{me}"))?;
    cluster.publish::<EchoApi>(&handle, "echo")?;
    let (l, m, count) = (log.clone(), me.clone(), Arc::new(AtomicU64::new(0)));
    cluster
        .kind::<EchoApi>(TMP)
        .idle(TMP_IDLE)
        .register(move |_, _| Ok(Echo { me: m.clone(), log: l.clone(), tmp: Some(count.fetch_add(1, Relaxed)) }))
        .await?;
    let others = cx.others();
    let tick = crate::Tick::default();
    let (weak, l) = (cluster.downgrade(), log.clone());
    tokio::spawn(async move {
        let mut every = tokio::time::interval(Duration::from_millis(250));
        loop {
            every.tick().await;
            let Some(c) = weak.upgrade() else { return };
            authz(&c, &l, &path, &mut last);
        }
    });
    tokio::spawn(calls(cluster.downgrade(), log.clone(), me.clone(), others.clone(), tick.clone()));
    tokio::spawn(spawns(cluster.downgrade(), log.clone(), me, others));
    let (weak, l) = (cluster.downgrade(), log);
    tokio::spawn(async move {
        let mut every = tokio::time::interval(crate::REPORT);
        loop {
            every.tick().await;
            let Some(c) = weak.upgrade() else { return };
            let (found, failed) = c.resolve_all::<EchoApi>("echo").await;
            let got: BTreeSet<&str> = found.iter().map(|r| r.node()).collect();
            let failed: Vec<(String, String)> = failed.into_iter().map(|(n, e)| (n, e.to_string())).collect();
            l.line("resolve", json!({"got": got, "failed": failed}));
        }
    });
    Ok(tick)
}

async fn calls(cluster: axon_cluster::WeakCluster, log: Arc<Log>, me: String, others: Vec<String>, tick: crate::Tick) {
    let Some(c) = cluster.upgrade() else { return };
    let named: Vec<_> = others.iter().map(|n| c.named::<EchoApi>(&format!("echo-{n}"))).collect();
    drop(c);
    let mut every = tokio::time::interval(Duration::from_millis(500));
    every.set_missed_tick_behavior(MissedTickBehavior::Skip);
    loop {
        every.tick().await;
        let id = tick.fetch_add(1, Relaxed);
        let i = id as usize % others.len();
        let sent_us = now_us();
        let ping = Ping { from: me.clone(), id, sent_us, within_ms: WITHIN.as_millis() as u64 };
        let r = named[i].request_within(ping, WITHIN).await;
        // Wall clock, which the clock faults step back.
        let ms = now_us().saturating_sub(sent_us) / 1000;
        match r {
            Ok(_) => log.line("ok", json!({"to": others[i], "id": id, "sent": sent_us, "ms": ms})),
            Err(e) => {
                log.line("err", json!({"to": others[i], "id": id, "sent": sent_us, "ms": ms, "err": e.to_string()}))
            }
        }
    }
}

async fn spawns(cluster: axon_cluster::WeakCluster, log: Arc<Log>, me: String, others: Vec<String>) {
    let mut every = tokio::time::interval(Duration::from_secs(2));
    every.set_missed_tick_behavior(MissedTickBehavior::Skip);
    for id in 0u64.. {
        every.tick().await;
        let Some(c) = cluster.upgrade() else { return };
        let (to, at) = (&others[id as usize % others.len()], now_us());
        let r = match c.spawn_on::<EchoApi>(to, TMP, Bytes::new()).await {
            Ok(r) => {
                let ping = Ping { from: me.clone(), id, sent_us: now_us(), within_ms: WITHIN.as_millis() as u64 };
                r.request_within(ping, WITHIN).await.map(drop).map_err(|e| e.to_string())
            }
            Err(e) => Err(e.to_string()),
        };
        log.line("spawn", json!({"to": to, "id": id, "at": at, "err": r.err()}));
    }
}

/// Sets the authorizer the file asks for, when it changed since `last`.
fn authz(cluster: &Cluster, log: &Log, path: &Path, last: &mut BTreeSet<String>) {
    let text = std::fs::read_to_string(path).unwrap_or_default();
    let refuse: BTreeSet<String> = text.split([',', ' ', '\n']).filter(|s| !s.is_empty()).map(Into::into).collect();
    if refuse == *last {
        return;
    }
    let authorizer: Option<Arc<dyn Authorizer>> =
        if refuse.is_empty() { None } else { Some(Arc::new(Refuse(refuse.clone()))) };
    cluster.set_authorizer(authorizer);
    log.line("authz", json!({"refuse": refuse}));
    *last = refuse;
}
