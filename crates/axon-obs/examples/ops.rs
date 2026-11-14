//! `ops <nodes|leases|streams|links> [--json]`: what a running cluster holds, from a member that serves nothing.
//!
//! It joins through `AXON_CONSUL`'s service `AXON_SERVICE` (`axon-playground`) with the shared secret `AXON_SECRET`
//! (the playground's), as the playground takes them, on `AXON_BIND` (`127.0.0.1:0`, an address the members can dial),
//! under the id `<host>-<pid>.<AXON_PRINCIPAL>` (`ops`), so a `Rules` table lets it in by its principal. It asks each
//! other member for its `NodeSnapshot`, or for `streams` its streams, at most 16 at once, each within the cluster's
//! `request_timeout`; a member that does not answer is a row that says so. It shuts down before it exits, on SIGINT
//! too, so it leaves every view at once rather than after a heartbeat's grace.
//!
//! ```text
//! AXON_CONSUL=http://127.0.0.1:8500 cargo run -p axon-obs --example ops -- nodes --json
//! ```

use axon::ActorSystem;
use axon_cluster::{Cluster, ClusterConfig, ConsulConfig, ConsulDiscovery, Member, View};
use axon_net::SharedSecret;
use axon_obs::{Request, fetch};
use futures_util::{StreamExt, stream};
use serde_json::{Value, json};

/// Members asked at once.
const AT_ONCE: usize = 16;
/// Streams listed per node, as the console lists them.
const STREAMS: usize = 1000;

#[derive(Clone, Copy, PartialEq)]
enum Command {
    Nodes,
    Leases,
    Streams,
    Links,
}

#[tokio::main]
async fn main() {
    let args: Vec<String> = std::env::args().skip(1).collect();
    let json = args.iter().any(|a| a == "--json");
    let command = match args.iter().find(|a| !a.starts_with("--")).map(String::as_str) {
        Some("nodes") => Command::Nodes,
        Some("leases") => Command::Leases,
        Some("streams") => Command::Streams,
        Some("links") => Command::Links,
        _ => {
            eprintln!("usage: ops <nodes|leases|streams|links> [--json]");
            std::process::exit(2);
        }
    };
    let system = ActorSystem::builder().build().expect("an actor system");
    let rows = tokio::select! {
        rows = run(&system, command) => rows,
        Ok(()) = tokio::signal::ctrl_c() => Err("interrupted".into()),
    };
    // Before it exits, so the members see a leave now. The handler above keeps SIGINT from ending the process, so a
    // second one ends the wait.
    tokio::select! {
        _ = system.shutdown() => {}
        Ok(()) = tokio::signal::ctrl_c() => {}
    }
    match rows {
        Ok(rows) if json => println!("{}", serde_json::to_string_pretty(&rows).expect("JSON")),
        Ok(rows) => rows.iter().for_each(|row| println!("{}", line(row))),
        Err(e) => {
            eprintln!("ops: {e}");
            std::process::exit(1);
        }
    }
}

fn env(name: &str, default: &str) -> String {
    std::env::var(name).unwrap_or_else(|_| default.into())
}

/// `<host>-<pid>.<principal>`, the host cut to what a DNS label takes.
fn node_id() -> String {
    let host = std::env::var("HOSTNAME").ok().or_else(|| std::fs::read_to_string("/etc/hostname").ok());
    let host = host.unwrap_or_else(|| "host".into());
    let label: String = host.trim().split('.').next().unwrap_or_default().chars().take(40).collect();
    let label = label.to_ascii_lowercase().replace(|c: char| !c.is_ascii_alphanumeric() && c != '-', "-");
    format!("{label}-{}.{}", std::process::id(), env("AXON_PRINCIPAL", "ops"))
}

async fn run(system: &ActorSystem, command: Command) -> Result<Vec<Value>, String> {
    let url = std::env::var("AXON_CONSUL").map_err(|_| "AXON_CONSUL names no Consul agent")?;
    let discovery = ConsulDiscovery::new(ConsulConfig::new(url, env("AXON_SERVICE", "axon-playground")))
        .map_err(|e| e.to_string())?;
    let secret = env("AXON_SECRET", "playground-secret-0123");
    let secret = SharedSecret::new(secret).map_err(|e| e.to_string())?;
    let mut transport = axon_net::Config::new(node_id(), secret);
    transport.allow_plaintext = true;
    // An address the members can dial back: `Cluster::start` refuses a wildcard without `advertise`.
    let bind = env("AXON_BIND", "127.0.0.1:0");
    let bind: std::net::SocketAddr = bind.parse().map_err(|e| format!("AXON_BIND {bind}: {e}"))?;
    let cfg = ClusterConfig::new(bind, transport, discovery);
    let cluster = Cluster::start(system, cfg).await.map_err(|e| e.to_string())?;
    let members = members(&cluster).await?;
    let request = if command == Command::Streams { Request::Streams { limit: STREAMS } } else { Request::Snapshot };
    let answers: Vec<Result<Value, String>> = stream::iter(&members)
        .map(|member| fetch::<Value>(&cluster, &member.record.id, &request))
        .buffered(AT_ONCE)
        .collect()
        .await;
    let peers = cluster.stats().engine.peers;
    Ok(answers
        .into_iter()
        .zip(&members)
        .map(|(answer, member)| {
            let id = &member.record.id;
            let answer = match answer {
                Ok(answer) => answer,
                Err(e) => return json!({"id": id, "error": e}),
            };
            match command {
                Command::Nodes => {
                    let labels: serde_json::Map<String, Value> = (member.record.metadata.iter())
                        .filter(|(k, _)| !k.starts_with("axon."))
                        .map(|(k, v)| (k.clone(), v.clone().into()))
                        .collect();
                    let version = peers.iter().find(|p| p.node == *id).map(|p| p.version);
                    json!({
                        "id": id,
                        "incarnation": format!("{:032x}", member.record.incarnation),
                        "labels": labels,
                        "reachable": member.reachable,
                        "isolated": answer.pointer("/cluster/isolated"),
                        "version": version,
                    })
                }
                Command::Leases => json!({
                    "id": id,
                    "ownership": answer.pointer("/cluster/ownership"),
                    "kinds": answer.pointer("/hosted/kinds"),
                    "keys": answer.pointer("/hosted/keys"),
                    "total": answer.pointer("/hosted/total"),
                }),
                Command::Streams => {
                    let streams: Vec<Value> = (answer["streams"].as_array().into_iter().flatten())
                        .map(|s| {
                            json!({
                                "key": format!("{}/{}", str(&s["namespace"]), str(&s["name"])),
                                "origin": s["origin"],
                                "parent": s["parent"],
                                "generation": s["generation"],
                                "sequence": s["sequence"],
                            })
                        })
                        .collect();
                    json!({"id": id, "streams": streams, "total": answer["total"]})
                }
                Command::Links => {
                    let peers = (answer.pointer("/cluster/engine/peers").and_then(Value::as_array).into_iter())
                        .flatten()
                        .map(|p| {
                            json!({
                                "peer": p["node"],
                                "lane": p["lane"],
                                "outgoing": p["outgoing"],
                                "up": p["down_ms"].is_null(),
                                "rtt_us": p.pointer("/session/rtt_ns").and_then(Value::as_u64).map(|ns| ns / 1000),
                                "version": p["version"],
                            })
                        })
                        .collect::<Vec<_>>();
                    json!({"id": id, "links": peers})
                }
            }
        })
        .collect())
}

/// The members other than this one, once the view lists any, within `request_timeout`.
async fn members(cluster: &Cluster) -> Result<Vec<Member>, String> {
    let me = cluster.identity().node_id.clone();
    let others = |view: &View| view.members.iter().any(|m| m.record.id != me);
    let view = tokio::time::timeout(cluster.request_timeout(), cluster.wait_view(others)).await;
    let view = view.map_err(|_| "no other member in view")?.map_err(|e| e.to_string())?;
    Ok(view.members.iter().filter(|m| m.record.id != me).cloned().collect())
}

fn str(value: &Value) -> &str {
    value.as_str().unwrap_or_default()
}

/// A row as one line: its id, then each field as `name=value`.
fn line(row: &Value) -> String {
    let fields = row.as_object().into_iter().flatten().filter(|(k, _)| *k != "id");
    let fields: Vec<String> = fields.map(|(k, v)| format!("{k}={v}")).collect();
    format!("{}\t{}", str(&row["id"]), fields.join("\t"))
}
