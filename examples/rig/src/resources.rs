//! A `res` line every 10 s: what the process holds, for check.py's `growth` (spec/fault-rig.md § Invariants).
//! Linux's /proc and cgroup v2 files; a value that could not be read is null, never 0, so growth skips it.

use axon::ActorSystem;
use axon_cluster::Cluster;
use serde_json::json;

use crate::account::Shared;

pub fn log(system: &ActorSystem, cluster: &Cluster, sh: &Shared) {
    let status = read("/proc/self/status");
    // `0::/system.slice/rig-node.service` on the rig: the unit's cgroup.
    let cgroup =
        read("/proc/self/cgroup").and_then(|c| c.lines().find_map(|l| l.strip_prefix("0::").map(str::to_owned)));
    let cg = |file: &str| cgroup.as_ref().and_then(|p| read(&format!("/sys/fs/cgroup{p}/{file}")));
    sh.node_log.line(
        "res",
        json!({"rss_kb": field(status.as_deref(), "VmRSS:"), "threads": field(status.as_deref(), "Threads:"),
               "fds": std::fs::read_dir("/proc/self/fd").ok().map(|d| d.count()),
               "mem_kb": cg("memory.current").and_then(|m| m.trim().parse::<u64>().ok()).map(|b| b / 1024),
               "anon_kb": field(cg("memory.stat").as_deref(), "anon ").map(|b| b / 1024),
               "actors": system.runtime_stats().actors, "sessions": cluster.stats().engine.peers.len()}),
    );
}

fn read(path: &str) -> Option<String> {
    std::fs::read_to_string(path).ok()
}

/// The number after `name` on its line.
fn field(text: Option<&str>, name: &str) -> Option<u64> {
    text?.lines().find_map(|l| l.strip_prefix(name)?.split_whitespace().next()?.parse().ok())
}
