use std::collections::BTreeMap;
use std::future::Future;
use std::sync::{Arc, Weak};
use std::time::Duration;

use axon_cluster::{AccessPolicy, CallError, Cluster, ClusterError, Rejection};
use bytes::Bytes;
use serde::Serialize;
use serde::de::DeserializeOwned;
use tokio::sync::Semaphore;
use tokio::task::JoinSet;

/// Serves `service` while `weak` lives: each request's JSON body goes to `answer` with the largest reply the caller
/// takes, and an answer larger than that is refused.
pub(crate) fn serve<T, R, F, Fut>(
    cluster: &Cluster,
    service: &str,
    access: AccessPolicy,
    weak: Weak<T>,
    stopped: &'static str,
    answer: F,
) -> Result<(), String>
where
    T: Send + Sync + 'static,
    R: DeserializeOwned,
    F: Fn(Arc<T>, R, usize) -> Fut + Send + Sync + 'static,
    Fut: Future<Output = Result<Bytes, String>> + Send + 'static,
{
    let answer = Arc::new(answer);
    let served = cluster.serve_control(service, access, move |request| {
        let (inner, answer, max) = (weak.upgrade(), answer.clone(), request.max_response);
        async move {
            let inner = inner.ok_or(stopped)?;
            let request = serde_json::from_slice(&request.body).map_err(|e| format!("bad request: {e}"))?;
            let reply = answer(inner, request, max).await?;
            if reply.len() > max {
                return Err(format!("the answer is {} bytes, limit {max}; ask for less", reply.len()));
            }
            Ok(reply)
        }
    });
    served.map_err(|e| e.to_string())
}

pub(crate) fn encode(value: &impl Serialize) -> Result<Bytes, String> {
    serde_json::to_vec(value).map(Bytes::from).map_err(|e| e.to_string())
}

/// `node`'s answer to `request`, as it came.
pub(crate) async fn ask(
    cluster: &Cluster,
    node: &str,
    service: &str,
    request: &impl Serialize,
) -> Result<Bytes, String> {
    let body = serde_json::to_vec(request).map_err(|e| e.to_string())?;
    cluster.control(node, service, body.into()).await.map_err(|e| refusal(node, service, e))
}

pub(crate) async fn call<T: DeserializeOwned>(
    cluster: &Cluster,
    node: &str,
    service: &str,
    request: &impl Serialize,
) -> Result<T, String> {
    serde_json::from_slice(&ask(cluster, node, service, request).await?).map_err(|e| e.to_string())
}

/// Calls `f` every `interval` while `weak` lives, the first time at once or, with `skip_first`, an interval on.
pub(crate) async fn every<T, Fut>(weak: Weak<T>, interval: Duration, skip_first: bool, f: impl Fn(Arc<T>) -> Fut)
where
    Fut: Future<Output = ()>,
{
    let mut tick = tokio::time::interval(interval);
    tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    if skip_first {
        tick.tick().await;
    }
    loop {
        tick.tick().await;
        let Some(inner) = weak.upgrade() else { break };
        f(inner).await;
    }
}

/// The most common misconfiguration, said in words rather than as `Call(Rejected(Forbidden))`. The console's summary
/// bar finds these by " refuses to answer: " (`ui/src/shell/SummaryBar.tsx`).
fn refusal(node: &str, service: &str, error: ClusterError) -> String {
    match error {
        ClusterError::Call(CallError::Rejected(Rejection::Forbidden)) => {
            let config = if service == crate::METRICS_SERVICE { "MetricsConfig" } else { "TelemetryConfig" };
            format!(
                "{node} refuses to answer: its {config}::access (default Deny) or ClusterConfig::authorizer does not admit this node"
            )
        }
        error => error.to_string(),
    }
}

/// Runs `work` for every node, `parallelism` at a time, under one deadline. Every node ends
/// up in exactly one of the two results.
pub(crate) async fn fan_out<T, F, Fut>(
    nodes: Vec<String>,
    parallelism: usize,
    deadline: Duration,
    work: F,
) -> (Vec<(String, T)>, BTreeMap<String, String>)
where
    T: Send + 'static,
    F: Fn(String) -> Fut,
    Fut: Future<Output = Result<T, String>> + Send + 'static,
{
    let slots = Arc::new(Semaphore::new(parallelism.max(1)));
    let mut errors: BTreeMap<_, _> =
        nodes.iter().map(|n| (n.clone(), "did not complete before the deadline".to_owned())).collect();
    let mut running = JoinSet::new();
    for node in nodes {
        let (slots, work) = (slots.clone(), work(node.clone()));
        running.spawn(async move {
            let _slot = slots.acquire().await;
            (node, work.await)
        });
    }
    let mut done = Vec::new();
    let _ = tokio::time::timeout(deadline, async {
        while let Some(joined) = running.join_next().await {
            let Ok((node, result)) = joined else { continue };
            match result {
                Ok(value) => {
                    errors.remove(&node);
                    done.push((node, value));
                }
                Err(error) => drop(errors.insert(node, error)),
            }
        }
    })
    .await;
    (done, errors)
}
