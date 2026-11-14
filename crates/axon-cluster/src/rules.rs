//! Who may do what, per principal: the node id after its first DNS label.

use std::collections::BTreeMap;

use axon_net::Peer;

use crate::engine::{Action, Authorizer};

/// The principal a node id belongs to: the id after its first DNS label, so `n3.svc-a.example` is `svc-a.example`.
/// An id with no dot is its own principal. One wildcard certificate per deployable (`*.svc-a.example`) then binds
/// each of its nodes to the principal; under `SharedSecret` the id is a claim, and so is its principal.
pub fn principal(node_id: &str) -> &str {
    node_id.split_once('.').map_or(node_id, |(_, rest)| rest)
}

/// The start of a principal's grants: `Allow::call("jobs.").spawn("session")`.
pub enum Allow {}

impl Allow {
    /// Calls to every endpoint whose name starts with `prefix`.
    pub fn call(prefix: impl Into<String>) -> Grants {
        Grants::default().call(prefix)
    }

    /// Starting actors of `kind`, `*` for any.
    pub fn spawn(kind: impl Into<String>) -> Grants {
        Grants::default().spawn(kind)
    }

    /// Asking where a key of `kind` runs, `*` for any.
    pub fn watch(kind: impl Into<String>) -> Grants {
        Grants::default().watch(kind)
    }

    /// `access` to the streams of `namespace`, `*` for any: axon-stream's `Access`.
    pub fn stream<A: Into<u8>>(namespace: impl Into<String>, access: impl IntoIterator<Item = A>) -> Grants {
        Grants::default().stream(namespace, access)
    }
}

/// What one principal may do; [`Allow`] starts one.
#[derive(Clone, Debug, Default)]
pub struct Grants {
    calls: Vec<String>,
    spawns: Vec<String>,
    watches: Vec<String>,
    streams: Vec<(String, Vec<u8>)>,
}

impl Grants {
    pub fn call(mut self, prefix: impl Into<String>) -> Self {
        self.calls.push(prefix.into());
        self
    }

    pub fn spawn(mut self, kind: impl Into<String>) -> Self {
        self.spawns.push(kind.into());
        self
    }

    pub fn watch(mut self, kind: impl Into<String>) -> Self {
        self.watches.push(kind.into());
        self
    }

    pub fn stream<A: Into<u8>>(mut self, namespace: impl Into<String>, access: impl IntoIterator<Item = A>) -> Self {
        self.streams.push((namespace.into(), access.into_iter().map(Into::into).collect()));
        self
    }

    fn merge(&mut self, more: Grants) {
        self.calls.extend(more.calls);
        self.spawns.extend(more.spawns);
        self.watches.extend(more.watches);
        self.streams.extend(more.streams);
    }

    fn may_call(&self, name: &str) -> bool {
        let named = |p: &String| name.starts_with(p.as_str());
        // The cluster's own endpoints stay open to every member unless this principal's rule names them.
        self.calls.iter().any(named) || name.starts_with("axon.") && !self.calls.iter().any(|p| p.starts_with("axon."))
    }
}

fn names(kinds: &[String], kind: &str) -> bool {
    kinds.iter().any(|k| k == "*" || k == kind)
}

/// A table of [`Grants`] per [`principal`]: what each deployable may call, start, watch and do to streams. Every
/// member may call the cluster's own `axon.*` endpoints unless its principal's rule names `axon.`; a principal with no
/// rule may do nothing else. It is the cluster's [`Authorizer`] and, in axon-stream with its `cluster` feature, the
/// stream one.
#[derive(Clone, Debug, Default)]
pub struct Rules {
    principals: BTreeMap<String, Grants>,
}

impl Rules {
    pub fn new() -> Self {
        Self::default()
    }

    /// Adds `grants` to what `principal` may do: a principal, not a node id, so `svc-a.example` for node
    /// `n1.svc-a.example`; a node id with a dot never matches as its own.
    pub fn allow(mut self, principal: impl Into<String>, grants: Grants) -> Self {
        self.principals.entry(principal.into()).or_default().merge(grants);
        self
    }

    /// A principal with no rule has none: the `axon.*` calls every member may make.
    fn grants(&self, node_id: &str) -> &Grants {
        const NONE: &Grants =
            &Grants { calls: Vec::new(), spawns: Vec::new(), watches: Vec::new(), streams: Vec::new() };
        self.principals.get(principal(node_id)).unwrap_or(NONE)
    }

    /// Whether node `node_id` may have `access` (axon-stream's `Access`) to the streams of `namespace`: what the
    /// stream authorizer asks.
    pub fn allows_stream(&self, node_id: &str, namespace: &str, access: impl Into<u8>) -> bool {
        let access = access.into();
        let streams = &self.grants(node_id).streams;
        streams.iter().any(|(ns, granted)| (ns == "*" || ns == namespace) && granted.contains(&access))
    }
}

impl Authorizer for Rules {
    fn authorize(&self, peer: &Peer, action: Action<'_>) -> bool {
        let grants = self.grants(&peer.identity.node_id);
        match action {
            Action::Call { name } => grants.may_call(name),
            Action::Spawn { kind } => names(&grants.spawns, kind),
            Action::Watch { kind } => names(&grants.watches, kind),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_principal_is_the_node_id_after_its_first_label() {
        assert_eq!(principal("n3.svc-a.example"), "svc-a.example");
        assert_eq!(principal("svc-a.example"), "example");
        assert_eq!(principal("n3"), "n3");
    }
}
