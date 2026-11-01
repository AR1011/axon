//! Mixed versions: a node serves two SCHEMAs of one protocol side by side, a call reaches a node that speaks its own,
//! keys are placed only where every caller's schema is spoken, and nodes whose wire versions do not meet are refused.
//! The rolling upgrade itself runs in the World: `crates/axon-stream/tests/world_upgrade.rs`.
use axon::{Actor, ActorSystem, Context};
use axon_cluster::*;
use axon_net::Negotiation;
use axon_testkit::eventually;
use bytes::{Buf, BufMut, Bytes, BytesMut};

mod common;

/// Adds what it is sent and answers the total.
struct Total(u64);

type Add = (u64, Option<Reply<u64>>);

impl Actor for Total {
    type Message = Add;
    type Error = ();

    fn on_message(&mut self, (n, reply): Add, _: &mut Context<'_, Self>) -> Result<(), ()> {
        self.0 += n;
        drop(reply.map(|r| r.send(self.0)));
        Ok(())
    }
}

/// Schema 1: big-endian. Schema 2 of the same protocol: a tag byte, then little-endian, so neither decodes the other.
struct V1;
struct V2;
/// A caller of a schema no node serves yet.
struct V3;
/// Another protocol, at a schema the name is not served at.
struct Other;

fn be(n: u64) -> Result<Bytes, CallError> {
    Ok(Bytes::copy_from_slice(&n.to_be_bytes()))
}

fn from_be(mut body: Bytes) -> Option<u64> {
    (body.len() == 8).then(|| body.get_u64())
}

fn le(n: u64) -> Result<Bytes, CallError> {
    let mut out = BytesMut::with_capacity(9);
    out.put_u8(2);
    out.put_u64_le(n);
    Ok(out.freeze())
}

fn from_le(mut body: Bytes) -> Option<u64> {
    (body.len() == 9 && body.get_u8() == 2).then(|| body.get_u64_le())
}

macro_rules! schema {
    ($t:ident, $id:literal, $schema:literal, $encode:ident, $decode:ident) => {
        impl WireProtocol for $t {
            type Message = u64;
            type Reply = u64;
            const ID: u64 = $id;
            const SCHEMA: u16 = $schema;

            fn encode(n: u64) -> Result<Bytes, CallError> {
                $encode(n)
            }

            fn decode_reply(body: Bytes) -> Result<u64, CallError> {
                $decode(body).ok_or(CallError::Codec("not this schema".into()))
            }
        }

        impl ActorProtocol for $t {
            type Mailbox = Add;

            fn encode_reply(n: u64) -> Result<Bytes, CallError> {
                $encode(n)
            }

            fn decode(body: Bytes, reply: Option<Reply<u64>>) -> Result<Add, Rejection> {
                $decode(body).map(|n| (n, reply)).ok_or(Rejection::InvalidMessage)
            }
        }
    };
}
schema!(V1, 0x5eed, 1, be, from_be);
schema!(V2, 0x5eed, 2, le, from_le);
schema!(V3, 0x5eed, 3, le, from_le);
schema!(Other, 0xbad, 4, be, from_be);

struct Node {
    system: ActorSystem,
    cluster: Cluster,
}

async fn node(id: &str, discovery: &MemoryDiscovery, tune: impl FnOnce(&mut ClusterConfig)) -> Node {
    let mut cfg = ClusterConfig::new("127.0.0.1:0".parse().unwrap(), common::transport(id), discovery.clone());
    common::short(&mut cfg);
    tune(&mut cfg);
    let system = ActorSystem::builder().build().unwrap();
    let cluster = Cluster::start(&system, cfg).await.unwrap();
    Node { system, cluster }
}

/// Until each node sees all `n` members, so that a call from any of them dials instead of failing with `NoRoute`.
async fn all_see(nodes: &[&Node], n: usize) {
    eventually("every node sees every member", || async { nodes.iter().all(|x| x.cluster.view().members.len() == n) })
        .await;
}

fn mismatch(e: &ClusterError) -> Option<(u16, Vec<u16>)> {
    match e {
        ClusterError::SchemaMismatch { wanted, served } => Some((*wanted, served.clone())),
        _ => None,
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn two_schemas_of_one_protocol_are_served_side_by_side_and_a_call_reaches_a_node_that_speaks_its_own() {
    let discovery = MemoryDiscovery::new();
    let (old, new, caller) = (
        node("old", &discovery, |_| {}).await,
        node("new", &discovery, |_| {}).await,
        node("c", &discovery, |_| {}).await,
    );
    all_see(&[&old, &new, &caller], 3).await;
    let on_old = old.system.spawn(Total(0)).start().unwrap();
    old.cluster.publish::<V1>(&on_old, "total").unwrap();
    old.cluster.publish::<V1>(&on_old, "only-old").unwrap();
    let on_new = new.system.spawn(Total(100)).start().unwrap();
    new.cluster.publish::<V1>(&on_new, "total").unwrap();
    let v2 = new.cluster.publish::<V2>(&on_new, "total").unwrap();

    // One name, one schema: a second publish of it, or of the name under another protocol, is refused.
    assert!(matches!(new.cluster.publish::<V2>(&on_new, "total"), Err(ClusterError::NameTaken)));
    assert!(matches!(new.cluster.publish::<Other>(&on_new, "total"), Err(ClusterError::NameTaken)));

    let (found, failed) = caller.cluster.resolve_all::<V1>("total").await;
    assert_eq!((found.len(), failed.len()), (2, 0));
    let (found, failed) = caller.cluster.resolve_all::<V2>("total").await;
    assert_eq!(found.iter().map(|r| r.node()).collect::<Vec<_>>(), ["new"]);
    assert_eq!(failed.len(), 1);
    let (at, error) = &failed[0];
    assert_eq!((at.as_str(), mismatch(error)), ("old", Some((2, vec![1]))));
    assert_eq!(error.retry(), Retry::Resolve, "the node may speak it after its upgrade");

    // Each schema's own encoding reaches the one actor, and its reply comes back in that encoding.
    assert_eq!(found[0].request(5).await.unwrap(), 105);
    let v1 = caller.cluster.resolve_on::<V1>("new", "total").await.unwrap();
    assert_eq!(v1.request(1).await.unwrap(), 106);
    let e = caller.cluster.resolve::<V2>("only-old").await.unwrap_err();
    assert_eq!(mismatch(&e), Some((2, vec![1])));
    assert!(matches!(caller.cluster.resolve::<Other>("only-old").await, Err(ClusterError::ProtocolMismatch)));

    // Withdrawing one schema leaves the other. `old` has no route to it cached.
    new.cluster.unpublish(&v2);
    let e = found[0].request(0).await.unwrap_err();
    assert_eq!(e.retry(), Retry::Resolve, "{e:?}");
    let e = old.cluster.resolve_on::<V2>("new", "total").await.unwrap_err();
    assert_eq!(mismatch(&e), Some((2, vec![1])), "{e:?}");
    assert_eq!(old.cluster.resolve_on::<V1>("new", "total").await.unwrap().request(0).await.unwrap(), 106);

    // A route cached before a node published a schema does not hide it: the lookup asks again.
    old.cluster.publish::<V1>(&on_old, "late").unwrap();
    caller.cluster.resolve_on::<V1>("old", "late").await.unwrap();
    old.cluster.publish::<V2>(&on_old, "late").unwrap();
    assert_eq!(caller.cluster.resolve_on::<V2>("old", "late").await.unwrap().request(1).await.unwrap(), 1);
    for n in [old, new, caller] {
        n.system.shutdown().await.unwrap();
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn keys_go_only_to_nodes_that_speak_every_callers_schema_of_their_kind_and_a_schema_none_speaks_places_nothing() {
    let discovery = MemoryDiscovery::new();
    let (old, new) = (node("old", &discovery, |_| {}).await, node("new", &discovery, |_| {}).await);
    let build = |_: &SpawnContext, _: Bytes| Ok(Total(0));
    old.cluster.kind::<V1>("total").register(build).await.unwrap();
    new.cluster.kind::<V2>("total").also::<V1>().register(build).await.unwrap();
    let refused = new.cluster.kind::<V2>("other").also::<Other>().register(build).await;
    assert!(matches!(refused, Err(ClusterError::Config(_))), "another protocol's schema: {refused:?}");
    let refused = new.cluster.kind::<V2>("other").also::<V2>().register(build).await;
    assert!(matches!(refused, Err(ClusterError::Config(_))), "a schema twice: {refused:?}");
    eventually("both advertise the kind", || async {
        let view = old.cluster.view();
        view.members.iter().filter(|m| m.record.has_kind("total")).count() == 2
    })
    .await;
    let view = old.cluster.view();
    let schemas = |id: &str| view.get(id).and_then(|m| m.record.kind_schemas("total"));
    assert_eq!((schemas("old"), schemas("new")), (Some(vec![1]), Some(vec![2, 1])), "its own schema first");

    // A kind registered again replaces what it advertised.
    new.cluster.kind::<V1>("again").register(build).await.unwrap();
    new.cluster.kind::<V2>("again").also::<V1>().register(build).await.unwrap();
    let record = new.cluster.record();
    assert_eq!(record.kind_schemas("again"), Some(vec![2, 1]));
    assert_eq!(record.metadata[KINDS].matches("again/").count(), 1, "{:?}", record.metadata[KINDS]);

    let any = Placement::default();
    for i in 0..20 {
        let key = format!("k{i}");
        // A caller of either schema reaches the key's one actor, on the node that speaks both.
        assert_eq!(old.cluster.owner("total", &key, &any).as_deref(), Some("new"));
        let first = old.cluster.get_or_spawn::<V1>("total", &key, Bytes::new(), &any).await.unwrap();
        assert_eq!((first.node(), first.request(1).await.unwrap()), ("new", 1));
        let second = new.cluster.get_or_spawn::<V2>("total", &key, Bytes::new(), &any).await.unwrap();
        assert_eq!(second.request(2).await.unwrap(), 3);
    }
    let spawned = new.cluster.stats().spawned;
    assert_eq!(spawned, 20);
    for caller in [&old, &new] {
        let e = caller.cluster.get_or_spawn::<V3>("total", "k20", Bytes::new(), &any).await.unwrap_err();
        assert_eq!(mismatch(&e), Some((3, vec![1, 2])), "{e:?}");
        assert_eq!(e.retry(), Retry::Resolve);
        let e = caller.cluster.keyed::<V3>("total", "k21", Bytes::new(), &any).request(1).await.unwrap_err();
        assert_eq!(mismatch(&e), Some((3, vec![1, 2])), "{e:?}");
    }
    assert_eq!(new.cluster.stats().spawned, spawned, "a caller no node can answer starts nothing");
    for n in [old, new] {
        n.system.shutdown().await.unwrap();
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn nodes_whose_wire_versions_do_not_meet_are_refused_naming_both_ranges_and_counted() {
    let discovery = MemoryDiscovery::new();
    let (a, b) =
        (node("a", &discovery, |_| {}).await, node("b", &discovery, |cfg| cfg.transport.versions = 9..=9).await);
    let (low, high) = (*axon_net::VERSIONS.start(), *axon_net::VERSIONS.end());
    let actor = a.system.spawn(Total(0)).start().unwrap();
    a.cluster.publish::<V1>(&actor, "total").unwrap();
    all_see(&[&a, &b], 2).await;
    let e = b.cluster.resolve_on::<V1>("a", "total").await.unwrap_err();
    let refused = match &e {
        ClusterError::Call(CallError::Unreachable(axon_net::Error::Negotiation(n))) => Some(*n),
        _ => None,
    };
    assert_eq!(refused, Some(Negotiation::Version { ours: (9, 9), theirs: (low, high) }), "{e:?}");
    let text = axon_net::Error::Negotiation(refused.expect("matched above")).to_string();
    assert!(text.contains("9..=9") && text.contains(&format!("{low}..={high}")), "{text}");
    // Each side's listener counts the other's dials under their own reason.
    let e = a.cluster.resolve_on::<V1>("b", "total").await.unwrap_err();
    let refused = match &e {
        ClusterError::Call(CallError::Unreachable(axon_net::Error::Negotiation(n))) => Some(*n),
        _ => None,
    };
    assert_eq!(refused, Some(Negotiation::Version { ours: (low, high), theirs: (9, 9) }), "{e:?}");
    let version = axon_net::HANDSHAKE_FAILURE_REASONS.iter().position(|r| *r == "version").unwrap();
    for n in [&a, &b] {
        eventually("the refusal is counted", || async {
            n.cluster.stats().engine.listener.handshake_failures[version] > 0
        })
        .await;
    }
    for n in [a, b] {
        n.system.shutdown().await.unwrap();
    }
}

/// A node of a build whose quota wire is another `SCHEMA` refuses a new replica's pull for good, as one of another
/// `replicas_per_region` does, so the pull drops it and never holds the new replica's readiness through a roll.
#[tokio::test(flavor = "multi_thread")]
async fn a_new_quota_replica_does_not_wait_to_pull_from_a_node_of_another_quota_schema() {
    let discovery = MemoryDiscovery::new();
    let other = node("other", &discovery, |cfg| drop(cfg.labels.insert("axon.quota".into(), String::new()))).await;
    // The quota protocol at the default 8 replicas per region, at a schema this build does not speak.
    let protocol = u64::from_be_bytes(*b"AXONQUOT") + 8;
    other.cluster.serve("axon.quota", protocol, 1, |_, _, _| Ok(())).unwrap();
    let new = node("new", &discovery, |_| {}).await;
    all_see(&[&other, &new], 2).await;
    let _quotas = Quotas::start(&new.cluster, QuotaConfig::default()).await.unwrap();
    eventually("the pull drops the node of the other schema", || async { !new.cluster.quotas_starting() }).await;
    for n in [other, new] {
        n.system.shutdown().await.unwrap();
    }
}
