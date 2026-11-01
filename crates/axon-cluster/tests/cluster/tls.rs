use super::*;

/// Access revoked at runtime: what was admitted finishes, and the next call, spawn and watch ask are refused
/// `Forbidden`. Access granted again lets the same reference through.
#[tokio::test(flavor = "multi_thread")]
async fn a_revoked_peer_is_refused_its_next_call_spawn_and_watch() {
    // Lets the node's calls to the system endpoints through, so its spawns and watches meet their own checks.
    struct Refuse(&'static str);
    impl Authorizer for Refuse {
        fn authorize(&self, peer: &axon_net::Peer, action: Action<'_>) -> bool {
            *peer.identity.node_id != *self.0 || matches!(action, Action::Call { name } if name.starts_with("axon."))
        }
    }
    let discovery = MemoryDiscovery::new();
    let (a, b) = (node("a", &discovery).await, node("b", &discovery).await);
    counter_kind(&a).await;
    publish_counter(&a, 3, "open");
    let open = b.cluster.resolve_on::<CounterApi>("a", "open").await.unwrap();
    assert_eq!(number(open.request(Op::Get).await.unwrap()), 3);
    let anywhere = Placement::default();
    eventually("b sees the kind on a", || async { b.cluster.owner("counter", "k", &anywhere).is_some() }).await;
    b.cluster.get_or_spawn::<CounterApi>("counter", "k", seed(0), &anywhere).await.unwrap();
    let mut watch = b.cluster.watch_key("counter", "k", &anywhere);
    async fn next(watch: &mut KeyWatch) -> Option<Owner> {
        tokio::time::timeout(Duration::from_secs(5), watch.next()).await.expect("the watch answers or ends")
    }
    assert!(matches!(next(&mut watch).await, Some(Owner::Running { .. })));
    // The watch's next question, held on a until the key changes or its hold (1 s) passes.
    let asked = a.cluster.stats().engine.inbound_calls;
    let held = tokio::spawn(async move { (next(&mut watch).await, watch) });
    eventually("a holds b's question", || async { a.cluster.stats().engine.inbound_calls > asked }).await;

    a.cluster.set_authorizer(Some(Arc::new(Refuse("b"))));
    let forbidden = |e: &CallError| matches!(e, CallError::Rejected(Rejection::Forbidden));
    assert!(open.request(Op::Get).await.is_err_and(|e| forbidden(&e)));
    let spawn = b.cluster.spawn_on::<CounterApi>("a", "counter", seed(0)).await;
    assert!(matches!(spawn, Err(ClusterError::Call(ref e)) if forbidden(e)), "{spawn:?}");
    // The question held before the revoke answers; the next one is refused, which ends the watch.
    a.system.lookup::<Msg>("counter/k").expect("the key runs on a").lifecycle.stop();
    let (answer, mut watch) = held.await.unwrap();
    assert_eq!(answer, Some(Owner::Stopped));
    assert_eq!(next(&mut watch).await, None);

    a.cluster.set_authorizer(None);
    assert_eq!(number(open.request(Op::Get).await.unwrap()), 3);
    let mut watch = b.cluster.watch_key("counter", "k", &anywhere);
    assert_eq!(next(&mut watch).await, Some(Owner::Stopped));
    // Each swap is on the node's timeline, so a console shows when access changed.
    let told = a.cluster.events().since(0, usize::MAX);
    let swaps: Vec<_> = told.iter().filter(|e| e.kind == "authorizer_changed").map(|e| e.detail.as_str()).collect();
    assert_eq!(swaps, ["set", "removed: every authenticated peer is allowed"]);
}

struct Ca {
    cert: rcgen::Certificate,
    key: rcgen::KeyPair,
}

fn ca(name: &str) -> Ca {
    let key = rcgen::KeyPair::generate().unwrap();
    let mut params = rcgen::CertificateParams::new(Vec::<String>::new()).unwrap();
    params.is_ca = rcgen::IsCa::Ca(rcgen::BasicConstraints::Unconstrained);
    params.distinguished_name.push(rcgen::DnType::CommonName, name);
    Ca { cert: params.self_signed(&key).unwrap(), key }
}

/// Mutual TLS as `name`, with a leaf `issuer` signed, trusting the `trusted` CAs.
fn mtls(name: &str, issuer: &Ca, trusted: &[&Ca]) -> Tls {
    mtls_with(name, issuer, trusted, axon_net::rustls::crypto::ring::default_provider())
}

/// `mtls`, its server on `provider`.
fn mtls_with(name: &str, issuer: &Ca, trusted: &[&Ca], provider: axon_net::rustls::crypto::CryptoProvider) -> Tls {
    use axon_net::rustls::pki_types::PrivatePkcs8KeyDer;
    use axon_net::rustls::{ClientConfig, RootCertStore, ServerConfig, server::WebPkiClientVerifier};
    let key = rcgen::KeyPair::generate().unwrap();
    let leaf = rcgen::CertificateParams::new(vec![name.to_owned()]).unwrap();
    let cert = leaf.signed_by(&key, &issuer.cert, &issuer.key).unwrap().der().clone();
    let key = PrivatePkcs8KeyDer::from(key.serialize_der());
    let mut roots = RootCertStore::empty();
    trusted.iter().for_each(|ca| roots.add(ca.cert.der().clone()).unwrap());
    let roots = Arc::new(roots);
    Tls {
        server: Arc::new(
            ServerConfig::builder_with_provider(Arc::new(provider))
                .with_safe_default_protocol_versions()
                .unwrap()
                .with_client_cert_verifier(WebPkiClientVerifier::builder(roots.clone()).build().unwrap())
                .with_single_cert(vec![cert.clone()], key.clone_key().into())
                .unwrap(),
        ),
        client: Arc::new(
            ClientConfig::builder()
                .with_root_certificates(roots)
                .with_client_auth_cert(vec![cert], key.into())
                .unwrap(),
        ),
    }
}

async fn mtls_node(id: &str, discovery: &MemoryDiscovery, tls: Tls) -> Node {
    let transport = axon_net::Config::new(id, axon_net::CertificateIdentity);
    let node = node_from(transport, discovery.clone(), |cfg| cfg.tls = Some(tls)).await;
    publish_counter(&node, 0, "counter");
    node
}

async fn calls(from: &Node, to: &Node) -> bool {
    let to = &to.cluster.identity().node_id;
    match from.cluster.resolve_on::<CounterApi>(to, "counter").await {
        Ok(remote) => remote.request(Op::Get).await.is_ok(),
        Err(_) => false,
    }
}

async fn probe(id: &str, discovery: &MemoryDiscovery, tls: Tls, nodes: &[&Node]) -> Node {
    let probe = mtls_node(id, discovery, tls).await;
    for n in nodes {
        eventually("the probe is in view", || async { n.cluster.view().get(id).is_some() }).await;
    }
    probe
}

/// `Some(true)` when the probe calls every node and every node calls it, `Some(false)` when no call either way gets
/// through.
async fn reach(probe: &Node, nodes: &[&Node]) -> Option<bool> {
    let mut through = Vec::new();
    for n in nodes {
        through.extend([calls(probe, n).await, calls(n, probe).await]);
    }
    [true, false].into_iter().find(|all| through.iter().all(|t| t == all))
}

/// Whether a node that joins with `tls` reaches the nodes, and they it, over new sessions.
async fn joins(id: &str, discovery: &MemoryDiscovery, tls: Tls, nodes: &[&Node]) -> Option<bool> {
    let probe = probe(id, discovery, tls, nodes).await;
    let reached = reach(&probe, nodes).await;
    drop(probe.cluster);
    probe.system.shutdown().await.unwrap();
    reached
}

/// Every ordered pair calls the other until stopped; each returns its failures.
fn call_throughout(nodes: &[&Node], stop: &Arc<std::sync::atomic::AtomicBool>) -> tokio::task::JoinSet<Vec<String>> {
    let mut callers = tokio::task::JoinSet::new();
    for from in nodes {
        for to in nodes.iter().filter(|to| to.cluster.identity() != from.cluster.identity()) {
            let (cluster, to, stop) = (from.cluster.clone(), to.cluster.identity().node_id.clone(), stop.clone());
            callers.spawn(async move {
                let remote = cluster.resolve_on::<CounterApi>(&to, "counter").await.unwrap();
                let mut failed = Vec::new();
                while !stop.load(SeqCst) {
                    if let Err(e) = remote.request(Op::Get).await {
                        failed.push(format!("{to}: {e:?}"));
                    }
                }
                failed
            });
        }
    }
    callers
}

#[tokio::test(flavor = "multi_thread")]
async fn every_node_rotates_to_a_new_ca_with_calls_in_flight_and_none_failing() {
    let (old, new) = (ca("old"), ca("new"));
    let discovery = MemoryDiscovery::new();
    let ids = ["node-a", "node-b", "node-c"];
    let mut nodes = Vec::new();
    for id in ids {
        nodes.push(mtls_node(id, &discovery, mtls(id, &old, &[&old])).await);
    }
    // Not called throughout: it crashes between passes 1 and 2 and comes back on the certificates of pass 2.
    let crashing = mtls_node("node-d", &discovery, mtls("node-d", &old, &[&old])).await;
    let nodes: Vec<&Node> = nodes.iter().collect();
    for n in nodes.iter().chain([&&crashing]) {
        sees(n, 4).await;
    }
    let stop = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let callers = call_throughout(&nodes, &stop);

    // A leaf of the new CA is refused until the nodes trust it. Then the dials that kept failing get through.
    let early = probe("probe-0", &discovery, mtls("probe-0", &new, &[&old, &new]), &nodes).await;
    assert_eq!(reach(&early, &nodes).await, Some(false));
    // 1. Trust both CAs.
    for (n, id) in nodes.iter().zip(ids) {
        n.cluster.set_tls(mtls(id, &old, &[&old, &new])).unwrap();
    }
    eventually("redials take the new trust", || async { reach(&early, &nodes).await == Some(true) }).await;
    drop(early.cluster);
    early.system.shutdown().await.unwrap();
    assert_eq!(joins("probe-1", &discovery, mtls("probe-1", &new, &[&old, &new]), &nodes).await, Some(true));
    drop(crashing);
    let restarted = mtls_node("node-d", &discovery, mtls("node-d", &new, &[&old, &new])).await;
    eventually("the restarted node is reached", || async { reach(&restarted, &nodes).await == Some(true) }).await;
    drop(restarted.cluster);
    restarted.system.shutdown().await.unwrap();
    // 2. Switch the leaves: a probe that trusts only the new CA now reaches every node, and every node it.
    for (n, id) in nodes.iter().zip(ids) {
        n.cluster.set_tls(mtls(id, &new, &[&old, &new])).unwrap();
    }
    assert_eq!(joins("probe-2", &discovery, mtls("probe-2", &old, &[&new]), &nodes).await, Some(true));
    // 3. Drop the old CA: a leaf of it is refused both ways.
    for (n, id) in nodes.iter().zip(ids) {
        n.cluster.set_tls(mtls(id, &new, &[&new])).unwrap();
    }
    assert_eq!(joins("probe-3", &discovery, mtls("probe-3", &new, &[&new]), &nodes).await, Some(true));
    assert_eq!(joins("probe-4", &discovery, mtls("probe-4", &old, &[&old, &new]), &nodes).await, Some(false));
    for n in &nodes {
        let told = n.cluster.events().since(0, usize::MAX);
        assert_eq!(told.iter().filter(|e| e.kind == "tls_rotated").count(), 3, "one event per rotation");
    }

    stop.store(true, SeqCst);
    let failed: Vec<String> = callers.join_all().await.into_iter().flatten().collect();
    assert!(failed.is_empty(), "calls failed during the rotation: {failed:?}");
    for n in nodes {
        assert!(n.cluster.stats().engine.succeeded > 100);
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn a_node_given_a_certificate_the_others_do_not_trust_keeps_its_sessions_and_takes_no_new_ones() {
    let (ours, rogue) = (ca("ours"), ca("rogue"));
    let discovery = MemoryDiscovery::new();
    let (a, c) = (
        mtls_node("node-a", &discovery, mtls("node-a", &ours, &[&ours])).await,
        mtls_node("node-c", &discovery, mtls("node-c", &ours, &[&ours])).await,
    );
    sees(&a, 2).await;
    sees(&c, 2).await;
    let stop = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let callers = call_throughout(&[&a, &c], &stop);

    // A rotation that would change who the node is, or one on a node without TLS, is refused and changes nothing.
    let refused = |tried: Result<(), ClusterError>| assert!(matches!(tried, Err(ClusterError::Config(_))), "{tried:?}");
    refused(c.cluster.set_tls(mtls("node-x", &ours, &[&ours])));
    let (served, dialled) = (mtls("node-c", &ours, &[&ours]), mtls("node-x", &ours, &[&ours]));
    refused(c.cluster.set_tls(Tls { server: served.server, client: dialled.client }));
    let plain = node("plain", &MemoryDiscovery::new()).await;
    refused(plain.cluster.set_tls(mtls("plain", &ours, &[&ours])));
    let rotated = |n: &Node| n.cluster.events().since(0, usize::MAX).iter().filter(|e| e.kind == "tls_rotated").count();
    assert_eq!((rotated(&c), rotated(&plain)), (0, 0), "a refused rotation is no event");
    // A shared secret takes any certificate, so only the handshake under the name peers dial guards these: the id, or
    // the address when the id is no server name.
    for (id, name) in [("node-s", "node-s"), ("node s", "127.0.0.1")] {
        let tls = mtls(name, &ours, &[&ours]);
        let secret = node_from(transport(id), MemoryDiscovery::new(), |cfg| cfg.tls = Some(tls.clone())).await;
        secret.cluster.set_tls(tls).unwrap();
        refused(secret.cluster.set_tls(mtls("node-x", &ours, &[&ours])));
    }
    // A server that offers only P-256 answers a client's first key share with a HelloRetryRequest: three round trips.
    let p256 = axon_net::rustls::crypto::CryptoProvider {
        kx_groups: vec![axon_net::rustls::crypto::ring::kx_group::SECP256R1],
        ..axon_net::rustls::crypto::ring::default_provider()
    };
    c.cluster.set_tls(mtls_with("node-c", &ours, &[&ours], p256)).unwrap();
    assert_eq!(joins("probe-0", &discovery, mtls("probe-0", &ours, &[&ours]), &[&a, &c]).await, Some(true));

    c.cluster.set_tls(mtls("node-c", &rogue, &[&ours, &rogue])).unwrap();
    assert_eq!(joins("probe-1", &discovery, mtls("probe-1", &ours, &[&ours]), &[&a]).await, Some(true));
    assert_eq!(joins("probe-2", &discovery, mtls("probe-2", &ours, &[&ours]), &[&c]).await, Some(false));

    let before = c.cluster.stats().engine.succeeded;
    eventually("calls go on over the sessions that were up", || async {
        c.cluster.stats().engine.succeeded > before + 100
    })
    .await;
    stop.store(true, SeqCst);
    let failed: Vec<String> = callers.join_all().await.into_iter().flatten().collect();
    assert!(failed.is_empty(), "calls failed on sessions that were up: {failed:?}");
}

/// `set_tls` asks the node's own authenticator whether it takes the new certificates as its id: a panic there is a
/// refusal, as in a handshake, and the caller is not unwound.
#[tokio::test(flavor = "multi_thread")]
async fn a_rotation_checked_by_an_authenticator_that_panics_is_refused_and_the_caller_goes_on() {
    struct Panics;
    impl axon_net::Authenticator for Panics {
        fn authorize(&self, _: &axon_net::PeerContext<'_>) -> bool {
            panic!("an authenticator's bug")
        }
    }
    let ours = ca("ours");
    let tls = mtls("node-p", &ours, &[&ours]);
    let panicking = axon_net::Config::new("node-p", Panics);
    let n = node_from(panicking, MemoryDiscovery::new(), |cfg| cfg.tls = Some(tls.clone())).await;
    let tried = n.cluster.set_tls(tls);
    assert!(matches!(tried, Err(ClusterError::Config(_))), "{tried:?}");
    drop(n.cluster);
    n.system.shutdown().await.unwrap();
}
