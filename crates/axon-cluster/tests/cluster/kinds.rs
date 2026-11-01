use super::*;

#[tokio::test(flavor = "multi_thread")]
async fn a_kind_name_with_a_slash_is_refused() {
    let n = node("a", &MemoryDiscovery::new()).await;
    let build = |_: &SpawnContext, _: Bytes| Ok(Counter { total: 0, tag: String::new() });
    for kind in ["a/b", "", "a,b"] {
        let refused = n.cluster.kind::<CounterApi>(kind).register(build).await;
        assert!(matches!(refused, Err(ClusterError::Config(_))), "{kind:?}");
    }
    n.cluster.kind::<CounterApi>("a-b").register(build).await.unwrap();
    n.system.shutdown().await.unwrap();
}

/// Only an authority that can list a dead node's keys can restart them: without one, `eager` would be a silent no-op.
/// Consul leases list them by the name each key's holder wrote, so one written under another name than the node id
/// cannot.
#[tokio::test(flavor = "multi_thread")]
async fn an_eager_kind_is_refused_without_an_authority_that_lists_a_nodes_keys() {
    let build = |_: &SpawnContext, _: Bytes| Ok(Counter { total: 0, tag: String::new() });
    let leases = MemoryLeases::new().authority();
    let with_memory = node_with("b", MemoryDiscovery::new(), move |cfg| cfg.ownership = Some(Arc::new(leases))).await;
    #[cfg_attr(not(feature = "consul"), expect(unused_mut, reason = "only Consul adds a node"))]
    let mut nodes = vec![node("a", &MemoryDiscovery::new()).await, with_memory];
    #[cfg(feature = "consul")]
    {
        let leases = ConsulLeases::new(ConsulConfig::new("http://127.0.0.1:1", "axon"), "not-c").unwrap();
        nodes.push(node_with("c", MemoryDiscovery::new(), move |cfg| cfg.ownership = Some(Arc::new(leases))).await);
    }
    for n in nodes {
        let eager = n.cluster.kind::<CounterApi>("counter").eager(seed(0), Placement::default());
        assert!(matches!(eager.register(build).await, Err(ClusterError::Config(_))));
        assert!(!n.cluster.record().has_kind("counter"), "a refused kind is not advertised");
        n.system.shutdown().await.unwrap();
    }
}

/// A wildcard bind on an ephemeral port can advertise the host it is reached at without knowing the port.
#[tokio::test(flavor = "multi_thread")]
async fn an_advertised_port_zero_takes_the_bound_port() {
    let discovery = MemoryDiscovery::new();
    let a = node_with("a", discovery.clone(), |cfg| {
        cfg.bind = "0.0.0.0:0".parse().unwrap();
        cfg.advertise = Some("127.0.0.1:0".parse().unwrap());
    })
    .await;
    let address = a.cluster.record().address;
    assert_eq!(address, SocketAddr::new([127, 0, 0, 1].into(), a.cluster.local_addr().port()));
    let b = node("b", &discovery).await;
    publish_counter(&a, 0, "counter");
    eventually("b reaches a", || async {
        b.cluster.resolve::<CounterApi>("counter").await.is_ok_and(|r| r.node() == "a")
    })
    .await;
    b.system.shutdown().await.unwrap();
    a.system.shutdown().await.unwrap();
}

#[tokio::test(flavor = "multi_thread")]
async fn a_kind_runs_its_actors_with_the_execution_and_mailbox_it_was_registered_with() {
    let pool = PoolConfig { name: "keys".into(), ..Default::default() };
    let runtime = RuntimeConfig { pools: vec![pool], ..Default::default() };
    let system = ActorSystem::builder().runtime(runtime).build().unwrap();
    let cfg = ClusterConfig::new("127.0.0.1:0".parse().unwrap(), transport("a"), MemoryDiscovery::new());
    let cluster = Cluster::start(&system, cfg).await.unwrap();
    let (on, hold) = (Arc::new(std::sync::Mutex::new(None)), Arc::new(std::sync::atomic::AtomicBool::new(true)));
    let build = {
        let (on, hold) = (on.clone(), hold.clone());
        move |_: &SpawnContext, _: Bytes| Ok(Busy { on: on.clone(), hold: hold.clone() })
    };
    let one = MailboxLimits { messages: 1, ..Default::default() };
    let unknown = cluster.kind::<CounterApi>("busy").execution(Execution::Pool("nope")).register(build.clone()).await;
    assert!(matches!(unknown, Err(ClusterError::Config(_))), "a pool the runtime lacks is refused up front");
    cluster.kind::<CounterApi>("busy").execution(Execution::Pool("keys")).mailbox(one).register(build).await.unwrap();

    let actor = cluster.get_or_spawn::<CounterApi>("busy", "k", Bytes::new(), &Placement::default()).await.unwrap();
    actor.send(Op::Add(1)).await.unwrap();
    eventually("the first message is in its handler", || async { on.lock().unwrap().is_some() }).await;
    assert_eq!(on.lock().unwrap().as_deref(), Some("keys-0"));
    // The one message the mailbox holds is the one in the handler.
    let refused = actor.send(Op::Add(1)).await;
    assert!(matches!(refused, Err(CallError::Rejected(Rejection::Full))), "{refused:?}");
    hold.store(false, SeqCst);
    system.shutdown().await.unwrap();
}

/// Keeps the cluster its factory reached, and answers `Tag` with that cluster's node id.
struct Keeper(Cluster);

impl Actor for Keeper {
    type Message = Msg;
    type Error = ();

    fn on_message(&mut self, msg: Msg, _: &mut Context<'_, Self>) -> Result<(), ()> {
        if let Msg::Tag(Some(reply)) = msg {
            return reply.send(Bytes::from(self.0.record().id)).map_err(drop);
        }
        Ok(())
    }
}

/// A factory reaches its cluster through `SpawnContext::cluster`, and neither the kind, its supervised restarts, nor
/// an actor that keeps the `Cluster` keeps it alive past the shutdown.
#[tokio::test(flavor = "multi_thread")]
async fn a_factory_reaches_its_cluster_and_an_actor_that_keeps_it_ends_with_the_shutdown() {
    let a = node("a", &MemoryDiscovery::new()).await;
    let build = |cx: &SpawnContext, _: Bytes| cx.cluster.upgrade().map(Keeper).ok_or_else(|| "gone".to_string());
    a.cluster.kind::<CounterApi>("keeper").register(build).await.unwrap();
    a.cluster.kind::<CounterApi>("kept").policy(RestartPolicy::default()).register(build).await.unwrap();
    for kind in ["keeper", "kept"] {
        let keyed = a.cluster.get_or_spawn::<CounterApi>(kind, "k", Bytes::new(), &Placement::default()).await.unwrap();
        assert_eq!(keyed.request(Op::Tag).await.unwrap(), "a", "{kind}");
    }
    let probe = a.cluster.downgrade();
    a.system.shutdown().await.unwrap();
    drop(a);
    eventually("the cluster is dropped", || async { probe.upgrade().is_none() }).await;
}
