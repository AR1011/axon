use super::*;

/// Quotas choose replicas from what nodes advertise under `axon.quota`: a node that set it by hand would be chosen as
/// one without serving.
#[tokio::test]
async fn metadata_the_cluster_keeps_is_refused_and_quotas_advertise_their_region() {
    let a = node("a", &MemoryDiscovery::new()).await;
    for key in [LIFECYCLE, LEAVE_REASON, KINDS, "axon.quota"] {
        let set = a.cluster.set_metadata(key, Some("x")).await;
        assert!(matches!(set, Err(ClusterError::Config(_))), "{key}: {set:?}");
    }
    let long = QuotaConfig { region: "x".repeat(600), ..QuotaConfig::default() };
    assert!(matches!(Quotas::start(&a.cluster, long).await, Err(ClusterError::Discovery(_))));
    let record = a.cluster.record();
    assert!(
        a.cluster.stats().quotas.is_none() && !record.metadata.contains_key("axon.quota"),
        "a failed start is undone"
    );
    let config = || QuotaConfig { region: "eu".into(), ..QuotaConfig::default() };
    let _quotas = Quotas::start(&a.cluster, config()).await.unwrap();
    assert_eq!(a.cluster.record().metadata.get("axon.quota").map(String::as_str), Some("eu"));
    let listed = a.cluster.view().get("a").and_then(|m| m.record.metadata.get("axon.quota").cloned());
    assert_eq!(listed.as_deref(), Some("eu"));
    assert!(matches!(Quotas::start(&a.cluster, config()).await, Err(ClusterError::NameTaken)));
    assert!(a.cluster.stats().quotas.is_some_and(|s| s.entries == 0 && !s.pulling) && !a.cluster.quotas_starting());
    a.system.shutdown().await.unwrap();
}

/// A lone quota node replicates its keys itself, and its heartbeats keep their grants while it is idle: a key takes the
/// fallback once, when new here, however long it rests.
#[tokio::test]
async fn an_idle_node_keeps_its_grants_by_heartbeat_and_a_lone_node_grants_itself() {
    const REQ: Rate = Rate::new("req/s", 1000, Duration::from_secs(1));
    let a = node("a", &MemoryDiscovery::new()).await;
    let quotas = Quotas::start(&a.cluster, QuotaConfig::default()).await.unwrap();
    let keys: Vec<String> = (0..20).map(|k| format!("k{k}")).collect();
    let take_all = || keys.iter().for_each(|key| quotas.take(key, &[(REQ, 1)]).unwrap());
    take_all();
    assert_eq!(quotas.stats().fallback, 20, "each key new here");
    tokio::time::sleep(Duration::from_millis(500)).await;
    take_all();
    assert_eq!(quotas.stats().fallback, 20, "then on its own grant");
    let reports = quotas.stats().reports;
    // Past a grant's hold after the last report that carried a key.
    tokio::time::sleep(Duration::from_secs(6)).await;
    assert!(quotas.stats().reports >= reports + 4, "a heartbeat a second");
    take_all();
    assert_eq!(quotas.stats().fallback, 20, "the heartbeats held the grants");
    assert!(matches!(quotas.take("k0", &[(REQ, 1001)]), Err(Denied::Limit { .. })));
    assert_eq!((quotas.stats().denied_limit, quotas.stats().denied_share), (1, 0));
    a.system.shutdown().await.unwrap();
}

#[tokio::test]
async fn a_permit_comes_back_when_its_holder_panics() {
    const ONE: Permits = Permits::new("one", 1);
    let a = node("a", &MemoryDiscovery::new()).await;
    let quotas = Quotas::start(&a.cluster, QuotaConfig::default()).await.unwrap();
    let permit = quotas.acquire("k", &ONE).unwrap();
    assert!(quotas.acquire("k", &ONE).is_err());
    let holder = tokio::spawn(async move {
        let _held = permit;
        panic!("the holder fails");
    });
    assert!(holder.await.unwrap_err().is_panic());
    drop(quotas.acquire("k", &ONE).unwrap());
    a.system.shutdown().await.unwrap();
}
