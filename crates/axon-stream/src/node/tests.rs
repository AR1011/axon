use std::pin::pin;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering::SeqCst;
use std::sync::mpsc;

use axon::ActorSystem;
use axon::files::{FileFuture, MemoryFiles};
use axon_cluster::{ClusterConfig, MemoryDiscovery};
use axon_net::SharedSecret;

use super::*;
use crate::disk::{FileOp, Files, OsFiles, Retention};
use crate::hub::StartPosition;
use crate::recovery::RecoveryError;

async fn start_cluster(id: &str, discovery: &MemoryDiscovery) -> (ActorSystem, Cluster) {
    start_cluster_with(id, discovery, |_| {}).await
}

async fn start_cluster_with(
    id: &str,
    discovery: &MemoryDiscovery,
    set: impl FnOnce(&mut ClusterConfig),
) -> (ActorSystem, Cluster) {
    let mut transport = axon_net::Config::new(id, SharedSecret::new(*b"0123456789abcdef0123").unwrap());
    transport.allow_plaintext = true;
    let mut cfg = ClusterConfig::new("127.0.0.1:0".parse().unwrap(), transport, discovery.clone());
    set(&mut cfg);
    let system = ActorSystem::builder().build().unwrap();
    let cluster = Cluster::start(&system, cfg).await.unwrap();
    (system, cluster)
}

/// A node keeping `namespaces` in `files`.
fn on_disk(files: Arc<dyn Files>, namespaces: &[&str]) -> StreamNodeConfig {
    let mut cfg = StreamNodeConfig::new(vec![]);
    let retention =
        |ns: &&str| Retention { namespace: ns.to_string(), max_bytes: u64::MAX, max_age: Duration::from_secs(3600) };
    cfg.disk = Some(DiskConfig::new(files, namespaces.iter().map(retention).collect()));
    cfg
}

fn scratch(name: &str) -> std::path::PathBuf {
    let dir = std::env::temp_dir().join(format!("axon-{name}-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    dir
}

/// Files whose next read, once armed, says it began and waits for a release, or five seconds.
struct Gated {
    files: Arc<dyn Files>,
    armed: AtomicBool,
    entered: Arc<Notify>,
    release: Arc<Notify>,
    timed_out: Arc<AtomicBool>,
}

impl Gated {
    fn new(files: Arc<dyn Files>) -> Arc<Self> {
        let (entered, release) = (Arc::new(Notify::new()), Arc::new(Notify::new()));
        Arc::new(Self { files, armed: false.into(), entered, release, timed_out: Arc::default() })
    }
}

impl Files for Gated {
    fn list(&self) -> FileFuture<Vec<(String, u64)>> {
        self.files.list()
    }

    fn read(&self, name: &str) -> FileFuture<Vec<u8>> {
        let read = self.files.read(name);
        if !self.armed.swap(false, SeqCst) {
            return read;
        }
        let (entered, release, timed_out) = (self.entered.clone(), self.release.clone(), self.timed_out.clone());
        Box::pin(async move {
            entered.notify_one();
            if tokio::time::timeout(Duration::from_secs(5), release.notified()).await.is_err() {
                timed_out.store(true, SeqCst);
            }
            read.await
        })
    }

    fn apply(&self, ops: Vec<FileOp>) -> FileFuture<()> {
        self.files.apply(ops)
    }
}

/// An open reads its files without the node's lock: another stream of the node publishes meanwhile, and another
/// open of the same key that takes it first leaves the reader to give up with `AlreadyExists`.
#[tokio::test(flavor = "multi_thread")]
async fn an_open_reads_its_files_without_the_node_lock() {
    let dir = scratch("open-read");
    let (system, cluster) = start_cluster("a", &MemoryDiscovery::new()).await;
    let (btc, eth, replay) = (StreamKey::new("md", "btc"), StreamKey::new("md", "eth"), DeliveryPolicy::ReplayRequired);
    let first = StreamNode::start(&cluster, on_disk(Arc::new(OsFiles::new(&dir)), &["md"])).await.unwrap();
    first.open_stream(&btc, 1, replay).await.unwrap();
    let at = first.publish(&btc, vec![Bytes::from_static(b"a")]).unwrap();
    first.wait_cursor(&btc, at).await.unwrap();
    drop(first);

    let gated = Gated::new(Arc::new(OsFiles::new(&dir)));
    let node = StreamNode::start(&cluster, on_disk(gated.clone(), &["md"])).await.unwrap();
    node.open_stream(&eth, 1, replay).await.unwrap();
    gated.armed.store(true, SeqCst);
    let opening = tokio::spawn(node.open_stream(&btc, 1, replay));
    tokio::time::timeout(Duration::from_secs(5), gated.entered.notified()).await.expect("the open reads its files");
    let published = node.publish(&eth, vec![Bytes::from_static(b"b")]).unwrap();
    node.open_stream(&btc, 1, replay).await.unwrap();
    assert!(!gated.timed_out.load(SeqCst), "the node waited for another stream's read");
    gated.release.notify_one();
    assert_eq!(opening.await.unwrap(), Err(StreamError::AlreadyExists));

    let next = node.publish(&btc, vec![Bytes::from_static(b"c")]).unwrap();
    assert_eq!(next, Cursor { generation: 1, sequence: 2 });
    assert_eq!(node.wait_cursor(&btc, next).await, Ok(next));
    assert_eq!(node.wait_cursor(&eth, published).await, Ok(published));
    assert_eq!(node.stats().disk.map(|d| (d.streams, d.failing)), Some((2, None)));
    drop(node);
    drop(cluster);
    system.shutdown().await.unwrap();
    std::fs::remove_dir_all(&dir).unwrap();
}

/// An open dropped while it reads changes nothing: the key stays unopened, the disk still holds it, and the next
/// open resumes the same records.
#[tokio::test(flavor = "multi_thread")]
async fn an_open_dropped_mid_read_leaves_the_key_unopened() {
    let (system, cluster) = start_cluster("a", &MemoryDiscovery::new()).await;
    let (key, replay, files) =
        (StreamKey::new("md", "btc"), DeliveryPolicy::ReplayRequired, Arc::new(MemoryFiles::new()));
    let first = StreamNode::start(&cluster, on_disk(files.clone(), &["md"])).await.unwrap();
    first.open_stream(&key, 1, replay).await.unwrap();
    let at = first.publish(&key, vec![Bytes::from_static(b"a"), Bytes::from_static(b"b")]).unwrap();
    first.wait_cursor(&key, at).await.unwrap();
    drop(first);

    let gated = Gated::new(files);
    let node = StreamNode::start(&cluster, on_disk(gated.clone(), &["md"])).await.unwrap();
    gated.armed.store(true, SeqCst);
    let opening = tokio::spawn(node.open_stream(&key, 1, replay));
    tokio::time::timeout(Duration::from_secs(5), gated.entered.notified()).await.expect("the open reads its files");
    opening.abort();
    assert!(opening.await.unwrap_err().is_cancelled());
    assert_eq!((node.stored(&key), node.cursor(&key)), (Some(at), None));
    let unopened = node.publish(&key, vec![Bytes::from_static(b"c")]).unwrap_err();
    assert_eq!(unopened.error, StreamError::UnknownStream);

    node.open_stream(&key, 1, replay).await.unwrap();
    assert_eq!((node.stored(&key), node.cursor(&key)), (None, Some(at)));
    let start = SubscribeOptions { start: StartPosition::First, ..Default::default() };
    let mut subscription = node.subscribe(&key, start).unwrap();
    let mut read = Vec::new();
    loop {
        match tokio::time::timeout(Duration::from_secs(5), subscription.next()).await.expect("caught up") {
            Some(StreamEvent::Batch(batch)) => read.extend(batch.records().iter().cloned()),
            Some(StreamEvent::CaughtUp { .. }) => break,
            Some(StreamEvent::Ready { .. }) => {}
            other => panic!("{other:?}"),
        }
    }
    assert_eq!(read, [Bytes::from_static(b"a"), Bytes::from_static(b"b")]);
    assert_eq!(node.publish(&key, vec![Bytes::from_static(b"c")]).unwrap().sequence, 3);
    drop((subscription, node, cluster));
    system.shutdown().await.unwrap();
}

/// Polls once: what a wait gives without waiting.
fn now<T>(future: impl Future<Output = T>) -> Poll<T> {
    pin!(future).poll(&mut Context::from_waker(Waker::noop()))
}

/// A wait for a cursor ends once the write that holds it is synced, with the cursor subscribers see, and not before.
#[tokio::test(flavor = "multi_thread")]
async fn a_wait_for_a_cursor_ends_once_its_write_is_synced() {
    let (system, cluster) = start_cluster("a", &MemoryDiscovery::new()).await;
    let (key, replay) = (StreamKey::new("md", "btc"), DeliveryPolicy::ReplayRequired);
    let held = Held::new(Arc::new(MemoryFiles::new()));
    let node = StreamNode::start(&cluster, on_disk(held.clone(), &["md"])).await.unwrap();
    node.open_stream(&key, 1, replay).await.unwrap();
    node.publish(&key, vec![Bytes::from_static(b"a")]).unwrap();
    let at = node.publish(&key, vec![Bytes::from_static(b"b")]).unwrap();
    let waiting = tokio::spawn(node.wait_cursor(&key, at));
    tokio::time::timeout(Duration::from_secs(5), held.began.notified()).await.expect("the write begins");
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert!(!waiting.is_finished(), "it ended before the write landed");
    assert_eq!(node.cursor(&key), Some(Cursor { generation: 1, sequence: 0 }));
    held.gate.add_permits(1);
    let waited = tokio::time::timeout(Duration::from_secs(5), waiting).await.expect("it ends").unwrap();
    assert_eq!((waited, node.cursor(&key)), (Ok(at), Some(at)));
    // One already reached ends at once, an earlier one with the cursor as it is now.
    assert_eq!(now(node.wait_cursor(&key, Cursor { sequence: 1, ..at })), Poll::Ready(Ok(at)));
    drop((node, cluster));
    system.shutdown().await.unwrap();
}

/// A wait ends with `UnknownStream` once the open that admitted its cursor ends first, whichever way, and is refused
/// at once for a cursor that open never admitted; without a disk it ends at once.
#[tokio::test(flavor = "multi_thread")]
async fn a_wait_for_a_cursor_ends_when_its_open_does() {
    let (system, cluster) = start_cluster("a", &MemoryDiscovery::new()).await;
    let replay = DeliveryPolicy::ReplayRequired;
    let held = Held::new(Arc::new(MemoryFiles::new()));
    let node = StreamNode::start(&cluster, on_disk(held.clone(), &["md"])).await.unwrap();
    let ended = async |waiting: tokio::task::JoinHandle<Result<Cursor, StreamError>>| {
        tokio::time::timeout(Duration::from_secs(5), waiting).await.expect("nothing hangs").unwrap()
    };
    for (i, end) in ["close", "restart", "drop"].into_iter().enumerate() {
        let key = StreamKey::new("md", format!("k{i}"));
        node.open_stream(&key, 1, replay).await.unwrap();
        let at = node.publish(&key, vec![Bytes::from_static(b"a")]).unwrap();
        assert_eq!(
            now(node.wait_cursor(&key, Cursor { generation: 2, ..at })),
            Poll::Ready(Err(RecoveryError::GenerationMismatch.into()))
        );
        assert_eq!(
            now(node.wait_cursor(&key, Cursor { sequence: 2, ..at })),
            Poll::Ready(Err(StreamError::InvalidOptions))
        );
        let waiting = tokio::spawn(node.wait_cursor(&key, at));
        tokio::time::sleep(Duration::from_millis(20)).await;
        assert!(!waiting.is_finished(), "{end}: its write is held");
        match end {
            "close" => node.close_stream(&key).unwrap(),
            "restart" => node.restart_stream(&key, 2).unwrap(),
            _ => drop(node.clone()),
        }
        if end == "drop" {
            assert!(!waiting.is_finished(), "a clone dropped ends nothing");
            let unknown = now(node.wait_cursor(&StreamKey::new("md", "none"), at));
            assert_eq!(unknown, Poll::Ready(Err(StreamError::UnknownStream)));
            drop(node);
            assert_eq!(ended(waiting).await, Err(StreamError::UnknownStream), "{end}");
            break;
        }
        assert_eq!(ended(waiting).await, Err(StreamError::UnknownStream), "{end}");
    }
    held.gate.add_permits(1);

    let node = StreamNode::start(&cluster, StreamNodeConfig::new(vec![])).await.unwrap();
    let key = StreamKey::new("md", "btc");
    node.open_stream(&key, 1, replay).await.unwrap();
    let at = node.publish(&key, vec![Bytes::from_static(b"a")]).unwrap();
    assert_eq!(now(node.wait_cursor(&key, at)), Poll::Ready(Ok(at)), "visible at once");
    assert_eq!(
        now(node.wait_cursor(&key, Cursor { sequence: 2, ..at })),
        Poll::Ready(Err(StreamError::InvalidOptions))
    );
    // Visible when it was asked: a restart before it is polled changes nothing.
    let waiting = node.wait_cursor(&key, at);
    node.restart_stream(&key, 2).unwrap();
    assert_eq!(now(waiting), Poll::Ready(Ok(at)));
    drop((node, cluster));
    system.shutdown().await.unwrap();
}

/// Files whose writes fail until told otherwise.
struct Failing {
    files: MemoryFiles,
    failing: AtomicBool,
}

impl Files for Failing {
    fn list(&self) -> FileFuture<Vec<(String, u64)>> {
        self.files.list()
    }

    fn read(&self, name: &str) -> FileFuture<Vec<u8>> {
        self.files.read(name)
    }

    fn apply(&self, ops: Vec<FileOp>) -> FileFuture<()> {
        if self.failing.load(SeqCst) {
            return Box::pin(std::future::ready(Err(std::io::Error::other("the disk failed"))));
        }
        self.files.apply(ops)
    }
}

/// A wait goes on while the disk fails, as each write is tried again, and ends once one lands.
#[tokio::test(flavor = "multi_thread")]
async fn a_wait_goes_on_through_a_failing_disk() {
    let (system, cluster) = start_cluster("a", &MemoryDiscovery::new()).await;
    let key = StreamKey::new("md", "btc");
    let failing = Arc::new(Failing { files: MemoryFiles::new(), failing: true.into() });
    let node = StreamNode::start(&cluster, on_disk(failing.clone(), &["md"])).await.unwrap();
    node.open_stream(&key, 1, DeliveryPolicy::ReplayRequired).await.unwrap();
    let at = node.publish(&key, vec![Bytes::from_static(b"a")]).unwrap();
    let waiting = tokio::spawn(node.wait_cursor(&key, at));
    axon_testkit::eventually("the writes fail", async || node.stats().disk.and_then(|d| d.failing).is_some()).await;
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert!(!waiting.is_finished(), "it gave up on a failing disk");
    failing.failing.store(false, SeqCst);
    let waited = tokio::time::timeout(Duration::from_secs(5), waiting).await.expect("it ends").unwrap();
    assert_eq!((waited, node.stats().disk.and_then(|d| d.failing)), (Ok(at), None));
    drop((node, cluster));
    system.shutdown().await.unwrap();
}

/// A ring and a compacted state resumed from disk count toward their namespace's `ring_bytes` as fresh ones do,
/// and a state the namespace has no room for refuses the open and leaves the files.
#[tokio::test(flavor = "multi_thread")]
async fn a_stream_resumed_from_disk_is_charged_to_its_namespace() {
    let dir = scratch("resume-charged");
    let (system, cluster) = start_cluster("a", &MemoryDiscovery::new()).await;
    let replay = DeliveryPolicy::ReplayRequired;
    let compacted = DeliveryPolicy::Compacted(crate::CompactLimits { max_keys: 8, max_bytes: 1 << 10 });
    let (btc, book) = (StreamKey::new("md", "btc"), StreamKey::new("st", "book"));
    let bytes = |n: usize| vec![Bytes::from(vec![0u8; n])];
    let first = StreamNode::start(&cluster, on_disk(Arc::new(OsFiles::new(&dir)), &["md", "st"])).await.unwrap();
    first.open_stream(&btc, 1, replay).await.unwrap();
    first.open_stream(&book, 1, compacted).await.unwrap();
    let ring = first.publish(&btc, vec![Bytes::from(vec![0u8; 10]); 6]).unwrap();
    let mut state = None;
    // Three keys of 23 bytes each.
    for k in [b"a", b"b", b"c"] {
        state = Some(first.publish(&book, vec![crate::keyed(k, &[1; 20])]).unwrap());
    }
    first.wait_cursor(&btc, ring).await.unwrap();
    first.wait_cursor(&book, state.unwrap()).await.unwrap();
    drop(first);

    let budget = NamespaceLimits { ring_bytes: Some(100), ..NamespaceLimits::default() };
    let mut cfg = on_disk(Arc::new(OsFiles::new(&dir)), &["md", "st"]).limit("md", budget).limit("st", budget);
    // A ring of one record, so a compacted stream's state is most of what it holds.
    cfg.core.rings = vec![crate::RingLimits { namespace: "st".into(), max_records: 1, max_bytes: 1 << 10 }];
    let node = StreamNode::start(&cluster, cfg).await.unwrap();
    let ring_bytes = |e: StreamError| matches!(e, StreamError::QuotaExceeded { limit: NamespaceLimit::RingBytes, .. });

    let other = StreamKey::new("md", "other");
    node.open_stream(&other, 1, replay).await.unwrap();
    node.open_stream(&btc, 1, replay).await.unwrap();
    let written = node.stats().disk.expect("on disk").bytes;
    assert!(ring_bytes(node.publish(&other, bytes(41)).unwrap_err().error), "the resumed 60 bytes are not held");
    assert_eq!(node.stats().disk.expect("on disk").bytes, written, "a refused publish is written");
    let at = node.publish(&other, bytes(40)).unwrap();
    assert_eq!(at.sequence, 1);
    node.wait_cursor(&other, at).await.unwrap();

    let filler = StreamKey::new("st", "filler");
    node.open_stream(&filler, 1, replay).await.unwrap();
    node.wait_cursor(&filler, node.publish(&filler, bytes(40)).unwrap()).await.unwrap();
    assert!(ring_bytes(node.open_stream(&book, 1, compacted).await.unwrap_err()));
    assert_eq!(node.stored(&book), Some(Cursor { generation: 1, sequence: 3 }));
    node.close_stream(&filler).unwrap();
    node.open_stream(&book, 1, compacted).await.unwrap();
    // 69 bytes of state and a 23-byte ring: 8 left.
    node.open_stream(&filler, 1, replay).await.unwrap();
    assert!(ring_bytes(node.publish(&filler, bytes(9)).unwrap_err().error), "the resumed state is not held");
    node.publish(&filler, bytes(8)).unwrap();
    assert_eq!(node.stats().hub.quota_refused[NamespaceLimit::RingBytes as usize], 3);

    // Published on disk, the state is charged as it changes: 92 held, and a fourth key would leave 115.
    node.close_stream(&filler).unwrap();
    assert!(ring_bytes(node.publish(&book, vec![crate::keyed(b"d", &[1; 20])]).unwrap_err().error));
    // Removing a key frees its 23 bytes: 46 of state and a 3-byte ring leave 51.
    node.wait_cursor(&book, node.publish(&book, vec![crate::keyed(b"a", &[])]).unwrap()).await.unwrap();
    node.open_stream(&filler, 1, replay).await.unwrap();
    assert!(ring_bytes(node.publish(&filler, bytes(52)).unwrap_err().error), "the state was charged");
    node.publish(&filler, bytes(51)).unwrap();
    drop(node);
    drop(cluster);
    system.shutdown().await.unwrap();
    std::fs::remove_dir_all(&dir).unwrap();
}

/// Files whose writes say they began, then wait for a permit.
struct Held {
    files: Arc<dyn Files>,
    began: Arc<Notify>,
    gate: Arc<tokio::sync::Semaphore>,
}

impl Held {
    fn new(files: Arc<dyn Files>) -> Arc<Self> {
        Arc::new(Self { files, began: Arc::default(), gate: Arc::new(tokio::sync::Semaphore::new(0)) })
    }
}

impl Files for Held {
    fn list(&self) -> FileFuture<Vec<(String, u64)>> {
        self.files.list()
    }

    fn read(&self, name: &str) -> FileFuture<Vec<u8>> {
        self.files.read(name)
    }

    fn apply(&self, ops: Vec<FileOp>) -> FileFuture<()> {
        let (files, gate) = (self.files.clone(), self.gate.clone());
        self.began.notify_one();
        Box::pin(async move {
            let _permit = gate.acquire().await;
            files.apply(ops).await
        })
    }
}

/// One node at a time keeps a directory: a start on it fails while another node keeps it, in this process or
/// another, and while the write a dropped node took is still landing, and goes on from what that write left once it
/// has.
#[tokio::test(flavor = "multi_thread")]
async fn a_directory_another_node_keeps_is_refused_until_its_last_write_lands() {
    let dir = scratch("one-node");
    let (system, cluster) = start_cluster("a", &MemoryDiscovery::new()).await;
    let (key, replay) = (StreamKey::new("md", "btc"), DeliveryPolicy::ReplayRequired);
    let in_use = |r: Result<StreamNode, ClusterError>| match r {
        Err(ClusterError::Control(e)) => e.contains("in use"),
        _ => false,
    };
    // Only its write keeps the files once the node is dropped.
    let held = Held::new(Arc::new(OsFiles::new(&dir)));
    let (began, gate) = (held.began.clone(), held.gate.clone());
    let first = StreamNode::start(&cluster, on_disk(held, &["md"])).await.unwrap();
    let (other_system, other) = start_cluster("a", &MemoryDiscovery::new()).await;
    assert!(in_use(StreamNode::start(&other, on_disk(Arc::new(OsFiles::new(&dir)), &["md"])).await), "the first lives");
    drop(other);
    other_system.shutdown().await.unwrap();
    first.open_stream(&key, 1, replay).await.unwrap();
    let at = first.publish(&key, vec![Bytes::from_static(b"a")]).unwrap();
    tokio::time::timeout(Duration::from_secs(10), began.notified()).await.expect("the write begins");
    drop(first);
    assert!(
        in_use(StreamNode::start(&cluster, on_disk(Arc::new(OsFiles::new(&dir)), &["md"])).await),
        "its write is landing"
    );
    gate.add_permits(1);
    let landed = async {
        loop {
            match StreamNode::start(&cluster, on_disk(Arc::new(OsFiles::new(&dir)), &["md"])).await {
                Ok(node) => break node,
                r => assert!(in_use(r)),
            }
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
    };
    let node = tokio::time::timeout(Duration::from_secs(10), landed).await.expect("the write lands and frees it");
    assert_eq!(node.stored(&key), Some(at), "the write landed before the start");
    drop(node);
    let node = StreamNode::start(&cluster, on_disk(Arc::new(OsFiles::new(&dir)), &["md"])).await.unwrap();
    drop(node);
    drop(cluster);
    system.shutdown().await.unwrap();
    std::fs::remove_dir_all(&dir).unwrap();
}

#[tokio::test(flavor = "multi_thread")]
async fn a_woken_subscription_polls_without_the_node_lock() {
    let (system, cluster) = start_cluster("a", &MemoryDiscovery::new()).await;
    let node = StreamNode::start(&cluster, StreamNodeConfig::new(vec![])).await.unwrap();
    let key = StreamKey::new("md", "btc");
    node.open_stream(&key, 1, DeliveryPolicy::ReplayRequired).await.unwrap();
    let mut subscription = node.subscribe(&key, SubscribeOptions::default()).unwrap();
    assert!(matches!(subscription.next().await, Some(StreamEvent::Ready { .. })));
    assert!(matches!(subscription.next().await, Some(StreamEvent::CaughtUp { .. })));
    node.publish(&key, vec![Bytes::from_static(b"a")]).unwrap();

    let held = node.0.core.lock();
    let (tx, rx) = mpsc::sync_channel(1);
    #[expect(clippy::disallowed_methods, reason = "a second thread polls while this one holds the node's lock")]
    std::thread::spawn(move || {
        let mut next = pin!(subscription.next());
        let polled = next.as_mut().poll(&mut Context::from_waker(Waker::noop()));
        let _ = tx.send(matches!(polled, Poll::Ready(Some(StreamEvent::Batch(_)))));
    });
    let batch = rx.recv_timeout(Duration::from_secs(5));
    drop(held);
    assert_eq!(batch, Ok(true), "the poll waited on the node's lock");
    drop((node, cluster));
    system.shutdown().await.unwrap();
}

/// The limits across the cluster are refused at start without a `Quotas`; with one, a node takes each from it.
#[tokio::test(flavor = "multi_thread")]
async fn namespace_limits_across_the_cluster_need_quotas_and_hold_on_one_node() {
    let (system, cluster) = start_cluster("a", &MemoryDiscovery::new()).await;
    let md = NamespaceLimits {
        open_streams: Some(1),
        records_per_sec: Some(3),
        ring_bytes: Some(2),
        ..NamespaceLimits::default()
    };
    let big = NamespaceLimits { records_per_sec: Some(1000), bytes_per_sec: Some(8), ..NamespaceLimits::default() };
    let cfg = StreamNodeConfig::new(vec![]).limit("md", md).limit("big", big);
    assert!(matches!(StreamNode::start(&cluster, cfg.clone()).await, Err(ClusterError::Config(_))));
    let local = NamespaceLimits { subscriptions: Some(1), ring_bytes: Some(64), ..NamespaceLimits::default() };
    drop(StreamNode::start(&cluster, StreamNodeConfig::new(vec![]).limit("md", local)).await.unwrap());

    let quotas = axon_cluster::Quotas::start(&cluster, axon_cluster::QuotaConfig::default()).await.unwrap();
    let node = StreamNode::start(&cluster, StreamNodeConfig { quotas: Some(quotas.clone()), ..cfg }).await.unwrap();
    let (btc, eth, fx) = (StreamKey::new("md", "btc"), StreamKey::new("md", "eth"), StreamKey::new("fx", "eur"));
    let replay = DeliveryPolicy::ReplayRequired;
    node.open_stream(&btc, 1, replay).await.unwrap();
    let refused = node.open_stream(&eth, 1, replay).await.unwrap_err();
    assert!(
        matches!(&refused, StreamError::QuotaExceeded { namespace, limit: NamespaceLimit::OpenStreams, retry_after: Some(_) } if &**namespace == "md"),
        "{refused:?}"
    );
    assert_eq!(node.open_stream(&btc, 1, replay).await, Err(StreamError::AlreadyExists));
    node.open_stream(&fx, 1, replay).await.unwrap();
    // A publish refused for any other reason takes none of the rate.
    for _ in 0..5 {
        let unopened = node.publish(&StreamKey::new("md", "sol"), vec![Bytes::from_static(b"a")]).unwrap_err();
        assert_eq!(unopened.error, StreamError::UnknownStream);
        let wider = node.publish(&btc, vec![Bytes::from_static(b"abc")]).unwrap_err().error;
        assert!(matches!(wider, StreamError::QuotaExceeded { limit: NamespaceLimit::RingBytes, .. }), "{wider:?}");
    }
    for _ in 0..3 {
        node.publish(&btc, vec![Bytes::from_static(b"a")]).unwrap();
    }
    let refused = node.publish(&btc, vec![Bytes::from_static(b"a")]).unwrap_err().error;
    assert!(matches!(refused, StreamError::QuotaExceeded { limit: NamespaceLimit::Records, .. }), "{refused:?}");
    for _ in 0..10 {
        node.publish(&fx, vec![Bytes::from_static(b"a")]).unwrap();
    }
    // Both rates are taken, and a batch past one could never be.
    let wide = StreamKey::new("big", "sol");
    node.open_stream(&wide, 1, replay).await.unwrap();
    let past = node.publish(&wide, vec![Bytes::from_static(b"123456789")]).unwrap_err().error;
    assert_eq!(past, StreamError::LimitExceeded);
    node.publish(&wide, vec![Bytes::from_static(b"12345678")]).unwrap();
    let refused = node.publish(&wide, vec![Bytes::from_static(b"1")]).unwrap_err().error;
    assert!(matches!(refused, StreamError::QuotaExceeded { limit: NamespaceLimit::Bytes, .. }), "{refused:?}");
    let stats = node.stats().hub;
    assert_eq!((stats.quota_refused, stats.publish_rejected), ([0, 5, 1, 1, 1], 8));
    node.close_stream(&btc).unwrap();
    assert_eq!(quotas.stats().permits, 0);
    node.open_stream(&eth, 1, replay).await.unwrap();
    // As a task holds the node for its turn, when the last handle goes.
    let turn = Arc::clone(&node.0.0);
    drop(node);
    assert_eq!(quotas.stats().permits, 0, "a node gives its permits back when it goes");
    drop(turn);
    drop((quotas, cluster));
    system.shutdown().await.unwrap();
}

/// Each node refuses on its own view: the relay ends the leaf's branch for its lost upstream a moment before it
/// sees the origin's node come back, which the leaf saw first (tests/world.rs, seed 378). That refusal is asked
/// again at once; the next, or one long after the change, waits the plain backoff.
#[test]
fn a_parent_that_loses_its_upstream_just_after_a_membership_change_is_asked_again_at_once() {
    let key = StreamKey::new("md", "btc");
    let (r, l) = (PeerId::new("r", 1), PeerId::new("l", 1));
    let cfg = CoreConfig::default();
    let ask = |leaf: &mut RelayCore, relay: &mut RelayCore, now| {
        let mut wanted = false;
        for (to, interest) in leaf.reconcile(std::slice::from_ref(&r), now) {
            wanted |= interest.wants.iter().any(|w| w.key == key);
            let reply = relay.handle_interest(&l, interest.clone(), now);
            leaf.interest_result(&to, interest.revision, reply, now);
        }
        wanted
    };
    let start = Instant::now().into_std();
    let lost = start + cfg.timings.upstream_timeout;
    // The membership changed at the loss, or longer than a backoff before it.
    for changed in [lost, lost - cfg.retry_refused - Duration::from_millis(1)] {
        let mut relay = RelayCore::new(&r, crate::AllowAll, cfg.clone()).unwrap();
        let mut leaf = RelayCore::new(&l, crate::AllowAll, cfg.clone()).unwrap();
        let _follower = leaf.subscribe(&key, SubscribeOptions::default()).unwrap();
        assert!(ask(&mut leaf, &mut relay, start));
        relay.reconcile(&[][..], start);
        relay.reconcile(&[][..], lost);
        leaf.membership_changed(changed);
        crate::core::tests::deliver(&mut relay, &r, &mut leaf, &l, lost);
        assert_eq!(leaf.parent_of(&key), None, "the relay ended the branch");
        if changed == lost {
            assert!(ask(&mut leaf, &mut relay, lost), "the relay was left to the refusal backoff");
        }
        assert!(!ask(&mut leaf, &mut relay, lost), "a relay still refusing is asked again at once");
        let backoff = lost + cfg.retry_refused;
        assert!(!ask(&mut leaf, &mut relay, backoff - Duration::from_millis(1)), "asked before the backoff");
        assert!(ask(&mut leaf, &mut relay, backoff), "the waived refusal counted as a strike");
    }
}

/// An open of a key the node pulls lets the pulled stream go before it reads the disk: a read that fails then pulls
/// the key again for the node's follower, as a close does. Before, the follower got nothing more.
#[test]
fn an_open_that_fails_after_letting_a_pulled_key_go_pulls_it_again() {
    use crate::disk::ReadError;
    let (key, replay) = (StreamKey::new("md", "btc"), DeliveryPolicy::ReplayRequired);
    let (o, r) = (PeerId::new("o", 1), PeerId::new("r", 1));
    let files: Arc<dyn Files> = Arc::new(MemoryFiles::new());
    let retention = Retention { namespace: "md".into(), max_bytes: u64::MAX, max_age: Duration::from_secs(3600) };
    let disk = || match now(DiskStore::open(DiskConfig::new(files.clone(), vec![retention.clone()]))) {
        Poll::Ready(disk) => disk.unwrap(),
        Poll::Pending => panic!("files in memory waited"),
    };
    // An earlier boot of r kept generation 1 of the key on its disk.
    let mut relay = RelayCore::new(&r, crate::AllowAll, CoreConfig::default()).unwrap();
    relay.disk = Some(disk());
    relay.open_stream(&key, 1, replay).unwrap();
    relay.publish(&key, vec![Bytes::from_static(b"a")]).unwrap();
    let (written, ops) = relay.take_disk_writes(0).unwrap();
    assert!(matches!(now(written.apply(ops)), Poll::Ready(Ok(()))));
    relay.disk_written(Ok(()));

    let mut relay = RelayCore::new(&r, crate::AllowAll, CoreConfig::default()).unwrap();
    relay.disk = Some(disk());
    let mut origin = RelayCore::new(&o, crate::AllowAll, CoreConfig { relay: false, ..CoreConfig::default() }).unwrap();
    origin.open_stream(&key, 2, replay).unwrap();
    let follower = relay.subscribe(&key, SubscribeOptions::default()).unwrap();
    let mut at = Instant::now().into_std();
    let mut rounds = |relay: &mut RelayCore, origin: &mut RelayCore| {
        let mut records = Vec::new();
        for _ in 0..4 {
            at += Duration::from_millis(500);
            for (to, interest) in relay.reconcile(std::slice::from_ref(&o), at) {
                let reply = origin.handle_interest(&r, interest.clone(), at);
                relay.interest_result(&to, interest.revision, reply, at);
            }
            crate::core::tests::deliver(origin, &o, relay, &r, at);
        }
        while let Some(event) = relay.poll(follower) {
            if let StreamEvent::Batch(batch) = event {
                records.extend(batch.records().iter().cloned());
            }
        }
        records
    };
    assert_eq!(rounds(&mut relay, &mut origin), [] as [Bytes; 0]);
    origin.publish(&key, vec![Bytes::from_static(b"b")]).unwrap();
    assert_eq!(rounds(&mut relay, &mut origin), [Bytes::from_static(b"b")]);

    let unreadable = ReadError::Io(std::io::Error::other("unreadable"));
    assert!(relay.open_read(&key, 1, replay, Some(Err(unreadable))).is_err());
    origin.publish(&key, vec![Bytes::from_static(b"c")]).unwrap();
    let after = rounds(&mut relay, &mut origin);
    assert_eq!(after.last(), Some(&Bytes::from_static(b"c")), "{after:?}");
    assert!(relay.is_subscribed(follower));
}

/// A node whose disk kept a key from an earlier boot relays the key meanwhile: its open from the disk goes on after
/// the last record the disk holds, past what reached it as a relay. A follower behind that position replays the rest
/// from the disk's segments behind the ring, each record once and in order, and goes on live.
#[test]
fn a_follower_behind_an_open_from_disk_replays_the_rest_from_the_disk() {
    use crate::core::tests::{ask, deliver};
    use crate::recovery::RingLimits;
    let (key, replay) = (StreamKey::new("md", "btc"), DeliveryPolicy::ReplayRequired);
    let (o, r) = (PeerId::new("o", 1), PeerId::new("r", 1));
    let record = |n: u64| Bytes::copy_from_slice(&n.to_be_bytes());
    let files: Arc<dyn Files> = Arc::new(MemoryFiles::new());
    let retention = Retention { namespace: "md".into(), max_bytes: u64::MAX, max_age: Duration::from_secs(3600) };
    let disk = || {
        let mut cfg = DiskConfig::new(files.clone(), vec![retention.clone()]);
        // A few records a segment: the gap spans several.
        cfg.segment_bytes = 64;
        match now(DiskStore::open(cfg)) {
            Poll::Ready(disk) => disk.unwrap(),
            Poll::Pending => panic!("files in memory waited"),
        }
    };
    let write = |relay: &mut RelayCore| {
        let (written, ops) = relay.take_disk_writes(0).unwrap();
        assert!(matches!(now(written.apply(ops)), Poll::Ready(Ok(()))));
        relay.disk_written(Ok(()));
    };
    // A ring of 4: most of the gap is on the disk only.
    let rings = vec![RingLimits { namespace: "md".into(), max_records: 4, max_bytes: 1 << 20 }];
    let cfg = CoreConfig { rings, ..CoreConfig::default() };
    // An earlier boot of r kept records 1 to 20 of generation 1 on its disk.
    let mut relay = RelayCore::new(&r, crate::AllowAll, cfg.clone()).unwrap();
    relay.disk = Some(disk());
    relay.open_stream(&key, 1, replay).unwrap();
    for n in 1..=20 {
        relay.publish(&key, vec![record(n)]).unwrap();
    }
    write(&mut relay);

    let mut relay = RelayCore::new(&r, crate::AllowAll, cfg.clone()).unwrap();
    relay.disk = Some(disk());
    let mut origin = RelayCore::new(&o, crate::AllowAll, CoreConfig { relay: false, ..cfg }).unwrap();
    origin.open_stream(&key, 1, replay).unwrap();
    let follower = relay.subscribe(&key, SubscribeOptions::default()).unwrap();
    let at = Instant::now().into_std();
    ask(&mut origin, &o, &mut relay, &r, at);
    origin.publish(&key, (1..=10).map(record).collect()).unwrap();
    deliver(&mut origin, &o, &mut relay, &r, at);
    let mut seen = Vec::new();
    let mut poll = |relay: &mut RelayCore| {
        while let Some(event) = relay.poll(follower) {
            match event {
                StreamEvent::Batch(batch) => seen.extend((batch.first()..).zip(batch.records().iter().cloned())),
                StreamEvent::Ready { .. } | StreamEvent::CaughtUp { .. } => {}
                other => panic!("{other:?} after {} records", seen.len()),
            }
        }
    };
    poll(&mut relay);
    let read = relay.disk_read(&key, 1, replay).unwrap().expect("the disk holds the key");
    let Poll::Ready(read) = now(read.read()) else { panic!("files in memory waited") };
    relay.open_read(&key, 1, replay, Some(read)).unwrap();
    relay.publish(&key, vec![record(21)]).unwrap();
    write(&mut relay);
    let mut reads = 0;
    poll(&mut relay);
    while let Some((files, asked)) = relay.take_disk_reads() {
        let Poll::Ready(read) = now(disk::read_asked(files, asked)) else { panic!("files in memory waited") };
        relay.disk_read_in(read);
        poll(&mut relay);
        reads += 1;
    }
    assert!(reads > 0, "the replay never read the disk");
    assert_eq!(seen, (1..=21).map(|n| (n, record(n))).collect::<Vec<_>>());
    assert!(relay.is_subscribed(follower));
}

/// More starts behind the ring at once than the disk keeps segments read in for: five replays, two segments. Each
/// follower gets every record once and in order, and polled after each read they take turns: each gets its first
/// record before any has all of its own. Before, the starts of the keys first by name were read first, and the others
/// waited for their whole replays. Read twice before the followers are polled, the second read evicts the segments
/// of the first unread, and those starts ask again: they still get every record.
#[test]
fn more_replays_from_the_disk_than_it_keeps_segments_for_take_turns() {
    use crate::recovery::RingLimits;
    let record = |n: u64| Bytes::copy_from_slice(&n.to_be_bytes());
    let keys: Vec<_> = (0..5).map(|k| StreamKey::new("md", format!("k{k}"))).collect();
    let retention = Retention { namespace: "md".into(), max_bytes: u64::MAX, max_age: Duration::from_secs(3600) };
    let rings = vec![RingLimits { namespace: "md".into(), max_records: 4, max_bytes: 1 << 20 }];
    let rows = [1, 2].map(|reads| {
        let mut cfg = DiskConfig::new(Arc::new(MemoryFiles::new()), vec![retention.clone()]);
        // A few records a segment, and two segments read in at a time.
        (cfg.segment_bytes, cfg.max_read_segments) = (64, 2);
        let Poll::Ready(disk) = now(DiskStore::open(cfg)) else { panic!("files in memory waited") };
        let cfg = CoreConfig { rings: rings.clone(), ..CoreConfig::default() };
        let mut origin = RelayCore::new(&PeerId::new("o", 1), crate::AllowAll, cfg).unwrap();
        origin.disk = Some(disk.unwrap());
        for key in &keys {
            origin.open_stream(key, 1, DeliveryPolicy::ReplayRequired).unwrap();
            for n in 1..=40 {
                origin.publish(key, vec![record(n)]).unwrap();
            }
        }
        let (written, ops) = origin.take_disk_writes(0).unwrap();
        assert!(matches!(now(written.apply(ops)), Poll::Ready(Ok(()))));
        origin.disk_written(Ok(()));
        let start = StartPosition::After(Cursor { generation: 1, sequence: 0 });
        let subscribe = |key| origin.subscribe(key, SubscribeOptions { start, ..Default::default() }).unwrap();
        let ids: Vec<u64> = keys.iter().map(subscribe).collect();
        let mut seen = vec![Vec::new(); keys.len()];
        // The round each follower got its first record in, and its last.
        let (mut first, mut last) = (vec![0; keys.len()], vec![0; keys.len()]);
        for round in 1..=1000 {
            for _ in 0..reads {
                if let Some((files, asked)) = origin.take_disk_reads() {
                    let Poll::Ready(read) = now(disk::read_asked(files, asked)) else {
                        panic!("files in memory waited")
                    };
                    origin.disk_read_in(read);
                }
            }
            for (i, id) in ids.iter().enumerate() {
                while let Some(event) = origin.poll(*id) {
                    match event {
                        StreamEvent::Batch(b) => seen[i].extend((b.first()..).zip(b.records().iter().cloned())),
                        StreamEvent::Ready { .. } | StreamEvent::CaughtUp { .. } => {}
                        other => panic!("{other:?} after {} records of {}", seen[i].len(), keys[i]),
                    }
                }
                if first[i] == 0 && !seen[i].is_empty() {
                    first[i] = round;
                }
                if last[i] == 0 && seen[i].len() >= 40 {
                    last[i] = round;
                }
            }
            if last.iter().all(|r| *r > 0) {
                break;
            }
        }
        let whole = seen.iter().all(|s| *s == (1..=40).map(|n| (n, record(n))).collect::<Vec<_>>());
        (reads, whole, first.iter().max() < last.iter().min(), first, last)
    });
    for (reads, whole, turns, first, last) in rows {
        assert!(
            whole && (turns || reads > 1),
            "{reads} reads a round: first records in rounds {first:?}, last in {last:?}"
        );
    }
}

/// History that must not be asked.
struct Never;

impl RecoverySource for Never {
    fn available_range(&self, _: &StreamKey) -> Result<crate::recovery::ReplayRange, RecoveryError> {
        unreachable!("asked for a key the node relays")
    }

    fn read_after(
        &self,
        _: &StreamKey,
        _: Cursor,
        _: u64,
        _: crate::recovery::ReadBounds,
    ) -> Result<Vec<crate::recovery::SequencedBatch>, RecoveryError> {
        unreachable!("asked for a key the node relays")
    }
}

/// The namespace's history serves the streams the node originates. A start behind the ring of a key it relays is
/// refused, as on a node without one, and the source is not asked.
#[test]
fn a_history_source_is_not_asked_for_a_key_the_node_relays() {
    use crate::core::tests::{ask, deliver, records};
    use crate::recovery::{MemoryLimits, MemoryStore, RingLimits, SequencedBatch, lock};
    let key = StreamKey::new("md", "btc");
    let (o, r) = (PeerId::new("o", 1), PeerId::new("r", 1));
    let rings = vec![RingLimits { namespace: "md".into(), max_records: 4, max_bytes: 1 << 20 }];
    let cfg = CoreConfig { rings, ..CoreConfig::default() };
    let log = Arc::new(std::sync::Mutex::new(MemoryStore::new(MemoryLimits::default()).unwrap()));
    lock(&log).open(&key, 1).unwrap();
    let mut origin = RelayCore::new(&o, crate::AllowAll, CoreConfig { relay: false, ..cfg.clone() }).unwrap();
    let mut relay = RelayCore::new(&r, crate::AllowAll, cfg).unwrap();
    origin.set_history("md", log.clone());
    relay.set_history("md", Arc::new(Never));
    origin.open_stream(&key, 1, DeliveryPolicy::ReplayRequired).unwrap();
    let _follower = relay.subscribe(&key, SubscribeOptions::default()).unwrap();
    let mut at = Instant::now().into_std();
    for n in 1..=12u64 {
        let record = Bytes::copy_from_slice(&n.to_be_bytes());
        lock(&log).append(&key, &SequencedBatch::new(1, n, vec![record.clone()]).unwrap()).unwrap();
        origin.publish(&key, vec![record]).unwrap();
        at += Duration::from_millis(500);
        ask(&mut origin, &o, &mut relay, &r, at);
        deliver(&mut origin, &o, &mut relay, &r, at);
    }
    assert_eq!(relay.cursor(&key), Some(Cursor { generation: 1, sequence: 12 }));
    let behind =
        SubscribeOptions { start: StartPosition::After(Cursor { generation: 1, sequence: 2 }), ..Default::default() };
    assert_eq!(relay.subscribe(&key, behind).err(), Some(StreamError::Recovery(RecoveryError::HistoryUnavailable)));
    // A ring of 4 holds 9..=12: the rest came from the history.
    let id = origin.subscribe(&key, behind).unwrap();
    assert_eq!(records(&mut origin, id), (3..=12).collect::<Vec<_>>());
}

/// `open_stream_after` serves every position up to its cursor from the namespace's history: refused without one, in a
/// namespace the disk keeps, which numbers a generation from record 1, for a compacted stream, whose state no
/// history holds, and after the last position there is. With one, a cursor behind the open replays from it and goes on live.
#[tokio::test(flavor = "multi_thread")]
async fn open_stream_after_without_a_history_source_is_refused() {
    use crate::recovery::{CompactLimits, MemoryLimits, MemoryStore, SequencedBatch, lock};
    let at = |sequence| Cursor { generation: 4, sequence };
    let record = |n: u64| Bytes::copy_from_slice(&n.to_be_bytes());
    let (system, cluster) = start_cluster("a", &MemoryDiscovery::new()).await;
    let log = Arc::new(std::sync::Mutex::new(MemoryStore::new(MemoryLimits::default()).unwrap()));
    let (logged, kept, plain) =
        (StreamKey::new("logged", "a"), StreamKey::new("kept", "a"), StreamKey::new("plain", "a"));
    for key in [&logged, &kept] {
        lock(&log).open(key, 4).unwrap();
        // The application's log: 4 is committed before it is published.
        lock(&log).append(key, &SequencedBatch::new(4, 1, (1..=4).map(record).collect::<Vec<_>>()).unwrap()).unwrap();
    }
    let cfg = on_disk(Arc::new(MemoryFiles::default()), &["kept"]).history("logged", log.clone()).history("kept", log);
    let node = StreamNode::start(&cluster, cfg).await.unwrap();
    let replay = DeliveryPolicy::ReplayRequired;
    let compacted = DeliveryPolicy::Compacted(CompactLimits { max_keys: 4, max_bytes: 1 << 10 });
    assert_eq!(node.open_stream_after(&plain, at(3), replay).await, Err(StreamError::InvalidOptions));
    assert_eq!(node.open_stream_after(&kept, at(3), replay).await, Err(StreamError::InvalidOptions));
    assert_eq!(node.open_stream_after(&logged, at(3), compacted).await, Err(StreamError::InvalidOptions));
    assert_eq!(node.open_stream_after(&logged, at(u64::MAX), replay).await, Err(StreamError::InvalidOptions));
    assert_eq!(node.cursor(&logged), None);

    node.open_stream_after(&logged, at(3), replay).await.unwrap();
    assert_eq!(node.publish(&logged, vec![record(4)]).map_err(|e| e.error), Ok(at(4)));
    let options = SubscribeOptions { start: StartPosition::After(at(1)), ..Default::default() };
    let mut sub = node.subscribe(&logged, options).unwrap();
    let mut seen = Vec::new();
    while seen.len() < 3 {
        if let StreamEvent::Batch(batch) =
            tokio::time::timeout(Duration::from_secs(5), sub.next()).await.unwrap().unwrap()
        {
            seen.extend((batch.first()..).zip(batch.records()).map(|(s, r)| (s, r == &record(s))));
        }
    }
    assert_eq!(seen, [(2, true), (3, true), (4, true)]);
    drop((sub, node));
    drop(cluster);
    system.shutdown().await.unwrap();
}

/// A push goes to the boot of the child that asked for it. Sent again after that boot went, it never reaches the next
/// boot, whose links are numbered from 1 again: taken there, it set the new boot to the old one's position and its
/// follower skipped records the origin still held (the rig's `stream-gap`). The origin gives the old boot up at once.
#[tokio::test(flavor = "multi_thread")]
async fn a_push_for_a_child_that_restarted_is_not_sent_to_its_next_boot() {
    use std::sync::atomic::{AtomicUsize, Ordering::SeqCst};

    use crate::{Interest, StartPosition, Want};
    let discovery = MemoryDiscovery::new();
    let (origin_system, origin) = start_cluster("o", &discovery).await;
    let node = StreamNode::start(&origin, StreamNodeConfig::new(vec![])).await.unwrap();
    let key = StreamKey::new("md", "btc");
    node.open_stream(&key, 1, DeliveryPolicy::ReplayRequired).await.unwrap();
    // The first boot asks for the key and serves no pushes, so the origin sends its push again until it gives up.
    let (system, child) = start_cluster("c", &discovery).await;
    axon_testkit::eventually("the child sees the origin", async || child.view().members.len() == 2).await;
    let start = StartPosition::After(Cursor { generation: 1, sequence: 0 });
    let want = Want { key: key.clone(), epoch: 1, start, snapshot: false };
    let interest = wire::encode_interest(&Interest { revision: 1, wants: vec![want], ..Default::default() });
    child.request("o", ENDPOINT, PROTOCOL, SCHEMA, interest).await.unwrap();
    node.publish(&key, vec![Bytes::from_static(b"a")]).unwrap();
    drop(child);
    system.shutdown().await.unwrap();

    let (system, child) = start_cluster("c", &discovery).await;
    let pushes = Arc::new(AtomicUsize::new(0));
    let counted = pushes.clone();
    child
        .serve(LIVE, PROTOCOL, SCHEMA, move |_, body, reply| {
            counted.fetch_add(usize::from(body.first() == Some(&wire::PUSH)), SeqCst);
            let retired = wire::encode_answer(&PushAnswer::Retired);
            reply.map_or(Ok(()), |reply| reply.send(retired).map_err(|_| Rejection::InvalidMessage))
        })
        .unwrap();
    axon_testkit::eventually("the origin gives the first boot up", async || {
        node.0.core.lock().downstream_peer("c", Lane::Live).is_none()
    })
    .await;
    assert_eq!(pushes.load(SeqCst), 0, "a push built for the first boot reached the second");
    drop((node, origin, child));
    system.shutdown().await.unwrap();
    origin_system.shutdown().await.unwrap();
}

/// A child whose interests arrive while every push to it fails, on a link that carries the one and not the other, is
/// dropped once its pushes have failed for `downstream_expiry`: otherwise it would hold its branches, and their
/// share of the node's pushes, for as long as it asks.
#[tokio::test(flavor = "multi_thread")]
async fn a_child_whose_pushes_fail_while_its_interests_arrive_is_dropped() {
    use std::sync::atomic::{AtomicUsize, Ordering::SeqCst};

    use crate::{Interest, StartPosition, Want};
    let discovery = MemoryDiscovery::new();
    // Short, so the test runs in seconds.
    let short = |cfg: &mut ClusterConfig| cfg.request_timeout = Duration::from_secs(1);
    let (origin_system, origin) = start_cluster_with("o", &discovery, short).await;
    let mut cfg = StreamNodeConfig::new(vec![]);
    // Short, as the cluster's request_timeout.
    (cfg.tick, cfg.downstream_expiry) = (Duration::from_millis(25), Duration::from_secs(2));
    // Short, as the cluster's request_timeout.
    (cfg.core.refresh, cfg.core.retry_failed) = (Duration::from_millis(300), Duration::from_millis(50));
    let node = StreamNode::start(&origin, cfg).await.unwrap();
    let key = StreamKey::new("md", "btc");
    node.open_stream(&key, 1, DeliveryPolicy::ReplayRequired).await.unwrap();

    let (system, child) = start_cluster("c", &discovery).await;
    let pushes = Arc::new(AtomicUsize::new(0));
    let counted = pushes.clone();
    child
        .serve(LIVE, PROTOCOL, SCHEMA, move |_, _, _| {
            counted.fetch_add(1, SeqCst);
            Err(Rejection::InvalidMessage)
        })
        .unwrap();
    axon_testkit::eventually("the child sees the origin", async || child.view().members.len() == 2).await;
    let start = StartPosition::After(Cursor { generation: 1, sequence: 0 });
    let want = Want { key: key.clone(), epoch: 1, start, snapshot: false };
    let interest = wire::encode_interest(&Interest { revision: 1, wants: vec![want], ..Default::default() });
    child.request("o", ENDPOINT, PROTOCOL, SCHEMA, interest).await.unwrap();
    // It asks again every 200 ms, as a child with nothing new to say does, so the origin always hears from it.
    let asking = child.clone();
    let refresh = tokio::spawn(async move {
        loop {
            let interest = wire::encode_interest(&Interest { revision: 1, base: 1, ..Default::default() });
            let _ = asking.request("o", ENDPOINT, PROTOCOL, SCHEMA, interest).await;
            tokio::time::sleep(Duration::from_millis(200)).await;
        }
    });
    node.publish(&key, vec![Bytes::from_static(b"a")]).unwrap();
    axon_testkit::eventually("the origin gives the child up", async || {
        node.0.core.lock().downstream_peer("c", Lane::Live).is_none()
    })
    .await;
    refresh.abort();
    assert!(pushes.load(SeqCst) > 1, "the push went again until the child was given up");
    let events = node.detail(0).events;
    let expired: Vec<_> = events.iter().filter(|e| e.kind == "downstream_expired").map(|e| &e.detail).collect();
    assert_eq!(expired, ["c could not be reached"]);
    drop((node, origin, child));
    system.shutdown().await.unwrap();
    origin_system.shutdown().await.unwrap();
}

fn host(wmem_max: u64, rmem_max: u64) -> axon_net::HostTcp {
    axon_net::HostTcp { wmem_max, rmem_max, congestion: "cubic".into(), slow_start_after_idle: true }
}

/// The window one connection moves a round trip is at most its host's send buffer: a host whose buffers are below
/// the push window's ceiling says so once, at start, and runs on.
#[tokio::test(flavor = "multi_thread")]
async fn a_host_below_the_push_window_records_one_event_at_start() {
    let discovery = MemoryDiscovery::new();
    let (system, cluster) = start_cluster("o", &discovery).await;
    let limits = |cluster: &Cluster| {
        let events = cluster.events().since(0, 256);
        events.into_iter().filter(|e| e.kind == "host_tcp_limits").map(|e| e.detail).collect::<Vec<_>>()
    };
    // build-1's tcp_wmem 4096 16384 4194304 and tcp_rmem 4096 131072 6291456.
    let node = StreamNode::start_on(&cluster, StreamNodeConfig::new(vec![]), Some(host(4 << 20, 6 << 20))).await;
    let detail = limits(&cluster);
    assert_eq!(detail.len(), 1, "{detail:?}");
    assert!(detail[0].contains("4194304") && detail[0].contains("67108864"), "{}", detail[0]);
    drop(node.unwrap());

    for (host, why) in [(Some(host(64 << 20, 128 << 20)), "the window fits"), (None, "a host without /proc")] {
        drop(StreamNode::start_on(&cluster, StreamNodeConfig::new(vec![]), host).await.unwrap());
        assert_eq!(limits(&cluster).len(), 1, "{why}");
    }
    let node = StreamNode::start_on(&cluster, StreamNodeConfig::new(vec![]), Some(host(64 << 20, 64 << 20))).await;
    assert_eq!(limits(&cluster).len(), 2, "a receive buffer below twice the window");
    drop((node.unwrap(), cluster));
    system.shutdown().await.unwrap();
}
