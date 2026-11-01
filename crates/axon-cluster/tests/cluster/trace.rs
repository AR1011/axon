use super::*;

#[tokio::test(flavor = "multi_thread")]
async fn a_trace_crosses_nodes_and_untraced_calls_carry_nothing() {
    let discovery = MemoryDiscovery::new();
    let (a, b) = (node("a", &discovery).await, node("b", &discovery).await);
    sees(&b, 2).await;
    let actor = a.system.spawn(Counter { total: 0, tag: "a".into() }).start().unwrap();
    let local = a.cluster.publish::<CounterApi>(&actor, "counter").unwrap();
    let remote = b.cluster.resolve::<CounterApi>("counter").await.unwrap();

    remote.send(Op::Add(1)).await.unwrap();
    let root = TraceContext::root();
    let total = root
        .scope(async {
            remote.tell(Op::Add(2)).await.unwrap();
            local.send(Op::Add(3)).await.unwrap();
            number(remote.request(Op::Get).await.unwrap())
        })
        .await;
    assert_eq!(total, 6);

    // A handle span is recorded when the handler returns, after the reply it sent.
    let handles = || a.system.tracer().since(0, 100).into_iter().filter(|s| s.kind == SpanKind::Handle).count();
    eventually("three handle spans", || async { handles() >= 3 }).await;
    let (calls, local_calls) = (b.system.tracer().trace(root.trace.get()), a.system.tracer().since(0, 100));
    assert_eq!(
        calls.iter().map(|s| (&*s.name, s.parent, s.peer.as_deref())).collect::<Vec<_>>(),
        [("tell", 0, Some("a")), ("request", 0, Some("a"))]
    );
    assert!(calls.iter().all(|s| s.kind == SpanKind::Call && !s.failed));
    let handled: HashSet<u64> = local_calls.iter().filter(|s| s.kind == SpanKind::Handle).map(|s| s.parent).collect();
    let sent = local_calls.iter().find(|s| s.kind == SpanKind::Call).expect("the local send");
    assert_eq!(handled, HashSet::from([calls[0].span, sent.span, calls[1].span]));
    assert_eq!(local_calls.len(), 4, "the untraced send left no span");
    assert!(local_calls.iter().all(|s| s.trace == root.trace.get()));
}

/// Keeps the fields of every `axon::trace` event in `CAPTURED`.
struct Capture;

static CAPTURED: std::sync::Mutex<Vec<HashMap<&'static str, String>>> = std::sync::Mutex::new(Vec::new());

impl tracing::Subscriber for Capture {
    fn enabled(&self, _: &tracing::Metadata<'_>) -> bool {
        true
    }
    fn new_span(&self, _: &tracing::span::Attributes<'_>) -> tracing::span::Id {
        tracing::span::Id::from_u64(1)
    }
    fn record(&self, _: &tracing::span::Id, _: &tracing::span::Record<'_>) {}
    fn record_follows_from(&self, _: &tracing::span::Id, _: &tracing::span::Id) {}
    fn event(&self, event: &tracing::Event<'_>) {
        if event.metadata().target() == "axon::trace" {
            let mut fields = HashMap::new();
            let mut visit =
                |f: &tracing::field::Field, v: &dyn std::fmt::Debug| drop(fields.insert(f.name(), format!("{v:?}")));
            event.record(&mut visit);
            CAPTURED.lock().unwrap().push(fields);
        }
    }
    fn enter(&self, _: &tracing::span::Id) {}
    fn exit(&self, _: &tracing::span::Id) {}
}

#[tokio::test(flavor = "multi_thread")]
async fn the_tracing_bridge_joins_a_call_to_the_handler_it_reached_on_the_other_node() {
    tracing::subscriber::set_global_default(Capture).unwrap();
    let discovery = MemoryDiscovery::new();
    let (a, b) = (node("a", &discovery).await, node("b", &discovery).await);
    sees(&b, 2).await;
    publish_counter(&a, 0, "counter");
    let remote = b.cluster.resolve::<CounterApi>("counter").await.unwrap();

    let root = TraceContext::root();
    root.scope(async { remote.request(Op::Get).await.unwrap() }).await;
    let trace = format!("{:016x}", root.trace);
    let spans = || -> Vec<_> {
        let captured = CAPTURED.lock().unwrap();
        captured.iter().filter(|f| f.get("trace") == Some(&trace) && f.contains_key("span")).cloned().collect()
    };
    eventually("the call and the handler reach the subscriber", || async { spans().len() == 2 }).await;
    let spans = spans();
    let kind = |kind: &str| spans.iter().find(|f| f["message"] == kind).unwrap_or_else(|| panic!("{spans:?}"));
    let (call, handle) = (kind("Call"), kind("Handle"));
    assert_eq!((call["name"].as_str(), call["peer"].as_str()), ("request", "\"a\""));
    assert_eq!(call["parent"], format!("{:016x}", root.span));
    assert_eq!(handle["parent"], call["span"], "the far node's handler is a child of the call");
}

#[tokio::test(flavor = "multi_thread")]
async fn a_body_that_fills_the_frame_is_sent_without_its_trace_rather_than_refused() {
    let discovery = MemoryDiscovery::new();
    let (a, b) = (node("a", &discovery).await, node("b", &discovery).await);
    sees(&b, 2).await;
    let echo = |_: &axon_net::Identity, body: Bytes, reply: Option<axon_cluster::Reply>| {
        let traced = TraceContext::current().is_some() as u8;
        reply
            .expect("a request")
            .send(Bytes::from(vec![traced, (body.len() % 251) as u8]))
            .map_err(|_| Rejection::Stopped)
    };
    a.cluster.serve("echo", 0xE0, 1, echo).unwrap();
    let full = b.cluster.max_frame(axon::Lane::Live);
    for (len, traced) in [(full, 0), (full - TraceContext::WIRE_LEN, 1)] {
        let ask = b.cluster.request("a", "echo", 0xE0, 1, Bytes::from(vec![7; len]));
        let answer = TraceContext::root().scope(ask).await.unwrap();
        assert_eq!(answer[..], [traced, (len % 251) as u8], "a body of {len} bytes");
    }
}

/// A request whose kind says it starts with a trace context that is short, or names no trace, is refused unrun.
#[tokio::test(flavor = "multi_thread")]
async fn a_call_with_a_short_or_zero_trace_context_is_refused_unrun() {
    let discovery = MemoryDiscovery::new();
    let a = node("a", &discovery).await;
    let ran = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let r = ran.clone();
    let count = move |_: &axon_net::Identity, _: Bytes, reply: Option<axon_cluster::Reply>| {
        r.fetch_add(1, SeqCst);
        reply.expect("a request").send(Bytes::new()).map_err(|_| Rejection::Stopped)
    };
    let endpoint = a.cluster.serve("count", 0xE2, 1, count).unwrap();
    let mut raw =
        axon_net::connect(a.cluster.local_addr(), axon_net::Lane::Control, None, &transport("z")).await.unwrap();
    // REQUEST with TRACED, from the call frame's pinned bytes; the answer is END with the rejection's code.
    let (traced_request, end) = (0x8003, 5);
    let zero = [&[0; 16][..], b"hi"].concat();
    for (id, body) in [(1, vec![7; 10]), (2, zero)] {
        let frame = axon_net::Frame {
            kind: traced_request,
            schema: 1,
            protocol: endpoint,
            id,
            payload: body.into(),
            ..axon_net::Frame::default()
        };
        raw.sender.submit(frame, None).await.unwrap();
        let answer = tokio::time::timeout(Duration::from_secs(5), raw.receiver.recv()).await.unwrap().unwrap();
        let answer = answer.into_frame();
        let refused = [Rejection::InvalidMessage as u8];
        assert_eq!((answer.kind, answer.id, &answer.payload[..]), (end, id, &refused[..]), "call {id}");
    }
    assert_eq!(ran.load(SeqCst), 0, "nothing ran");
}
