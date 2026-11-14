use std::rc::Rc;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering::Relaxed};
use std::time::Duration;

use axon::*;

mod common;
use common::Recv;

enum Back {
    Work,
    Fail,
}

struct Worker;

impl Actor for Worker {
    type Message = Back;
    type Error = &'static str;

    fn on_message(&mut self, msg: Back, _: &mut Context<'_, Self>) -> Result<(), Self::Error> {
        match msg {
            Back::Work => Ok(()),
            Back::Fail => Err("failed"),
        }
    }

    fn label(msg: &Back) -> &'static str {
        match msg {
            Back::Work => "work",
            Back::Fail => "fail",
        }
    }
}

enum Front {
    Go,
    Later,
    Fetched,
}

struct Router(ActorHandle<Back>);

impl Actor for Router {
    type Message = Front;
    type Error = ();

    fn on_message(&mut self, msg: Front, cx: &mut Context<'_, Self>) -> Result<(), ()> {
        match msg {
            Front::Go => {
                self.0.tell(Back::Work).unwrap();
                cx.schedule_after(Duration::from_millis(1), Front::Later).unwrap();
            }
            Front::Later => {
                self.0.tell(Back::Work).unwrap();
                cx.submit_future(async { Front::Fetched }).unwrap();
            }
            Front::Fetched => drop(self.0.tell(Back::Fail)),
        }
        Ok(())
    }

    fn label(msg: &Front) -> &'static str {
        match msg {
            Front::Go => "go",
            Front::Later => "later",
            Front::Fetched => "fetched",
        }
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn a_trace_follows_tells_timers_and_jobs_and_nothing_else_is_recorded() {
    let system = ActorSystem::builder().build().unwrap();
    let worker = system.spawn(Worker).start().unwrap();
    let router = system.spawn(Router(worker.clone())).start().unwrap();

    worker.tell(Back::Work).unwrap();
    let root = TraceContext::root();
    root.scope(async { router.send(Front::Go).await.unwrap() }).await;
    assert_eq!(TraceContext::current(), None);
    worker.join().await;

    let spans = system.tracer().trace(root.trace);
    assert_eq!(system.tracer().recorded(), spans.len() as u64, "the untraced message left no span");
    let named = |name: &str| spans.iter().find(|s| s.name == name).unwrap_or_else(|| panic!("no span {name}"));
    let (go, later, fetched, fail) = (named("go"), named("later"), named("fetched"), named("fail"));
    assert_eq!(spans.len(), 6);
    assert_eq!(go.parent, 0);
    assert_eq!((later.parent, fetched.parent), (go.span, later.span));
    assert_eq!(fail.parent, fetched.span);
    assert_eq!((fail.error, go.error), (Some("error"), None));
    let work: Vec<u64> = spans.iter().filter(|s| s.name == "work").map(|s| s.parent).collect();
    assert!(work.contains(&go.span) && work.contains(&later.span), "{work:?}");
    assert_eq!(go.actor.unwrap().0, router.address.incarnation());
    assert!(go.actor.unwrap().1.ends_with("Router") && go.start_us > 0);
    assert_eq!(system.tracer().since(spans[1].seq, 2).len(), 2);
}

/// Items from an attached source are the source's, not the message's that attached it: a traced
/// subscribe must not parent every record that follows for the life of the subscription.
#[tokio::test(flavor = "multi_thread")]
async fn an_attached_source_does_not_join_the_trace_that_attached_it() {
    enum Feed {
        Attach(tokio::sync::mpsc::Receiver<u32>),
        Item(Option<u32>),
    }
    struct Sink;
    impl Actor for Sink {
        type Message = Feed;
        type Error = ();
        fn on_message(&mut self, msg: Feed, cx: &mut Context<'_, Self>) -> Result<(), ()> {
            match msg {
                Feed::Attach(rx) => drop(cx.attach(Recv(rx), Feed::Item)),
                Feed::Item(None) => cx.stop(),
                Feed::Item(Some(_)) => {}
            }
            Ok(())
        }
        fn label(msg: &Feed) -> &'static str {
            match msg {
                Feed::Attach(_) => "attach",
                Feed::Item(_) => "item",
            }
        }
    }
    let system = ActorSystem::builder().build().unwrap();
    let sink = system.spawn(Sink).start().unwrap();
    let (tx, rx) = tokio::sync::mpsc::channel(8);
    let root = TraceContext::root();
    root.scope(async { sink.send(Feed::Attach(rx)).await.unwrap() }).await;
    for i in 0..3 {
        tx.send(i).await.unwrap();
    }
    drop(tx);
    sink.join().await;
    let names: Vec<_> = system.tracer().trace(root.trace).iter().map(|s| s.name.to_string()).collect();
    assert_eq!(names, ["attach".to_string()], "the items were handled outside the trace");
}

#[tokio::test(flavor = "multi_thread")]
async fn one_in_n_untraced_messages_starts_a_trace() {
    let system = ActorSystem::builder().tracing(2, 3).build().unwrap();
    let worker = system.spawn(Worker).start().unwrap();
    for _ in 0..7 {
        worker.tell(Back::Work).unwrap();
    }
    worker.tell(Back::Fail).unwrap();
    worker.join().await;
    assert_eq!(system.tracer().recorded(), 3, "messages 1, 4 and 7");
    let kept = system.tracer().since(0, 10);
    assert_eq!(kept.iter().map(|s| s.seq).collect::<Vec<_>>(), [2, 3], "the ring keeps the newest");
    assert_ne!(kept[0].trace, kept[1].trace);
}

/// Calls `unix_micros` from its destructor, which runs after the wall hook's thread-local is gone.
struct ReadsTheClockOnDrop;

impl Drop for ReadsTheClockOnDrop {
    fn drop(&mut self) {
        assert!(axon::trace::unix_micros() > 0);
    }
}

thread_local! {
    static LATE: ReadsTheClockOnDrop = const { ReadsTheClockOnDrop };
}

#[test]
fn the_wall_clock_can_be_read_while_a_thread_tears_down() {
    #[expect(clippy::disallowed_methods, reason = "the thread's teardown is the point")]
    let thread = std::thread::spawn(|| {
        LATE.with(|_| ());
        axon::trace::unix_micros();
    });
    thread.join().expect("the thread's destructors did not abort");
}

enum Step {
    Back,
    After,
}

/// Steps its thread's wall clock back an hour and takes 5 ms of the actor clock, then asks for a timer.
struct Stepper(Arc<ManualClock>, Arc<AtomicU64>);

impl Actor for Stepper {
    type Message = Step;
    type Error = ();

    fn on_message(&mut self, msg: Step, cx: &mut Context<'_, Self>) -> Result<(), ()> {
        if let Step::Back = msg {
            self.1.fetch_sub(3_600_000_000, Relaxed);
            self.0.advance(Duration::from_millis(5));
            cx.schedule_after(Duration::from_millis(1), Step::After).map_err(drop)?;
        }
        Ok(())
    }

    fn label(msg: &Step) -> &'static str {
        match msg {
            Step::Back => "back",
            Step::After => "after",
        }
    }
}

#[test]
fn a_wall_step_back_inside_a_trace_keeps_its_order_parents_and_durations() {
    let wall = Arc::new(AtomicU64::new(1_800_000_000_000_000));
    let w = wall.clone();
    axon::trace::simulate_wall(Some(Rc::new(move || w.load(Relaxed))));
    let clock = Arc::new(Clock::manual());
    let tracer = Tracer::new(16, 0);
    let config = MailboxConfig { tracer: Some(tracer.clone()), ..MailboxConfig::from(MailboxLimits::default()) };
    let (mut driver, spawn) = EmbeddedDriver::new(Stepper(clock.clone(), wall), config, clock.clock()).unwrap();
    let root = TraceContext::root();
    root.enter(|| spawn.address.try_tell(Step::Back).ok()).expect("admitted");
    driver.run_turn(TurnBudget::default());
    clock.advance(Duration::from_millis(1));
    driver.run_turn(TurnBudget::default());
    axon::trace::simulate_wall(None);

    let spans = tracer.since(0, 10);
    let names: Vec<_> = spans.iter().map(|s| s.name.as_ref()).collect();
    assert_eq!(names, ["back", "after"], "in the order they ran");
    let (back, after) = (&spans[0], &spans[1]);
    assert_eq!((back.trace, after.trace, after.parent), (root.trace, root.trace, back.span));
    assert_eq!(back.busy, Duration::from_millis(5), "a duration is read on the actor clock");
    assert!(after.start_us < back.start_us, "a start is the wall's, which stepped back");
}

/// The label and context of each message a [`Witness`] handled.
type Witnessed = Arc<parking_lot::Mutex<Vec<(&'static str, Option<TraceContext>)>>>;

/// Keeps the label and context of each message it handles.
struct Witness(Witnessed);

impl Actor for Witness {
    type Message = Back;
    type Error = ();

    fn on_message(&mut self, msg: Back, _: &mut Context<'_, Self>) -> Result<(), ()> {
        self.0.lock().push((Self::label(&msg), TraceContext::current()));
        Ok(())
    }

    fn label(msg: &Back) -> &'static str {
        Worker::label(msg)
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn a_context_from_outside_keeps_its_id_and_flags_through_tells_timers_and_jobs() {
    let system = ActorSystem::builder().build().unwrap();
    let seen = Arc::new(parking_lot::Mutex::new(Vec::new()));
    let witness = system.spawn(Witness(seen.clone())).start().unwrap();
    let router = system.spawn(Router(witness.clone())).start().unwrap();
    let id = TraceId::new(0x4bf9_2f35_77b3_4da6_a3ce_929d_0e0e_4736).unwrap();
    let outside = TraceContext::new(id, 0x00f0_67aa_0ba9_02b7, 0);

    outside.scope(async { router.send(Front::Go).await.unwrap() }).await;
    for _ in 0..250 {
        if seen.lock().len() == 3 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }

    // Go tells, Later is a timer's and tells, Fetched is a job's completion and tells.
    let seen = seen.lock().clone();
    assert_eq!(seen.iter().map(|(label, _)| *label).collect::<Vec<_>>(), ["work", "work", "fail"]);
    assert!(seen.iter().all(|(_, c)| c.is_some_and(|c| (c.trace, c.flags) == (id, 0))), "{seen:?}");
    let spans = system.tracer().trace(id);
    assert_eq!(spans.len(), 6, "every hop is in the trace begun outside: {spans:?}");
    let go = spans.iter().find(|s| s.name == "go").unwrap();
    assert_eq!(go.parent, outside.span, "the first span's parent is the one from outside");
    assert_eq!(TraceContext::root().flags, TraceContext::SAMPLED);
}

#[test]
fn traceparent_text_round_trips_and_a_malformed_one_is_refused() {
    let text = "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01";
    let parsed = TraceContext::from_traceparent(text).unwrap();
    assert_eq!(parsed.trace.get(), 0x4bf9_2f35_77b3_4da6_a3ce_929d_0e0e_4736);
    assert_eq!((parsed.span, parsed.flags), (0x00f0_67aa_0ba9_02b7, 1));
    assert_eq!(parsed.traceparent(), text);
    assert_eq!(parsed.trace.to_string(), "4bf92f3577b34da6a3ce929d0e0e4736");
    let child = parsed.child();
    assert_eq!((child.trace, child.flags), (parsed.trace, parsed.flags));
    assert_eq!(TraceContext::from_traceparent(&child.traceparent()), Some(child));

    // A later version may add fields after a dash; the four it shares with 00 are read as 00's.
    let later = "cc-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01-what-comes-next";
    assert_eq!(TraceContext::from_traceparent(later), Some(parsed));
    for bad in [
        "",
        "00-4BF92F3577B34DA6A3CE929D0E0E4736-00f067aa0ba902b7-01",
        "00-00000000000000000000000000000000-00f067aa0ba902b7-01",
        "00-4bf92f3577b34da6a3ce929d0e0e4736-0000000000000000-01",
        "ff-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01",
        "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01-",
        "cc-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01x",
        "00-4bf92f3577b34da6a3ce929d0e0e473-600f067aa0ba902b7-01",
        "00_4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01",
        "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-0g",
        "00-4bf92f3577b34da6a3ce929d0e0e47é-00f067aa0ba902b7-01",
    ] {
        assert_eq!(TraceContext::from_traceparent(bad), None, "{bad}");
    }

    // On the wire: the same fields, big-endian, in the same order.
    let mut wire = 0x4bf9_2f35_77b3_4da6_a3ce_929d_0e0e_4736_u128.to_be_bytes().to_vec();
    wire.extend(0x00f0_67aa_0ba9_02b7_u64.to_be_bytes());
    wire.push(1);
    assert_eq!(parsed.to_bytes()[..], wire[..]);
    assert_eq!(TraceContext::from_bytes(parsed.to_bytes()), Some(parsed));
    assert_eq!(TraceContext::from_bytes([0; TraceContext::WIRE_LEN]), None, "a trace id is never 0");
}

enum Step2 {
    Go,
}

/// Records a step of its own around the tell it makes.
struct Stepping(Arc<Tracer>, ActorHandle<Back>);

impl Actor for Stepping {
    type Message = Step2;
    type Error = ();

    fn on_message(&mut self, Step2::Go: Step2, _: &mut Context<'_, Self>) -> Result<(), ()> {
        let mut step = self.0.app("resize").expect("handled inside a trace");
        step.attr("items", 3u64).attr("delta", -2i64).attr("region", "west").attr("note", format!("n{}", 7));
        step.enter(|| self.1.tell(Back::Work)).map_err(drop)?;
        step.error("stale_input");
        Ok(())
    }

    fn label(_: &Step2) -> &'static str {
        "go"
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn an_app_span_records_its_attributes_and_parents_what_it_sends() {
    let system = ActorSystem::builder().build().unwrap();
    let worker = system.spawn(Worker).start().unwrap();
    let stepping = system.spawn(Stepping(system.tracer().clone(), worker.clone())).start().unwrap();
    let root = TraceContext::root();
    root.scope(async { stepping.send(Step2::Go).await.unwrap() }).await;
    for _ in 0..250 {
        if system.tracer().trace(root.trace).len() == 3 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }

    let spans = system.tracer().trace(root.trace);
    let named = |name: &str| spans.iter().find(|s| s.name == name).unwrap_or_else(|| panic!("no {name}: {spans:?}"));
    let (go, resize, work) = (named("go"), named("resize"), named("work"));
    assert_eq!((resize.kind, resize.parent, work.parent), (SpanKind::App, go.span, resize.span));
    let attrs: Vec<_> = resize.attrs.as_deref().unwrap_or_default().iter().map(|(k, v)| (*k, v.to_string())).collect();
    assert_eq!(
        attrs,
        [("items", "3"), ("delta", "-2"), ("region", "west"), ("note", "n7")].map(|(k, v)| (k, v.into()))
    );
    assert_eq!(resize.attrs.as_deref().unwrap_or_default()[0].1, Attr::U64(3));
    assert_eq!((resize.error, go.error), (Some("stale_input"), None));
    assert!(go.attrs.is_none(), "axon's own spans have no attributes");
}

#[test]
fn an_app_span_outside_a_trace_is_none_and_records_nothing() {
    let tracer = Tracer::new(16, 0);
    assert!(tracer.app("step").is_none());
    assert_eq!(tracer.recorded(), 0);
    let root = TraceContext::root();
    let step = root.enter(|| tracer.app("step")).expect("inside a trace");
    assert_eq!(tracer.recorded(), 0, "recorded when dropped, not before");
    drop(step);
    let spans = tracer.since(0, 16);
    assert_eq!((spans.len(), spans[0].kind, spans[0].trace, spans[0].parent), (1, SpanKind::App, root.trace, 0));
}

#[test]
fn reading_the_whole_ring_holds_its_lock_for_one_page_at_most() {
    const SPANS: usize = 65_536;
    let tracer = Tracer::new(SPANS, 0);
    let (one, other) = (TraceContext::root(), TraceContext::root());
    for i in 0..SPANS + 10 {
        let parent = if i % 1000 == 0 { one } else { other };
        tracer.record(Span::begin(parent, i as u64 + 1, SpanKind::Handle, "work"));
    }
    assert_eq!(tracer.evicted(), 10);
    // What `Request::Traces` asks for.
    let all = tracer.since(0, usize::MAX);
    assert_eq!((all.len(), all[0].seq, all[SPANS - 1].seq), (SPANS, 11, SPANS as u64 + 10));
    assert!(all.windows(2).all(|w| w[1].seq == w[0].seq + 1), "no span is skipped or repeated between pages");
    assert_eq!(tracer.trace(one.trace).len(), 65, "every thousandth, but the first, which the ring let go");
    assert_eq!(tracer.since(SPANS as u64, 1000).len(), 10);
    assert_eq!(tracer.most_held(), Tracer::PAGE);
}

#[test]
fn a_ring_of_any_capacity_keeps_exactly_its_newest_spans_from_any_position() {
    let seqs = |spans: Vec<Span>| spans.iter().map(|s| s.seq).collect::<Vec<_>>();
    for capacity in [1, 5, 31, 32, 33, 100, 300] {
        let tracer = Tracer::new(capacity, 0);
        let root = TraceContext::root();
        for recorded in 1..=700u64 {
            tracer.record(Span::begin(root, recorded, SpanKind::Handle, "work"));
            let oldest = recorded.saturating_sub(capacity as u64) + 1;
            assert_eq!(tracer.evicted(), oldest - 1, "capacity {capacity} after {recorded}");
            for after in [0, oldest - 1, oldest, (oldest + recorded) / 2, recorded - 1, recorded] {
                let want: Vec<u64> = ((after + 1).max(oldest)..=recorded).take(300).collect();
                assert_eq!(
                    seqs(tracer.since(after, 300)),
                    want,
                    "capacity {capacity}, {recorded} recorded, after {after}"
                );
            }
        }
        assert_eq!(tracer.trace(root.trace).len(), capacity);
        // A cursor from a peer, past any span there will be.
        assert!(tracer.since(u64::MAX, 300).is_empty(), "capacity {capacity}");
    }
}

/// Each `axon::trace` event as its fields, with the `tracing` span it was emitted in.
#[cfg(feature = "tracing")]
mod bridge {
    use std::cell::RefCell;
    use std::collections::HashMap;
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::sync::{Mutex, Once};

    use tracing::field::Field;
    use tracing::level_filters::LevelFilter;
    use tracing::span::{Attributes, Id, Record};
    use tracing::{Event, Metadata, Subscriber};

    type Fields = HashMap<&'static str, String>;

    static EVENTS: Mutex<Vec<(Option<u64>, Fields)>> = Mutex::new(Vec::new());
    static NEXT: AtomicU64 = AtomicU64::new(1);
    thread_local!(static ENTERED: RefCell<Vec<u64>> = const { RefCell::new(Vec::new()) });

    /// Filters at `DEBUG`, as `RUST_LOG=axon::trace=debug` would.
    pub struct Capture;

    impl Subscriber for Capture {
        fn enabled(&self, metadata: &Metadata<'_>) -> bool {
            *metadata.level() <= LevelFilter::DEBUG
        }
        fn max_level_hint(&self) -> Option<LevelFilter> {
            Some(LevelFilter::DEBUG)
        }
        fn new_span(&self, _: &Attributes<'_>) -> Id {
            Id::from_u64(NEXT.fetch_add(1, Ordering::Relaxed))
        }
        fn record(&self, _: &Id, _: &Record<'_>) {}
        fn record_follows_from(&self, _: &Id, _: &Id) {}
        fn event(&self, event: &Event<'_>) {
            if event.metadata().target() == "axon::trace" {
                let mut fields = Fields::new();
                event.record(&mut |f: &Field, v: &dyn std::fmt::Debug| drop(fields.insert(f.name(), format!("{v:?}"))));
                EVENTS.lock().unwrap().push((ENTERED.with(|e| e.borrow().last().copied()), fields));
            }
        }
        fn enter(&self, span: &Id) {
            ENTERED.with(|e| e.borrow_mut().push(span.into_u64()));
        }
        fn exit(&self, _: &Id) {
            ENTERED.with(|e| e.borrow_mut().pop());
        }
    }

    pub fn capture() {
        static INSTALL: Once = Once::new();
        INSTALL.call_once(|| tracing::subscriber::set_global_default(Capture).unwrap());
    }

    pub fn events(trace: axon::TraceId) -> Vec<(Option<u64>, Fields)> {
        let trace = trace.to_string();
        EVENTS.lock().unwrap().iter().filter(|(_, fields)| fields.get("trace") == Some(&trace)).cloned().collect()
    }
}

#[cfg(feature = "tracing")]
#[tokio::test(flavor = "multi_thread")]
async fn the_tracing_bridge_sees_each_span_the_ring_holds_and_the_app_span_that_started_the_trace() {
    bridge::capture();
    let system = ActorSystem::builder().build().unwrap();
    let worker = system.spawn(Worker).start().unwrap();
    let router = system.spawn(Router(worker.clone())).start().unwrap();

    let request = tracing::info_span!("request");
    let root = request.in_scope(TraceContext::root);
    root.scope(async { router.send(Front::Go).await.unwrap() }).await;
    worker.join().await;
    for _ in 0..250 {
        if system.tracer().trace(root.trace).len() == 6 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }

    let hex = |id: u64| format!("{id:016x}");
    let events = bridge::events(root.trace);
    let (started, spans): (Vec<_>, Vec<_>) = events.into_iter().partition(|(_, f)| !f.contains_key("span"));
    assert_eq!(started.len(), 1, "{started:?}");
    assert_eq!(started[0].0, request.id().map(|id| id.into_u64()), "logged in the span that started the trace");

    let keys = ["span", "parent", "name", "start_us", "wait_us", "busy_us"];
    let mut bridged: Vec<_> = spans.iter().map(|(_, f)| keys.map(|k| f[k].clone())).collect();
    let ring = system.tracer().trace(root.trace);
    let mut ring: Vec<_> = ring
        .iter()
        .map(|s| {
            let (wait, busy) = (s.wait.as_micros().to_string(), s.busy.as_micros().to_string());
            [hex(s.span), hex(s.parent), s.name.to_string(), s.start_us.to_string(), wait, busy]
        })
        .collect();
    bridged.sort();
    ring.sort();
    assert_eq!(bridged, ring, "one event per recorded span, with its ids");
    assert_eq!(ring.len(), 6);

    let named = |name: &str| spans.iter().map(|(_, f)| f).find(|f| f["name"] == name).unwrap();
    let (fetched, fail) = (named("fetched"), named("fail"));
    assert_eq!(fail["parent"], fetched["span"], "the worker's span is a child of the router's");
    assert!(fetched["actor"].contains("Router") && fail["actor"].contains("Worker"));
    assert_eq!((fetched["message"].as_str(), fail["error"].as_str()), ("Handle", "\"error\""));
}

#[cfg(feature = "tracing")]
#[test]
fn a_subscriber_that_panics_loses_its_event_and_nothing_else() {
    use tracing::span::{Attributes, Id, Record};
    struct Panics;
    impl tracing::Subscriber for Panics {
        fn enabled(&self, _: &tracing::Metadata<'_>) -> bool {
            true
        }
        fn new_span(&self, _: &Attributes<'_>) -> Id {
            Id::from_u64(1)
        }
        fn record(&self, _: &Id, _: &Record<'_>) {}
        fn record_follows_from(&self, _: &Id, _: &Id) {}
        fn event(&self, _: &tracing::Event<'_>) {
            panic!("the subscriber failed");
        }
        fn enter(&self, _: &Id) {}
        fn exit(&self, _: &Id) {}
    }
    let tracer = Tracer::new(8, 0);
    let parent = TraceContext::new(TraceId::new(1).unwrap(), 0, 0);
    tracing::subscriber::with_default(Panics, || {
        tracer.record(Span::begin(parent, 1, SpanKind::Handle, "work"));
        let _started = TraceContext::root();
    });
    assert_eq!(tracer.recorded(), 1, "the span still reached the ring");
}

#[cfg(feature = "tracing")]
#[test]
fn a_tracer_that_keeps_no_ring_still_emits_each_span() {
    let tracer = Tracer::new(0, 0);
    let parent = TraceContext::new(TraceId::new(0xca5e).unwrap(), 0, 0);
    tracing::subscriber::with_default(bridge::Capture, || {
        tracer.record(Span::begin(parent, 1, SpanKind::Handle, "work"))
    });
    assert_eq!(bridge::events(parent.trace).len(), 1);
}

/// A sampled trace starts at the receiver, under the mailbox's lock: it is not logged in the sender's `tracing` span.
#[cfg(feature = "tracing")]
#[tokio::test(flavor = "multi_thread")]
async fn a_sampled_trace_is_not_logged_as_started() {
    bridge::capture();
    let system = ActorSystem::builder().tracing(64, 1).build().unwrap();
    let worker = system.spawn(Worker).start().unwrap();
    tracing::info_span!("sender").in_scope(|| worker.tell(Back::Fail).unwrap());
    worker.join().await;
    let spans = system.tracer().since(0, 64);
    assert_eq!(spans.len(), 1, "{spans:?}");
    let events = bridge::events(spans[0].trace);
    assert_eq!(events.len(), 1, "the handle span alone: {events:?}");
    assert!(events[0].1.contains_key("span"));
}
