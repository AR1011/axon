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

    let spans = system.tracer().trace(root.trace.get());
    assert_eq!(system.tracer().recorded(), spans.len() as u64, "the untraced message left no span");
    let named = |name: &str| spans.iter().find(|s| s.name == name).unwrap_or_else(|| panic!("no span {name}"));
    let (go, later, fetched, fail) = (named("go"), named("later"), named("fetched"), named("fail"));
    assert_eq!(spans.len(), 6);
    assert_eq!(go.parent, 0);
    assert_eq!((later.parent, fetched.parent), (go.span, later.span));
    assert_eq!(fail.parent, fetched.span);
    assert!(fail.failed && !go.failed);
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
    let names: Vec<_> = system.tracer().trace(root.trace.get()).iter().map(|s| s.name.to_string()).collect();
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
    assert_eq!((back.trace, after.trace, after.parent), (root.trace.get(), root.trace.get(), back.span));
    assert_eq!(back.busy, Duration::from_millis(5), "a duration is read on the actor clock");
    assert!(after.start_us < back.start_us, "a start is the wall's, which stepped back");
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

    pub fn events(trace: u64) -> Vec<(Option<u64>, Fields)> {
        let trace = format!("{trace:016x}");
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
        if system.tracer().trace(root.trace.get()).len() == 6 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }

    let hex = |id: u64| format!("{id:016x}");
    let events = bridge::events(root.trace.get());
    let (started, spans): (Vec<_>, Vec<_>) = events.into_iter().partition(|(_, f)| !f.contains_key("span"));
    assert_eq!(started.len(), 1, "{started:?}");
    assert_eq!(started[0].0, request.id().map(|id| id.into_u64()), "logged in the span that started the trace");

    let keys = ["span", "parent", "name", "start_us", "wait_us", "busy_us"];
    let mut bridged: Vec<_> = spans.iter().map(|(_, f)| keys.map(|k| f[k].clone())).collect();
    let ring = system.tracer().trace(root.trace.get());
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
    assert_eq!((fetched["message"].as_str(), fail["failed"].as_str()), ("Handle", "true"));
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
    let parent = TraceContext { trace: std::num::NonZeroU64::MIN, span: 0 };
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
    let parent = TraceContext { trace: std::num::NonZeroU64::new(0xca5e).unwrap(), span: 0 };
    tracing::subscriber::with_default(bridge::Capture, || {
        tracer.record(Span::begin(parent, 1, SpanKind::Handle, "work"))
    });
    assert_eq!(bridge::events(0xca5e).len(), 1);
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
