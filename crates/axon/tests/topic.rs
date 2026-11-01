use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering::SeqCst};
use std::time::Duration;

use axon::*;

mod common;
use common::{eventually, threadless};

#[derive(Clone, Copy, Debug, PartialEq)]
struct Note {
    from: u64,
    seq: u64,
}

enum Msg {
    Note(Note),
    /// Publishes this many notes to the topic the subscriber holds, from its handler.
    Relay(u64),
    Seen(ReplyTo<Vec<Note>>),
    Crash,
    Panic,
    /// Spins on its worker until the flag is cleared, having set it.
    Hog(Arc<AtomicBool>),
}

impl From<Note> for Msg {
    fn from(note: Note) -> Self {
        Msg::Note(note)
    }
}

#[derive(Default)]
struct Sub {
    seen: Vec<Note>,
    topic: Option<Topic<Msg>>,
}

impl Actor for Sub {
    type Message = Msg;
    type Error = &'static str;

    fn on_message(&mut self, msg: Msg, _cx: &mut Context<'_, Self>) -> Result<(), Self::Error> {
        match msg {
            Msg::Note(note) => self.seen.push(note),
            Msg::Relay(n) => {
                let topic = self.topic.as_ref().expect("a relay holds a topic");
                for seq in 1..=n {
                    topic.publish(note(seq));
                }
            }
            Msg::Seen(reply) => drop(reply.reply(self.seen.clone())),
            Msg::Crash => return Err("crash"),
            Msg::Panic => panic!("panic"),
            Msg::Hog(held) => {
                held.store(true, SeqCst);
                while held.load(SeqCst) {
                    std::hint::spin_loop();
                }
            }
        }
        Ok(())
    }
}

fn note(seq: u64) -> Note {
    Note { from: 0, seq }
}

fn small() -> MailboxLimits {
    MailboxLimits { messages: 2, ..Default::default() }
}

fn in_order(notes: &[Note], publishers: u64) {
    for from in 0..publishers {
        let seqs: Vec<_> = notes.iter().filter(|n| n.from == from).map(|n| n.seq).collect();
        assert!(seqs.windows(2).all(|w| w[0] < w[1]), "publisher {from} out of order");
    }
}

/// Clears the hog's flag when dropped, so a failing test does not leave a worker spinning.
struct Release(Arc<AtomicBool>);

impl Drop for Release {
    fn drop(&mut self) {
        self.0.store(false, SeqCst);
    }
}

async fn seen(sub: &ActorHandle<Msg>) -> Vec<u64> {
    sub.ask(Msg::Seen).await.unwrap().iter().map(|n| n.seq).collect()
}

/// Publishes a topic missed, by reason, as the dead letters count them.
fn missed(system: &ActorSystem, reason: &str) -> u64 {
    let letters = system.dead_letters().recent(usize::MAX);
    letters.iter().filter(|l| l.label == "publish" && l.reason == reason).map(|l| l.count).sum()
}

#[tokio::test(start_paused = true)]
async fn a_publish_never_waits_a_full_subscriber_misses_into_dead_letters_and_one_subscribed_twice_gets_it_once() {
    let (system, _) = threadless();
    let topic = system.topic::<Msg>("notes").unwrap();
    let roomy = system.spawn(Sub::default()).start().unwrap();
    let tight = system.spawn(Sub::default()).mailbox(small()).start().unwrap();
    for sub in [&roomy, &tight] {
        sub.ready().await.unwrap();
    }
    for _ in 0..3 {
        topic.subscribe(roomy.clone()).unwrap();
    }
    topic.subscribe(tight.clone()).unwrap();
    assert_eq!(topic.subscribers(), 2);

    // Nothing runs until the test awaits: the third publish finds `tight` full and goes on without it.
    let reached: Vec<_> = (1..=3).map(|seq| topic.publish(note(seq))).collect();
    assert_eq!(reached, [2, 2, 1]);
    assert_eq!(missed(&system, "full"), 1);
    assert_eq!(system.stats().dead_letters, 1, "counted, as `runtime.dead_letters` exports it");
    let letter = &system.dead_letters().recent(1)[0];
    assert_eq!(letter.target.as_deref(), Some("notes"));
    assert_eq!(seen(&roomy).await, [1, 2, 3], "once each, however often subscribed");
    assert_eq!(seen(&tight).await, [1, 2]);

    // From a handler, to a topic its own actor is subscribed to with a full mailbox: it misses and goes on.
    let relay = Sub { topic: Some(topic.clone()), ..Sub::default() };
    let relay = system.spawn(relay).mailbox(small()).start().unwrap();
    topic.subscribe(relay.clone()).unwrap();
    relay.tell(Msg::Relay(5)).unwrap();
    tokio::time::sleep(Duration::from_millis(1)).await;
    let got = seen(&relay).await;
    assert!(!got.is_empty() && got.len() < 5 && got.iter().copied().eq(1..=got.len() as u64), "{got:?}");
    assert_eq!(missed(&system, "full"), 1 + 3 + (5 - got.len() as u64), "`tight` took two of five");
    assert_eq!(system.stats().dead_letters, missed(&system, "full"));
    assert_eq!(seen(&roomy).await, [1, 2, 3, 1, 2, 3, 4, 5]);
    system.shutdown().await.unwrap();
}

#[tokio::test(start_paused = true)]
async fn a_name_holds_one_type_and_its_topic_lasts_while_a_handle_or_a_subscriber_does() {
    let (system, _) = threadless();
    assert!(matches!(system.topic::<Msg>(""), Err(SystemError::Configuration(_))));
    let topic = system.topic::<Msg>("notes").unwrap();
    assert!(matches!(system.topic::<u64>("notes"), Err(SystemError::NameTaken(name)) if name == "notes"));

    // Topic names are apart from actor names.
    let sub = system.spawn(Sub::default()).name("notes").start().unwrap();
    topic.subscribe(sub.clone()).unwrap();
    drop(topic);
    let again = system.topic::<Msg>("notes").unwrap();
    assert_eq!(again.subscribers(), 1, "its subscriber held it");
    assert_eq!(again.publish(note(1)), 1);
    drop(again);
    assert!(system.topic::<u64>("notes").is_err());
    sub.lifecycle.stop();
    sub.join().await;
    let other = system.topic::<u64>("notes").unwrap();
    assert_eq!(other.subscribers(), 0, "with its last handle and subscriber gone, so was the topic");

    let one = system.topic::<Msg>("clones").unwrap();
    let two = one.clone();
    drop(one);
    assert!(system.topic::<u64>("clones").is_err(), "a clone holds it");
    drop(two);
    assert!(system.topic::<u64>("clones").is_ok());
    system.shutdown().await.unwrap();
}

#[tokio::test(flavor = "multi_thread")]
async fn a_subscriber_that_ends_is_dropped_before_its_join_returns_and_unsubscribe_takes_one_out() {
    let system = ActorSystem::builder().build().unwrap();
    let topic = system.topic::<Msg>("notes").unwrap();
    let subs: Vec<_> = (0..4).map(|_| system.spawn(Sub::default()).start().unwrap()).collect();
    for sub in &subs {
        topic.subscribe(sub.clone()).unwrap();
    }
    subs[0].tell(Msg::Crash).unwrap();
    subs[0].join().await;
    assert_eq!(topic.subscribers(), 3);
    subs[1].tell(Msg::Panic).unwrap();
    subs[1].join().await;
    assert_eq!(topic.subscribers(), 2);
    subs[2].lifecycle.stop();
    subs[2].join().await;
    assert_eq!(topic.subscribers(), 1);
    assert_eq!(topic.publish(note(1)), 1);
    assert_eq!(missed(&system, "closed"), 0, "no publish after a join finds its subscriber closed");

    topic.unsubscribe(&subs[3].lifecycle);
    topic.unsubscribe(&subs[3].lifecycle);
    assert_eq!((topic.subscribers(), topic.publish(note(2))), (0, 0));
    assert_eq!(seen(&subs[3]).await, [1]);
    topic.subscribe(subs[0].clone()).unwrap();
    assert_eq!(topic.subscribers(), 0, "one that ended already is dropped at once");
    system.shutdown().await.unwrap();
}

#[tokio::test(start_paused = true)]
async fn subscribers_past_the_limit_are_refused_and_one_leaving_makes_room() {
    let (system, _) = threadless();
    let topic = system.topic::<Msg>("crowd").unwrap();
    let crowd: Vec<_> = (0..Topic::<Msg>::MAX_SUBSCRIBERS)
        .map(|_| {
            let sub = system.spawn(Sub::default()).start().unwrap();
            topic.subscribe(sub.clone()).unwrap();
            sub
        })
        .collect();
    let late = system.spawn(Sub::default()).start().unwrap();
    assert!(matches!(topic.subscribe(late.clone()), Err(SystemError::SubscriberLimit)));
    topic.subscribe(crowd[0].clone()).expect("one already in is not refused");
    crowd[0].lifecycle.stop();
    crowd[0].join().await;
    topic.subscribe(late).unwrap();
    assert_eq!(topic.publish(note(1)), Topic::<Msg>::MAX_SUBSCRIBERS);
    system.shutdown().await.unwrap();
}

#[tokio::test(flavor = "multi_thread")]
async fn each_publishers_order_is_kept_at_every_subscriber_and_what_it_missed_is_counted() {
    let system = ActorSystem::builder().build().unwrap();
    let topic = system.topic::<Msg>("notes").unwrap();
    let limits = MailboxLimits { messages: 64, ..Default::default() };
    let subs: Vec<_> = (0..4).map(|_| system.spawn(Sub::default()).mailbox(limits).start().unwrap()).collect();
    for sub in &subs {
        sub.ready().await.unwrap();
        topic.subscribe(sub.clone()).unwrap();
    }
    let (publishers, per) = (4, 20_000);
    let tasks: Vec<_> = (0..publishers)
        .map(|from| {
            let topic = topic.clone();
            tokio::spawn(async move {
                let mut reached = 0;
                for seq in 1..=per {
                    reached += topic.publish(Note { from, seq });
                    if seq % 256 == 0 {
                        tokio::task::yield_now().await;
                    }
                }
                reached as u64
            })
        })
        .collect();
    let mut reached = 0;
    for task in tasks {
        reached += task.await.unwrap();
    }
    let mut got = 0;
    for sub in &subs {
        let notes = sub.ask(Msg::Seen).await.unwrap();
        got += notes.len() as u64;
        in_order(&notes, publishers);
    }
    assert_eq!(got, reached);
    assert_eq!(missed(&system, "full"), publishers * per * subs.len() as u64 - reached);
    system.shutdown().await.unwrap();
}

#[tokio::test(flavor = "multi_thread")]
async fn subscribers_starved_of_their_worker_miss_into_dead_letters_while_publishes_go_on_in_order() {
    let pool = PoolConfig { name: "subs".into(), ..Default::default() };
    let runtime = RuntimeConfig { pools: vec![pool], ..Default::default() };
    let system = ActorSystem::builder().runtime(runtime).build().unwrap();
    let topic = system.topic::<Msg>("notes").unwrap();
    let limits = MailboxLimits { messages: 64, ..Default::default() };
    let on_pool = || system.spawn(Sub::default()).mailbox(limits).execution(Execution::Pool("subs")).start();
    let subs: Vec<_> = (0..4).map(|_| on_pool().unwrap()).collect();
    for sub in &subs {
        sub.ready().await.unwrap();
        topic.subscribe(sub.clone()).unwrap();
    }
    // The pool's one worker spins in another actor's turn until released: the subscribers get no CPU meanwhile.
    let held = Arc::new(AtomicBool::new(false));
    let release = Release(held.clone());
    let hog = on_pool().unwrap();
    hog.tell(Msg::Hog(held.clone())).unwrap();
    while !held.load(SeqCst) {
        tokio::task::yield_now().await;
    }
    let (publishers, per) = (4, 1_000);
    let tasks: Vec<_> = (0..publishers)
        .map(|from| {
            let topic = topic.clone();
            tokio::spawn(async move { (1..=per).map(|seq| topic.publish(Note { from, seq }) as u64).sum::<u64>() })
        })
        .collect();
    let mut reached = 0;
    for task in tasks {
        let published = tokio::time::timeout(Duration::from_secs(10), task).await;
        reached += published.expect("a publish waited for a starved subscriber").unwrap();
    }
    drop(release);

    assert_eq!(reached, 64 * subs.len() as u64, "each subscriber took what its mailbox holds, and missed the rest");
    assert_eq!(missed(&system, "full"), publishers * per * subs.len() as u64 - reached);
    assert_eq!(system.stats().dead_letters, missed(&system, "full"));
    for sub in &subs {
        let notes = sub.ask(Msg::Seen).await.unwrap();
        assert_eq!(notes.len(), 64);
        in_order(&notes, publishers);
    }
    for sub in subs.iter().chain([&hog]) {
        sub.lifecycle.stop();
        sub.join().await;
    }
    assert_eq!(topic.subscribers(), 0);
    drop(topic);
    assert!(system.topic::<u64>("notes").is_ok(), "with its handle and subscribers gone, so was the topic");
    system.shutdown().await.unwrap();
}

#[tokio::test(start_paused = true)]
async fn a_supervised_subscriber_stays_subscribed_across_restarts_and_misses_only_between_generations() {
    let (system, _) = threadless();
    let topic = system.topic::<Msg>("notes").unwrap();
    // A restart a second away, on the threadless clock the test steps.
    let backoff = Backoff { initial: Duration::from_secs(1), jitter: 0.0, ..RestartPolicy::BACKOFF };
    let policy = RestartPolicy { backoff, ..Default::default() };
    let family = system.supervise(Sub::default).policy(policy).start_supervised().unwrap();
    family.ask(Msg::Seen).await.unwrap();
    topic.subscribe(family.clone()).unwrap();
    topic.subscribe(family.clone()).unwrap();
    assert_eq!(topic.publish(note(1)), 1);
    family.tell(Msg::Crash).unwrap();
    eventually("the generation ends", async || family.family.current().is_none()).await;
    assert_eq!(topic.publish(note(2)), 0, "between generations");
    assert_eq!(missed(&system, "not_ready"), 1);
    assert!(family.ask(Msg::Seen).await.unwrap().is_empty(), "the next generation starts fresh");
    assert_eq!(topic.publish(note(3)), 1);
    let seqs: Vec<_> = family.ask(Msg::Seen).await.unwrap().iter().map(|n| n.seq).collect();
    assert_eq!(seqs, [3]);
    assert_eq!(topic.subscribers(), 1);
    family.lifecycle.stop();
    family.join().await;
    assert_eq!(topic.subscribers(), 0, "the family ended for good");
    system.shutdown().await.unwrap();
}
