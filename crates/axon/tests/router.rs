use std::collections::BTreeMap;
use std::time::Duration;

use axon::*;

mod common;
use common::threadless;

enum Msg {
    Add(u64),
    Get(ReplyTo<u64>),
    Crash,
    Panic,
}

struct Counter(u64);

impl Actor for Counter {
    type Message = Msg;
    type Error = &'static str;

    fn on_message(&mut self, msg: Msg, _cx: &mut Context<'_, Self>) -> Result<(), Self::Error> {
        match msg {
            Msg::Add(n) => self.0 += n,
            Msg::Get(reply) => drop(reply.reply(self.0)),
            Msg::Crash => return Err("crash"),
            Msg::Panic => panic!("panic"),
        }
        Ok(())
    }
}

async fn members(system: &ActorSystem, n: usize, messages: usize) -> Vec<ActorHandle<Msg>> {
    let mut out = Vec::new();
    for _ in 0..n {
        let limits = MailboxLimits { messages, ..Default::default() };
        let member = system.spawn(Counter(0)).mailbox(limits).start().unwrap();
        member.ready().await.unwrap();
        out.push(member);
    }
    out
}

fn depths(members: &[ActorHandle<Msg>]) -> Vec<usize> {
    members.iter().map(|m| m.address.diagnostics().mailbox_depth).collect()
}

fn router(routing: Routing, members: &[ActorHandle<Msg>]) -> Router<Msg> {
    let router = Router::new(routing);
    for member in members {
        router.add(member.clone());
    }
    router
}

#[tokio::test(start_paused = true)]
async fn each_strategy_spreads_messages_as_it_says_and_a_full_member_refuses_with_full() {
    let (system, _) = threadless();
    let empty = Router::<Msg>::new(Routing::RoundRobin);
    assert!(matches!(empty.tell(Msg::Add(1)), Err(Reject { reason: RejectReason::NotReady, message: Msg::Add(1) })));
    assert!(matches!(empty.ask(Msg::Get).await, Err(RequestError::NotAdmitted(RejectReason::NotReady))));
    assert!(empty.member(&"k").is_none());
    // Neither holds the router's lock across its await.
    fn is_send<T: Send>(_: T) {}
    is_send(empty.send(Msg::Add(1)));
    is_send(empty.ask(Msg::Get));

    // Round-robin: each member in turn, and a full one refuses with the message back; no other member is tried.
    let pool = members(&system, 4, 25).await;
    let rr = router(Routing::RoundRobin, &pool);
    for _ in 0..100 {
        rr.tell(Msg::Add(1)).unwrap();
    }
    assert_eq!(depths(&pool), [25, 25, 25, 25]);
    assert!(matches!(rr.tell(Msg::Add(7)), Err(Reject { reason: RejectReason::Full, message: Msg::Add(7) })));
    for member in &pool {
        assert_eq!(member.ask(Msg::Get).await.unwrap(), 25);
    }

    // Least-loaded: the shortest mailboxes fill first.
    let pool = members(&system, 4, 64).await;
    for (member, queued) in pool.iter().zip([5, 2, 0, 0]) {
        for _ in 0..queued {
            member.tell(Msg::Add(0)).unwrap();
        }
    }
    let ll = router(Routing::LeastLoaded, &pool);
    for _ in 0..7 {
        ll.tell(Msg::Add(1)).unwrap();
    }
    assert_eq!(depths(&pool), [5, 3, 3, 3]);

    // Least-loaded over members that keep up: each tie goes to the next in turn, not to the first.
    let idle = members(&system, 4, 64).await;
    let ties = router(Routing::LeastLoaded, &idle);
    for _ in 0..8 {
        ties.tell(Msg::Add(1)).unwrap();
        counts(&idle).await;
    }
    assert_eq!(counts(&idle).await, [2, 2, 2, 2]);

    // Consistent hash: one key, one member; 4000 keys over 4 members about evenly.
    let mut owners = BTreeMap::new();
    for key in 0..4000u32 {
        let owner = ll.member(&key).unwrap().address.incarnation();
        assert_eq!(ll.member(&key).unwrap().address.incarnation(), owner);
        *owners.entry(owner).or_insert(0) += 1;
    }
    assert_eq!(owners.len(), 4);
    assert!(owners.values().all(|n| (850..1150).contains(n)), "{owners:?}");
    system.shutdown().await.unwrap();
}

#[tokio::test(flavor = "multi_thread")]
async fn a_member_that_ends_is_never_chosen_once_its_exit_is_delivered() {
    let system = ActorSystem::builder().build().unwrap();
    for (routing, end) in [(Routing::RoundRobin, Msg::Crash), (Routing::LeastLoaded, Msg::Panic)] {
        let pool = members(&system, 3, 1024).await;
        let router = router(routing, &pool);
        let clone = router.clone(); // shares the members
        assert_eq!(clone.members().len(), 3);
        pool[0].tell(end).unwrap();
        pool[0].join().await;
        // Dropped before the exit was visible: no tell after the join reaches the ended member or finds it Closed.
        let left: Vec<_> = clone.members().iter().map(|m| m.address.incarnation()).collect();
        assert_eq!(left, [pool[1].address.incarnation(), pool[2].address.incarnation()]);
        for _ in 0..1000 {
            router.tell(Msg::Add(1)).unwrap();
        }
        router.send(Msg::Add(1)).await.unwrap();
        assert!(router.ask(Msg::Get).await.unwrap() <= 1001);
        assert_eq!(counts(&pool[1..]).await.iter().sum::<u64>(), 1001);

        // A member can be added, and one added after it ended is dropped at once.
        let late = system.spawn(Counter(0)).start().unwrap();
        router.add(late.clone());
        assert_eq!(router.members().len(), 3);
        late.lifecycle.stop();
        late.join().await;
        assert_eq!(router.members().len(), 2);
        router.add(late);
        router.add(pool[0].clone());
        assert_eq!(router.members().len(), 2);
    }
    system.shutdown().await.unwrap();
}

#[tokio::test(start_paused = true)]
async fn a_member_stopping_or_draining_is_passed_over_before_it_ends_and_refuses_only_when_all_are() {
    let (system, _) = threadless();
    for routing in [Routing::RoundRobin, Routing::LeastLoaded] {
        let pool = members(&system, 3, 64).await;
        for _ in 0..3 {
            pool[1].tell(Msg::Add(0)).unwrap();
        }
        let router = router(routing, &pool);
        // Its queue only shrinks now, so least-loaded would take it every time.
        pool[0].lifecycle.drain(None);
        for _ in 0..10 {
            router.tell(Msg::Add(1)).unwrap();
        }
        let [drained, one, two] = depths(&pool)[..] else { unreachable!() };
        assert_eq!((drained, one + two), (0, 13));
        match routing {
            // The two open members share evenly: the one after the drained member does not take its turns too.
            Routing::RoundRobin => assert_eq!((one, two), (8, 5)),
            Routing::LeastLoaded => assert!(one.abs_diff(two) <= 1, "{one} {two}"),
        }
        assert_eq!(router.members().len(), 3, "not ended yet");
        pool[1].lifecycle.stop();
        pool[2].lifecycle.stop();
        assert!(matches!(router.tell(Msg::Add(1)), Err(Reject { reason: RejectReason::Closed, .. })));
    }
    system.shutdown().await.unwrap();
}

#[tokio::test(flavor = "multi_thread")]
async fn when_a_member_leaves_or_joins_only_the_keys_it_had_or_wins_move() {
    let system = ActorSystem::builder().build().unwrap();
    let pool = members(&system, 5, 1024).await;
    let router = router(Routing::RoundRobin, &pool);
    let owners = |router: &Router<Msg>| -> Vec<ActorIncarnation> {
        (0..2000u32).map(|key| router.member(&key).unwrap().address.incarnation()).collect()
    };
    let before = owners(&router);
    let gone = pool[2].address.incarnation();
    pool[2].lifecycle.stop();
    pool[2].join().await;
    let after = owners(&router);
    let mut moved = 0;
    for (b, a) in before.iter().zip(&after) {
        assert_ne!(*a, gone);
        if *b != gone {
            assert_eq!(a, b, "a key moved whose member stayed");
        } else {
            moved += 1;
        }
    }
    assert!((300..500).contains(&moved), "{moved}");

    let joined = system.spawn(Counter(0)).start().unwrap();
    let new = joined.address.incarnation();
    router.add(joined);
    let later = owners(&router);
    for (a, l) in after.iter().zip(&later) {
        assert!(l == a || *l == new, "a key moved to a member that was there already");
    }
    assert!(later.iter().filter(|l| **l == new).count() > 250);
    system.shutdown().await.unwrap();
}

#[tokio::test(flavor = "multi_thread")]
#[expect(clippy::disallowed_methods, reason = "the race needs real threads")]
async fn tells_from_several_threads_reach_each_member_in_turn_and_none_is_lost() {
    let system = ActorSystem::builder().build().unwrap();
    let pool = members(&system, 4, 1 << 16).await;
    for routing in [Routing::RoundRobin, Routing::LeastLoaded] {
        let router = router(routing, &pool);
        let before = counts(&pool).await;
        std::thread::scope(|scope| {
            for _ in 0..4 {
                let router = router.clone();
                scope.spawn(move || {
                    for _ in 0..10_000 {
                        router.tell(Msg::Add(1)).unwrap();
                    }
                });
            }
        });
        let after = counts(&pool).await;
        let got: Vec<u64> = after.iter().zip(&before).map(|(a, b)| a - b).collect();
        assert_eq!(got.iter().sum::<u64>(), 40_000);
        if routing == Routing::RoundRobin {
            assert_eq!(got, [10_000; 4]);
        }
    }
    system.shutdown().await.unwrap();
}

#[tokio::test(flavor = "multi_thread")]
#[expect(clippy::disallowed_methods, reason = "callers on real threads while the test churns members; no simulation")]
async fn members_ending_and_joining_under_tells_from_several_threads_neither_block_nor_linger() {
    let system = ActorSystem::builder().build().unwrap();
    let router = router(Routing::LeastLoaded, &members(&system, 4, 1 << 16).await);
    let stop = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
    let callers: Vec<_> = (0..4)
        .map(|_| {
            let (router, stop) = (router.clone(), stop.clone());
            tokio::task::spawn_blocking(move || {
                while !stop.load(std::sync::atomic::Ordering::Relaxed) {
                    // Closed from a member that is ending when every other one is too; nothing else.
                    if let Err(reject) = router.tell(Msg::Add(1)) {
                        assert_eq!(reject.reason, RejectReason::Closed);
                    }
                }
            })
        })
        .collect();
    // Exits publish on the workers, taking the router's write lock, while the callers hold its read lock.
    for round in 0..200 {
        router.add(members(&system, 1, 1 << 16).await.remove(0));
        let gone = router.members().remove(0);
        if round % 2 == 0 {
            gone.tell(Msg::Panic).unwrap();
        } else {
            gone.lifecycle.stop();
        }
        tokio::time::timeout(Duration::from_secs(10), gone.join()).await.unwrap();
        assert!(router.members().iter().all(|m| m.address.incarnation() != gone.address.incarnation()));
    }
    stop.store(true, std::sync::atomic::Ordering::Relaxed);
    for caller in callers {
        caller.await.unwrap();
    }
    assert_eq!(router.members().len(), 4);
    system.shutdown().await.unwrap();
}

async fn counts(pool: &[ActorHandle<Msg>]) -> Vec<u64> {
    let mut out = Vec::new();
    for member in pool {
        out.push(tokio::time::timeout(Duration::from_secs(10), member.ask(Msg::Get)).await.unwrap().unwrap());
    }
    out
}
