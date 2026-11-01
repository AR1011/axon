use super::*;

const SEC: Duration = Duration::from_secs(1);
const WS: Limit = Limit { name: "ws", max: 10, window: Duration::ZERO };
const REQ: Limit = Limit { name: "req/s", max: 8, window: SEC };
const HOUR: Limit = Limit { name: "weight/h", max: 1200, window: Duration::from_secs(3600) };
const HOLD: Duration = Duration::from_millis(4750);
const LEASE: Duration = Duration::from_secs(5);

fn at(ms: u64) -> Duration {
    Duration::from_millis(ms)
}

fn grants(answers: u64, gseq: u64, entries: Vec<(u64, u64, u64)>) -> Grants {
    Grants { incarnation: 1, answers, gseq, young: false, entries }
}

fn report(seq: u64, acked: u64, items: Vec<Item>) -> Report {
    Report { incarnation: 1, seq, replica: 1, acked, items }
}

/// A report to a replica at incarnation 2.
fn m2(seq: u64, acked: u64, items: Vec<Item>) -> Report {
    Report { replica: 2, ..report(seq, acked, items) }
}

fn permits(id: u64, held: u64, demand: u64) -> Item {
    Item { id, max: 10, usage: Reported::Permits(held), demand }
}

/// Entry 2, of a limit of 8 a second, in buckets of 125 ms from `phase`.
fn rate(phase: u64, buckets: [u64; BUCKETS], demand: u64) -> Item {
    Item { id: 2, max: 8, usage: Reported::Rate { width: at(125), phase: at(phase), buckets }, demand }
}

/// The node reports what is dirty to replica `r` over `link`, as `n`, and takes the answer in.
fn exchange(table: &Table, link: usize, r: &mut Replica, n: &Arc<str>, now: Duration) -> Grants {
    let items = table.dirty(now);
    let (seq, replica, acked) = table.send(link, now);
    let g = r.report(n, &Report { incarnation: 1, seq, replica, acked, items }, now).expect("a newer report");
    table.apply(link, &g, |_| true, now);
    g
}

/// The node reports to `replica`, at incarnation 1, and takes in its answer granting `entries`, where `replicates`.
fn answer(table: &Table, replica: &str, young: bool, entries: Vec<(u64, u64, u64)>, replicates: bool, now: Duration) {
    let link = table.link(replica, 1);
    let (seq, _, acked) = table.send(link, now);
    table.apply(link, &Grants { young, ..grants(seq, acked + 1, entries) }, |_| replicates, now);
}

fn replica(now: Duration) -> Replica {
    let mut r = Replica::new(1, LEASE, now);
    r.pulled();
    r
}

/// A view of `(node, incarnation)`, all in one region.
fn members(nodes: &[(&str, u128)]) -> Members {
    Members::new(7, Candidates::PER_REGION, nodes.iter().map(|(node, inc)| (*node, *inc, "")))
}

/// A view of `(node, region)`, each of incarnation 1.
fn view(nodes: &[(&str, &str)]) -> Members {
    Members::new(7, Candidates::PER_REGION, nodes.iter().map(|(node, region)| (*node, 1, *region)))
}

fn grant_of(g: &Grants, id: u64) -> Option<u64> {
    g.entries.iter().find(|e| e.0 == id).map(|e| e.1)
}

/// A node's link to one replica, with the replica: `send` reports and takes the answer in.
struct Pair {
    node: Arc<str>,
    link: Link,
    replica: Replica,
}

impl Pair {
    fn new(now: Duration) -> Self {
        let mut link = Link::default();
        link.retarget(1);
        Self { node: Arc::from("n"), link, replica: replica(now) }
    }

    /// The answer, and whether the node took it in.
    fn send(&mut self, items: Vec<Item>, now: Duration) -> (Grants, bool) {
        let (seq, replica, acked) = self.link.send(now);
        let report = Report { incarnation: 1, seq, replica, acked, items };
        let g = self.replica.report(&self.node, &report, now).expect("a newer report");
        let taken = self.link.accept(&g, true, now).is_some();
        if taken {
            self.link.commit(&g, true, HOLD, now);
        }
        (g, taken)
    }
}

#[test]
fn a_rate_reads_high_by_at_most_one_bucket_never_low() {
    let mut e = Entry::new(&REQ, at(1000), false);
    e.add(3, at(1010));
    assert_eq!(e.usage(at(1999)), 3, "inside the window");
    assert_eq!(e.usage(at(2124)), 3, "a bucket past the window: counted high, never low");
    assert_eq!(e.usage(at(2125)), 0);
    e.add(2, at(3000));
    assert_eq!(e.usage(at(60_000)), 0, "a long gap clears the ring");
}

#[test]
fn the_fallback_admits_ceil_of_the_limit_over_the_nodes_and_says_which_refusal() {
    let table = Table::new(7, HOLD, 1000);
    table.set_nodes(4);
    let now = at(10);
    for _ in 0..3 {
        table.take(b"k", &[(WS, 1)], now).unwrap();
    }
    let share = Denied::Share { limit: "ws", retry_after: REPORT_EVERY };
    assert_eq!(table.take(b"k", &[(WS, 1)], now), Err(share), "ceil(10 / 4) = 3");
    assert_eq!(table.take(b"k", &[(WS, 11)], now), Err(Denied::Limit { limit: "ws", retry_after: HEARTBEAT }));
    assert_eq!(table.take(b"j", &[(REQ, 1), (WS, 3)], now), Ok(()));
    assert!(table.take(b"j", &[(REQ, 1), (WS, 1)], now).is_err());
    assert_eq!(table.take(b"j", &[(REQ, 1)], now), Ok(()), "the refused call took nothing");
    assert_eq!(table.take(b"j", &[(REQ, 1)], now), Err(Denied::Share { limit: "req/s", retry_after: REPORT_EVERY }));
    assert_eq!(table.permits(), 6, "3 of k and 3 of j");
    table.release(table.id(&WS, b"k"), 3, now);
    assert_eq!(table.take(b"k", &[(WS, 2), (WS, 1)], now), Ok(()), "one limit twice counts both costs");
    assert!(table.take(b"k", &[(WS, 1)], now).is_err());
    table.release(table.id(&WS, b"k"), 9, now);
    table.release(table.id(&WS, b"j"), 3, now);
    assert_eq!(table.permits(), 0, "a release past what is held gives back what is");
}

#[test]
#[expect(clippy::disallowed_methods, reason = "a thread, so that a take locking a shard twice fails, not hangs")]
fn two_limits_in_one_shard_lock_it_once() {
    let table = Arc::new(Table::new(7, HOLD, 1000));
    let other = Limit { name: "other", ..WS };
    let key = (0u32..)
        .map(u32::to_be_bytes)
        .find(|k| Table::shard(table.id(&WS, k)) == Table::shard(table.id(&other, k)))
        .unwrap();
    let (done, taken) = std::sync::mpsc::sync_channel(1);
    let t = table.clone();
    std::thread::spawn(move || done.send(t.take(&key, &[(WS, 1), (other, 1)], at(0))));
    assert_eq!(taken.recv_timeout(Duration::from_secs(5)), Ok(Ok(())));
}

/// One of five nodes, linked to key `k`'s one replica `r` at incarnation 1, holding 1 of `k` on its grant of 4.
fn granted_one_of_four() -> Table {
    let table = Table::new(7, HOLD, 1000);
    table.set_nodes(5);
    table.set_key_replicas(1);
    table.take(b"k", &[(WS, 1)], at(0)).unwrap();
    answer(&table, "r", false, vec![(table.id(&WS, b"k"), 4, 0)], true, at(0));
    table
}

#[test]
#[expect(clippy::disallowed_methods, reason = "a thread, so that a check waiting on the links fails, not hangs")]
fn a_check_admits_on_its_grant_while_the_links_are_written() {
    let table = Arc::new(granted_one_of_four());
    let writing = table.links.write();
    let (done, taken) = std::sync::mpsc::sync_channel(1);
    let t = table.clone();
    std::thread::spawn(move || done.send(t.take(b"k", &[(WS, 3)], at(1))));
    let taken = taken.recv_timeout(Duration::from_secs(5));
    drop(writing);
    assert_eq!(taken, Ok(Ok(())), "4 on the grant, where the fallback allows 2");
}

#[test]
fn a_restart_the_view_shows_ends_the_old_grant_at_once() {
    for relink in [false, true] {
        let table = granted_one_of_four();
        let id = table.id(&WS, b"k");
        assert_eq!(table.take(b"k", &[(WS, 3)], at(1)), Ok(()), "4 on the grant, where the fallback allows 2");
        table.release(id, 4, at(2));
        match relink {
            true => table.relink(|_| Some(2)),
            false => _ = table.link("r", 2),
        }
        assert!(table.take(b"k", &[(WS, 3)], at(3)).is_err(), "relink {relink}: back on the fallback");
    }
}

#[test]
fn a_view_showing_a_replica_restarted_points_its_link_at_the_new_incarnation() {
    let table = Table::new(7, HOLD, 1000);
    let mut node = Node::new("n", 1, members(&[("n", 1), ("r", 1)]), &table, LEASE, at(0));
    let link = table.link("r", 1);
    node.see(&table, members(&[("n", 1), ("r", 2)]), at(10));
    assert_eq!(table.send(link, at(10)).1, 2, "what r's predecessor granted no longer counts");
}

#[test]
fn a_node_sweeps_every_shard_within_sweep_every_however_long_its_cycles() {
    for cycle in [100, 400] {
        let table = Table::new(7, HOLD, 1000);
        let mut node = Node::new("r", 1, members(&[("r", 1)]), &table, LEASE, at(0));
        for k in 0..1000u32 {
            table.take(&k.to_be_bytes(), &[(REQ, 1)], at(0)).unwrap();
        }
        let mut now = at(0);
        while now < KEEP {
            node.round(&table, now);
            now += at(cycle);
        }
        assert_eq!(table.len(), 1000, "cycles of {cycle} ms: none before KEEP");
        while now < KEEP + SWEEP_EVERY + at(cycle) {
            node.round(&table, now);
            now += at(cycle);
        }
        assert_eq!(table.len(), 0, "cycles of {cycle} ms: every shard swept");
    }
}

/// Takes `n` keys of `limit` from `keys` in every shard.
fn fill_each_shard(table: &Table, limit: Limit, keys: std::ops::RangeFrom<u32>, n: usize, now: Duration) {
    let mut filled = [0; SHARDS];
    for k in keys {
        let s = Table::shard(table.id(&limit, &k.to_be_bytes()));
        if filled[s] < n {
            filled[s] += 1;
            table.take(&k.to_be_bytes(), &[(limit, 1)], now).unwrap();
        }
        if filled.iter().all(|f| *f == n) {
            return;
        }
    }
}

fn shard_lens(table: &Table) -> Vec<usize> {
    table.shards.iter().map(|s| s.lock().entries.len()).collect()
}

#[test]
fn a_node_sweeps_a_share_of_every_shard_each_cycle_and_starts_a_walk_again_after_a_pause() {
    let table = Table::new(7, HOLD, 4 * SLICE * SHARDS);
    let mut node = Node::new("r", 1, members(&[("r", 1)]), &table, LEASE, at(0));
    fill_each_shard(&table, REQ, 0.., 2 * SLICE, at(1));
    fill_each_shard(&table, WS, 0.., 2 * SLICE, at(1));
    node.round(&table, at(100));
    node.round(&table, KEEP);
    assert_eq!(
        table.len(),
        4 * SLICE * SHARDS,
        "after a pause, one walk ends the one under way: none unused for KEEP yet"
    );
    node.round(&table, KEEP + SWEEP_EVERY / 4);
    assert_eq!(shard_lens(&table), [3 * SLICE; SHARDS], "a quarter of the way: half of each shard's unused gone");
    node.round(&table, KEEP + SWEEP_EVERY);
    assert_eq!(shard_lens(&table), [2 * SLICE; SHARDS], "the rest of the way, the held permits kept");
}

#[test]
fn an_eviction_during_a_sweep_moves_its_place_back_so_it_skips_no_entry() {
    let table = Table::new(7, HOLD, 16 * SHARDS);
    fill_each_shard(&table, REQ, 0.., 8, at(0));
    fill_each_shard(&table, REQ, 1 << 20.., 8, at(1000));
    table.sweep(SWEEP_EVERY / 2, KEEP - at(5000));
    assert_eq!(table.len(), 16 * SHARDS, "half way through, none unused for KEEP yet");
    fill_each_shard(&table, REQ, 2 << 20.., 1, KEEP - at(4000));
    assert_eq!(table.len(), 15 * SHARDS, "two of the oldest evicted from each full shard, both passed");
    table.sweep(SWEEP_EVERY, KEEP + at(2000));
    assert_eq!(shard_lens(&table), [7; SHARDS], "the rest of the way: the 8 newer idle ones go, the passed stay");
}

#[test]
fn a_node_admits_against_the_smallest_fresh_grant_of_a_majority_of_replicas_not_young() {
    let (mut links, mut e): ([Link; 3], _) = (Default::default(), Entry::new(&WS, at(0), false));
    let apply = |links: &mut [Link; 3], e: &mut Entry, r: usize, g: Grants| {
        if let Some(epoch) = links[r].accept(&g, true, Duration::ZERO) {
            g.entries.iter().for_each(|(id, v, u)| assert_eq!(*id, 1, "{v} {u}"));
            e.set_grant(r, epoch, g.entries[0].1, g.entries[0].2);
            links[r].commit(&g, true, HOLD, Duration::ZERO);
        }
    };
    let sent = at(1000);
    for link in &mut links {
        link.retarget(1);
        assert_eq!(link.send(sent), (1, 1, 0));
    }
    let late = at(3000);
    apply(&mut links, &mut e, 0, grants(1, 1, vec![(1, 6, 2)]));
    apply(&mut links, &mut e, 1, grants(1, 1, vec![(1, 4, 3)]));
    apply(&mut links, &mut e, 2, Grants { young: true, ..grants(1, 1, vec![(1, 1, 0)]) });
    assert_eq!(e.bound(&fresh(&links), late, 5, 2), Bound::Grant { value: 4, used: 3 }, "the young one is ignored");
    assert_eq!(e.bound(&fresh(&links), late, 5, 3), Bound::Fallback(2), "fewer than a majority of the replicas");
    assert_eq!(e.bound(&fresh(&links), sent + HOLD - at(1), 5, 2), Bound::Grant { value: 4, used: 3 });
    assert_eq!(
        e.bound(&fresh(&links), sent + HOLD, 5, 2),
        Bound::Fallback(2),
        "a grant holds from when the report left"
    );
    links[0].send(at(2000));
    apply(&mut links, &mut e, 0, grants(1, 2, vec![(1, 6, 2)]));
    assert_eq!(
        e.bound(&fresh(&links), sent + HOLD, 5, 1),
        Bound::Fallback(2),
        "from the report it answers, not a later one"
    );

    apply(&mut links, &mut e, 1, grants(1, 1, vec![(1, 0, 0)]));
    assert_eq!(e.bound(&fresh(&links), late, 5, 2), Bound::Grant { value: 4, used: 3 }, "a duplicate is old news");
    links[1].retarget(2);
    assert_eq!(
        e.bound(&fresh(&links), late, 5, 2),
        Bound::Fallback(2),
        "the view shows a restart: the old grant is gone"
    );
    apply(&mut links, &mut e, 1, Grants { incarnation: 2, ..grants(1, 1, vec![(1, 9, 0)]) });
    assert_eq!(e.bound(&fresh(&links), late, 5, 2), Bound::Grant { value: 6, used: 2 }, "the new incarnation's counts");
    apply(&mut links, &mut e, 1, grants(1, 2, vec![(1, 0, 0)]));
    assert_eq!(
        e.bound(&fresh(&links), late, 5, 2),
        Bound::Grant { value: 6, used: 2 },
        "a late one of its predecessor not"
    );
    assert_eq!(links[1].send(late), (2, 2, 1), "a report acknowledges the grants of the replica it has now");
    links[2].retarget(0);
    apply(&mut links, &mut e, 2, Grants { incarnation: 0, ..grants(1, 1, vec![(1, 5, 0)]) });
    assert_eq!(
        e.bound(&fresh(&links), late, 5, 3),
        Bound::Grant { value: 5, used: 2 },
        "an incarnation the view shows counts, though its clock stepped back"
    );
}

#[test]
fn a_report_carries_what_moved_by_an_eighth_of_the_grant_and_the_cost_refused() {
    let mut e = Entry::new(&WS, at(0), false);
    assert!(e.report(1, at(0)).is_some(), "a new key");
    assert_eq!(e.report(1, at(0)), None);
    e.set_grant(0, 0, 16, 0);
    e.add(1, at(1));
    assert_eq!(e.report(1, at(1)), None, "moved by less than 16 / 8");
    e.add(1, at(2));
    assert_eq!(e.report(1, at(2)), Some(Item { id: 1, max: 10, usage: Reported::Permits(2), demand: 0 }));
    let bound = Bound::Grant { value: 2, used: 2 };
    assert_eq!(e.check(5, bound, at(3)), Err(Denied::Share { limit: "ws", retry_after: HEARTBEAT }));
    assert_eq!(e.report(1, at(3)), Some(Item { id: 1, max: 10, usage: Reported::Permits(2), demand: 5 }));
    let full = Bound::Grant { value: 2, used: 9 };
    assert!(matches!(e.check(2, full, at(4)), Err(Denied::Limit { .. })), "the replicas count 9 of 10 in use");
    e.report(1, at(4));
    e.add(1, at(4) + REFRESH - at(1));
    e.report(1, at(4));
    e.release(1, at(4) + REFRESH);
    assert_eq!(e.report(1, at(4) + REFRESH).map(|i| i.usage), Some(Reported::Permits(2)), "not reported for long");

    let mut r = Entry::new(&REQ, at(1000), false);
    r.add(3, at(1010));
    r.report(2, at(1010));
    r.add(1, at(1260));
    let item = r.report(2, at(1260)).unwrap();
    let buckets = [0, 0, 0, 0, 0, 0, 3, 0, 1];
    let usage = Reported::Rate { width: at(125), phase: at(10), buckets };
    assert_eq!(item, Item { id: 2, max: 8, usage, demand: 0 }, "nothing refused");
    let refused = r.check(1, Bound::Grant { value: 4, used: 4 }, at(1260));
    let drained = at(125 - 10 + 6 * 125);
    assert_eq!(refused, Err(Denied::Share { limit: "req/s", retry_after: drained }), "until 3 units leave the window");
    let refused = r.check(4, Bound::Grant { value: 4, used: 4 }, at(1260));
    assert_eq!(
        refused,
        Err(Denied::Share { limit: "req/s", retry_after: HEARTBEAT }),
        "until all of them did, or later"
    );
    let refused = r.check(8, Bound::Grant { value: 8, used: 5 }, at(1260));
    assert_eq!(refused, Err(Denied::Limit { limit: "req/s", retry_after: at(115) }), "until the next bucket");
}

#[test]
fn eviction_drops_the_least_recently_used_and_refuses_them_until_a_majority_grants() {
    let table = Table::new(7, HOLD, SHARDS);
    let keys: Vec<Vec<u8>> = (0u32..).map(|i| i.to_be_bytes().to_vec()).take(4000).collect();
    let shard = |k: &[u8]| Table::shard(table.id(&WS, k));
    let same: Vec<&Vec<u8>> = keys.iter().filter(|k| shard(k) == shard(&keys[0])).take(3).collect();
    let hourly = keys.iter().find(|k| Table::shard(table.id(&HOUR, k)) == shard(&keys[0])).unwrap();
    table.take(same[0], &[(WS, 1)], at(0)).unwrap();
    table.release(table.id(&WS, same[0]), 1, at(0));
    table.take(hourly, &[(HOUR, 1)], at(0)).unwrap();
    table.take(same[1], &[(WS, 1)], at(1)).unwrap();
    table.take(same[2], &[(WS, 1)], at(2)).unwrap();
    let (a, b) = (table.id(&WS, same[0]), table.id(&WS, same[1]));
    assert!(table.evicted.contains(a), "the oldest without usage went");
    assert!(!table.evicted.contains(b), "one holding permits stays");
    assert!(!table.evicted.contains(table.id(&HOUR, hourly)), "and one with units in the window");
    let refused = table.take(same[0], &[(WS, 1)], at(3));
    assert_eq!(refused, Err(Denied::Share { limit: "ws", retry_after: REPORT_EVERY }), "no second fallback");

    table.set_key_replicas(3);
    let grant = |r| answer(&table, r, false, vec![(a, 5, 0)], true, at(3));
    grant("r0");
    assert!(table.take(same[0], &[(WS, 1)], at(4)).is_err(), "one grant of three is not a majority");
    grant("r1");
    assert_eq!(table.take(same[0], &[(WS, 1)], at(4)), Ok(()), "until a majority grants");
    assert_eq!(table.take(same[0], &[(WS, 1)], at(3) + HOLD), Ok(()), "then, once they lapse, the fallback");

    let evicted = Evicted::new();
    for id in 0..(EVICTED_BITS / 16) as u64 {
        evicted.insert(id);
    }
    assert!(evicted.contains(0));
    evicted.insert(u64::MAX);
    assert!(evicted.contains(0) && evicted.contains(u64::MAX), "the older generation still answers");
    for id in 0..(EVICTED_BITS / 16) as u64 {
        evicted.insert(id + (EVICTED_BITS / 2) as u64);
    }
    assert!(!evicted.contains(0), "until the newer one fills");
}

#[test]
fn entries_go_a_minute_after_their_last_use_unless_in_use() {
    let table = Table::new(7, HOLD, 1000);
    table.take(b"a", &[(WS, 1)], at(0)).unwrap();
    table.take(b"b", &[(REQ, 1)], at(0)).unwrap();
    table.charge(b"c", &[(REQ, 100)], at(0));
    table.take(b"d", &[(HOUR, 1000)], at(0)).unwrap();
    table.sweep(SWEEP_EVERY, KEEP - at(1));
    assert_eq!(table.len(), 4);
    table.sweep(SWEEP_EVERY, KEEP);
    assert_eq!(table.len(), 2, "the permit is still held, the hour's units still in the window");
    let mut items = table.dirty(KEEP);
    items.sort_by_key(|i| i.max);
    assert_eq!(items.len(), 2);
    assert_eq!(items[0].usage, Reported::Permits(1));
    assert!(matches!(items[1].usage, Reported::Rate { buckets, .. } if buckets.iter().sum::<u64>() == 1000));
}

#[test]
fn a_node_that_dropped_an_entry_gets_its_grant_again_when_it_reports_it() {
    let table = Table::new(7, HOLD, 1000);
    table.set_nodes(5);
    table.set_key_replicas(1);
    let (n, mut r, link) = (Arc::from("n"), replica(at(0)), table.link("r", 1));
    let mut round = |now| drop(exchange(&table, link, &mut r, &n, now));
    table.take(b"k", &[(WS, 1)], at(0)).unwrap();
    round(at(0));
    table.release(table.id(&WS, b"k"), 1, at(1));
    assert_eq!(table.take(b"k", &[(WS, 10)], at(2)), Ok(()), "the whole limit");
    table.release(table.id(&WS, b"k"), 10, at(3));
    for s in 1..=61 {
        round(at(1000 * s));
    }
    table.sweep(SWEEP_EVERY, at(61_000));
    assert_eq!(table.len(), 0);
    table.take(b"k", &[(WS, 1)], at(61_100)).unwrap();
    round(at(61_100));
    assert_eq!(table.take(b"k", &[(WS, 9)], at(61_200)), Ok(()), "granted again, though its row stayed");
}

#[test]
fn a_report_starts_where_the_last_ran_out_so_that_churn_starves_no_entry() {
    let table = Table::new(7, HOLD, 2 * MAX_ITEMS);
    let keys: Vec<[u8; 4]> = (0..MAX_ITEMS as u32 + 10).map(u32::to_be_bytes).collect();
    for k in &keys {
        table.take(k, &[(WS, 1)], at(0)).unwrap();
    }
    let first: BTreeSet<u64> = table.dirty(at(0)).iter().map(|i| i.id).collect();
    for k in keys.iter().filter(|k| first.contains(&table.id(&WS, *k))) {
        table.take(k, &[(WS, 1)], at(1)).unwrap();
    }
    let second: BTreeSet<u64> = table.dirty(at(1)).iter().map(|i| i.id).collect();
    assert_eq!(second.len(), MAX_ITEMS);
    let mut left = keys.iter().map(|k| table.id(&WS, k)).filter(|id| !first.contains(id));
    assert!(left.all(|id| second.contains(&id)), "the entries the first report left out go before the churn");
    assert_eq!(table.dirty(at(1)).len(), 10);
    assert_eq!(table.dirty(at(1)).len(), 0);
}

#[test]
fn grants_taken_in_while_the_view_retargets_their_link_acknowledge_nothing() {
    let table = Table::new(7, HOLD, 1000);
    let link = table.link("r", 1);
    let (seq, _, acked) = table.send(link, at(0));
    table.apply(link, &grants(seq, acked + 9, vec![(1, 4, 0)]), |_| table.link("r", 2) == link, at(0));
    assert_eq!(table.send(link, at(1)), (2, 2, 0), "the new incarnation's numbering starts over");
}

#[test]
fn only_the_grants_of_a_keys_current_replicas_count() {
    let table = Table::new(7, HOLD, 1000);
    table.set_nodes(5);
    table.set_key_replicas(3);
    let id = table.id(&WS, b"k");
    let grant = |r, value, replicates| answer(&table, r, false, vec![(id, value, 0)], replicates, at(0));
    table.take(b"k", &[(WS, 1)], at(0)).unwrap();
    grant("r0", 4, true);
    grant("r2", 4, true);
    assert_eq!(table.take(b"k", &[(WS, 3)], at(1)), Ok(()), "a majority granted 4");
    table.release(table.id(&WS, b"k"), 3, at(1));
    table.forget(|_, replica| replica != "r2");
    assert!(table.take(b"k", &[(WS, 3)], at(2)).is_err(), "the view moved the key off r2: one grant is too few");
    grant("r2", 0, false);
    grant("r1", 4, true);
    assert_eq!(table.take(b"k", &[(WS, 3)], at(3)), Ok(()), "and a later grant of r2's is not taken in");
}

#[test]
fn grants_give_every_row_that_asks_one_level_within_its_usage_and_demand_remainders_to_the_first() {
    assert_eq!(targets(10, &[(2, Some(9)), (0, Some(9))]), vec![5, 5], "one level");
    assert_eq!(targets(3, &[(1, Some(5)), (0, Some(5))]), vec![2, 1], "a remainder goes to the first");
    assert_eq!(targets(4, &[(0, Some(1)), (0, Some(9)), (0, Some(9))]), vec![1, 2, 1], "that is below its need");
    assert_eq!(targets(10, &[(7, Some(1)), (0, Some(9))]), vec![5, 5], "a row over the level gets the level");
    assert_eq!(targets(10, &[(0, Some(2)), (0, Some(20))]), vec![2, 8], "never above usage and demand");
    assert_eq!(targets(10, &[(2, Some(1)), (0, Some(3))]), vec![5, 5], "what no row needs, evenly");
    assert_eq!(targets(9, &[(0, Some(0)), (0, Some(0)), (0, Some(0))]), vec![3, 3, 3], "evenly without demand");
    assert_eq!(targets(9, &[(0, Some(0)), (0, Some(5)), (4, None)]), vec![0, 5, 0], "none for a row not to grant");
    assert_eq!(targets(3, &[(5, Some(1)), (0, Some(1))]), vec![2, 1], "over the limit, still one level");
    assert_eq!(targets(10, &[(0, Some(0)), (0, Some(1))]), vec![5, 5], "once those that ask have their need, evenly");
    assert_eq!(targets(10, &[(0, Some(0)), (0, Some(20))]), vec![0, 10], "until then, to those that ask");
    let (one, other) = ([(1, Some(20)), (3, Some(15)), (2, Some(30))], [(4, Some(12)), (0, Some(25)), (6, Some(9))]);
    assert_eq!(targets(30, &one), targets(30, &other), "replicas that saw other moves below and above the level");
    for max in 0..20 {
        let rows = [(1, Some(3)), (0, Some(1)), (2, Some(7)), (0, None)];
        let t = targets(max, &rows);
        assert_eq!(t.iter().sum::<u64>(), max, "{t:?}");
    }
}

#[test]
fn a_report_carries_the_demand_of_the_last_heartbeat_not_only_that_since_the_last_report() {
    let mut e = Entry::new(&WS, at(0), false);
    e.report(1, at(0));
    e.set_grant(0, 0, 2, 0);
    e.add(2, at(10));
    assert!(e.check(1, Bound::Grant { value: 2, used: 2 }, at(20)).is_err());
    assert_eq!(e.report(1, at(20)).map(|i| i.demand), Some(1));
    e.release(1, at(120));
    let item = e.report(1, at(120));
    assert_eq!(item.map(|i| i.demand), Some(1), "a report cycle without a refusal: replicas would cap it at usage");
    assert_eq!(e.report(1, at(1119)), None, "not before a heartbeat");
    assert_eq!(e.report(1, at(1120)).map(|i| i.demand), Some(1), "this heartbeat's refusals count in the next");
    assert_eq!(e.report(1, at(2120)).map(|i| i.demand), Some(0), "it stopped asking: replicas hear so");
    assert_eq!(e.report(1, at(4120)), None);
}

#[test]
fn an_entry_that_asked_asks_while_in_use_so_that_a_key_with_room_does_not_refuse_it_again() {
    // m acquires a permit of `k` and drops it every 100 ms; n asks for more than the key has all along; the key's
    // one replica hears both. m's usage reads 0 of 10: new to the key, it takes the fallback's floor over its grant of
    // 0 and asks for it, and once that got it a grant it must not be refused when the floor ends.
    let (table, mut r, n) = (Table::new(7, HOLD, 1000), replica(at(0)), Arc::from("n"));
    table.set_nodes(2);
    table.set_key_replicas(1);
    let (link, id) = (table.link("r", 1), table.id(&WS, b"k"));
    let (mut n_seq, mut n_acked, mut refused) = (0, 0, Vec::new());
    for t in (0..5000).step_by(100) {
        match table.take(b"k", &[(WS, 1)], at(t)) {
            Ok(()) => table.release(id, 1, at(t)),
            Err(_) => refused.push(t),
        }
        exchange(&table, link, &mut r, &Arc::from("m"), at(t));
        n_seq += 1;
        n_acked = r.report(&n, &report(n_seq, n_acked, vec![permits(id, 0, 20)]), at(t)).unwrap().gseq;
    }
    assert!(refused.is_empty(), "refused at {refused:?}");
}

#[test]
fn a_node_that_uses_a_key_now_and_then_is_not_refused_while_another_uses_it_steadily_and_it_has_room() {
    // m and n acquire a permit of `k` and drop it every 100 ms for a second; then m goes on, and n acquires one
    // only every 2550 ms: more than two heartbeats apart, so it no longer asks when it comes back.
    let mut r = replica(at(0));
    let nodes = ["m", "n"].map(|name| {
        let table = Table::new(7, HOLD, 1000);
        table.set_nodes(2);
        table.set_key_replicas(1);
        let link = table.link("r", 1);
        (Arc::<str>::from(name), table, link)
    });
    let id = nodes[0].1.id(&WS, b"k");
    let mut refused = Vec::new();
    for t in (0..40_000).step_by(50) {
        for (name, table, link) in &nodes {
            let steady = t % 100 == 0 && (&**name == "m" || t < 1000);
            if steady || (&**name == "n" && t >= 3000 && (t - 3000) % 2550 == 0) {
                match table.take(b"k", &[(WS, 1)], at(t)) {
                    Ok(()) => table.release(id, 1, at(t)),
                    Err(_) => refused.push((name.clone(), t)),
                }
            }
            if t % 100 == 0 {
                exchange(table, *link, &mut r, name, at(t));
            }
        }
    }
    assert!(refused.iter().all(|(_, t)| *t < 1000), "refused at {refused:?}");
}

#[test]
fn a_node_asks_for_the_most_it_holds_at_once_not_for_what_it_took() {
    // Refused once, then holds `steady` and acquires and drops `burst` more one by one every 5 ms, past two
    // heartbeats: it asks for the burst beyond what it holds.
    for (steady, burst) in [(0, 1), (10, 3)] {
        let mut e = Entry::new(&WS, at(0), false);
        assert!(e.check(1, Bound::Grant { value: 0, used: 0 }, at(0)).is_err());
        let grant = Bound::Grant { value: steady + burst, used: 0 };
        for _ in 0..steady {
            assert_eq!(e.check(1, grant, at(5)), Ok(false));
            e.add(1, at(5));
        }
        for t in (10..2500).step_by(5) {
            for _ in 0..burst {
                assert_eq!(e.check(1, grant, at(t)), Ok(false));
                e.add(1, at(t));
            }
            e.release(burst, at(t));
        }
        let item = e.report(1, at(2500)).expect("it asked");
        assert_eq!((item.usage, item.demand), (Reported::Permits(steady), burst));
        // 49 such rows beside one that holds 1000 of 2000 and wants all of it: they must not keep its share.
        let rows: Vec<(u64, Option<u64>)> =
            std::iter::once((1000, Some(1000))).chain(std::iter::repeat_n((steady, Some(burst)), 49)).collect();
        assert_eq!(targets(2000, &rows)[0], 2000 - 49 * (steady + burst));
    }
}

#[test]
fn a_take_refused_by_a_sibling_limit_adds_no_demand_to_the_limit_that_admitted_it() {
    const B: Limit = Limit { name: "b", max: 2, window: Duration::ZERO };
    let table = Table::new(7, HOLD, 1000);
    table.set_nodes(2);
    assert!(table.take(b"k", &[(WS, 6)], at(0)).is_err());
    assert!(table.take(b"k", &[(WS, 5), (B, 2)], at(1000)).is_err());
    let id = table.id(&WS, b"k");
    let item = table.dirty(at(1000)).into_iter().find(|i| i.id == id).expect("reported");
    assert_eq!(item.demand, 6);
}

#[test]
fn demand_counts_two_heartbeats_after_the_report_that_carried_it() {
    let (n, m) = (Arc::from("n"), Arc::from("m"));
    let mut r = replica(at(0));
    r.report(&n, &report(1, 0, vec![permits(1, 0, 4)]), at(0)).unwrap();
    r.report(&m, &report(1, 0, vec![permits(1, 0, 4)]), at(1000)).unwrap();
    r.report(&n, &report(2, 0, vec![]), at(1900)).unwrap();
    let plan = |t| r.plan(1, at(t)).iter().map(|p| (p.1.1, p.2)).collect::<Vec<_>>();
    assert_eq!(plan(1999), vec![(Some(4), 5), (Some(4), 5)]);
    assert_eq!(plan(2000), vec![(Some(4), 7), (Some(0), 3)], "n's report that it stopped asking was lost");
}

#[test]
fn a_raise_counts_when_sent_and_a_lower_once_acknowledged() {
    let (n, m) = (Arc::from("n"), Arc::from("m"));
    let mut r = replica(at(0));
    let g = r.report(&n, &report(1, 0, vec![permits(1, 0, 4)]), at(0)).unwrap();
    assert_eq!(grant_of(&g, 1), Some(10), "all of it: nobody else asks");
    let n1 = g.gseq;
    let g = r.report(&m, &report(1, 0, vec![permits(1, 0, 4)]), at(1)).unwrap();
    assert_eq!(grant_of(&g, 1), Some(0), "n's grant counts until it acknowledges a lower");
    let m1 = g.gseq;
    let g = r.report(&n, &report(2, 0, vec![]), at(2)).unwrap();
    assert_eq!(grant_of(&g, 1), Some(5), "the lower, sent again until acknowledged");
    let n2 = g.gseq;
    let g = r.report(&m, &report(2, m1, vec![]), at(3)).unwrap();
    assert_eq!(grant_of(&g, 1), None, "acknowledged, and nothing new: n has not acknowledged");
    assert_eq!(r.report(&n, &report(2, n2, vec![]), at(4)), None, "an old report is not answered");
    let g = r.report(&n, &report(3, n1, vec![]), at(4)).unwrap();
    assert_eq!(grant_of(&g, 1), Some(5), "an acknowledgement of the grants before the lower");
    let g = r.report(&n, &report(4, n2, vec![]), at(4)).unwrap();
    assert_eq!(grant_of(&g, 1), None);
    let g = r.report(&m, &report(3, m1, vec![permits(1, 0, 4)]), at(5)).unwrap();
    assert_eq!(grant_of(&g, 1), Some(5), "raised once n's lower was acknowledged");
    assert_eq!(g.entries[0].2, 0, "no usage yet");
}

#[test]
fn a_lower_another_row_waits_for_is_pushed_and_its_acknowledgement_gives_the_room_at_once() {
    let (n, m): (Arc<str>, Arc<str>) = (Arc::from("n"), Arc::from("m"));
    let (pn, pm) = ((n.clone(), 1), (m.clone(), 1));
    let mut r = replica(at(0));
    let g = r.report(&n, &report(1, 0, vec![permits(1, 0, 4)]), at(0)).unwrap();
    assert_eq!(grant_of(&g, 1), Some(10), "all of it: nobody else asks");
    assert!(r.pushes(at(100)).is_empty(), "nobody waits for it");
    let g = r.report(&m, &report(1, 0, vec![permits(1, 0, 4)]), at(100)).unwrap();
    assert_eq!(grant_of(&g, 1), Some(0), "n's grant counts until it acknowledges a lower");
    let pushes = r.pushes(at(100));
    let [(to, push)] = &pushes[..] else { panic!("one push, to n: {pushes:?}") };
    assert_eq!((to, push.answers, grant_of(push, 1)), (&pn, 1, Some(5)), "the lower, answering n's last report");
    r.ack(&pn, 1, push.gseq, at(110));
    let pushes = r.pushes(at(200));
    let [(to, push)] = &pushes[..] else { panic!("one push, to m: {pushes:?}") };
    assert_eq!(
        (to, push.answers, grant_of(push, 1)),
        (&pm, 1, Some(5)),
        "the room n handed back, not at m's next report"
    );
    assert!(r.pushes(at(300)).is_empty(), "each push went once");
}

#[test]
fn a_push_is_taken_in_only_where_it_answers_one_of_the_last_two_reports() {
    let table = granted_one_of_four();
    let (id, link) = (table.id(&WS, b"k"), table.link("r", 1));
    for _ in 0..2 {
        table.send(link, at(1));
    }
    // r heard report 1 alone, as where this node's reports do not reach it, and lowers a row that ages.
    let declined = table.pushed("r", &grants(1, 2, vec![(id, 0, 0)]), |_| true, at(2000));
    assert_eq!(declined, None, "not taken in, nor acknowledged");
    assert_eq!(table.take(b"k", &[(WS, 3)], at(2000)), Ok(()), "still on the grant of 4");
    table.release(id, 3, at(2000));
    assert_eq!(table.pushed("r", &grants(2, 2, vec![(id, 1, 0)]), |_| true, at(2000)), None, "report 3 not heard");
    assert!(table.take(b"k", &[(WS, 1)], at(2000)).is_err(), "one held, on a grant of 1");
}

#[test]
fn a_push_is_acknowledged_only_where_the_replica_heard_every_change_of_its_entries() {
    let table = granted_one_of_four();
    let (id, link) = (table.id(&WS, b"k"), table.link("r", 1));
    // The permit held was taken after report 1: freed on this acknowledgement, its room would go to another node.
    let lower = |answers, gseq| grants(answers, gseq, vec![(id, 0, 0)]);
    assert_eq!(table.pushed("r", &lower(1, 2), |_| true, at(10)), None, "a take not reported");
    assert_eq!(table.dirty(at(20)).len(), 1);
    let (seq, ..) = table.send(link, at(20));
    assert_eq!(table.pushed("r", &lower(seq - 1, 3), |_| true, at(30)), None, "a report the replica has not heard");
    assert_eq!(table.pushed("r", &lower(seq, 4), |_| true, at(40)), Some((1, 4)), "every change heard");
}

#[test]
fn usage_within_the_limit_can_become_a_grant_and_usage_over_it_cannot() {
    let (n, m) = (Arc::from("n"), Arc::from("m"));
    let mut r = replica(at(0));
    let g = r.report(&n, &report(1, 0, vec![permits(1, 1, 1)]), at(0)).unwrap();
    assert_eq!(grant_of(&g, 1), Some(10), "the first grant after a take on the fallback is all of it");
    let g = r.report(&m, &report(1, 0, vec![permits(1, 1, 1)]), at(1)).unwrap();
    assert_eq!(grant_of(&g, 1), Some(0), "n may hold all 10, so m's 1 is over the limit: it drains");
}

#[test]
fn rows_raised_at_once_share_the_room() {
    let (a, b, c) = (Arc::from("a"), Arc::from("b"), Arc::from("c"));
    let mut r = replica(at(0));
    let g = r.report(&a, &report(1, 0, vec![permits(1, 0, 1)]), at(0)).unwrap();
    assert_eq!(grant_of(&g, 1), Some(10));
    r.report(&b, &report(1, 0, vec![permits(1, 0, 1)]), at(1)).unwrap();
    let g = r.report(&a, &report(2, 0, vec![]), at(2)).unwrap();
    assert_eq!(grant_of(&g, 1), Some(5));
    r.report(&a, &report(3, g.gseq, vec![]), at(3)).unwrap();
    r.report(&c, &report(1, 0, vec![permits(1, 0, 1)]), at(4)).unwrap();
    let plan: Vec<u64> = r.plan(1, at(4)).iter().map(|p| p.2).collect();
    assert_eq!(plan, vec![4, 3, 3]);
    let held: u64 = r.keys[&1].rows.values().map(Row::held).sum();
    assert_eq!(held, 10, "a holds 5 until it acknowledges its lower to 4: b and c share the other 5");
}

#[test]
fn a_row_over_its_share_is_planned_the_level_and_the_others_keep_theirs() {
    // n and m share 10, 5 each; then m holds 8, taken on the fallback while it heard too few replicas, and n's
    // permits drop to 0. Floored at usage, n was lowered to 2 as m's excess drained, and to 0 had the key's usage
    // reached its limit without it: a node that was not faulted refused, in the World.
    let (m, n) = (Arc::from("m"), Arc::from("n"));
    let mut r = replica(at(0));
    let g = r.report(&n, &report(1, 0, vec![permits(1, 5, 5)]), at(0)).unwrap();
    assert_eq!(grant_of(&g, 1), Some(10));
    r.report(&m, &report(1, 0, vec![permits(1, 0, 5)]), at(1)).unwrap();
    let g = r.report(&n, &report(2, g.gseq, vec![]), at(2)).unwrap();
    assert_eq!(grant_of(&g, 1), Some(5), "lowered to the level at once");
    let n_acked = r.report(&n, &report(3, g.gseq, vec![]), at(3)).unwrap().gseq;
    let g = r.report(&m, &report(2, 0, vec![permits(1, 0, 5)]), at(4)).unwrap();
    assert_eq!(grant_of(&g, 1), Some(5), "raised once n acknowledged its lower");
    r.report(&m, &report(3, g.gseq, vec![permits(1, 8, 5)]), at(5)).unwrap();
    let g = r.report(&n, &report(4, n_acked, vec![permits(1, 0, 5)]), at(6)).unwrap();
    assert_eq!(grant_of(&g, 1), Some(5), "n keeps its share while m's excess drains");
    let plan: Vec<u64> = r.plan(1, at(6)).iter().map(|p| p.2).collect();
    assert_eq!(plan, vec![5, 5], "m holds 8, and admits again once it holds fewer than the level");
}

#[test]
fn room_short_of_the_raises_is_split_fairly_not_handed_out_in_node_order() {
    let [a, b, c, s, t]: [Arc<str>; 5] = ["a", "b", "c", "s", "t"].map(Arc::from);
    let mut r = replica(at(0));
    let asking = || vec![permits(1, 0, 20)];
    let g = r.report(&s, &report(1, 0, asking()), at(0)).unwrap();
    r.report(&t, &report(1, 0, asking()), at(1)).unwrap();
    let g = r.report(&s, &report(2, g.gseq, vec![]), at(2)).unwrap();
    r.report(&s, &report(3, g.gseq, asking()), at(3)).unwrap();
    let grants = |r: &Replica| r.keys[&1].rows.values().map(|row| row.grant).collect::<Vec<u64>>();
    assert_eq!(grants(&r), vec![5, 5], "s acknowledged its lower to 5, and t took the other 5");
    for (i, n) in [&a, &b, &c].into_iter().enumerate() {
        r.report(n, &report(1, 0, asking()), at(4 + i as u64)).unwrap();
    }
    assert_eq!(grants(&r), vec![0, 0, 0, 2, 2], "s and t may hold 5 each until they acknowledge their lowers");
    let g = r.report(&s, &report(4, g.gseq, vec![]), at(10)).unwrap();
    r.report(&s, &report(5, g.gseq, asking()), at(11)).unwrap();
    assert_eq!(
        grants(&r),
        vec![1, 1, 1, 2, 2],
        "the 3 s gave back go one each: in node order c, last, would get none while t holds its 5"
    );
}

#[test]
fn a_lower_that_moves_again_before_each_acknowledgement_still_frees_what_the_node_no_longer_holds() {
    let (a, b) = (Arc::from("a"), Arc::from("b"));
    let mut r = replica(at(0));
    let g = r.report(&a, &report(1, 0, vec![permits(1, 0, 10)]), at(0)).unwrap();
    assert_eq!(grant_of(&g, 1), Some(10));
    let mut acked = g.gseq;
    for i in 1..=6 {
        // b's usage moves a's grant between 4 and 5 after each answer to a, before a acknowledges it.
        r.report(&b, &report(i, 0, vec![permits(1, 5 + i % 2, 10)]), at(10 * i)).unwrap();
        acked = r.report(&a, &report(i + 1, acked, vec![]), at(10 * i + 5)).unwrap().gseq;
    }
    let held = r.keys[&1].rows[&(a.clone(), 1)].held();
    assert!(held <= 5, "a took in grants of 4 and 5 after its 10, so the 10 is gone: it counts {held}");
}

#[test]
fn a_silent_nodes_grant_is_freed_a_lease_after_it_was_heard_and_starts_from_zero_when_it_returns() {
    let (n, m) = (Arc::from("n"), Arc::from("m"));
    let mut r = replica(at(0));
    r.report(&n, &report(1, 0, vec![permits(1, 2, 4)]), at(0)).unwrap();
    let g = r.report(&m, &report(1, 0, vec![permits(1, 0, 4)]), at(4999)).unwrap();
    assert_eq!(grant_of(&g, 1), Some(0), "n may hold all 10");
    let g = r.report(&m, &report(2, g.gseq, vec![permits(1, 0, 4)]), at(5000)).unwrap();
    assert_eq!(grant_of(&g, 1), Some(8), "freed but for the 2 it reported in use");
    let g = r.report(&n, &report(2, 1, vec![]), at(6000)).unwrap();
    assert_eq!(grant_of(&g, 1), Some(2), "back: the 2 it holds, and m holds the rest of the room");
    let plan: Vec<u64> = r.plan(1, at(6000)).iter().map(|p| p.2).collect();
    assert_eq!(plan, vec![6, 4], "where m's grant goes once it acknowledges the lower: n's demand is 6 s old");
}

#[test]
fn a_node_the_replica_forgot_takes_its_grants_in_when_it_returns() {
    let mut pair = Pair::new(at(0));
    for i in 0..50 {
        assert!(pair.send(vec![], at(100 * i)).1);
    }
    for s in 5..=30 {
        pair.replica.tick(at(1000 * s));
    }
    assert!(pair.replica.peers.is_empty(), "silent 15 s, with no rows");
    let (g, taken) = pair.send(vec![permits(1, 0, 1)], at(30_100));
    assert!(taken && grant_of(&g, 1) == Some(10), "{g:?}");
}

#[test]
fn an_answer_carries_at_most_max_items_and_the_rest_follow_once_it_is_acknowledged() {
    let mut pair = Pair::new(at(0));
    let items: Vec<Item> = (0..6000).map(|id| permits(id, 0, 1)).collect();
    let (seq, replica, acked) = pair.link.send(at(0));
    let lost = Report { incarnation: 1, seq, replica, acked, items: items[..3000].to_vec() };
    pair.replica.report(&pair.node, &lost, at(0)).unwrap();
    let drain = |pair: &mut Pair, mut items: Vec<Item>, mut now: Duration| {
        let mut carried = BTreeSet::new();
        loop {
            let (g, taken) = pair.send(std::mem::take(&mut items), now);
            assert!(taken && g.entries.len() <= MAX_ITEMS, "{} entries", g.entries.len());
            if g.entries.is_empty() {
                return (carried, now);
            }
            carried.extend(g.entries.iter().map(|e| e.0));
            now += REPORT_EVERY;
        }
    };
    let (carried, now) = drain(&mut pair, items[3000..].to_vec(), at(100));
    assert_eq!(carried.len(), 6000);
    let (carried, _) = drain(&mut pair, vec![], now + LEASE);
    assert_eq!(carried.len(), 6000, "after a lease of silence, every grant freed and given again");
}

#[test]
fn a_node_behind_gets_at_most_resend_grants_it_has_not_acknowledged_a_message_and_every_one_in_the_end() {
    let mut pair = Pair::new(at(0));
    let keys = 3 * RESEND as u64 + 5;
    let (seq, replica, acked) = pair.link.send(at(0));
    let items = (0..keys).map(|id| permits(id, 0, 1)).collect();
    pair.replica.report(&pair.node, &Report { incarnation: 1, seq, replica, acked, items }, at(0)).unwrap();
    let (mut carried, mut now) = (BTreeSet::new(), at(100));
    loop {
        let (g, taken) = pair.send(vec![], now);
        assert!(taken && g.entries.len() <= RESEND, "{} entries", g.entries.len());
        if g.entries.is_empty() {
            break;
        }
        carried.extend(g.entries.iter().map(|e| e.0));
        now += REPORT_EVERY;
    }
    assert_eq!((carried.len(), now), (keys as usize, at(500)), "the lost answer's grants, each once, in four answers");
    assert!(pair.replica.peers.values().all(|p| p.pending.is_empty()), "and each acknowledged");
}

#[test]
fn a_lower_left_out_of_a_message_still_counts_the_grant_before_it_however_many_are_acknowledged() {
    let (n, m) = (Arc::from("n"), Arc::from("m"));
    let key = 2 * RESEND as u64;
    let all: Vec<Item> = (0..=key).map(|id| permits(id, 0, 10)).collect();
    let mut r = replica(at(0));
    let g = r.report(&n, &report(1, 0, all.clone()), at(0)).unwrap();
    assert_eq!(grant_of(&g, key), Some(10));
    r.report(&m, &report(1, 0, all), at(1)).unwrap();
    // Each of n's grants is lowered to 5: an answer carries `RESEND` of them as new, and `RESEND` more as not
    // acknowledged, so the key, last, waits for the third.
    let mut acked = g.gseq;
    for seq in 2..=3 {
        let g = r.report(&n, &report(seq, acked, vec![]), at(seq)).unwrap();
        assert_eq!(grant_of(&g, key), (seq == 3).then_some(5), "the lower to 5 waits for room in an answer");
        assert_eq!(r.keys[&key].rows[&(n.clone(), 1)].held(), 10, "n may hold 10 until it acknowledges the lower");
        acked = g.gseq;
    }
    r.report(&n, &report(4, acked, vec![]), at(4)).unwrap();
    assert_eq!(r.keys[&key].rows[&(n, 1)].held(), 5);
}

#[test]
fn a_push_carries_only_grants_set_since_the_last_message_and_a_report_acknowledges_the_last_answer() {
    let (n, m): (Arc<str>, Arc<str>) = (Arc::from("n"), Arc::from("m"));
    let mut r = replica(at(0));
    // n never takes this answer in: both grants go again in answers, at n's pace, not in pushes.
    r.report(&n, &report(1, 0, vec![permits(1, 0, 10), permits(2, 0, 10)]), at(0)).unwrap();
    r.report(&m, &report(1, 0, vec![permits(1, 0, 10)]), at(100)).unwrap();
    let pushes = r.pushes(at(100));
    let [(to, push)] = &pushes[..] else { panic!("one push, to n: {pushes:?}") };
    let ids: Vec<u64> = push.entries.iter().map(|e| e.0).collect();
    assert_eq!((to, ids), (&(n, 1), vec![1]), "the lower of key 1 alone");
    let mut link = Link::default();
    link.retarget(1);
    link.send(at(0));
    link.commit(&grants(1, 1, vec![]), true, HOLD, at(10));
    link.commit(&grants(1, 2, vec![]), false, HOLD, at(20));
    assert_eq!(link.send(at(30)).2, 1, "a report acknowledges the last answer, and a push's reply the push");
}

#[test]
fn permit_rows_go_after_a_silence_or_at_once_when_retired_and_departed_rates_compact() {
    let (n, m) = (Arc::from("n"), Arc::from("m"));
    let mut r = replica(at(0));
    r.report(&n, &report(1, 0, vec![permits(1, 3, 1), rate(0, [0, 0, 0, 0, 0, 0, 0, 0, 2], 1)]), at(0)).unwrap();
    r.report(&m, &report(1, 0, vec![permits(1, 1, 1), rate(100, [0, 0, 0, 0, 0, 0, 0, 1, 1], 1)]), at(0)).unwrap();
    let used = |r: &Replica, id, now| r.keys.get(&id).map_or(0, |k| k.used(now));
    assert_eq!((used(&r, 1, at(0)), used(&r, 2, at(0))), (4, 4));
    assert_eq!(used(&r, 2, at(900)), 3, "m's older bucket left the window");
    r.tick(at(1000));
    r.retire(|node, incarnation| (node, incarnation) == ("m", 1), at(1000));
    assert_eq!(used(&r, 1, at(1000)), 3, "m's permits went with it");
    assert_eq!(used(&r, 2, at(1000)), 3, "its rate usage stays until it leaves the window");
    let (n2, n3) = ((Arc::from("n"), 2), (Arc::from("n"), 3));
    for (peer, phase) in [(&n2, 3), (&n3, 7)] {
        let from = Report { incarnation: peer.1, ..report(1, 0, vec![rate(phase, [1; BUCKETS], 1)]) };
        r.report(&peer.0, &from, at(1000));
        r.retire(|node, incarnation| (node, incarnation) == (&*peer.0, peer.1), at(1000));
    }
    let departed = &r.keys[&2].rows[&departed()];
    let Used::Rate(drops) = &departed.used else { panic!() };
    assert_eq!(drops.len(), BUCKETS, "three processes, a drop per bucket");
    assert_eq!(r.keys[&2].rows.len(), 2);
    for s in 1..=15 {
        r.tick(at(1000 * s));
    }
    assert_eq!(used(&r, 1, at(15_000)), 0, "n was silent for 15 s: its permits are dropped");
    assert!(r.keys.is_empty() && r.peers.is_empty(), "nothing left");
}

#[test]
fn a_permit_row_its_node_no_longer_refreshes_goes_though_the_node_still_heartbeats() {
    let table = Table::new(7, HOLD, 1000);
    table.take(b"k", &[(WS, 2)], at(0)).unwrap();
    assert_eq!(table.dirty(at(0)).len(), 1);
    assert!(table.dirty(REFRESH - at(1)).is_empty());
    assert_eq!(table.dirty(REFRESH)[0].usage, Reported::Permits(2), "held untouched, reported again");
    // The node's view moved the key elsewhere: it heartbeats here without it.
    let n = Arc::from("n");
    let mut r = replica(at(0));
    let mut acked = r.report(&n, &report(1, 0, vec![permits(1, 2, 0)]), at(0)).unwrap().gseq;
    for s in 1..=ROW_KEEP.as_secs() + 3 {
        r.tick(at(1000 * s));
        assert_eq!(r.keys.is_empty(), s > ROW_KEEP.as_secs() + 1, "at {s} s");
        acked = r.report(&n, &report(s + 1, acked, vec![]), at(1000 * s)).unwrap().gseq;
    }
}

#[test]
fn a_view_showing_a_later_incarnation_retires_every_earlier_one_even_one_it_never_showed() {
    let n = Arc::from("n");
    let mut r = replica(at(0));
    for incarnation in [1, 2, 4] {
        r.report(&n, &Report { incarnation, ..report(1, 0, vec![permits(1, 3, 0)]) }, at(0)).unwrap();
    }
    // The view went from n#1 to n#3 while n#2 lived and died unseen; n#4 is ahead of it.
    r.see("r", &members(&[("n", 1), ("m", 1)]), &members(&[("n", 3), ("m", 1)]), at(10));
    assert_eq!(r.keys[&1].used(at(10)), 3, "only n#4's permits count");
    assert_eq!(r.peers.keys().map(|p| p.1).collect::<Vec<_>>(), vec![4]);
    r.see("r", &members(&[("n", 3), ("m", 1)]), &members(&[("n", 3)]), at(20));
    assert_eq!(r.keys[&1].used(at(20)), 3, "m left; n#4 is later than the n#3 the view showed");
    r.see("r", &members(&[("n", 4)]), &members(&[]), at(30));
    assert!(r.peers.is_empty(), "n#4 left");
}

#[test]
fn a_replica_the_view_hands_keys_is_young_for_those_alone_until_it_pulled() {
    let (four, three) =
        (view(&[("a", "r0"), ("b", "r0"), ("c", "r1"), ("d", "r2")]), view(&[("a", "r0"), ("c", "r1"), ("d", "r2")]));
    let serving = |node| (0..).find(|id| four.candidates.serves(*id, node)).unwrap();
    let (kept, handed) = (serving("a"), serving("b"));
    let n = Arc::from("n");
    let mut r = replica(at(0));
    let g = r.report(&n, &report(1, 0, vec![permits(kept, 0, 1), permits(handed, 0, 1)]), at(0)).unwrap();
    assert!(!r.see("a", &three, &four, at(10)), "b joined: a only gives keys away");
    assert!(r.see("a", &four, &three, at(20)), "b left: a takes its keys, and pulls");
    assert!(r.pulling() && !r.starting(), "a handover's pull leaves /readyz green");
    assert!(!r.see("a", &three, &three, at(25)), "nothing moved");
    let g = r.report(&n, &report(2, g.gseq, vec![permits(kept, 1, 1), permits(handed, 1, 1)]), at(30)).unwrap();
    assert!(!g.young, "young for b's keys alone");
    assert_eq!((grant_of(&g, kept), grant_of(&g, handed)), (Some(10), None), "{g:?}");
    let (page, _) = r.pull(0, 9, |_| true, at(30));
    let mut other = replica(at(0));
    other.see("a", &four, &three, at(30));
    other.absorb(page, &three, at(30));
    assert!(other.keys.keys().eq([&handed]), "a page is taken in for the keys handed over alone");
    r.pulled();
    let g = r.report(&n, &report(3, g.gseq, vec![]), at(40)).unwrap();
    assert_eq!(grant_of(&g, handed), Some(10), "pulled: granted again");
}

#[test]
fn a_young_replica_grants_nothing_until_it_pulled_the_rows_of_the_others() {
    let (n, m) = (Arc::from("n"), Arc::from("m"));
    let mut old = replica(at(0));
    old.report(&n, &report(1, 0, vec![permits(1, 1, 1)]), at(0)).unwrap();
    old.report(&m, &report(1, 0, vec![rate(0, [1; BUCKETS], 1)]), at(0)).unwrap();

    let mut new = Replica::new(2, LEASE, at(500));
    assert!(new.starting(), "a new process holds /readyz until it pulled");
    let g = new.report(&m, &m2(1, 0, vec![permits(1, 0, 9)]), at(500)).unwrap();
    assert!(g.young && g.entries.iter().all(|e| e.1 == 0), "{g:?}");
    let (page, next) = old.pull(0, 1, |_| true, at(100));
    assert_eq!((page.len(), next), (1, Some(2)), "a page ends between keys");
    let view = members(&[("n", 1), ("m", 1)]);
    new.absorb(page, &view, at(600));
    let (page, next) = old.pull(2, 1, |_| true, at(100));
    assert_eq!((page.len(), next), (1, None));
    new.absorb(page, &view, at(600));
    assert!(old.pull(0, 9, |id| id == 3, at(100)).0.is_empty());
    new.pulled();
    let (last, used) = (at(1125 - 100 + 600), |t| new.keys[&2].used(t));
    assert_eq!((used(last - at(1)), used(last)), (1, 0), "rate drops move to the puller's clock");
    assert!(!new.report(&m, &m2(2, 1, vec![]), at(700)).unwrap().young);
    let g = new.report(&m, &m2(3, 1, vec![permits(1, 0, 9)]), at(800)).unwrap();
    assert_eq!(grant_of(&g, 1), Some(0), "n may hold all 10 the other replica let it");
    let g = new.report(&n, &Report { replica: 1, ..report(1, 9, vec![permits(1, 1, 0)]) }, at(900)).unwrap();
    assert_eq!(grant_of(&g, 1), Some(1));
    let to_n = g.gseq;
    let g = new.report(&m, &m2(4, 1, vec![permits(1, 0, 9)]), at(950)).unwrap();
    assert_eq!(grant_of(&g, 1), Some(0), "an acknowledgement of the predecessor's grants frees nothing");
    new.report(&n, &m2(2, to_n, vec![]), at(1000)).unwrap();
    let g = new.report(&m, &m2(5, 1, vec![permits(1, 0, 9)]), at(1100)).unwrap();
    assert_eq!(grant_of(&g, 1), Some(9), "once n acknowledged a grant of this replica");
}

#[test]
fn a_pulled_hold_ends_once_the_node_reported_the_key_here_and_acknowledged_an_answer_after() {
    let (n, m) = (Arc::from("n"), Arc::from("m"));
    let pulled = || {
        let mut old = replica(at(0));
        old.report(&n, &report(1, 0, vec![permits(1, 0, 0)]), at(0)).unwrap();
        let g = old.report(&m, &report(1, 0, vec![permits(1, 0, 10)]), at(50)).unwrap();
        assert_eq!(grant_of(&g, 1), Some(0), "n was lowered to 0 but has not acknowledged it");
        let mut new = Replica::new(2, LEASE, at(100));
        new.absorb(old.pull(0, 9, |_| true, at(100)).0, &members(&[("n", 1), ("m", 1)]), at(100));
        new.pulled();
        let g = new.report(&m, &m2(1, 0, vec![permits(1, 0, 10)]), at(200)).unwrap();
        assert_eq!(grant_of(&g, 1), Some(0), "n may hold all 10 the other replica let it");
        new
    };
    // n's view lags: it leaves the key out of its reports and of the answers it takes in, and may still admit on
    // the grant of the replica this one replaces.
    let mut new = pulled();
    let mut acked = 0;
    for seq in 1..=3 {
        let g = new.report(&n, &m2(seq, acked, vec![]), at(200 + 100 * seq)).unwrap();
        assert_eq!(grant_of(&g, 1), Some(0), "answers carry the key n does not report");
        acked = g.gseq;
    }
    let g = new.report(&m, &m2(2, 0, vec![permits(1, 0, 10)]), at(600)).unwrap();
    assert_eq!(grant_of(&g, 1), Some(0), "acknowledged answers without the key end nothing");
    let g = new.report(&n, &m2(4, acked, vec![permits(1, 0, 0)]), at(700)).unwrap();
    new.report(&n, &m2(5, g.gseq, vec![]), at(800)).unwrap();
    let g = new.report(&m, &m2(3, 0, vec![permits(1, 0, 10)]), at(900)).unwrap();
    assert_eq!(grant_of(&g, 1), Some(10), "n reported the key here, then acknowledged an answer");

    // n dropped the key: its heartbeats end the hold once it would have reported the key.
    let mut new = pulled();
    let mut acked = 0;
    for (seq, s) in (1..).zip(1..=122) {
        acked = new.report(&n, &m2(seq, acked, vec![]), at(1000 * s)).unwrap().gseq;
        new.tick(at(1000 * s));
        if s == 119 {
            let g = new.report(&m, &m2(2, 0, vec![permits(1, 0, 10)]), at(1000 * s)).unwrap();
            assert_eq!(grant_of(&g, 1), Some(0), "held until ROW_KEEP after the pull");
        }
    }
    let g = new.report(&m, &m2(3, 0, vec![permits(1, 0, 10)]), at(122_500)).unwrap();
    assert_eq!(grant_of(&g, 1), Some(10));
}

#[test]
fn a_pulled_hold_adds_to_the_row_of_a_node_that_reported_before_the_page_came() {
    let (n, m) = (Arc::from("n"), Arc::from("m"));
    let mut old = replica(at(0));
    let g = old.report(&n, &report(1, 0, vec![permits(1, 0, 4)]), at(0)).unwrap();
    assert_eq!(grant_of(&g, 1), Some(10));
    let mut new = Replica::new(2, LEASE, at(100));
    for peer in [&n, &m] {
        assert!(new.report(peer, &m2(1, 0, vec![permits(1, 0, 0)]), at(100)).unwrap().young);
    }
    new.absorb(old.pull(0, 9, |_| true, at(100)).0, &members(&[("n", 1), ("m", 1)]), at(150));
    new.pulled();
    let g = new.report(&m, &m2(2, 0, vec![permits(1, 0, 5)]), at(200)).unwrap();
    assert_eq!(grant_of(&g, 1), Some(0), "n may hold the 10 the other replica gave it");
}

#[test]
fn pulled_rows_of_an_incarnation_the_view_shows_a_later_one_of_are_retired_as_they_arrive() {
    let (n, j) = (Arc::from("n"), Arc::from("j"));
    let mut old = replica(at(0));
    old.report(&n, &report(1, 0, vec![permits(1, 3, 0)]), at(0)).unwrap();
    old.report(&n, &report(2, 0, vec![rate(0, [1; BUCKETS], 0)]), at(0)).unwrap();
    old.report(&j, &Report { incarnation: 2, ..report(1, 0, vec![permits(1, 2, 0)]) }, at(0)).unwrap();
    let mut new = Replica::new(2, LEASE, at(100));
    // n restarted; j#2 is later than the j#1 the view shows, and may be up.
    new.absorb(old.pull(0, 9, |_| true, at(100)).0, &members(&[("n", 2), ("j", 1), ("m", 1)]), at(100));
    new.pulled();
    assert!(
        new.keys[&2].rows.iter().map(|(peer, _)| peer).eq([&departed()]) && new.keys[&2].used(at(100)) > 0,
        "a rate stays a window"
    );
    assert_eq!(new.keys[&1].used(at(100)), 2, "n#1's permits went with it, j#2's stay");
}

#[test]
fn a_node_reports_again_what_a_replica_may_have_missed() {
    fn exchange(table: &Table, r: &mut Replica, items: Vec<Item>, now: Duration) {
        let (seq, replica, acked) = table.send(0, now);
        let report = Report { incarnation: 1, seq, replica, acked, items };
        table.apply(0, &r.report(&Arc::from("n"), &report, now).expect("a newer report"), |_| true, now);
    }
    let (table, mut r) = (Table::new(7, HOLD, 1000), replica(at(0)));
    table.link("r", 1);
    table.take(b"k", &[(WS, 1)], at(0)).unwrap();
    exchange(&table, &mut r, table.dirty(at(0)), at(0));
    table.release(table.id(&WS, b"k"), 1, at(100));
    table.take(b"j", &[(WS, 1)], at(100)).unwrap();
    assert_eq!(table.dirty(at(100)).len(), 2);
    table.send(0, at(100));
    exchange(&table, &mut r, table.dirty(at(200)), at(200));
    assert!(table.dirty(at(1099)).is_empty(), "the answer may still come");
    let mut again = table.dirty(at(1100));
    again.sort_by_key(|i| i.id != table.id(&WS, b"k"));
    let lost = [(table.id(&WS, b"k"), Reported::Permits(0)), (table.id(&WS, b"j"), Reported::Permits(1))];
    let got: Vec<_> = again.iter().map(|i| (i.id, i.usage.clone())).collect();
    assert_eq!(got, lost, "the lost report: what the replica granted, and what nothing granted");
    exchange(&table, &mut r, again, at(1100));
    r.tick(at(3000));
    exchange(&table, &mut r, table.dirty(at(3000)), at(3000));
    assert!(table.dirty(at(3100)).is_empty());
    exchange(&table, &mut r, vec![], at(4200));
    assert_eq!(table.dirty(at(4300)).len(), 2, "the replica woke from a pause and may have missed a report");
}

#[test]
fn an_answer_that_comes_after_a_later_one_has_nothing_reported_again() {
    let (table, mut r) = (Table::new(7, HOLD, 1000), replica(at(0)));
    table.link("r", 1);
    table.take(b"k", &[(WS, 1)], at(0)).unwrap();
    let mut answers = Vec::new();
    for now in [at(0), at(100)] {
        let items = table.dirty(now);
        let (seq, replica, acked) = table.send(0, now);
        answers.push(r.report(&Arc::from("n"), &Report { incarnation: 1, seq, replica, acked, items }, now).unwrap());
    }
    // Under load, the task that takes the second answer in can run first.
    table.apply(0, &answers[1], |_| true, at(200));
    table.apply(0, &answers[0], |_| true, at(200));
    assert!(table.dirty(at(5000)).is_empty(), "the replica had both reports");
}

#[test]
fn a_replica_that_stops_answering_has_nothing_reported_again_until_it_answers() {
    // A hold of a minute, so that the grant has not lapsed when the replica answers again.
    let (table, n): (_, Arc<str>) = (Table::new(7, 60 * SEC, 1000), Arc::from("n"));
    let mut r = Replica::new(1, 60 * SEC, at(0));
    r.pulled();
    table.set_key_replicas(1);
    table.link("r", 1);
    table.take(b"k", &[(WS, 1)], at(0)).unwrap();
    exchange(&table, 0, &mut r, &n, at(0));
    // Down for longer than SENT_KEPT reports: a report each cycle, none answered.
    for t in 1..=100 {
        assert!(table.dirty(at(t * 100)).is_empty(), "at {} ms, nothing changed and the replica is silent", t * 100);
        table.send(0, at(t * 100));
    }
    exchange(&table, 0, &mut r, &n, at(10_100));
    assert!(!table.dirty(at(10_200)).is_empty(), "back, the replica hears again what it may have missed");
}

#[test]
fn an_answer_from_another_incarnation_leaves_its_report_to_be_lost() {
    let (table, n): (_, Arc<str>) = (Table::new(7, HOLD, 1000), Arc::from("n"));
    let (mut old, mut new) = (replica(at(0)), Replica::new(2, LEASE, at(0)));
    new.pulled();
    table.link("r", 2);
    table.take(b"k", &[(WS, 1)], at(0)).unwrap();
    // The view shows the restart, but the old incarnation's session takes the report that carries k.
    for (now, r) in [(at(0), &mut old), (at(100), &mut new)] {
        exchange(&table, 0, r, &n, now);
    }
    assert!(!table.dirty(at(0) + HEARTBEAT).is_empty(), "the new incarnation hears of k");
}

#[test]
fn a_grant_that_lapsed_counts_no_more_after_a_message_that_leaves_it_out() {
    let (table, mut r, n): (_, _, Arc<str>) = (Table::new(7, HOLD, 10_000), replica(at(0)), Arc::from("n"));
    table.set_key_replicas(1);
    table.link("r", 1);
    let keys: Vec<Vec<u8>> = (0..3 * RESEND).map(|i| format!("k{i}").into_bytes()).collect();
    for key in &keys {
        table.take(key, &[(WS, 1)], at(0)).unwrap();
    }
    exchange(&table, 0, &mut r, &n, at(0));
    assert!(matches!(table.bound(&keys[0], &WS, at(100)), Bound::Grant { .. }));
    // Silent for a lease: the node's grants lapsed, and the replica freed them. The answer to its next report carries
    // a bounded share of them, and the grants it leaves out must not count again until it carries them.
    table.take(b"other", &[(WS, 1)], LEASE).unwrap();
    let g = exchange(&table, 0, &mut r, &n, LEASE);
    let left = keys.iter().find(|k| grant_of(&g, table.id(&WS, k)).is_none()).expect("a key left out");
    assert!(matches!(table.bound(left, &WS, LEASE + at(100)), Bound::Fallback(_)), "the replica freed its grant");
}

#[test]
fn a_key_a_replica_answered_young_is_reported_again_once_it_is_not() {
    let table = Table::new(7, HOLD, 1000);
    table.set_key_replicas(3);
    let id = table.id(&WS, b"k");
    table.take(b"k", &[(WS, 1)], at(0)).unwrap();
    for r in ["r0", "r1", "r2"] {
        table.link(r, 1);
    }
    assert_eq!(table.dirty(at(0)).len(), 1);
    let reply = |r, young, entries| answer(&table, r, young, entries, true, at(0));
    reply("r0", true, vec![]);
    for r in ["r1", "r2"] {
        reply(r, false, vec![(id, 4, 1)]);
    }
    assert!(table.dirty(at(1)).is_empty());
    reply("r0", false, vec![]);
    assert_eq!(table.dirty(at(2)).len(), 1, "r0 never granted it");
    reply("r0", false, vec![(id, 4, 1)]);
    reply("r1", false, vec![]);
    assert!(table.dirty(at(3)).is_empty(), "every replica granted it, and no link wants a resync");
    table.link("r0", 2);
    assert_eq!(table.dirty(at(4)).len(), 1, "r0 restarted: its predecessor's grant no longer counts");
}

#[test]
fn a_paused_replica_is_young_until_a_round_of_reports_came() {
    let mut r = replica(at(0));
    r.tick(at(2500));
    assert!(!r.young(at(2500)));
    r.tick(at(5001));
    assert!(r.young(at(5001)) && r.young(at(6100)) && !r.young(at(6101)));
    let n = Arc::from("n");
    let g = r.report(&n, &report(1, 0, vec![permits(1, 0, 1)]), at(5100)).unwrap();
    assert!(g.young && g.entries.iter().all(|e| e.1 == 0));
}

#[test]
fn a_node_back_from_a_pause_answers_young_before_its_next_cycle() {
    let table = Table::new(7, HOLD, 1000);
    let mut node = Node::new("r", 1, members(&[("r", 1)]), &table, LEASE, at(0));
    node.round(&table, at(0));
    let n = Arc::from("n");
    let g = node.answer(&n, &report(1, 0, vec![permits(1, 0, 1)]), at(100)).unwrap();
    assert!(!g.young && grant_of(&g, 1) == Some(10));
    let g = node.answer(&n, &report(2, g.gseq, vec![permits(1, 1, 1)]), at(3000)).unwrap();
    assert!(g.young, "unticked for 3 s: its rows are stale");
}

#[test]
fn a_pull_ends_once_every_candidate_answered_refused_or_left() {
    let table = Table::new(7, HOLD, 1000);
    let all = [("me", "r0"), ("a", "r1"), ("a2", "r1"), ("b", "r2")];
    let mut node = Node::new("me", 1, view(&all), &table, LEASE, at(0));
    let (round, asks) = node.pulls(at(0));
    assert_eq!(asks.iter().map(|(peer, start)| (&**peer, *start)).collect::<Vec<_>>(), [("a", 0), ("a2", 0), ("b", 0)]);
    assert!(node.pulls(at(499)).1.is_empty() && node.pulls(at(500)).1.len() == 3, "asked again after a patience");
    node.page("a", round, Some((Vec::new(), Some(9))), at(600));
    node.page("a", round - 1, Some((Vec::new(), None)), at(600));
    let next = [(Arc::<str>::from("a"), 9)];
    assert_eq!(node.pulls(at(600)).1, next, "the next page at once, and an old round's is dropped");
    node.page("a", round, Some((Vec::new(), None)), at(700));
    node.page("b", round, None, at(700));
    assert!(node.replica.starting(), "b refused for good, and a2 has not answered");
    node.see(&table, view(&[("me", "r0"), ("a", "r1"), ("b", "r2")]), at(800));
    assert!(!node.replica.pulling(), "a2 left, which hands this node nothing: nothing is left to pull");
}

#[test]
fn a_candidate_serves_the_keys_the_puller_replicates_in_the_pullers_view_not_in_its_own() {
    let all = [("me", "r0"), ("a", "r1"), ("a2", "r1"), ("b", "r2")];
    let (table, a_table) = (Table::new(7, HOLD, 1000), Table::new(7, HOLD, 1000));
    let mut me = Node::new("me", 1, view(&all), &table, LEASE, at(0));
    for peer in ["a", "a2", "b"] {
        me.page(peer, 1, Some((Vec::new(), None)), at(0));
    }
    let id = (0..).find(|id| me.members.candidates.serves(*id, "a2")).expect("a key of a2");
    me.answer(&Arc::from("b"), &report(1, 0, vec![permits(id, 3, 1)]), at(100)).unwrap();
    let mut a = Node::new("a", 1, view(&all), &a_table, LEASE, at(0));
    for peer in ["me", "a2", "b"] {
        a.page(peer, 1, Some((Vec::new(), None)), at(0));
    }
    a.see(&a_table, view(&[("me", "r0"), ("a", "r1"), ("b", "r2")]), at(200));
    let (round, asks) = a.pulls(at(200));
    assert_eq!(asks.len(), 2, "a2 left and handed its keys to a");
    let regions = a.members.candidates.regions().to_vec();
    a.page("me", round, Some(me.serve_pull("a", regions, 0, at(200))), at(250));
    a.page("b", round, Some((Vec::new(), None)), at(250));
    assert!(!a.replica.pulling() && a.replica.rows() == 1, "me's view still shows a2, whose key a took over");

    let (new_table, n_table) = (Table::new(7, HOLD, 1000), Table::new(7, HOLD, 1000));
    let mut n = Node::new("n", 1, members(&[("n", 1), ("old", 1)]), &n_table, LEASE, at(0));
    n.page("old", 1, Some((Vec::new(), None)), at(0));
    n.answer(&Arc::from("old"), &report(1, 0, vec![permits(1, 3, 1)]), at(100)).unwrap();
    let mut new = Node::new("new", 1, members(&[("n", 1), ("old", 1), ("new", 1)]), &new_table, LEASE, at(200));
    let (round, _) = new.pulls(at(200));
    let regions = new.members.candidates.regions().to_vec();
    new.page("n", round, Some(n.serve_pull("new", regions, 0, at(200))), at(250));
    new.page("old", round, Some((Vec::new(), None)), at(250));
    assert!(!new.replica.starting() && new.replica.rows() == 1, "n's view does not show new yet");
}

#[test]
fn the_candidates_a_puller_sends_choose_the_replicas_it_chose() {
    for (per_region, regions) in [3, 8].into_iter().flat_map(|p| (1..=7).map(move |r| (p, r))) {
        // 23 nodes: regions of unequal sizes.
        let nodes: Vec<(String, String)> = (0..23).map(|i| (format!("n{i}"), format!("r{}", i % regions))).collect();
        let puller = Candidates::new(7, per_region, nodes.iter().map(|(n, r)| (n.as_str(), r.as_str())));
        let sent = Candidates::new(7, per_region, [("x", "r0")]).of(puller.regions().to_vec());
        for id in 0..500 {
            assert_eq!(sent.replicas(id), puller.replicas(id), "{per_region} a region, {regions} regions, key {id}");
        }
    }
}

#[test]
fn a_row_nobody_reported_for_long_is_lowered_to_zero_then_dropped() {
    let n = Arc::from("n");
    let mut r = replica(at(0));
    let mut g = r.report(&n, &report(1, 0, vec![permits(1, 0, 1)]), at(0)).unwrap();
    assert_eq!(grant_of(&g, 1), Some(10));
    let beat = |r: &mut Replica, acked, now| r.report(&n, &report(now / 500 + 1, acked, vec![]), at(now));
    for now in (1000..=120_000).step_by(1000) {
        g = beat(&mut r, g.gseq, now).unwrap();
        r.tick(at(now));
    }
    g = beat(&mut r, g.gseq, 120_500).unwrap();
    assert_eq!(grant_of(&g, 1), Some(0), "heartbeats, but nobody reported the key for two minutes");
    beat(&mut r, g.gseq, 121_000).unwrap();
    r.tick(at(121_000));
    assert!(r.keys.is_empty(), "acknowledged, then dropped");
    r.report(&n, &report(300, 0, vec![Item { max: 12, ..permits(1, 0, 1) }]), at(121_000)).unwrap();
    r.report(&n, &report(301, 0, vec![Item { max: 11, ..permits(1, 0, 1) }]), at(121_000)).unwrap();
    assert_eq!((r.mismatched, r.keys[&1].max), (1, 11), "the newest maximum wins");
}

#[test]
fn replicas_are_one_a_region_at_least_three_of_eight_candidates_a_region_and_move_only_with_their_node() {
    for regions in 1..=4 {
        let nodes: Vec<(String, &str)> =
            (0..50).map(|i| (format!("n{i}"), ["r0", "r1", "r2", "r3"][i % regions])).collect();
        let region = |node: &str| nodes.iter().find(|n| n.0 == node).expect("a member").1;
        let of = |gone: &str| {
            Candidates::new(
                7,
                Candidates::PER_REGION,
                nodes.iter().filter(|n| n.0 != gone).map(|(n, r)| (n.as_str(), *r)),
            )
        };
        let (all, listed_backwards) =
            (of(""), Candidates::new(7, Candidates::PER_REGION, nodes.iter().rev().map(|(n, r)| (n.as_str(), *r))));
        assert!(all.regions.iter().all(|c| c.len() == Candidates::PER_REGION), "{regions} regions");
        assert_eq!(all.per_key(), regions.max(3));
        let gone = all.regions[0][0].clone();
        let without = of(&gone);
        let entering = without.all().find(|c| !all.all().any(|a| a == *c)).expect("a ninth").clone();
        for id in 0..500 {
            let replicas = all.replicas(id);
            assert_eq!(replicas.iter().collect::<BTreeSet<_>>().len(), regions.max(3), "{replicas:?}");
            if regions >= 3 {
                assert_eq!(replicas.iter().map(|r| region(r)).collect::<BTreeSet<_>>().len(), regions, "one a region");
            }
            assert_eq!(replicas, listed_backwards.replicas(id), "whatever order the view lists them in");
            let moved: Vec<&Arc<str>> = without.replicas(id).into_iter().filter(|r| !replicas.contains(r)).collect();
            assert!(replicas.contains(&&gone) || moved.iter().all(|r| **r == entering), "{id}: {moved:?}");
        }
        for (node, r) in &nodes {
            // A candidate leaving hands its keys to its region's others, and so does the ninth, leaving again.
            let takes = |c: &Candidates| c.all().any(|c| **c == **node) && (regions < 3 || *r == region(&gone));
            assert_eq!(without.gains(&all, node), takes(&without), "{node} in {regions} regions, {gone} left");
            assert_eq!(all.gains(&without, node), takes(&all), "{node} in {regions} regions, {gone} back");
        }
    }
}

#[test]
fn ids_are_seeded_and_spread_over_the_shards() {
    assert_ne!(entry_id(1, &WS, b"k"), entry_id(2, &WS, b"k"));
    assert_ne!(entry_id(1, &WS, b"k"), entry_id(1, &Limit { name: "wsk", ..WS }, b""));
    let mut per = [0u32; SHARDS];
    for i in 0u32..64_000 {
        per[Table::shard(entry_id(9, &WS, &i.to_be_bytes()))] += 1;
    }
    assert!(per.iter().all(|n| (800..1200).contains(n)), "{per:?}");
}
