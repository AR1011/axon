//! One `Account` per key, strictly owned. It loads its records from the store before handling anything, and appends
//! one record per operation: the store's `OK` is the commit point, then a journal line and `fdatasync`, then the
//! reply. A retry is answered from `done`; nothing is answered from memory before the store has the record.

use std::collections::{HashMap, VecDeque};
use std::fs::File;
use std::io::Write;
use std::sync::atomic::AtomicU64;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use axon::{Actor, Context, RestartPolicy, StopReason};
use axon_cluster::{ActorProtocol, CallError, Rejection, Reply, SpawnContext, WireProtocol};
use bytes::Bytes;
use serde_json::{Value, json};

use crate::hooks::{self, Point};
use crate::load::GIVE_UP;
use crate::store::{Client, Verdict, now_us};

pub const KIND: &str = "acct";

/// The `accounts` workload: the kind, and `KEYS` clients calling it (src/load.rs). Its tick counts client tries.
pub async fn start(cx: &crate::Ctx<'_>) -> Result<crate::Tick, Box<dyn std::error::Error>> {
    let kind = cx.cluster.kind::<Api>(KIND);
    #[cfg(feature = "v2")]
    let kind = kind.also::<AcctApi>();
    kind.policy(RestartPolicy { max_restarts: 100, ..RestartPolicy::default() })
        .register(factory(cx.sh.clone()))
        .await?;
    let (keys, every) = (crate::env("KEYS", "2000").parse()?, crate::env("EVERY_MS", "20000").parse()?);
    Ok(crate::load::clients(cx.cluster, cx.sh.clone(), KIND, "k", keys, Duration::from_millis(every), cx.boot_ms))
}

/// The factory of a kind of accounts: `acct`, and those of src/eager.rs and src/idle.rs.
pub fn factory(sh: Arc<Shared>) -> impl Fn(&SpawnContext, Bytes) -> Result<Account, String> + Send + Sync + 'static {
    move |cx, _| {
        let (key, epoch) = (cx.key.clone().ok_or("keyed only")?, cx.epoch.ok_or("leased only")?);
        Ok(Account::new(key, epoch, sh.clone()))
    }
}

/// How long `done` answers an op after its record: clients send no try after `GIVE_UP`, and the rest is margin for a
/// try held on its way (spec/fault-rig.md § Workloads says why that is enough).
pub const KEEP: Duration = Duration::from_secs(GIVE_UP.as_secs() + 180);

/// The answers of the ops a retry can still ask for, by op id.
#[derive(Default)]
struct Done {
    answers: HashMap<String, Answer>,
    /// The op ids in `answers` and when their records were written, oldest first.
    written: VecDeque<(Instant, String)>,
}

impl Done {
    /// Adds an op whose record was written at `at`, and forgets those written more than `KEEP` before it.
    fn insert(&mut self, at: Instant, op: String, answer: Answer) {
        while let Some((t, _)) = self.written.front()
            && at.saturating_duration_since(*t) > KEEP
            && let Some((_, old)) = self.written.pop_front()
        {
            self.answers.remove(&old);
        }
        self.written.push_back((at, op.clone()));
        self.answers.insert(op, answer);
    }

    /// Records `age_ms e node op d b a o`, in sequence order: keeps those younger than `KEEP`, and returns how many
    /// there are and the last one's balance.
    fn load(&mut self, now: Instant, records: Vec<String>) -> Result<(u64, i64), String> {
        let (mut seq, mut balance) = (0, 0);
        for r in records {
            let w: Vec<&str> = r.split(' ').collect();
            let bad = || format!("bad record {r}");
            let [age, e, n, op, _, _, a, o] = w.as_slice() else { return Err(bad()) };
            let age = Duration::from_millis(age.parse().map_err(|_| bad())?);
            let (after, epoch) = (a.parse().map_err(|_| bad())?, e.parse().map_err(|_| bad())?);
            (seq, balance) = (seq + 1, after);
            if age <= KEEP {
                let answer = Answer { outcome: (*o).into(), after, epoch, node: (*n).into() };
                self.insert(now.checked_sub(age).unwrap_or(now), (*op).into(), answer);
            }
        }
        Ok((seq, balance))
    }
}

/// A JSON-lines file. Each line is one write, unbuffered, so a killed process loses none it wrote.
pub struct Log(Mutex<File>);

impl Log {
    pub fn create(path: &std::path::Path) -> std::io::Result<Self> {
        Ok(Log(Mutex::new(File::options().create(true).append(true).open(path)?)))
    }

    pub fn line(&self, r: &str, v: Value) {
        let _ = self.write(r, v, false);
    }

    /// Writes the line, and with `sync` `fdatasync`s it; returns the sync's µs.
    fn write(&self, r: &str, mut v: Value, sync: bool) -> std::io::Result<u64> {
        if let Value::Object(m) = &mut v {
            m.insert("t".into(), now_us().into());
            m.insert("r".into(), r.into());
        }
        let mut file = self.0.lock().map_err(|_| std::io::Error::other("poisoned"))?;
        file.write_all(format!("{v}\n").as_bytes())?;
        if !sync {
            return Ok(0);
        }
        // Synced outside the lock, so the syncs that wait together share one commit: under it, five nodes' journals on
        // one box's disk (RIG_BOX) synced 35 lines a second each, a third of the night's ledger.
        let synced = file.try_clone()?;
        drop(file);
        let at = Instant::now();
        synced.sync_data()?;
        Ok(at.elapsed().as_micros() as u64)
    }
}

/// What every account and client on this node shares.
pub struct Shared {
    pub node: String,
    /// Serve mode: answer at once, no store, no journal.
    pub serve: bool,
    pub store: Client,
    pub journal: Log,
    pub node_log: Log,
    pub client_log: Log,
    /// Samples in µs (store round trip, `fdatasync`) and ms (serve-mode latency) since the last `stats`.
    pub rtt: Mutex<Vec<u64>>,
    pub fsync: Mutex<Vec<u64>>,
    pub latency: Mutex<Vec<u64>>,
    pub acks: AtomicU64,
    pub give_ups: AtomicU64,
    pub errors: AtomicU64,
}

pub fn sample(into: &Mutex<Vec<u64>>, v: u64) {
    if let Ok(mut s) = into.lock() {
        s.push(v);
    }
}

pub struct Op {
    pub id: String,
    pub delta: i64,
}

/// An op's result, with the epoch and node that wrote its record.
#[derive(Clone)]
pub struct Answer {
    pub outcome: String,
    pub after: i64,
    pub epoch: u64,
    pub node: String,
}

/// This build and the schemas it serves, its own first, for its `up` line; the clients call `Api`. The `v2` build
/// is the other side of a rolling upgrade (spec/fault-rig.md § Fault classes, `roll`): it calls schema 2 and serves
/// 1 beside it.
#[cfg(not(feature = "v2"))]
pub const BUILD: (&str, &[u16]) = ("v1", &[1]);
#[cfg(not(feature = "v2"))]
pub type Api = AcctApi;
#[cfg(feature = "v2")]
pub const BUILD: (&str, &[u16]) = ("v2", &[2, 1]);
#[cfg(feature = "v2")]
pub type Api = AcctApiV2;

pub struct AcctApi;

fn encode_op(op: Op, sep: char) -> Result<Bytes, CallError> {
    Ok(Bytes::from(format!("{}{sep}{}", op.id, op.delta)))
}

fn decode_answer(body: &[u8], sep: char) -> Result<Answer, CallError> {
    let bad = || CallError::Codec("bad answer".into());
    let s = std::str::from_utf8(body).map_err(|_| bad())?;
    match s.split(sep).collect::<Vec<_>>().as_slice() {
        [o, a, e, n] => Ok(Answer {
            outcome: (*o).into(),
            after: a.parse().map_err(|_| bad())?,
            epoch: e.parse().map_err(|_| bad())?,
            node: (*n).into(),
        }),
        _ => Err(bad()),
    }
}

fn encode_answer(a: Answer, sep: char) -> Result<Bytes, CallError> {
    Ok(Bytes::from(format!("{}{sep}{}{sep}{}{sep}{}", a.outcome, a.after, a.epoch, a.node)))
}

/// An op id goes into the store's space-separated requests: never a space or a newline.
fn decode_op(body: &[u8], sep: char, reply: Option<Reply<Answer>>) -> Result<Msg, Rejection> {
    let s = std::str::from_utf8(body).map_err(|_| Rejection::InvalidMessage)?;
    let (id, delta) = s.split_once(sep).ok_or(Rejection::InvalidMessage)?;
    let delta = delta.parse().map_err(|_| Rejection::InvalidMessage)?;
    if id.is_empty() || id.contains([' ', '\n', sep]) {
        return Err(Rejection::InvalidMessage);
    }
    Ok(Msg::Op(id.into(), delta, reply))
}

impl WireProtocol for AcctApi {
    type Message = Op;
    type Reply = Answer;
    const ID: u64 = 0x5163;
    const SCHEMA: u16 = 1;

    fn encode(op: Op) -> Result<Bytes, CallError> {
        encode_op(op, ' ')
    }

    fn decode_reply(body: Bytes) -> Result<Answer, CallError> {
        decode_answer(&body, ' ')
    }
}

impl ActorProtocol for AcctApi {
    type Mailbox = Msg;

    fn encode_reply(a: Answer) -> Result<Bytes, CallError> {
        encode_answer(a, ' ')
    }

    fn decode(body: Bytes, reply: Option<Reply<Answer>>) -> Result<Msg, Rejection> {
        decode_op(&body, ' ', reply)
    }
}

/// Schema 2 of the same protocol: the same fields `|`-separated, so neither schema decodes the other's bytes.
#[cfg(feature = "v2")]
pub struct AcctApiV2;

#[cfg(feature = "v2")]
impl WireProtocol for AcctApiV2 {
    type Message = Op;
    type Reply = Answer;
    const ID: u64 = AcctApi::ID;
    const SCHEMA: u16 = 2;

    fn encode(op: Op) -> Result<Bytes, CallError> {
        encode_op(op, '|')
    }

    fn decode_reply(body: Bytes) -> Result<Answer, CallError> {
        decode_answer(&body, '|')
    }
}

#[cfg(feature = "v2")]
impl ActorProtocol for AcctApiV2 {
    type Mailbox = Msg;

    fn encode_reply(a: Answer) -> Result<Bytes, CallError> {
        encode_answer(a, '|')
    }

    fn decode(body: Bytes, reply: Option<Reply<Answer>>) -> Result<Msg, Rejection> {
        decode_op(&body, '|', reply)
    }
}

pub struct Rec {
    op: String,
    after: i64,
    outcome: &'static str,
}

pub enum Msg {
    Op(String, i64, Option<Reply<Answer>>),
    Owned(Verdict),
    Appended(Rec, Verdict, Option<Reply<Answer>>),
}

pub struct Account {
    key: String,
    epoch: u64,
    sh: Arc<Shared>,
    seq: u64,
    balance: i64,
    /// Each answer names the epoch and node that wrote the record, which are the ones with its journal line.
    done: Done,
}

impl Account {
    pub fn new(key: String, epoch: u64, sh: Arc<Shared>) -> Self {
        Account { key, epoch, sh, seq: 0, balance: 0, done: Done::default() }
    }

    fn answer(reply: Option<Reply<Answer>>, a: Answer) {
        if let Some(reply) = reply {
            let _ = reply.send(a);
        }
    }

    fn stale(&self, fence: u64, cx: &mut Context<'_, Self>) {
        self.sh.node_log.line("stale", json!({"k": self.key, "e": self.epoch, "f": fence}));
        cx.stop();
    }
}

impl Actor for Account {
    type Message = Msg;
    type Error = String;

    fn on_start(&mut self, cx: &mut Context<'_, Self>) -> Result<(), String> {
        let stamp = Instant::now();
        self.sh.node_log.line("start", json!({"k": self.key, "e": self.epoch}));
        hooks::at(Point::Start, &self.sh, &self.key);
        if self.sh.serve {
            return Ok(());
        }
        let (sh, k, e) = (self.sh.clone(), self.key.clone(), self.epoch);
        let own = cx
            .submit_io(move || {
                let (v, _, rtt) = sh.store.call(stamp, &format!("O {k} {e} {}", sh.node));
                sample(&sh.rtt, rtt);
                Msg::Owned(v)
            })
            .map_err(|e| format!("own refused: {e:?}"))
            .and_then(|job| cx.pause_until(&job).map_err(|e| format!("{e:?}")));
        // A failed start gets no on_stop: end the epoch its start line began.
        if let Err(why) = &own {
            self.sh.node_log.line("stop", json!({"k": self.key, "e": self.epoch, "why": why}));
        }
        own
    }

    fn on_message(&mut self, msg: Msg, cx: &mut Context<'_, Self>) -> Result<(), String> {
        let stamp = Instant::now();
        match msg {
            Msg::Op(_, _, reply) if self.sh.serve => {
                let node = self.sh.node.clone();
                Self::answer(reply, Answer { outcome: "ok".into(), after: 0, epoch: self.epoch, node });
            }
            Msg::Op(id, _, reply) if let Some(a) = self.done.answers.get(&id) => Self::answer(reply, a.clone()),
            // Its caller gave up and tries again: a try never runs past its deadline (spec/fault-rig.md § Workloads).
            Msg::Op(_, _, Some(reply)) if reply.is_abandoned() => {}
            Msg::Op(id, delta, reply) => {
                hooks::at(Point::Message, &self.sh, &self.key);
                let b = self.balance;
                let (outcome, after) = if b + delta < 0 { ("overdraft", b) } else { ("ok", b + delta) };
                let (sh, k, e, s) = (self.sh.clone(), self.key.clone(), self.epoch, self.seq);
                let job = cx.submit_io(move || {
                    hooks::at(Point::Job, &sh, &k);
                    let request = format!("A {k} {e} {s} {} {id} {delta} {b} {after} {outcome}", sh.node);
                    let (mut v, lag, rtt) = sh.store.call(stamp, &request);
                    sample(&sh.rtt, rtt);
                    if matches!(v, Verdict::Ok(_)) {
                        let line = json!({"k": k, "e": e, "n": sh.node, "s": s, "op": id, "d": delta, "b": b,
                                          "a": after, "o": outcome, "lag": lag, "v": "ok", "rtt": rtt});
                        match sh.journal.write("A", line, true) {
                            Ok(us) => sample(&sh.fsync, us),
                            Err(err) => {
                                // The store has the record: a retry is answered from it with no journal line here.
                                let err = err.to_string();
                                sh.node_log.line("journal_err", json!({"k": k, "e": e, "op": id, "err": err}));
                                v = Verdict::Failed(format!("journal: {err}"));
                            }
                        }
                    }
                    Msg::Appended(Rec { op: id, after, outcome }, v, reply)
                });
                // Refused (draining, or the job pool full): the reply is dropped and the caller retries.
                if let Ok(job) = job {
                    cx.pause_until(&job).map_err(|e| format!("{e:?}"))?;
                }
            }
            Msg::Owned(Verdict::Ok(records)) => (self.seq, self.balance) = self.done.load(Instant::now(), records)?,
            Msg::Owned(Verdict::Stale(fence)) => self.stale(fence, cx),
            Msg::Appended(rec, Verdict::Ok(_), reply) => {
                (self.seq, self.balance) = (self.seq + 1, rec.after);
                let node = self.sh.node.clone();
                let a = Answer { outcome: rec.outcome.into(), after: rec.after, epoch: self.epoch, node };
                self.done.insert(Instant::now(), rec.op, a.clone());
                Self::answer(reply, a);
            }
            Msg::Appended(_, Verdict::Stale(fence), _) => self.stale(fence, cx),
            // A conflict, or a store or journal error: fail, and the policy rebuilds the actor from the store.
            Msg::Owned(Verdict::Conflict) | Msg::Appended(_, Verdict::Conflict, _) => {
                return Err(format!("{} conflict at epoch {}", self.key, self.epoch));
            }
            Msg::Owned(Verdict::Failed(why)) | Msg::Appended(_, Verdict::Failed(why), _) => {
                return Err(format!("{} at epoch {}: {why}", self.key, self.epoch));
            }
        }
        Ok(())
    }

    fn on_stop(&mut self, reason: &StopReason) {
        self.sh.node_log.line("stop", json!({"k": self.key, "e": self.epoch, "why": format!("{reason:?}")}));
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn answer(after: i64) -> Answer {
        Answer { outcome: "ok".into(), after, epoch: 5, node: "b1".into() }
    }

    /// Each schema reads its own bytes back, and refuses the other's: a caller of one never runs on the other.
    #[test]
    fn each_schema_decodes_its_own_bytes_and_neither_the_others() {
        fn op<P: ActorProtocol<Mailbox = Msg> + WireProtocol<Message = Op, Reply = Answer>>(
            body: Bytes,
        ) -> Option<(String, i64)> {
            match P::decode(body, None) {
                Ok(Msg::Op(id, d, _)) => Some((id, d)),
                _ => None,
            }
        }
        let one = |sep| encode_op(Op { id: "b1.7.3".into(), delta: -42 }, sep).expect("encodes");
        assert_eq!(op::<AcctApi>(one(' ')), Some(("b1.7.3".into(), -42)));
        assert_eq!(op::<AcctApi>(one('|')), None);
        assert!(decode_answer(&encode_answer(answer(9), '|').expect("encodes"), ' ').is_err());
        let a = AcctApi::decode_reply(AcctApi::encode_reply(answer(9)).expect("encodes")).expect("decodes");
        assert_eq!((a.after, a.epoch, a.node.as_str()), (9, 5, "b1"));
        #[cfg(feature = "v2")]
        {
            assert_eq!(op::<AcctApiV2>(one('|')), Some(("b1.7.3".into(), -42)));
            assert_eq!(op::<AcctApiV2>(one(' ')), None);
            assert!(op::<AcctApiV2>(Bytes::from("a b|1")).is_none(), "an id holding a space");
            let a = AcctApiV2::decode_reply(AcctApiV2::encode_reply(answer(9)).expect("encodes")).expect("decodes");
            assert_eq!((a.after, a.epoch, a.node.as_str()), (9, 5, "b1"));
            assert!(AcctApiV2::decode_reply(AcctApi::encode_reply(answer(9)).expect("encodes")).is_err());
        }
    }

    #[test]
    fn done_answers_an_op_until_keep_has_passed_and_then_forgets_it() {
        let (mut done, t0) = (Done::default(), Instant::now());
        done.insert(t0, "x.1".into(), answer(1));
        done.insert(t0 + KEEP, "x.2".into(), answer(2));
        assert_eq!(done.answers.get("x.1").map(|a| a.after), Some(1), "a retry at the horizon is still answered");
        done.insert(t0 + KEEP + Duration::from_millis(1), "x.3".into(), answer(3));
        assert!(!done.answers.contains_key("x.1"));
        assert_eq!(done.answers.get("x.2").map(|a| a.after), Some(2));
        // An op a second, all night: the table holds KEEP's worth, not the night's.
        for i in 0..20_000u64 {
            done.insert(t0 + KEEP + Duration::from_secs(i), format!("y.{i}"), answer(i as i64));
        }
        assert_eq!(done.answers.len(), KEEP.as_secs() as usize + 1);
    }

    #[test]
    fn a_rebuild_keeps_only_the_records_a_retry_can_still_ask_for() {
        // Ahead of the clock, so `now - age` exists however long ago the machine booted.
        let (mut done, now) = (Done::default(), Instant::now() + KEEP);
        let ms = |d: Duration| d.as_millis();
        let records = vec![
            format!("{} 5 b1 x.1 10 0 10 ok", ms(KEEP) + 1),
            format!("{} 6 b2 x.2 -20 10 10 overdraft", ms(KEEP - Duration::from_secs(1))),
            "20 6 b2 x.3 5 10 15 ok".into(),
        ];
        assert_eq!(done.load(now, records), Ok((3, 15)));
        assert!(!done.answers.contains_key("x.1"));
        let a = done.answers.get("x.2").expect("a record younger than KEEP");
        assert_eq!((a.outcome.as_str(), a.after, a.epoch, a.node.as_str()), ("overdraft", 10, 6, "b2"));
        assert_eq!(done.answers.len(), 2);
        // Dated by its age: forgotten KEEP after its record was written, not after the load.
        done.insert(now + Duration::from_secs(1), "x.4".into(), answer(4));
        assert!(done.answers.contains_key("x.2"));
        done.insert(now + Duration::from_millis(1001), "x.5".into(), answer(5));
        assert!(!done.answers.contains_key("x.2"));
        assert!(done.load(Instant::now(), vec!["5 b1 x.1 10 0 10 ok".into()]).is_err(), "a record without its age");
    }
}
