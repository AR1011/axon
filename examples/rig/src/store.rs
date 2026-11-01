//! The fencing store: the World's `Ledger` oracle made real. It lives on the control machine, where no fault reaches
//! it, keeps every key's records in memory, and refuses a write from an epoch older than the newest owner it has
//! seen. Every request and verdict goes to `store.jsonl`, whose clock is the run's timeline.
//!
//! Protocol, one line each way (the records of an `OK n` follow it, one per line):
//!   `O key epoch node lag`                                        → `OK n` | `STALE fence` | `CONFLICT`
//!   `A key epoch seq node op delta before after outcome lag`      → `OK` | `STALE fence` | `CONFLICT [len]`
//! A record is `epoch node op delta before after outcome`; an `OK n` puts each one's age before it, ms on the store's
//! monotonic clock since it was appended, so a new owner keeps the op ids a retry can still ask for (`account::KEEP`).

use std::collections::BTreeMap;
use std::fmt::Write as _;
use std::fs::File;
use std::io::{self, BufRead, BufReader, BufWriter, Write};
use std::net::{TcpListener, TcpStream, ToSocketAddrs};
use std::sync::atomic::{AtomicUsize, Ordering::Relaxed};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use serde_json::{Value, json};

/// Wall µs, the `t` of every log line.
pub use axon::trace::unix_micros as now_us;

#[derive(Default)]
pub struct Store {
    keys: BTreeMap<String, Key>,
}

#[derive(Default)]
struct Key {
    /// The newest epoch that owned the key, and its node.
    fence: u64,
    owner: Option<String>,
    records: Vec<(Instant, String)>,
}

impl Store {
    /// The reply to one request line, and what to log about it.
    pub fn handle(&mut self, line: &str, now: Instant) -> (String, Option<Value>) {
        let w: Vec<&str> = line.split(' ').collect();
        let num = |s: &str| s.parse::<i64>().ok();
        match w.as_slice() {
            ["O", k, e, n, lag] => {
                let (Some(e), Some(lag)) = (num(e), num(lag)) else { return ("ERR bad own".into(), None) };
                let key = self.keys.entry((*k).to_owned()).or_default();
                let e = e as u64;
                let (reply, v) = if e < key.fence {
                    (format!("STALE {}", key.fence), "stale")
                } else if e == key.fence && key.owner.as_deref().is_some_and(|o| o != *n) {
                    ("CONFLICT".into(), "conflict")
                } else {
                    (key.fence, key.owner) = (e, Some((*n).to_owned()));
                    let mut reply = format!("OK {}", key.records.len());
                    for (at, r) in &key.records {
                        let _ = write!(reply, "\n{} {r}", now.saturating_duration_since(*at).as_millis());
                    }
                    (reply, "ok")
                };
                let len = key.records.len();
                let log = json!({"r": "O", "k": k, "e": e, "n": n, "lag": lag, "v": v, "f": key.fence, "len": len});
                (reply, Some(log))
            }
            ["A", k, e, s, n, op, d, b, a, o, lag] => {
                let (Some(e), Some(s), Some(d), Some(b), Some(a), Some(lag)) =
                    (num(e), num(s), num(d), num(b), num(a), num(lag))
                else {
                    return ("ERR bad append".into(), None);
                };
                let key = self.keys.entry((*k).to_owned()).or_default();
                let (e, len) = (e as u64, key.records.len() as i64);
                let (reply, v) = if e < key.fence {
                    (format!("STALE {}", key.fence), "stale")
                } else if e > key.fence || key.owner.as_deref() != Some(*n) {
                    // No own seen for this epoch, or two writers in one.
                    ("CONFLICT".into(), "conflict")
                } else if s != len {
                    (format!("CONFLICT {len}"), "conflict")
                } else {
                    key.records.push((now, format!("{e} {n} {op} {d} {b} {a} {o}")));
                    ("OK".into(), "ok")
                };
                let log = json!({"r": "A", "k": k, "e": e, "n": n, "s": s, "op": op, "d": d, "b": b, "a": a, "o": o,
                                 "lag": lag, "v": v, "f": key.fence});
                (reply, Some(log))
            }
            _ => ("ERR unknown request".into(), None),
        }
    }
}

/// Serves until the process ends: one thread per connection, one lock over the state and the log, so the log's
/// order is the order of the verdicts. The log is flushed every 100 ms, and at once after an own.
pub fn serve(bind: &str, log: &str) -> io::Result<()> {
    let listener = TcpListener::bind(bind)?;
    let shared = Arc::new(Mutex::new((Store::default(), BufWriter::new(File::create(log)?))));
    let flusher = shared.clone();
    std::thread::spawn(move || {
        loop {
            std::thread::sleep(Duration::from_millis(100));
            if let Ok(mut s) = flusher.lock() {
                let _ = s.1.flush();
            }
        }
    });
    println!("store on {}, log {log}", listener.local_addr()?);
    for conn in listener.incoming() {
        let Ok(conn) = conn else { continue };
        let shared = shared.clone();
        std::thread::spawn(move || {
            let _ = conn.set_nodelay(true);
            let Ok(mut out) = conn.try_clone() else { return };
            for line in BufReader::new(conn).lines() {
                let Ok(line) = line else { return };
                let reply = {
                    let Ok(mut guard) = shared.lock() else { return };
                    let (store, log) = &mut *guard;
                    let (reply, entry) = store.handle(&line, Instant::now());
                    if let Some(Value::Object(mut entry)) = entry {
                        entry.insert("t".into(), now_us().into());
                        // A trigger fires on an own within milliseconds (spec/fault-rig.md § The nemesis and its
                        // schedules).
                        let own = entry.get("r").is_some_and(|r| r == "O");
                        let _ = writeln!(log, "{}", Value::Object(entry));
                        if own {
                            let _ = log.flush();
                        }
                    }
                    reply
                };
                if writeln!(out, "{reply}").is_err() {
                    return;
                }
            }
        });
    }
    Ok(())
}

#[derive(Debug)]
pub enum Verdict {
    Ok(Vec<String>),
    Stale(u64),
    Conflict,
    Failed(String),
}

/// A blocking client for jobs: `CONNS` connections, each reconnected after an error.
pub struct Client {
    addr: String,
    conns: Vec<Mutex<Option<BufReader<TcpStream>>>>,
    next: AtomicUsize,
}

const CONNS: usize = 8;

impl Client {
    pub fn new(addr: String) -> Self {
        Client { addr, conns: (0..CONNS).map(|_| Mutex::new(None)).collect(), next: AtomicUsize::new(0) }
    }

    /// Sends `request lag`, `lag` being µs from `stamp` to the moment it is written, and returns the verdict, the lag
    /// and the round trip in µs.
    pub fn call(&self, stamp: Instant, request: &str) -> (Verdict, u64, u64) {
        let slot = &self.conns[self.next.fetch_add(1, Relaxed) % CONNS];
        let Ok(mut conn) = slot.lock() else { return (Verdict::Failed("poisoned".into()), 0, 0) };
        let (mut sent, mut lag) = (Instant::now(), 0);
        let result = (|| -> io::Result<Verdict> {
            if conn.is_none() {
                let addr = self.addr.to_socket_addrs()?.next().ok_or(io::ErrorKind::AddrNotAvailable)?;
                let stream = TcpStream::connect_timeout(&addr, Duration::from_secs(2))?;
                stream.set_nodelay(true)?;
                stream.set_read_timeout(Some(Duration::from_secs(10)))?;
                *conn = Some(BufReader::new(stream));
            }
            let c = conn.as_mut().ok_or(io::ErrorKind::NotConnected)?;
            (sent, lag) = (Instant::now(), stamp.elapsed().as_micros() as u64);
            c.get_mut().write_all(format!("{request} {lag}\n").as_bytes())?;
            let mut read = || -> io::Result<String> {
                let mut s = String::new();
                if c.read_line(&mut s)? == 0 {
                    return Err(io::ErrorKind::UnexpectedEof.into());
                }
                Ok(s.trim_end().to_owned())
            };
            let head = read()?;
            let mut w = head.split(' ');
            Ok(match (w.next(), w.next().map(str::parse::<u64>)) {
                (Some("OK"), None) => Verdict::Ok(Vec::new()),
                (Some("OK"), Some(Ok(n))) => Verdict::Ok((0..n).map(|_| read()).collect::<io::Result<_>>()?),
                (Some("STALE"), Some(Ok(f))) => Verdict::Stale(f),
                (Some("CONFLICT"), _) => Verdict::Conflict,
                _ => Verdict::Failed(head),
            })
        })();
        let rtt = sent.elapsed().as_micros() as u64;
        match result {
            Ok(v) => (v, lag, rtt),
            Err(e) => {
                *conn = None;
                (Verdict::Failed(format!("store: {e}")), lag, rtt)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// One instant for every request, so each record's age is 0.
    static NOW: std::sync::LazyLock<Instant> = std::sync::LazyLock::new(Instant::now);

    fn ask(s: &mut Store, line: &str) -> String {
        s.handle(line, *NOW).0
    }

    #[test]
    fn the_store_refuses_old_epochs_other_nodes_and_gaps() {
        let mut s = Store::default();
        assert_eq!(ask(&mut s, "O k 5 b1 10"), "OK 0");
        assert_eq!(ask(&mut s, "A k 5 0 b1 x.1 10 0 10 ok 7"), "OK");
        // Two writers in one epoch, an append with no own seen for its epoch, and a sequence gap.
        assert_eq!(ask(&mut s, "A k 5 1 b2 x.2 10 10 20 ok 7"), "CONFLICT");
        assert_eq!(ask(&mut s, "A k 6 1 b1 x.2 10 10 20 ok 7"), "CONFLICT");
        assert_eq!(ask(&mut s, "A k 5 2 b1 x.2 10 10 20 ok 7"), "CONFLICT 1");
        // The same epoch from another node is a conflict; from the same node, a restart in place.
        assert_eq!(ask(&mut s, "O k 5 b2 10"), "CONFLICT");
        assert_eq!(ask(&mut s, "O k 5 b1 10"), "OK 1\n0 5 b1 x.1 10 0 10 ok");
        // A newer owner bumps the fence: the old one's own and append are refused, the new one's taken.
        assert_eq!(ask(&mut s, "O k 7 b2 10"), "OK 1\n0 5 b1 x.1 10 0 10 ok");
        assert_eq!(ask(&mut s, "A k 5 1 b1 x.2 -20 10 10 overdraft 7"), "STALE 7");
        assert_eq!(ask(&mut s, "O k 5 b1 10"), "STALE 7");
        assert_eq!(ask(&mut s, "A k 7 1 b2 x.2 -20 10 10 overdraft 7"), "OK");
        let (_, log) = s.handle("A k 6 2 b1 x.3 1 10 11 ok 3", *NOW);
        let log = log.unwrap();
        assert_eq!((log["v"].as_str(), log["f"].as_u64(), log["lag"].as_i64()), (Some("stale"), Some(7), Some(3)));
        assert!(ask(&mut s, "A k 7 x b2 x.3 1 10 11 ok 3").starts_with("ERR"));
        assert_eq!(ask(&mut s, "O other 1 b3 0"), "OK 0");
    }

    #[test]
    fn an_own_gets_each_record_with_its_age() {
        let mut s = Store::default();
        let at = |ms| *NOW + Duration::from_millis(ms);
        s.handle("O k 5 b1 10", at(0));
        s.handle("A k 5 0 b1 x.1 10 0 10 ok 7", at(0));
        s.handle("A k 5 1 b1 x.2 5 10 15 ok 7", at(2500));
        assert_eq!(s.handle("O k 6 b2 10", at(4000)).0, "OK 2\n4000 5 b1 x.1 10 0 10 ok\n1500 5 b1 x.2 5 10 15 ok");
    }
}
