use std::collections::VecDeque;

use loom::sync::atomic::{AtomicUsize, Ordering::SeqCst};
use loom::sync::{Arc, Mutex};
use loom::thread;

use super::*;

const SENDERS: usize = 2;
const FRAMES: usize = 2;
/// A capped session's bucket, in frames of one unit each: as deep as one frame, so a sender that wrote past the writer
/// task's bucket would write more than it refilled.
const DEPTH: usize = 1;

/// `Pace`: what it holds, and all it was refilled with.
struct Bucket {
    tokens: usize,
    refilled: usize,
}

/// A session's send side as `Sender` and `write_loop` drive it, with tokio's outbox and `Notify` modelled.
struct Session {
    queued: Queued,
    /// The writer lock, over `Writer::out`: what a submit left for the writer task to finish.
    writer: Mutex<Vec<usize>>,
    stream: Mutex<Vec<usize>>,
    outbox: Mutex<VecDeque<usize>>,
    /// `Notify::notify_one` with no waiter keeps one permit. Under a lock, as tokio keeps it: loom 0.7.2 lets a swap of
    /// an AtomicBool read past another thread's store that happened before it.
    unfinished: Mutex<bool>,
    finished: AtomicUsize,
    socket: Socket,
    /// A capped session's: the writer task takes a frame from the outbox only for a token.
    bucket: Option<Mutex<Bucket>>,
    written: AtomicUsize,
}

/// Whether the socket takes a write on the caller's thread at once (`Poll::Ready`) or leaves it in `Writer::out`.
#[derive(Clone, Copy)]
enum Socket {
    Takes,
    Refuses,
    /// A sender's first frame is left in `out`, its next ones are taken.
    RefusesThenTakes,
}

impl Session {
    fn new(socket: Socket, paced: bool) -> Self {
        Self {
            queued: Queued::new(paced),
            writer: Mutex::new(Vec::new()),
            stream: Mutex::new(Vec::new()),
            outbox: Mutex::new(VecDeque::new()),
            unfinished: Mutex::new(false),
            finished: AtomicUsize::new(0),
            socket,
            bucket: paced.then(|| Mutex::new(Bucket { tokens: DEPTH, refilled: 0 })),
            written: AtomicUsize::new(0),
        }
    }

    /// `Sender::write_now`, else `Sender::queue`. The socket is always idle here, which a submit writing itself
    /// only makes more likely.
    fn submit(&self, frame: usize) {
        if let Some(mut out) = self.queued.write_now(|| self.writer.try_lock().ok(), |out| out.is_empty()) {
            out.push(frame);
            let takes = match self.socket {
                Socket::Takes => true,
                Socket::Refuses => false,
                Socket::RefusesThenTakes => !frame.is_multiple_of(10),
            };
            if takes {
                self.write(&mut out);
            } else {
                *self.unfinished.lock().unwrap() = true;
            }
            return;
        }
        self.queued.queue(|| self.outbox.lock().unwrap().push_back(frame));
    }

    /// Header and payload go in apart, so two writers on the socket at once would tear a frame.
    fn write(&self, out: &mut Vec<usize>) {
        for frame in out.drain(..) {
            self.stream.lock().unwrap().push(frame);
            self.stream.lock().unwrap().push(frame);
            let written = self.written.fetch_add(1, SeqCst) + 1;
            if let Some(bucket) = &self.bucket {
                let refilled = bucket.lock().unwrap().refilled;
                assert!(written <= DEPTH + refilled, "{written} frames written on a budget of {DEPTH} + {refilled}");
            }
        }
    }

    /// The next frame of the outbox, if a capped session's bucket holds a token for it.
    fn next(&self) -> Option<usize> {
        let mut outbox = self.outbox.lock().unwrap();
        if let Some(bucket) = &self.bucket {
            let mut bucket = bucket.lock().unwrap();
            if outbox.is_empty() || bucket.tokens == 0 {
                return None;
            }
            bucket.tokens -= 1;
        }
        outbox.pop_front()
    }

    /// `write_loop`: wakes for a frame or the notify, then under the writer lock takes what else is queued, writes
    /// it all and counts it written. Returns once the senders are done and nothing would wake it.
    fn write_loop(&self) {
        loop {
            let done = self.finished.load(SeqCst) == SENDERS;
            let first = self.next();
            if first.is_none() && !std::mem::take(&mut *self.unfinished.lock().unwrap()) {
                if done && self.outbox.lock().unwrap().is_empty() {
                    return;
                }
                thread::yield_now();
                continue;
            }
            let mut out = self.writer.lock().unwrap();
            let mut dequeued = 0;
            let mut next = first;
            while let Some(frame) = next {
                out.push(frame);
                dequeued += 1;
                next = self.next();
            }
            self.write(&mut out);
            self.queued.written(dequeued);
        }
    }

    /// The clock filling a capped session's bucket a token at a time, as many as the frames past its depth. It never
    /// waits for the writer, so the model has no spin of its own.
    fn refill(&self, bucket: &Mutex<Bucket>) {
        for _ in DEPTH..SENDERS * FRAMES {
            let mut bucket = bucket.lock().unwrap();
            bucket.tokens += 1;
            bucket.refilled += 1;
        }
    }
}

fn check(model: impl Fn() + Send + Sync + 'static) {
    let mut builder = loom::model::Builder::new();
    builder.preemption_bound = Some(3);
    builder.check(model);
}

/// Two senders submit two frames each against the writer task, and on a capped session the clock refills its bucket.
fn run(socket: Socket, paced: bool) {
    check(move || {
        let session = Arc::new(Session::new(socket, paced));
        let senders: Vec<_> = (0..SENDERS)
            .map(|s| {
                let session = session.clone();
                thread::spawn(move || {
                    for f in 0..FRAMES {
                        session.submit(s * 10 + f);
                    }
                    session.finished.fetch_add(1, SeqCst);
                })
            })
            .collect();
        let clock = paced.then(|| {
            let session = session.clone();
            thread::spawn(move || session.refill(session.bucket.as_ref().expect("a paced session's")))
        });
        session.write_loop();
        senders.into_iter().chain(clock).for_each(|s| s.join().unwrap());
        let out = std::mem::take(&mut *session.writer.lock().unwrap());
        let stream = std::mem::take(&mut *session.stream.lock().unwrap());
        assert!(
            out.is_empty(),
            "an admitted frame is left unwritten while the writer task waits: {out:?} after {stream:?}"
        );
        assert!(stream.chunks(2).all(|p| p[0] == p[1]), "a frame torn by two writers at once: stream {stream:?}");
        for s in 0..SENDERS {
            let sent: Vec<_> = stream.chunks(2).map(|p| p[0]).filter(|&f| f / 10 == s).collect();
            let submitted: Vec<_> = (0..FRAMES).map(|f| s * 10 + f).collect();
            assert_eq!(sent, submitted, "sender {s}'s frames out of order or lost: stream {stream:?}");
        }
        assert_eq!(session.queued.count.load(SeqCst), 0, "frames counted queued after all were written");
    });
}

#[test]
fn each_senders_frames_reach_the_stream_in_submission_order() {
    run(Socket::Takes, false);
}

#[test]
fn a_frame_the_socket_did_not_take_at_once_is_written_by_the_writer_task_in_order() {
    run(Socket::Refuses, false);
}

#[test]
fn a_frame_left_to_the_writer_task_is_not_passed_by_the_senders_next_direct_write() {
    run(Socket::RefusesThenTakes, false);
}

/// Mutation check: let `Queued::write_now` ignore `paced`, and a sender's direct write passes the budget.
#[test]
fn a_paced_writer_keeps_each_senders_order_and_never_writes_past_its_budget() {
    run(Socket::Takes, true);
}
