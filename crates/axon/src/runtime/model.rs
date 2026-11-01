use loom::thread;

use super::*;

/// Set once, waited for by others.
#[derive(Default)]
struct Done(Mutex<bool>, Condvar);

impl Done {
    fn set(&self) {
        *self.0.lock() = true;
        self.1.notify_all();
    }

    fn wait(&self) {
        let mut done = self.0.lock();
        while !*done {
            self.1.wait(&mut done);
        }
    }
}

/// An actor's turn: sets `ran`, after waiting for `after` to be set, a turn that does not end until then.
struct Turn {
    ran: Arc<Done>,
    after: Option<Arc<Done>>,
}

impl Drive for Turn {
    fn turn(&mut self, _: TurnBudget) -> (bool, bool) {
        if let Some(after) = &self.after {
            after.wait();
        }
        self.ran.set();
        (false, false)
    }

    fn abandon(&mut self) {}

    fn owed(&self) -> Option<&'static str> {
        None
    }

    fn forsake(&mut self) {}
}

fn cell(pool: &Arc<Pool>, turn: Turn) -> Arc<Cell> {
    Cell::bare(pool.clone(), Box::new(turn), false)
}

fn check(preemptions: usize, model: impl Fn() + Send + Sync + 'static) {
    let mut builder = loom::model::Builder::new();
    builder.preemption_bound = Some(preemptions);
    builder.check(model);
}

/// Two workers that sleep as soon as they find nothing (spin zero), and the test's thread, outside the pool. An actor
/// on worker 0 takes a turn that ends only once a second actor has run, which the test's thread wakes; its home is
/// worker 0 too, so it queues behind the turn, where only worker 1 can take it. Its sender wakes only worker 0, so
/// worker 1 must be awake or watching. A watching sleeper's `wait_for` is a wait that may time out
/// (`sync::Condvar::wait_for`); every other wait is untimed, so a schedule that leaves the actor queued with every
/// worker in one is a loom deadlock. Without `busy_before`, the long turn is queued before the workers start, so
/// worker 0 never sleeps before it. With it, worker 0 first runs a turn that ends, which worker 1 may watch through
/// and stop watching just as the long turn starts, and the long turn's push may find worker 0 asleep. Only that shape
/// holds `work`'s fence and `push`'s wake of a sleeping owner, so keep both.
fn queued_behind_a_turn_that_does_not_end(busy_before: bool) {
    // Three preemptions after a first turn take over ten minutes.
    check(if busy_before { 2 } else { 3 }, move || {
        let pool = Pool::new(2, true, false);
        let (long_ran, short_ran) = (Arc::new(Done::default()), Arc::new(Done::default()));
        let long = cell(&pool, Turn { ran: long_ran.clone(), after: Some(short_ran.clone()) });
        let short = cell(&pool, Turn { ran: short_ran, after: None });
        if busy_before {
            cell(&pool, Turn { ran: Arc::new(Done::default()), after: None }).notify();
        } else {
            long.notify();
        }
        let workers: Vec<_> = (0..2)
            .map(|i| {
                let pool = pool.clone();
                thread::spawn(move || pool.work(i, Duration::ZERO))
            })
            .collect();
        if busy_before {
            long.notify();
        }
        short.notify();
        long_ran.wait();
        pool.shutdown();
        workers.into_iter().for_each(|w| w.join().expect("a worker panicked"));
    });
}

#[test]
fn an_actor_queued_behind_a_turn_that_does_not_end_is_taken_by_a_sleeper() {
    queued_behind_a_turn_that_does_not_end(false);
}

/// Worker 0's fence before it reads `watching` holds this one: without it, it may read the count before worker 1's
/// decrement while worker 1 reads it still looking.
#[test]
fn a_sleeper_that_stops_watching_as_a_worker_turns_busy_again_is_counted_or_sees_it_busy() {
    queued_behind_a_turn_that_does_not_end(true);
}
