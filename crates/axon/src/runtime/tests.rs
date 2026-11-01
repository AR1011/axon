use super::*;
use crate::test_support::Idle;

struct Unwinds(Arc<AtomicUsize>);

impl Drive for Unwinds {
    fn turn(&mut self, _: TurnBudget) -> (bool, bool) {
        panic!("a turn that unwinds")
    }

    fn abandon(&mut self) {
        self.0.fetch_add(1, Ordering::Relaxed);
    }

    fn owed(&self) -> Option<&'static str> {
        None
    }

    fn forsake(&mut self) {}
}

impl Drop for Unwinds {
    fn drop(&mut self) {
        self.0.fetch_add(10, Ordering::Relaxed);
    }
}

// Like the mailbox's, every byte of a driver is a megabyte of a million idle actors.
#[cfg(not(axon_loom))]
const _: () = assert!(size_of::<crate::driver::Driver<Idle>>() <= 200);

/// Lost wakeups made by hand on an idle actor, each in a child process the watchdog must abort: a message or a
/// drain arriving while the actor is unscheduled, and a scheduled actor in no worker's queue. And one it must not:
/// a wake that arrives late, every time.
#[cfg(debug_assertions)]
#[test]
fn the_watchdog_aborts_on_a_lost_wakeup() {
    const CHILD: &str = "AXON_LOST_WAKEUP";
    if let Ok(kind) = std::env::var(CHILD) {
        let rt = Runtime::new(RuntimeConfig { workers: 2, ..Default::default() }).unwrap();
        let handle = rt.spawn(Idle, Execution::Shared, MailboxLimits::default()).unwrap();
        handle.wait_ready(Duration::from_secs(5)).unwrap();
        std::thread::sleep(Duration::from_millis(50));
        let cell = rt.0.registry.lock().values().next().and_then(|(_, c)| c.upgrade()).unwrap();
        assert!(cell.scheduled.claim(), "idle after its start");
        if kind == "late" {
            // A machine too loaded to run a woken worker at once: every wake is sent, and arrives 100 ms later,
            // so every look finds a worker asleep with the actor queued, but never in the same sleep twice.
            let queue = &rt.0.pools[0].pool.queues[cell.home];
            for _ in 0..STALL_LOOKS * 4 {
                let parks = {
                    let mut ready = queue.ready.lock();
                    assert!(ready.sleeping);
                    cell.scheduled.0.store(true, Ordering::SeqCst);
                    ready.cells.push_back(cell.clone());
                    queue.queued.len.store(ready.cells.len(), Ordering::Relaxed);
                    ready.notified = true;
                    queue.parks.load(Ordering::Relaxed)
                };
                std::thread::sleep(STALL_LOOK / 2 - Duration::from_millis(25));
                queue.wake.notify_one();
                while queue.parks.load(Ordering::Relaxed) == parks {
                    std::thread::sleep(Duration::from_millis(1));
                }
            }
            return;
        }
        match kind.as_str() {
            "message" => _ = handle.address.try_tell(()).unwrap(),
            "drain" => handle.lifecycle.drain(None),
            _ => {}
        }
        if kind != "scheduled" {
            cell.scheduled.begin();
        }
        // Far past the abort, which a loaded machine may delay well beyond its two seconds.
        std::thread::sleep(STALL_LOOK * STALL_LOOKS * 20);
        panic!("not aborted");
    }
    let children = [
        ("message", Some("has a message to handle and is not scheduled")),
        ("drain", Some("has the end of a drain to handle and is not scheduled")),
        ("scheduled", Some("is scheduled and every worker of its pool sleeps")),
        ("late", None),
    ]
    .map(|(kind, expected)| {
        let child = std::process::Command::new(std::env::current_exe().unwrap())
            .args(["--exact", "runtime::tests::the_watchdog_aborts_on_a_lost_wakeup", "--nocapture"])
            .env(CHILD, kind)
            .stderr(std::process::Stdio::piped())
            .spawn()
            .unwrap();
        (kind, expected, child)
    });
    for (kind, expected, child) in children {
        let out = child.wait_with_output().unwrap();
        let stderr = String::from_utf8_lossy(&out.stderr);
        let held = expected.map_or(out.status.success(), |e| !out.status.success() && stderr.contains(e));
        assert!(held, "{kind}: {:?}\n{stderr}", out.status);
    }
}

#[test]
fn a_sleeping_worker_is_not_counted_busy() {
    let rt = Runtime::new(RuntimeConfig { workers: 1, ..Default::default() }).unwrap();
    std::thread::sleep(Duration::from_millis(300));
    let busy = rt.stats().workers[0].busy;
    assert!(busy < Duration::from_millis(100), "{busy:?}");
}

#[test]
fn a_turn_that_unwinds_ends_the_actor_and_frees_its_thread() {
    let seen = Arc::new(AtomicUsize::new(0));
    let cell = Cell::bare(Pool::new(1, true, false), Box::new(Unwinds(seen.clone())), true);
    cell.run();
    assert_eq!(seen.load(Ordering::Relaxed), 11, "abandoned once, then dropped");
    assert!(cell.drive.lock().is_none() && cell.pool.queues[0].ready.lock().shutdown);
}
