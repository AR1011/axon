//! Faults inside the workload (spec/fault-rig.md § Fault classes): `fault.sh on hook <name> <ms>` writes
//! `<name> <ms>` to `LOG_DIR/ctl`, which this polls every 500 ms, logging a `hook` line at each change (`off` when the
//! file is gone). The accounts call `at` where each hook acts. A panic is logged as a `panic` line first, and panics
//! here, so the unit's log names this file: check.py's `panic-contained` and `unexpected-restart` read both.

use std::sync::atomic::{AtomicU8, AtomicU32, Ordering::Relaxed};
use std::time::Duration;

use serde_json::json;

use crate::account::Shared;

#[derive(Clone, Copy)]
pub enum Point {
    /// `on_start`, before it asks the store for the key.
    Start,
    /// `on_message`, for an op not answered from `done`.
    Message,
    /// The op's job, before it calls the store.
    Job,
}

const NAMES: [&str; 6] = ["off", "panic", "panic_start", "panic_job", "block", "jobsleep"];
static HOOK: AtomicU8 = AtomicU8::new(0);
static MS: AtomicU32 = AtomicU32::new(0);

pub fn start(cx: &crate::Ctx<'_>) {
    let (path, sh) = (cx.log_dir.join("ctl"), cx.sh.clone());
    tokio::spawn(async move {
        let mut tick = tokio::time::interval(Duration::from_millis(500));
        let mut last = String::new();
        loop {
            tick.tick().await;
            let now = std::fs::read_to_string(&path).unwrap_or_default();
            if now == last {
                continue;
            }
            let mut w = now.split_whitespace();
            let hook = w.next().and_then(|h| NAMES.iter().position(|n| *n == h)).unwrap_or(0);
            let ms = w.next().and_then(|m| m.parse().ok()).unwrap_or(0);
            MS.store(ms, Relaxed);
            HOOK.store(hook as u8, Relaxed);
            sh.node_log.line("hook", json!({"hook": NAMES[hook], "ms": ms}));
            last = now;
        }
    });
}

/// An account at `point`: panics, or holds its thread, as the hook on says.
pub fn at(point: Point, sh: &Shared, key: &str) {
    let hook = NAMES[HOOK.load(Relaxed) as usize];
    match (hook, point) {
        ("panic", Point::Message) | ("panic_start", Point::Start) | ("panic_job", Point::Job) => {
            sh.node_log.line("panic", json!({"k": key, "hook": hook}));
            panic!("rig hook {hook} on {key}");
        }
        // An axon worker held in a handler, or a job thread held in its job.
        ("block", Point::Message) | ("jobsleep", Point::Job) => {
            std::thread::sleep(Duration::from_millis(MS.load(Relaxed).into()))
        }
        _ => {}
    }
}
