//! `ReplyState` alone: its callers' `if … { finish }` are outside the model, and the cluster tests cover them.

use loom::sync::Arc;
use loom::thread;

use super::*;

#[derive(Debug, PartialEq)]
enum Told {
    Reply,
    End(u8),
}

fn check(model: impl Fn() + Send + Sync + 'static) {
    let mut builder = loom::model::Builder::new();
    builder.preemption_bound = Some(3);
    builder.check(model);
}

/// The endpoint returns `admitted` on this thread while the actor, on another, sends the reply and then drops it, as
/// `Reply::send` does, or only drops it. Everything the caller is told.
fn race(admitted: Result<(), Rejection>, sends: bool) -> Vec<Told> {
    let state = Arc::new(ReplyState::new());
    let actor = state.clone();
    let answered = thread::spawn(move || {
        let sent = (sends && actor.reply()).then_some(Told::Reply);
        [sent, actor.drop_reply().then_some(Told::End(NO_REPLY))]
    });
    let settled = state.settle(admitted).map(Told::End);
    answered.join().unwrap().into_iter().chain([settled]).flatten().collect()
}

#[test]
fn a_reply_racing_an_admission_is_told_once() {
    check(|| assert_eq!(race(Ok(()), true), [Told::Reply]));
}

#[test]
fn a_reply_dropped_racing_an_admission_is_told_once_as_no_reply() {
    check(|| assert_eq!(race(Ok(()), false), [Told::End(NO_REPLY)]));
}

#[test]
fn a_reply_dropped_during_a_failed_admission_is_told_once_as_the_rejection() {
    check(|| assert_eq!(race(Err(Rejection::Full), false), [Told::End(Rejection::Full as u8)]));
}

#[test]
fn a_reply_racing_a_failed_admission_is_told_once_either_way() {
    check(|| {
        let told = race(Err(Rejection::Full), true);
        let rejected = [Told::End(Rejection::Full as u8)];
        assert!(told == [Told::Reply] || told == rejected, "told {told:?}");
    });
}
