//! What a crash inside a write can leave on disk, every case of it, for a store's tests over `MemoryFiles`: write,
//! crash at each point, open what is left, and check that what the store confirmed before reads back.

use std::task::{Context, Poll, Waker};

use axon::files::{FileFuture, FileOp, Files, MemoryFiles};

/// How many ends [`crashed`] tells apart for a write of `ops`, one for each `n` below it (saturating at `u64::MAX`,
/// where a test draws `n` rather than going through them).
pub fn crash_points(ops: &[FileOp]) -> u64 {
    landed(ops).map(|at| ends(&grown(&ops[at..]))).fold(0, u64::saturating_add)
}

/// What a crash inside a write leaves of its `ops`, `n` picking which (modulo [`crash_points`]), as `Files` allows and
/// the World's `Crash::Machine` may: the ops before some op that is not an append, which lands whole, with every
/// append before it, which `Files` synced first; then of the appends up to the next such op, each file keeps some of
/// what they add to it, the first bytes, from none to all, or not even the file a first append creates. So every byte
/// of each append ends a write, and every op that is not one lands whole or not at all: a `Replace` never in part.
/// Some ends come twice: keeping none of a file that was there is the same as not appending to it. A `Write` is taken
/// as such an op, whole or not at all, though `Files` lets it land in part: a store that writes over bytes tears its
/// writes itself (`files::Log`'s tests do).
pub fn crashed(ops: &[FileOp], n: u64) -> Vec<FileOp> {
    let mut n = n % crash_points(ops);
    for at in landed(ops) {
        let files = grown(&ops[at..]);
        let count = ends(&files);
        if n >= count {
            n -= count;
            continue;
        }
        // None: not appended to at all; Some(k): its first k bytes.
        let mut keep: Vec<_> = files
            .into_iter()
            .map(|(name, len)| {
                let kept = (n % (len + 2)).checked_sub(1);
                n /= len + 2;
                (name, kept)
            })
            .collect();
        let mut left = ops[..at].to_vec();
        for op in &ops[at..] {
            let FileOp::Append(name, data) = op else { break };
            let kept = &mut keep.iter_mut().find(|(file, _)| *file == name).expect("each file appended to").1;
            if let Some(kept) = kept {
                let part = (*kept).min(data.len() as u64);
                *kept -= part;
                left.push(FileOp::Append(name.clone(), data.slice(..part as usize)));
            }
        }
        return left;
    }
    unreachable!("n is below the count of every end")
}

/// Every crash inside a write of `ops` to `files`, each on a copy of them, with the `n` that [`crashed`] takes for
/// it: `files` stays as it is.
pub fn crashes(files: &MemoryFiles, ops: &[FileOp]) -> impl Iterator<Item = (u64, MemoryFiles)> + use<> {
    let held: Vec<FileOp> = ready(files.list())
        .into_iter()
        .map(|(name, _)| {
            let data = ready(files.read(&name));
            FileOp::Replace(name, data.into())
        })
        .collect();
    let ops = ops.to_vec();
    (0..crash_points(&ops)).map(move |n| {
        let copy = MemoryFiles::new();
        ready(copy.apply(held.clone()));
        ready(copy.apply(crashed(&ops, n)));
        (n, copy)
    })
}

/// Where the ops kept whole end: none, or each op that is not an append, just after it.
fn landed(ops: &[FileOp]) -> impl Iterator<Item = usize> + '_ {
    let barriers = ops.iter().enumerate().filter(|(_, op)| !matches!(op, FileOp::Append(..)));
    std::iter::once(0).chain(barriers.map(|(i, _)| i + 1))
}

/// Each file the appends at the start of `ops` add to, in order, with how much they add.
fn grown(ops: &[FileOp]) -> Vec<(&str, u64)> {
    let mut files: Vec<(&str, u64)> = Vec::new();
    for op in ops {
        let FileOp::Append(name, data) = op else { break };
        match files.iter_mut().find(|(file, _)| *file == name) {
            Some((_, len)) => *len += data.len() as u64,
            None => files.push((name, data.len() as u64)),
        }
    }
    files
}

/// How many ways the files can keep a part of what they grow by, or be left alone.
fn ends(files: &[(&str, u64)]) -> u64 {
    files.iter().map(|(_, len)| len.saturating_add(2)).fold(1, u64::saturating_mul)
}

/// A `MemoryFiles` call's result: ready at once.
fn ready<T>(mut future: FileFuture<T>) -> T {
    match future.as_mut().poll(&mut Context::from_waker(Waker::noop())) {
        Poll::Ready(out) => out.expect("files in memory"),
        Poll::Pending => panic!("files in memory waited"),
    }
}
