//! The three `Files` agree: `OsFiles`, `MemoryFiles` and the World's `Disk` hold the same after the same writes, and
//! `crashes` goes through every end of a crash inside a write.
use std::collections::BTreeSet;
use std::io::ErrorKind;
use std::path::Path;

use axon::files::{FileOp, Files, MemoryFiles, OsFiles};
use axon_testkit::world::Disk;
use axon_testkit::{check_seeds, crash_points, crashed, crashes, seeds};
use bytes::Bytes;
use common::{NAMES, contents, now, writes_over};

mod common;

type Seen = (Vec<(String, u64)>, Vec<Result<Vec<u8>, ErrorKind>>);

/// What a store sees of the files: the names and lengths listed, and each name's bytes or `NotFound`, whole and in
/// ranges that start and end inside it and past it.
async fn seen(files: &dyn Files) -> Seen {
    let mut listed = files.list().await.expect("listed");
    listed.sort();
    let mut read = Vec::new();
    for name in NAMES {
        read.push(files.read(name).await.map_err(|e| e.kind()));
        for (offset, len) in [(0, 0), (0, 2), (1, 1), (2, 9), (9, 1)] {
            read.push(files.read_range(name, offset, len).await.map_err(|e| e.kind()));
        }
    }
    (listed, read)
}

/// Applies `writes` to each of the files, from empty, and compares what each shows with what `OsFiles` does.
fn agree(dir: &Path, case: &str, writes: &[Vec<FileOp>]) {
    let runtime = tokio::runtime::Builder::new_current_thread().enable_time().start_paused(true).build().unwrap();
    // A directory of a disk whose other files it never shows.
    let machine = Disk::default();
    machine.append("a", b"other").unwrap();
    let stores: [(&str, Box<dyn Files>); 4] = [
        ("os", Box::new(OsFiles::new(dir))),
        ("memory", Box::new(MemoryFiles::new())),
        ("disk", Box::new(Disk::default())),
        ("a disk's directory", Box::new(machine.dir("store").dir("x"))),
    ];
    for (i, ops) in writes.iter().enumerate() {
        let mut views = Vec::new();
        for (what, files) in &stores {
            let applied = runtime.block_on(async { files.apply(ops.clone()).await });
            applied.unwrap_or_else(|e| panic!("{case} {what}: {e}"));
            views.push((what, runtime.block_on(seen(files.as_ref()))));
        }
        for (what, view) in &views[1..] {
            assert_eq!(view, &views[0].1, "{case}, write {i}, {what} against os: {writes:?}");
        }
    }
    assert_eq!(machine.read("a").as_deref(), Some(&b"other"[..]), "{case}");
}

#[test]
fn os_memory_and_world_files_agree_after_the_same_writes() {
    use FileOp::{Append, Remove, Replace, Truncate, Write};
    let fixed = [
        vec![vec![
            Append("a".into(), "0000".into()),
            Write("a".into(), 1, "xy".into()),
            Append("a".into(), "12".into()),
            Write("a".into(), 4, "ab".into()),
        ]],
        vec![vec![Append("a".into(), "12".into()), Remove("a".into()), Append("a".into(), "3".into())]],
        vec![vec![Append("a".into(), "12345".into()), Truncate("a".into(), 2), Truncate("a".into(), 4)]],
        vec![vec![Append("a".into(), "1".into())], vec![Remove("a".into())]],
        vec![vec![Append("a".into(), "12".into()), Replace("a".into(), "345".into()), Append("a".into(), "6".into())]],
        vec![
            vec![Replace("b".into(), "1".into())],
            vec![Append("b".into(), "2".into()), Replace("b".into(), "".into())],
        ],
    ];
    let root = std::env::temp_dir().join(format!("axon-files-agree-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&root);
    for (case, writes) in fixed.iter().enumerate() {
        agree(&root.join(format!("fixed-{case}")), &format!("case {case}"), writes);
    }
    let random = |seed: u64| agree(&root.join(seed.to_string()), &format!("seed {seed}"), &writes_over(seed));
    assert_eq!(check_seeds(seeds(0..200), random), Ok(()));
    std::fs::remove_dir_all(&root).unwrap();
}

fn append(name: &str, data: &'static [u8]) -> FileOp {
    FileOp::Append(name.into(), Bytes::from_static(data))
}

/// A crash inside a write ends in each way `Files` allows, and in no other: the ops before one that is not an append
/// land whole, and the appends before that op too, synced first; of the appends after it, each file keeps its first
/// bytes, from none to all, whatever the other files keep, and a file the write creates may not be there; a `Replace`
/// lands whole or not at all.
#[test]
fn a_crash_inside_a_write_ends_in_every_way_files_allow() {
    let files = MemoryFiles::new();
    now(files.apply(vec![
        append("a", b"0"),
        append("c", b"c"),
        FileOp::Replace("d".into(), Bytes::from_static(b"old")),
    ]));
    let held = contents(&files);
    let write = vec![
        append("a", b"12"),
        append("b", b"x"),
        append("a", b"3"),
        FileOp::Remove("c".into()),
        append("b", b"yz"),
        FileOp::Replace("d".into(), Bytes::from_static(b"new")),
    ];
    let file = |name: &str, data: &[u8]| (name.to_owned(), data.to_vec());
    let mut expected = BTreeSet::new();
    for a in 0..=3 {
        let a = file("a", &b"0123"[..1 + a]);
        expected.insert(vec![a.clone(), file("c", b"c"), file("d", b"old")]);
        for b in 0..=1 {
            expected.insert(vec![a.clone(), file("b", &b"x"[..b]), file("c", b"c"), file("d", b"old")]);
        }
    }
    for b in 0..=2 {
        expected.insert(vec![file("a", b"0123"), file("b", &b"xyz"[..1 + b]), file("d", b"old")]);
    }
    expected.insert(vec![file("a", b"0123"), file("b", b"xyz"), file("d", b"new")]);

    // a and b before the removal, each untouched or 0..=len bytes kept: 5 * 3; b after it, 4; the replacement, 1.
    assert_eq!(crash_points(&write), 5 * 3 + 4 + 1);
    let ends: BTreeSet<_> = crashes(&files, &write).map(|(_, copy)| contents(&copy)).collect();
    assert_eq!(ends, expected);
    assert_eq!(crashed(&write, 20), crashed(&write, 0), "n wraps");
    assert_eq!(contents(&files), held, "the files crashed are copies");
}
