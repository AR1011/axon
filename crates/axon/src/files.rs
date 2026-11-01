//! A directory of files, as the stores that keep state across a restart use it: a stream origin's history
//! (`axon_stream::DiskConfig`), a Paxos acceptor's leases (`axon_cluster::PaxosConfig::store`) and a [`Log`] of keyed
//! records. [`OsFiles`] is the file system, [`MemoryFiles`] keeps them in memory, and a test's own may drop what was not
//! synced.

use std::collections::BTreeMap;
use std::future::Future;
use std::io;
use std::pin::Pin;
use std::sync::Arc;

use bytes::Bytes;
use parking_lot::Mutex;

mod log;
pub use log::{Address, Appended, Entry, KeyInfo, Log, LogConfig, LogError, LogStats, Lsn};

/// What a [`Files`] call returns: it starts nothing until it is polled.
pub type FileFuture<T> = Pin<Box<dyn Future<Output = io::Result<T>> + Send>>;

/// A directory of files, as a store uses it. Each method returns a future that starts nothing until it is polled and
/// borrows nothing, so an actor's handler may call them and await them in one `cx.submit_future` job: a store reads
/// and writes in one step. A read sees every write whose future completed before the read was called.
pub trait Files: Send + Sync {
    /// Each file's name and length.
    fn list(&self) -> FileFuture<Vec<(String, u64)>>;

    /// The file's bytes, synced first: a process that crashed mid-write may have left some in the page cache only,
    /// and what is read is served as durable.
    fn read(&self, name: &str) -> FileFuture<Vec<u8>>;

    /// Up to `len` of the file's bytes from `offset` on, fewer past its end, synced first as `read` is. This default
    /// reads the whole file; `OsFiles` reads only the range.
    fn read_range(&self, name: &str, offset: u64, len: usize) -> FileFuture<Vec<u8>> {
        let read = self.read(name);
        Box::pin(async move {
            let mut data = read.await?;
            let start = offset.min(data.len() as u64) as usize;
            data.truncate(start.saturating_add(len));
            data.drain(..start);
            Ok(data)
        })
    }

    /// The ops in order, all durable once it returns `Ok`.
    ///
    /// What it appends or writes before a `Remove`, `Truncate` or `Replace` is synced before that op takes effect, so
    /// a crash never keeps the op and loses the appends before it: `[Append(new, ..), Remove(old)]` leaves one of the
    /// two whole. An apply with no such op after an append syncs once, at its end. Otherwise a crash inside an apply may
    /// leave any of its ops done or not, an append in part (a torn tail: frame what you append, with [`crc32`]), any
    /// part of a `Write`, and a `Replace` whole or not at all.
    ///
    /// On an error a truncation, removal or replacement may have happened, and the appends before the last that did
    /// stay, synced, as it does: they may be all that is left of what it removed. None of the appends after it stay. A
    /// `Write` may have landed in whole or in part. Applied again, the same ops end the same, but for appends that
    /// stayed, which are appended twice: frame what you append, and skip a frame read twice.
    fn apply(&self, ops: Vec<FileOp>) -> FileFuture<()>;
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum FileOp {
    /// Creates the file if it is missing.
    Append(String, Bytes),
    /// Cuts the file to at most this length. A missing file is not an error, nor for `Remove`: a write tried again
    /// may find it gone.
    Truncate(String, u64),
    Remove(String),
    /// The file's whole content, created if it is missing: after any crash it holds the old content or the new, never
    /// part of either.
    Replace(String, Bytes),
    /// Writes over the file's bytes from the offset on: it never grows the file, and fails with `InvalidInput` past
    /// its end and `NotFound` for a missing file. Synced as an append is. A file zero-filled once and then written over
    /// syncs faster than one that grows, whose new length must be synced too.
    Write(String, u64, Bytes),
}

const CRC: [u32; 256] = {
    let mut table = [0u32; 256];
    let mut i = 0;
    while i < 256 {
        let mut c = i as u32;
        let mut k = 0;
        while k < 8 {
            c = if c & 1 == 1 { 0xedb8_8320 ^ (c >> 1) } else { c >> 1 };
            k += 1;
        }
        table[i] = c;
        i += 1;
    }
    table
};

/// CRC-32 (IEEE), as zlib has it: what the stores' frames carry, so a torn or rotten tail is told from a record.
pub fn crc32(data: &[u8]) -> u32 {
    !data.iter().fold(!0u32, |c, b| CRC[((c ^ u32::from(*b)) & 0xff) as usize] ^ (c >> 8))
}

/// The bytes a `Write` of `len` at `offset` covers in a file of `file_len`: `InvalidInput` past its end.
#[doc(hidden)]
pub fn written_range(file_len: u64, offset: u64, len: usize) -> io::Result<std::ops::Range<usize>> {
    match offset.checked_add(len as u64) {
        Some(end) if end <= file_len => Ok(offset as usize..end as usize),
        _ => Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("a write of {len} bytes at {offset} is past the end, {file_len}"),
        )),
    }
}

/// Files in memory, shared by every clone and gone with the last: for tests, and for a store whose node never
/// restarts. Every write is durable at once.
#[derive(Clone, Default)]
pub struct MemoryFiles(Arc<Mutex<BTreeMap<String, Vec<u8>>>>);

impl MemoryFiles {
    pub fn new() -> Self {
        Self::default()
    }
}

impl Files for MemoryFiles {
    fn list(&self) -> FileFuture<Vec<(String, u64)>> {
        Box::pin(std::future::ready(Ok(self
            .0
            .lock()
            .iter()
            .map(|(name, data)| (name.clone(), data.len() as u64))
            .collect())))
    }

    fn read(&self, name: &str) -> FileFuture<Vec<u8>> {
        Box::pin(std::future::ready(self.0.lock().get(name).cloned().ok_or_else(|| io::ErrorKind::NotFound.into())))
    }

    fn apply(&self, ops: Vec<FileOp>) -> FileFuture<()> {
        let mut files = self.0.lock();
        let applied = ops.into_iter().try_for_each(|op| {
            match op {
                FileOp::Append(name, bytes) => files.entry(name).or_default().extend_from_slice(&bytes),
                FileOp::Truncate(name, len) => files.get_mut(&name).into_iter().for_each(|d| d.truncate(len as usize)),
                FileOp::Remove(name) => drop(files.remove(&name)),
                FileOp::Replace(name, bytes) => drop(files.insert(name, bytes.into())),
                FileOp::Write(name, offset, bytes) => {
                    let data = files.get_mut(&name).ok_or(io::ErrorKind::NotFound)?;
                    let range = written_range(data.len() as u64, offset, bytes.len())?;
                    data[range].copy_from_slice(&bytes);
                }
            }
            Ok(())
        });
        Box::pin(std::future::ready(applied))
    }
}

/// The file system, in one directory, created when it is first listed or written. Its reads and writes block, so each
/// runs on tokio's blocking pool when polled inside a tokio runtime, and in place, inside the poll, where none is
/// entered: on a `JobExecutor::thread_pool` job's thread, which is there to block. A custom executor that polls jobs on
/// a reactor of another kind would block that reactor. The files a write appended to are synced together, up to 32 at
/// once on threads of the write's own: a sync waits for the device, which takes those of several files together.
///
/// The first list or write locks the directory (its file `.lock`) until this is dropped and every write it started has
/// landed: another `OsFiles` on the directory, in this process or another, fails meanwhile with `ResourceBusy`, and a
/// store starting on it with that error. A `Replace` writes `.replacing.<name>`, syncs it and renames it over the file;
/// `list` never shows one, and the first lock removes what a crash left of one.
#[cfg(feature = "system")]
pub struct OsFiles(Arc<Dir>);

#[cfg(feature = "system")]
struct Dir {
    path: std::path::PathBuf,
    lock: Mutex<Option<std::fs::File>>,
}

#[cfg(feature = "system")]
const LOCK: &str = ".lock";
#[cfg(feature = "system")]
const REPLACING: &str = ".replacing.";

#[cfg(feature = "system")]
impl OsFiles {
    pub fn new(dir: impl Into<std::path::PathBuf>) -> Self {
        Self(Arc::new(Dir { path: dir.into(), lock: Mutex::new(None) }))
    }

    fn apply_now(dir: &std::path::Path, ops: &[FileOp]) -> io::Result<()> {
        std::fs::create_dir_all(dir)?;
        let mut write = Batch { dir, appended: Vec::new(), written: Vec::new(), created: false, removed: false };
        let result = ops.iter().try_for_each(|op| write.op(op)).and_then(|()| write.sync(true));
        if result.is_err() {
            for appended in &write.appended {
                let _ = appended.file.set_len(appended.before);
            }
        }
        result
    }
}

#[cfg(feature = "system")]
impl Dir {
    fn lock(&self) -> io::Result<()> {
        let mut lock = self.lock.lock();
        if lock.is_none() {
            std::fs::create_dir_all(&self.path)?;
            let file =
                std::fs::OpenOptions::new().create(true).truncate(false).write(true).open(self.path.join(LOCK))?;
            file.try_lock().map_err(|e| match e {
                std::fs::TryLockError::WouldBlock => io::Error::new(
                    io::ErrorKind::ResourceBusy,
                    format!("{} is in use by another store", self.path.display()),
                ),
                std::fs::TryLockError::Error(e) => e,
            })?;
            // No write of this `OsFiles` has begun: each takes the lock first.
            for entry in std::fs::read_dir(&self.path)? {
                let name = entry?.file_name();
                if name.to_str().is_some_and(|n| n.starts_with(REPLACING)) {
                    std::fs::remove_file(self.path.join(name))?;
                }
            }
            *lock = Some(file);
        }
        Ok(())
    }
}

/// One `apply` under way.
#[cfg(feature = "system")]
struct Batch<'a> {
    dir: &'a std::path::Path,
    /// Each file appended to since the last op that removed, cut or replaced a file, cut back to its length before on
    /// an error. What was appended before that op stays: synced before it, it may be all that is left of what it
    /// removed.
    appended: Vec<AppendedFile<'a>>,
    /// Each file written over since the last sync. What a `Write` wrote over cannot be cut back.
    written: Vec<(&'a str, std::fs::File)>,
    /// Names made, and removed, since the directory was last synced.
    created: bool,
    removed: bool,
}

#[cfg(feature = "system")]
struct AppendedFile<'a> {
    file: std::fs::File,
    name: &'a str,
    before: u64,
    unsynced: bool,
}

#[cfg(feature = "system")]
impl<'a> Batch<'a> {
    fn op(&mut self, op: &'a FileOp) -> io::Result<()> {
        use std::fs::{self, OpenOptions};
        use std::io::Write;
        match op {
            FileOp::Append(name, bytes) => {
                let at = match self.appended.iter().position(|a| a.name == name) {
                    Some(at) => at,
                    None => {
                        let path = self.dir.join(name);
                        self.created |= !path.exists();
                        let file = OpenOptions::new().create(true).append(true).open(path)?;
                        let before = file.metadata()?.len();
                        self.appended.push(AppendedFile { file, name, before, unsynced: false });
                        self.appended.len() - 1
                    }
                };
                let appended = &mut self.appended[at];
                appended.file.write_all(bytes)?;
                appended.unsynced = true;
            }
            FileOp::Truncate(name, len) => {
                self.sync(false)?;
                match OpenOptions::new().write(true).open(self.dir.join(name)) {
                    Err(e) if e.kind() == io::ErrorKind::NotFound => {}
                    file => {
                        let file = file?;
                        // Synced at once: a tail a boot cut that came back after a machine crash would be cut again,
                        // and every segment written since removed with it. Cut, it has landed, though its sync fails.
                        if file.metadata()?.len() > *len {
                            file.set_len(*len)?;
                            self.appended.clear();
                            #[cfg(test)]
                            if tests::fails(tests::SyncPoint::Truncated) {
                                return Err(io::Error::other("the truncated file's sync failed"));
                            }
                            file.sync_data()?;
                        }
                    }
                }
            }
            FileOp::Remove(name) => {
                self.sync_but(name)?;
                // Gone already, perhaps by a try of this write whose directory sync failed: synced at the end too.
                self.removed = true;
                match fs::remove_file(self.dir.join(name)) {
                    Err(e) if e.kind() == io::ErrorKind::NotFound => self.appended.retain(|a| a.name != name),
                    Err(e) => return Err(e),
                    Ok(()) => self.appended.clear(),
                }
            }
            FileOp::Replace(name, bytes) => {
                self.sync_but(name)?;
                let temp = self.dir.join(format!("{REPLACING}{name}"));
                let mut file = OpenOptions::new().create(true).write(true).truncate(true).open(&temp)?;
                file.write_all(bytes)?;
                file.sync_data()?;
                fs::rename(&temp, self.dir.join(name))?;
                self.created = true;
                self.appended.clear();
            }
            FileOp::Write(name, offset, bytes) => {
                use std::io::{Seek, SeekFrom};
                // Not the append handle: on Linux a positioned write to a file opened to append lands at its end.
                let mut file = OpenOptions::new().write(true).open(self.dir.join(name))?;
                written_range(file.metadata()?.len(), *offset, bytes.len())?;
                file.seek(SeekFrom::Start(*offset))?;
                file.write_all(bytes)?;
                self.written.push((name, file));
            }
        }
        Ok(())
    }

    /// `sync`, but for `name`, which the op about to run replaces: its appends stay to be cut back if the op fails, and
    /// what was written over it is not synced, as the World's `Disk` charges it.
    fn sync_but(&mut self, name: &str) -> io::Result<()> {
        self.appended.iter_mut().filter(|a| a.name == name).for_each(|a| a.unsynced = false);
        self.written.retain(|(written, _)| *written != name);
        self.sync(false)
    }

    /// What was appended or written and the names made, before an op that may remove the only other copy; at the end
    /// also the names removed. Nothing when none of it changed since the last sync.
    fn sync(&mut self, end: bool) -> io::Result<()> {
        let appended = || self.appended.iter().filter(|a| a.unsynced);
        // A sync of a file takes what was written through any of its handles: one written over and appended to once.
        let written = self.written.iter().filter(|(name, _)| !appended().any(|a| a.name == *name));
        let unsynced: Vec<&std::fs::File> = appended().map(|a| &a.file).chain(written.map(|(_, file)| file)).collect();
        sync_each(&unsynced)?;
        self.appended.iter_mut().for_each(|a| a.unsynced = false);
        self.written.clear();
        if self.created || end && self.removed {
            #[cfg(test)]
            if end && tests::fails(tests::SyncPoint::Dir) {
                return Err(io::Error::other("the directory's sync failed"));
            }
            std::fs::File::open(self.dir)?.sync_all()?;
            (self.created, self.removed) = (false, false);
        }
        Ok(())
    }
}

/// At most this many files of one apply are synced at once.
#[cfg(feature = "system")]
const SYNCS_AT_ONCE: usize = 32;

/// Syncs each file, up to `SYNCS_AT_ONCE` at a time: a sync waits for the device, and the device takes syncs of
/// different files together. Measured on two NVMe drives without power-loss protection, 256 files synced one after
/// another took 343-379 ms and on 32 threads 86-95 ms.
#[cfg(feature = "system")]
fn sync_each(files: &[&std::fs::File]) -> io::Result<()> {
    use std::sync::atomic::{AtomicUsize, Ordering};
    #[cfg(test)]
    let (delay, fail) = (tests::SYNC_DELAY.get(), tests::FAIL_FILE.take());
    let next = AtomicUsize::new(0);
    let work = || -> io::Result<()> {
        loop {
            let i = next.fetch_add(1, Ordering::Relaxed);
            let Some(file) = files.get(i) else { return Ok(()) };
            #[cfg(test)]
            std::thread::sleep(delay);
            // A test's delay stands in for the sync, so a timing test measures overlap, not the shared disk.
            #[cfg(test)]
            let synced = if fail == Some(i) {
                Err(io::Error::other("a file's sync failed"))
            } else if delay.is_zero() {
                file.sync_data()
            } else {
                Ok(())
            };
            #[cfg(not(test))]
            let synced = file.sync_data();
            if let Err(e) = synced {
                // The apply fails and cuts every append back: the files left need no sync.
                next.fetch_max(files.len(), Ordering::Relaxed);
                return Err(e);
            }
        }
    };
    if files.len() < 2 {
        return work();
    }
    std::thread::scope(|scope| {
        // Threads of their own, not the blocking pool's: `OsFiles` also runs where no tokio runtime is entered. Each
        // lives for one apply, which is short next to the syncs it overlaps. One the OS refuses leaves its share to
        // the others.
        #[expect(clippy::disallowed_methods, reason = "fdatasync blocks; OsFiles never runs in a simulation")]
        let helpers: Vec<_> = (1..files.len().min(SYNCS_AT_ONCE))
            .map_while(|_| std::thread::Builder::new().spawn_scoped(scope, work).ok())
            .collect();
        let mine = work();
        helpers.into_iter().fold(mine, |out, helper| {
            out.and(helper.join().unwrap_or_else(|_| Err(io::Error::other("a file's sync panicked"))))
        })
    })
}

#[cfg(feature = "system")]
impl OsFiles {
    /// `f` on the directory once polled: on the blocking pool inside a tokio runtime, in place outside one.
    fn run<T: Send + 'static>(&self, f: impl FnOnce(&Dir) -> io::Result<T> + Send + 'static) -> FileFuture<T> {
        let dir = self.0.clone();
        Box::pin(async move {
            let Ok(runtime) = tokio::runtime::Handle::try_current() else { return f(&dir) };
            #[expect(
                clippy::disallowed_methods,
                reason = "file reads, writes and fsync block; they run on the blocking pool"
            )]
            let job = runtime.spawn_blocking(move || f(&dir));
            job.await.unwrap_or_else(|e| Err(io::Error::other(e)))
        })
    }
}

#[cfg(feature = "system")]
impl Files for OsFiles {
    fn list(&self) -> FileFuture<Vec<(String, u64)>> {
        self.run(|dir| {
            dir.lock()?;
            let mut files = Vec::new();
            for entry in std::fs::read_dir(&dir.path)? {
                let entry = entry?;
                if let Ok(name) = entry.file_name().into_string()
                    && name != LOCK
                    && !name.starts_with(REPLACING)
                {
                    files.push((name, entry.metadata()?.len()));
                }
            }
            // The names are read as durable too.
            std::fs::File::open(&dir.path)?.sync_all()?;
            Ok(files)
        })
    }

    fn read(&self, name: &str) -> FileFuture<Vec<u8>> {
        self.read_range(name, 0, usize::MAX)
    }

    fn read_range(&self, name: &str, offset: u64, len: usize) -> FileFuture<Vec<u8>> {
        let name = name.to_owned();
        self.run(move |dir| {
            use std::io::{Read, Seek, SeekFrom};
            let mut file = std::fs::File::open(dir.path.join(name))?;
            file.sync_data()?;
            if offset > 0 {
                file.seek(SeekFrom::Start(offset))?;
            }
            let left = file.metadata()?.len().saturating_sub(offset);
            let mut data = Vec::with_capacity(left.min(len as u64) as usize);
            file.take(len as u64).read_to_end(&mut data)?;
            Ok(data)
        })
    }

    fn apply(&self, ops: Vec<FileOp>) -> FileFuture<()> {
        self.run(move |dir| {
            dir.lock()?;
            Self::apply_now(&dir.path, &ops)
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A sync that `FAIL_SYNC` fails.
    #[derive(Clone, Copy, Debug, PartialEq, Eq)]
    pub(super) enum SyncPoint {
        /// The directory's, at the end of an `apply_now`.
        Dir,
        /// A truncated file's.
        Truncated,
    }

    thread_local! {
        /// The next such sync of an `apply_now` on this thread fails.
        pub(super) static FAIL_SYNC: std::cell::Cell<Option<SyncPoint>> = const { std::cell::Cell::new(None) };
        /// The sync of this file of the next files synced together, by index, fails.
        pub(super) static FAIL_FILE: std::cell::Cell<Option<usize>> = const { std::cell::Cell::new(None) };
        /// How long each file's sync takes on top of its own, for a disk slower than the test's.
        pub(super) static SYNC_DELAY: std::cell::Cell<std::time::Duration> =
            const { std::cell::Cell::new(std::time::Duration::ZERO) };
    }

    /// Whether `sync` is the one to fail, once.
    pub(super) fn fails(sync: SyncPoint) -> bool {
        FAIL_SYNC.get() == Some(sync) && FAIL_SYNC.take().is_some()
    }

    #[test]
    fn crc32_is_zlibs() {
        assert_eq!(crc32(b"123456789"), 0xcbf4_3926);
        assert_eq!(crc32(b""), 0);
    }

    #[cfg(feature = "system")]
    fn temp_dir(test: &str) -> std::path::PathBuf {
        let dir = std::env::temp_dir().join(format!("axon-files-{test}-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        dir
    }

    /// Polls once where no tokio runtime is entered, as a `JobExecutor::thread_pool` job's thread does: `OsFiles`
    /// works in place there, so its future is ready at once.
    #[cfg(feature = "system")]
    fn in_place<T>(future: FileFuture<T>) -> io::Result<T> {
        let mut future = future;
        match future.as_mut().poll(&mut std::task::Context::from_waker(std::task::Waker::noop())) {
            std::task::Poll::Ready(out) => out,
            std::task::Poll::Pending => panic!("OsFiles waited where no tokio runtime runs"),
        }
    }

    /// An actor's handler runs where no tokio runtime does: it calls them there and hands the futures to
    /// `cx.submit_future`, which polls them on one. Nothing is read, written or locked before.
    #[cfg(feature = "system")]
    #[test]
    fn os_files_start_nothing_until_polled() {
        let dir = temp_dir("unpolled");
        let files = OsFiles::new(&dir);
        let (listed, read) = (files.list(), files.read("a"));
        let write = files.apply(vec![FileOp::Append("a".into(), Bytes::from_static(b"x"))]);
        assert!(!dir.exists(), "the calls made the directory");
        let runtime = tokio::runtime::Builder::new_current_thread().build().expect("a runtime");
        let ((listed, read), written) = runtime.block_on(async { ((listed.await, read.await), write.await) });
        assert_eq!((listed.expect("listed"), read.map_err(|e| e.kind())), (vec![], Err(io::ErrorKind::NotFound)));
        written.expect("written");
        assert_eq!(runtime.block_on(files.read("a")).expect("a"), b"x");
        drop(files);
        std::fs::remove_dir_all(&dir).expect("cleaned up");
    }

    #[cfg(feature = "system")]
    #[test]
    fn os_files_work_in_place_where_no_tokio_runtime_runs() {
        let dir = temp_dir("in-place");
        let files = OsFiles::new(&dir);
        in_place(files.apply(vec![FileOp::Append("a".into(), Bytes::from_static(b"xyz"))])).expect("written");
        assert_eq!(in_place(files.list()).expect("listed"), [("a".to_owned(), 3)]);
        assert_eq!(in_place(files.read_range("a", 1, 1)).expect("read"), b"y");
        drop(files);
        std::fs::remove_dir_all(&dir).expect("cleaned up");
    }

    /// What a crash left of a `Replace` is never listed, and goes with the first lock; the file it was to replace keeps
    /// its old content.
    #[cfg(feature = "system")]
    #[test]
    fn os_files_drop_what_a_crash_left_of_a_replace() {
        let dir = temp_dir("replacing");
        OsFiles::apply_now(&dir, &[FileOp::Replace("f".into(), Bytes::from_static(b"old"))]).expect("written");
        std::fs::write(dir.join(format!("{REPLACING}f")), b"ne").expect("a torn replacement");
        let files = OsFiles::new(&dir);
        assert_eq!(in_place(files.list()).expect("listed"), [("f".to_owned(), 3)]);
        assert!(!dir.join(format!("{REPLACING}f")).exists(), "removed");
        assert_eq!(in_place(files.read_range("f", 1, 5)).expect("f"), b"ld");
        drop(files);
        std::fs::remove_dir_all(&dir).expect("cleaned up");
    }

    /// A write that fails partway leaves none of its appends after the last op that changed a file, and taken again it
    /// goes through: what it truncated and removed before the failure is not an error the second time.
    #[cfg(feature = "system")]
    #[test]
    fn os_files_cut_a_failed_write_back_and_take_it_again() {
        let dir = temp_dir("failed");
        let apply = |ops: &[FileOp]| OsFiles::apply_now(&dir, ops);
        apply(&[FileOp::Append("a".into(), Bytes::from_static(b"12345")), FileOp::Append("b".into(), Bytes::new())])
            .expect("a first write");
        // A directory where a file is to be appended to stands for a disk that fails mid-write.
        std::fs::create_dir(dir.join("c")).expect("a directory");
        let ops = [
            FileOp::Truncate("a".into(), 3),
            FileOp::Truncate("b".into(), 0),
            FileOp::Remove("b".into()),
            FileOp::Append("a".into(), Bytes::from_static(b"xy")),
            FileOp::Append("c".into(), Bytes::from_static(b"z")),
        ];
        assert!(apply(&ops).is_err());
        assert_eq!(std::fs::read(dir.join("a")).expect("a"), b"123");
        std::fs::remove_dir(dir.join("c")).expect("the directory");
        apply(&ops).expect("the write again");
        let files = OsFiles::new(&dir);
        let mut listed = in_place(files.list()).expect("listed");
        listed.sort();
        assert_eq!(listed, [("a".to_owned(), 5), ("c".to_owned(), 1)]);
        assert_eq!(in_place(files.read("a")).expect("a"), b"123xy");
        assert!(in_place(OsFiles::new(&dir).list()).is_err_and(|e| e.kind() == io::ErrorKind::ResourceBusy), "locked");
        drop(files);
        std::fs::remove_dir_all(&dir).expect("cleaned up");
    }

    /// A `Write` lands over bytes inside the file, those an apply appended before it too, and is refused past the end
    /// or on a missing file, as `MemoryFiles` and the World's `Disk` do (`axon-testkit/tests/files.rs`).
    #[cfg(feature = "system")]
    #[test]
    fn os_files_write_over_bytes_inside_the_file_only() {
        let dir = temp_dir("write");
        let apply = |ops: &[FileOp]| OsFiles::apply_now(&dir, ops).map_err(|e| e.kind());
        let write = |offset, data: &'static [u8]| FileOp::Write("a".into(), offset, Bytes::from_static(data));
        apply(&[FileOp::Append("a".into(), Bytes::from_static(b"0000"))]).expect("zeros");
        let ops = [write(1, b"xy"), FileOp::Append("a".into(), Bytes::from_static(b"12")), write(4, b"ab")];
        apply(&ops).expect("written");
        assert_eq!(std::fs::read(dir.join("a")).expect("a"), b"0xy0ab");
        assert_eq!(apply(&[write(5, b"zz")]), Err(io::ErrorKind::InvalidInput));
        assert_eq!(apply(&[FileOp::Write("b".into(), 0, Bytes::new())]), Err(io::ErrorKind::NotFound));
        assert_eq!(std::fs::read(dir.join("a")).expect("a"), b"0xy0ab");
        std::fs::remove_dir_all(&dir).expect("cleaned up");
    }

    /// The files one apply appended to are synced together, not one after another: each sync waits for the device, and
    /// 256 busy streams would wait for 256 syncs in a row.
    #[cfg(feature = "system")]
    #[test]
    fn os_files_sync_the_files_of_an_apply_together() {
        let dir = temp_dir("together");
        let delay = std::time::Duration::from_millis(20);
        let ops: Vec<FileOp> = (0..16).map(|i| FileOp::Append(format!("f{i}"), Bytes::from_static(b"x"))).collect();
        SYNC_DELAY.set(delay);
        #[expect(clippy::disallowed_methods, reason = "the test times real syncs")]
        let started = std::time::Instant::now();
        OsFiles::apply_now(&dir, &ops).expect("written");
        #[expect(clippy::disallowed_methods, reason = "the test times real syncs")]
        let took = started.elapsed();
        SYNC_DELAY.set(std::time::Duration::ZERO);
        assert!(took < delay * 8, "16 syncs of {delay:?} took {took:?}");
        std::fs::remove_dir_all(&dir).expect("cleaned up");
    }

    /// One file's sync that fails, of several synced together, fails the apply, and every file it appended to is cut
    /// back: none of its appends stays, whichever sync failed.
    #[cfg(feature = "system")]
    #[test]
    fn os_files_cut_back_every_append_when_one_of_the_syncs_together_fails() {
        let dir = temp_dir("together-failed");
        let apply = |ops: &[FileOp]| OsFiles::apply_now(&dir, ops);
        let names: Vec<String> = (0..8).map(|i| format!("f{i}")).collect();
        let ops = |data: &'static [u8]| -> Vec<FileOp> {
            names.iter().map(|n| FileOp::Append(n.clone(), Bytes::from_static(data))).collect()
        };
        apply(&ops(b"1")).expect("a first write");
        for failing in [0, 5, 7] {
            FAIL_FILE.set(Some(failing));
            assert!(apply(&ops(b"23")).is_err(), "the sync of file {failing} failed");
            assert_eq!(FAIL_FILE.get(), None);
            for name in &names {
                assert_eq!(std::fs::read(dir.join(name)).expect("there"), b"1", "{name} after {failing} failed");
            }
        }
        apply(&ops(b"2")).expect("the write again");
        assert!(names.iter().all(|name| std::fs::read(dir.join(name)).expect("there") == b"12"));
        std::fs::remove_dir_all(&dir).expect("cleaned up");
    }

    /// A `Replace` that fails leaves none of the appends before it to the same file (as a `Remove` that fails).
    #[cfg(feature = "system")]
    #[test]
    fn os_files_cut_back_the_appends_to_a_file_whose_replacement_failed() {
        let dir = temp_dir("failed-replace");
        let apply = |ops: &[FileOp]| OsFiles::apply_now(&dir, ops);
        apply(&[FileOp::Append("a".into(), Bytes::from_static(b"1"))]).expect("a first write");
        // A directory where the replacement is to be written fails it.
        std::fs::create_dir(dir.join(format!("{REPLACING}a"))).expect("a directory");
        let ops = [FileOp::Append("a".into(), Bytes::from_static(b"23")), FileOp::Replace("a".into(), Bytes::new())];
        assert!(apply(&ops).is_err());
        assert_eq!(std::fs::read(dir.join("a")).expect("a"), b"1");
        std::fs::remove_dir_all(&dir).expect("cleaned up");
    }

    /// A write that fails after a `Remove`, `Truncate` or `Replace` changed a file, at that op's own sync, at the
    /// directory's sync at its end or at an op after it, keeps what it appended before that op: cut back, it would
    /// leave neither the old file whole nor the new one.
    #[cfg(feature = "system")]
    #[test]
    fn os_files_keep_the_appends_an_op_that_landed_replaced() {
        let dir = temp_dir("landed");
        let apply = |ops: &[FileOp]| OsFiles::apply_now(&dir, ops);
        let new = || FileOp::Append("new".into(), Bytes::from_static(b"12"));
        let landed = [
            (FileOp::Remove("old".into()), Some(SyncPoint::Dir)),
            (FileOp::Truncate("old".into(), 0), Some(SyncPoint::Truncated)),
            (FileOp::Truncate("old".into(), 0), None),
            (FileOp::Replace("old".into(), Bytes::new()), None),
        ];
        for (op, fails) in landed {
            let _ = std::fs::remove_file(dir.join("new"));
            apply(&[FileOp::Replace("old".into(), Bytes::from_static(b"1"))]).expect("a first write");
            let failing = match fails {
                Some(sync) => {
                    FAIL_SYNC.set(Some(sync));
                    vec![new(), op]
                }
                None => {
                    // A directory where a file is to be appended to stands for a disk that fails mid-write.
                    std::fs::create_dir_all(dir.join("c")).expect("a directory");
                    vec![new(), op, FileOp::Append("c".into(), Bytes::from_static(b"z"))]
                }
            };
            assert!(apply(&failing).is_err(), "{failing:?}");
            assert_eq!(FAIL_SYNC.get(), None, "{failing:?}");
            assert_eq!(std::fs::read(dir.join("new")).ok().as_deref(), Some(&b"12"[..]), "{failing:?}");
            assert_ne!(std::fs::read(dir.join("old")).ok().as_deref(), Some(&b"1"[..]), "{failing:?}");
        }
        std::fs::remove_dir_all(&dir).expect("cleaned up");
    }
}
