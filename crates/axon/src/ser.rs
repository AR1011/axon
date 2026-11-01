//! `serialize_with` helpers shared by the axon crates: durations as nanoseconds, `u128` as hex.

use std::time::Duration;

use serde::Serializer;

pub fn ns<S: Serializer>(d: &Duration, s: S) -> Result<S::Ok, S::Error> {
    s.serialize_u64(crate::clock::nanos(*d))
}

pub fn opt_ns<S: Serializer>(d: &Option<Duration>, s: S) -> Result<S::Ok, S::Error> {
    match d {
        Some(d) => ns(d, s),
        None => s.serialize_none(),
    }
}

pub fn hex<S: Serializer>(v: &u128, s: S) -> Result<S::Ok, S::Error> {
    s.collect_str(&format_args!("{v:032x}"))
}
