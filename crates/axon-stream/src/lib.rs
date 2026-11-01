//! Sequenced streams for axon.
//!
//! [`StreamHub`] is the synchronous core: open a stream, publish numbered batches, subscribe
//! from live or from a position, and get a gap-free, duplicate-free sequence or an explicit
//! reset. With the `cluster` feature, [`StreamNode`] runs a hub on a cluster node and links
//! hubs into a tree: leaves pull each stream once through a preferred relay, fall back to
//! the next candidate when it fails, and move back when it returns.
//!
//! Guide: `axon::guide::streams`, in the `axon` crate.

#![deny(clippy::unwrap_used, clippy::iter_over_hash_type)]
mod core;
mod disk;
mod hub;
#[cfg(feature = "cluster")]
mod node;
mod recovery;
pub mod wire;

pub use core::*;
pub use disk::*;
pub use hub::*;
#[cfg(feature = "cluster")]
pub use node::*;
pub use recovery::*;
