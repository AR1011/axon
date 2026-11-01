//! Observability for axon. `Telemetry` samples one node and serves it to the cluster;
//! `MetricsService` collects every node's samples into a provider and answers history queries.
//!
//! Guide: [`axon::guide::observability`].

#![deny(clippy::unwrap_used)]
#[cfg(feature = "clickhouse")]
mod clickhouse;
mod control;
#[cfg(feature = "dashboard")]
mod dashboard;
#[cfg(feature = "dashboard")]
mod health;
mod metrics;
mod model;
mod prometheus;
mod provider;
mod sources;
mod telemetry;
mod traces;

#[cfg(feature = "clickhouse")]
pub use clickhouse::{ClickHouseConfig, ClickHouseProvider};
#[cfg(feature = "dashboard")]
pub use dashboard::{Dashboard, DashboardConfig, SessionConfig, User, hash_password};
#[cfg(feature = "dashboard")]
pub use health::Health;
pub use metrics::{
    HISTORY_SOURCE, METRICS_SERVICE, MetricsConfig, MetricsHealth, MetricsRequest, MetricsService, OWN_SEQUENCE,
    history_sources, query_metrics,
};
pub use model::*;
pub use provider::*;
pub use telemetry::{Telemetry, TelemetryConfig, fetch, fetch_samples};
pub use traces::*;

#[cfg(doctest)]
#[doc = include_str!("../../axon/guide/actors.md")]
#[doc = include_str!("../../axon/guide/cluster.md")]
#[doc = include_str!("../../axon/guide/streams.md")]
#[doc = include_str!("../../axon/guide/tracing.md")]
#[doc = include_str!("../../../README.md")]
struct Guides;

#[cfg(all(doctest, feature = "dashboard", feature = "clickhouse"))]
#[doc = include_str!("../../axon/guide/observability.md")]
#[doc = include_str!("../../axon/guide/dashboard.md")]
struct ObservabilityGuide;
