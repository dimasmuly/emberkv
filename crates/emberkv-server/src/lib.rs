//! # emberkv-server
//!
//! Network front-end for [`emberkv_engine`], with two interfaces:
//!
//! * a **JSON/HTTP API** for application backends (namespaces, TTL, atomic
//!   batches, counters, rate limits, conditional writes, API-key auth), and
//! * a compact **binary TCP protocol** for high-throughput and admin use.
//!
//! The binary in `main.rs` is a thin shell around [`Server`], which keeps the
//! server embeddable and easy to drive from integration tests.

#![warn(missing_docs)]

pub mod auth;
mod config;
mod connection;
mod http;
mod keyspace;
mod maintenance;
mod server;
pub mod telemetry;

pub use config::{CompactionPolicy, ServerConfig, SyncMode};
pub use server::Server;
