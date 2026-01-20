//! HTTP transport for `ClickHouse` using `ArrowStream` format.
//!
//! This module provides an alternative to the native TCP protocol, using HTTP
//! with `ClickHouse`'s `FORMAT ArrowStream` for Arrow-native data exchange.
//!
//! # Example
//!
//! ```rust,ignore
//! use clickhouse_arrow::prelude::*;
//!
//! let client = ClientBuilder::new()
//!     .with_endpoint("http://localhost:8123")
//!     .with_username("default")
//!     .build_http()
//!     .await?;
//!
//! // Query returns Arrow RecordBatches
//! let batches: Vec<RecordBatch> = client.query("SELECT * FROM my_table").await?;
//!
//! // Insert Arrow data
//! client.insert("my_table", batch).await?;
//! ```

mod arrow_stream;
mod client;
mod config;
pub mod escape;

pub use client::HttpClient;
pub use config::{DEFAULT_TIMEOUT_SECS, HttpOptions};
