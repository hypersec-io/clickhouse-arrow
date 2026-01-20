// Project:   py-clickhouse-arrow
// File:      lib.rs
// Purpose:   PyO3 module entry point
// Language:  Rust
//
// License:   Apache-2.0
// Copyright: (c) 2026 HyperSec

//! Python bindings for clickhouse-arrow.
//!
//! High-performance ClickHouse client with native protocol and Arrow integration.
//!
//! This module provides Python bindings for the `clickhouse-arrow` Rust crate,
//! following the [Polars monorepo model](https://github.com/pola-rs/polars)
//! where Python bindings (`py-polars`) live alongside the core Rust library.
//!
//! ## Features
//!
//! - **Native Protocol**: Direct TCP connection to ClickHouse (not HTTP)
//! - **Arrow Integration**: Zero-copy data transfer via PyArrow
//! - **Sync API**: Blocking operations suitable for data science workflows
//! - **Compression**: LZ4 and ZSTD support
//!
//! ## Quick Start
//!
//! ```python
//! import clickhouse_arrow
//!
//! # Connect with convenience function
//! client = clickhouse_arrow.connect("localhost:9000")
//!
//! # Or use builder for more control
//! client = (
//!     clickhouse_arrow.ClientBuilder()
//!     .endpoint("localhost:9000")
//!     .username("default")
//!     .password("")
//!     .compression("lz4")
//!     .build()
//! )
//!
//! # Query returns PyArrow RecordBatches
//! batches = client.query("SELECT * FROM system.numbers LIMIT 10")
//! for batch in batches:
//!     print(batch.to_pandas())
//!
//! # Insert PyArrow data
//! import pyarrow as pa
//! batch = pa.RecordBatch.from_pydict({"id": [1, 2, 3]})
//! client.insert("INSERT INTO my_table", batch)
//! ```

mod arrow_ffi;
mod builder;
mod client;
mod error;
mod runtime;

use pyo3::prelude::*;

/// High-performance ClickHouse client with Arrow integration.
///
/// This module provides Python bindings for the clickhouse-arrow Rust library.
#[pymodule]
fn _internal(py: Python<'_>, m: &Bound<'_, PyModule>) -> PyResult<()> {
    // Register exception types
    error::register_exceptions(py, m)?;

    // Register classes
    m.add_class::<client::Client>()?;
    m.add_class::<builder::PyClientBuilder>()?;

    // Add version info
    m.add("__version__", env!("CARGO_PKG_VERSION"))?;

    Ok(())
}
