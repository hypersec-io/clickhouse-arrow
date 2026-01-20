//! Python bindings for clickhouse-arrow.
//!
//! Native TCP protocol w/ Arrow integration. Sync API for data science workflows.
//! Follows the Polars monorepo model (py-polars alongside polars crate).
#![allow(clippy::doc_markdown)]
#![allow(clippy::borrow_as_ptr)]

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
