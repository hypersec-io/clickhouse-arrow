// Project:   py-clickhouse-arrow
// File:      client.rs
// Purpose:   Python wrapper for ClickHouse Arrow client
// Language:  Rust
//
// License:   Apache-2.0
// Copyright: (c) 2026 HyperSec

//! Python bindings for the ClickHouse Arrow client.
//!
//! Provides query, insert, and execute operations with PyArrow integration:
//!
//! ```python
//! import pyarrow as pa
//!
//! # Query returns list of PyArrow RecordBatches
//! batches = client.query("SELECT * FROM table")
//!
//! # Insert accepts PyArrow RecordBatch
//! client.insert("INSERT INTO table", batch)
//!
//! # Execute for DDL/DML without results
//! client.execute("CREATE TABLE ...")
//! ```

use arrow::array::RecordBatch;
use futures_util::StreamExt;
use pyo3::prelude::*;

use clickhouse_arrow::prelude::ArrowClient;

use crate::arrow_ffi::{record_batch_from_pyarrow, record_batch_to_pyarrow};
use crate::error::to_py_result;
use crate::runtime::block_on;

/// ClickHouse client with Arrow integration.
///
/// Use `ClientBuilder` or `connect()` to create a client instance.
/// All methods are synchronous (blocking) from Python's perspective.
#[pyclass(name = "Client")]
#[expect(unnameable_types)]
pub struct Client {
    inner: ArrowClient,
}

impl Client {
    /// Create a new Client wrapper around an ArrowClient.
    pub fn new(client: ArrowClient) -> Self {
        Self { inner: client }
    }
}

#[pymethods]
impl Client {
    /// Execute a query and return results as a list of PyArrow RecordBatches.
    ///
    /// Args:
    ///     query: SQL query string
    ///
    /// Returns:
    ///     List of PyArrow RecordBatch objects
    ///
    /// Raises:
    ///     QueryError: If query execution fails
    ///     ConnectionError: If connection is lost
    ///
    /// Example:
    ///     >>> batches = client.query("SELECT * FROM system.numbers LIMIT 10")
    ///     >>> for batch in batches:
    ///     ...     print(batch.to_pandas())
    fn query(&self, py: Python<'_>, query: &str) -> PyResult<Vec<PyObject>> {
        // Execute query and collect all batches
        let batches: Vec<RecordBatch> = to_py_result(block_on(async {
            let stream = self.inner.query(query, None).await?;
            stream
                .collect::<Vec<_>>()
                .await
                .into_iter()
                .collect::<Result<Vec<_>, _>>()
        }))?;

        // Convert to PyArrow RecordBatches
        batches
            .iter()
            .map(|batch| record_batch_to_pyarrow(py, batch))
            .collect()
    }

    /// Insert a PyArrow RecordBatch into ClickHouse.
    ///
    /// Args:
    ///     query: INSERT query (e.g., "INSERT INTO table")
    ///     batch: PyArrow RecordBatch containing the data
    ///
    /// Raises:
    ///     SerializationError: If data serialization fails
    ///     QueryError: If insert fails
    ///     ConnectionError: If connection is lost
    ///
    /// Example:
    ///     >>> import pyarrow as pa
    ///     >>> batch = pa.RecordBatch.from_pydict({
    ///     ...     "id": pa.array([1, 2, 3]),
    ///     ...     "name": pa.array(["a", "b", "c"]),
    ///     ... })
    ///     >>> client.insert("INSERT INTO my_table", batch)
    fn insert(&self, py: Python<'_>, query: &str, batch: &Bound<'_, PyAny>) -> PyResult<()> {
        let record_batch = record_batch_from_pyarrow(py, batch)?;

        to_py_result(block_on(async {
            let mut stream = self.inner.insert(query, record_batch, None).await?;
            while let Some(result) = stream.next().await {
                result?;
            }
            Ok::<_, clickhouse_arrow::Error>(())
        }))?;

        Ok(())
    }

    /// Execute a query without returning results.
    ///
    /// Use for DDL (CREATE, DROP, ALTER) and DML (INSERT without data) operations.
    ///
    /// Args:
    ///     query: SQL query string
    ///
    /// Raises:
    ///     QueryError: If execution fails
    ///     ConnectionError: If connection is lost
    ///
    /// Example:
    ///     >>> client.execute("CREATE TABLE test (id UInt64) ENGINE = Memory")
    ///     >>> client.execute("DROP TABLE test")
    fn execute(&self, query: &str) -> PyResult<()> {
        to_py_result(block_on(self.inner.execute(query, None)))?;
        Ok(())
    }

    /// Check connection health.
    ///
    /// Args:
    ///     ping: If True, send a ping packet to verify server responsiveness
    ///
    /// Raises:
    ///     ConnectionError: If health check fails
    ///
    /// Example:
    ///     >>> client.health_check(ping=True)
    #[pyo3(signature = (ping=false))]
    fn health_check(&self, ping: bool) -> PyResult<()> {
        to_py_result(block_on(self.inner.health_check(ping)))?;
        Ok(())
    }

    /// Gracefully shutdown the client connection.
    ///
    /// Raises:
    ///     ConnectionError: If shutdown fails
    fn shutdown(&self) -> PyResult<()> {
        to_py_result(block_on(self.inner.shutdown()))?;
        Ok(())
    }

    /// String representation showing connection status.
    fn __repr__(&self) -> String {
        format!("Client(status={:?})", self.inner.status())
    }
}
