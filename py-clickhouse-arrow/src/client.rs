//! Python client wrapper – query, insert, execute w/ PyArrow.

use arrow::array::RecordBatch;
use futures_util::StreamExt;
use pyo3::prelude::*;

use clickhouse_arrow::prelude::ArrowClient;

use crate::arrow_ffi::{record_batch_from_pyarrow, record_batch_to_pyarrow};
use crate::error::to_py_result;
use crate::runtime::block_on;

/// ClickHouse client w/ Arrow integration. Sync API (blocking).
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
    /// Execute query, returns list of PyArrow RecordBatches.
    fn query(&self, py: Python<'_>, query: &str) -> PyResult<Vec<PyObject>> {
        // Execute query and collect all batches
        let batches: Vec<RecordBatch> = to_py_result(block_on(async {
            let stream = self.inner.query(query, None).await?;
            stream.collect::<Vec<_>>().await.into_iter().collect::<Result<Vec<_>, _>>()
        }))?;

        // Convert to PyArrow RecordBatches
        batches.iter().map(|batch| record_batch_to_pyarrow(py, batch)).collect()
    }

    /// Insert a PyArrow RecordBatch.
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

    /// Execute query w/o returning results (DDL, DML).
    fn execute(&self, query: &str) -> PyResult<()> {
        to_py_result(block_on(self.inner.execute(query, None)))?;
        Ok(())
    }

    /// Check connection health. Pass ping=True for active server check.
    #[pyo3(signature = (ping=false))]
    fn health_check(&self, ping: bool) -> PyResult<()> {
        to_py_result(block_on(self.inner.health_check(ping)))?;
        Ok(())
    }

    /// Gracefully shutdown the connection.
    fn shutdown(&self) -> PyResult<()> {
        to_py_result(block_on(self.inner.shutdown()))?;
        Ok(())
    }

    /// String representation showing connection status.
    fn __repr__(&self) -> String {
        format!("Client(status={:?})", self.inner.status())
    }
}
