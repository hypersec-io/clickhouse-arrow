// Project:   py-clickhouse-arrow
// File:      builder.rs
// Purpose:   Python wrapper for ClientBuilder
// Language:  Rust
//
// License:   Apache-2.0
// Copyright: (c) 2026 HyperSec

//! Python bindings for the `ClientBuilder` fluent configuration API.
//!
//! Provides a Pythonic builder pattern for configuring ClickHouse connections:
//!
//! ```python
//! client = (
//!     ClientBuilder()
//!     .endpoint("localhost:9000")
//!     .username("default")
//!     .password("")
//!     .database("my_db")
//!     .compression("lz4")
//!     .build()
//! )
//! ```

use pyo3::prelude::*;

use clickhouse_arrow::prelude::{ClientBuilder as RustClientBuilder, CompressionMethod};

use crate::client::Client;
use crate::error::to_py_result;
use crate::runtime::block_on;

/// Builder for configuring a ClickHouse client connection.
///
/// Use method chaining to configure connection parameters, then call `build()`
/// to create a connected `Client` instance.
///
/// Example:
///     >>> client = ClientBuilder().endpoint("localhost:9000").build()
#[pyclass(name = "ClientBuilder")]
#[derive(Clone)]
#[expect(unnameable_types)]
pub struct PyClientBuilder {
    inner: RustClientBuilder,
}

#[pymethods]
impl PyClientBuilder {
    /// Create a new ClientBuilder with default configuration.
    #[new]
    fn new() -> Self {
        Self {
            inner: RustClientBuilder::new(),
        }
    }

    /// Set the ClickHouse server endpoint (host:port).
    ///
    /// Args:
    ///     endpoint: Server address, e.g. "localhost:9000"
    ///
    /// Returns:
    ///     Self for method chaining
    fn endpoint(&mut self, endpoint: &str) -> Self {
        self.inner = std::mem::take(&mut self.inner).with_endpoint(endpoint);
        self.clone()
    }

    /// Set the username for authentication.
    ///
    /// Args:
    ///     username: ClickHouse username (default: "default")
    ///
    /// Returns:
    ///     Self for method chaining
    fn username(&mut self, username: &str) -> Self {
        self.inner = std::mem::take(&mut self.inner).with_username(username);
        self.clone()
    }

    /// Set the password for authentication.
    ///
    /// Args:
    ///     password: ClickHouse password
    ///
    /// Returns:
    ///     Self for method chaining
    fn password(&mut self, password: &str) -> Self {
        self.inner = std::mem::take(&mut self.inner).with_password(password);
        self.clone()
    }

    /// Set the default database.
    ///
    /// Args:
    ///     database: Database name to use for queries
    ///
    /// Returns:
    ///     Self for method chaining
    fn database(&mut self, database: &str) -> Self {
        self.inner = std::mem::take(&mut self.inner).with_database(database);
        self.clone()
    }

    /// Enable or disable TLS encryption.
    ///
    /// Args:
    ///     enabled: Whether to use TLS (default: False)
    ///
    /// Returns:
    ///     Self for method chaining
    fn tls(&mut self, enabled: bool) -> Self {
        self.inner = std::mem::take(&mut self.inner).with_tls(enabled);
        self.clone()
    }

    /// Set the TLS domain for certificate verification.
    ///
    /// Args:
    ///     domain: Domain name for TLS verification
    ///
    /// Returns:
    ///     Self for method chaining
    fn domain(&mut self, domain: &str) -> Self {
        self.inner = std::mem::take(&mut self.inner).with_domain(domain);
        self.clone()
    }

    /// Set the CA certificate file path for TLS.
    ///
    /// Args:
    ///     path: Path to CA certificate file
    ///
    /// Returns:
    ///     Self for method chaining
    fn cafile(&mut self, path: &str) -> Self {
        self.inner = std::mem::take(&mut self.inner).with_cafile(path);
        self.clone()
    }

    /// Set the compression method.
    ///
    /// Args:
    ///     method: Compression method - "none", "lz4" (default), or "zstd"
    ///
    /// Returns:
    ///     Self for method chaining
    ///
    /// Raises:
    ///     ValueError: If method is not one of the supported values
    fn compression(&mut self, method: &str) -> PyResult<Self> {
        let compression = match method.to_lowercase().as_str() {
            "none" => CompressionMethod::None,
            "lz4" => CompressionMethod::LZ4,
            "zstd" => CompressionMethod::ZSTD,
            _ => {
                return Err(pyo3::exceptions::PyValueError::new_err(format!(
                    "Unknown compression method: '{method}'. Use 'none', 'lz4', or 'zstd'"
                )))
            }
        };
        self.inner = std::mem::take(&mut self.inner).with_compression(compression);
        Ok(self.clone())
    }

    /// Force IPv4-only address resolution.
    ///
    /// Args:
    ///     enabled: Whether to use IPv4 only (default: False)
    ///
    /// Returns:
    ///     Self for method chaining
    fn ipv4_only(&mut self, enabled: bool) -> Self {
        self.inner = std::mem::take(&mut self.inner).with_ipv4_only(enabled);
        self.clone()
    }

    /// Build and connect the client.
    ///
    /// This method establishes a connection to the ClickHouse server using
    /// the configured parameters.
    ///
    /// Returns:
    ///     Client: A connected ClickHouse client
    ///
    /// Raises:
    ///     ConnectionError: If connection fails
    ///     ConfigurationError: If configuration is invalid
    fn build(&self) -> PyResult<Client> {
        let builder = self.inner.clone();
        let client = to_py_result(block_on(builder.build_arrow()))?;
        Ok(Client::new(client))
    }
}

impl Default for PyClientBuilder {
    fn default() -> Self {
        Self::new()
    }
}
