"""
Type stubs for the clickhouse_arrow._internal Rust extension module.

These stubs provide type information for IDE autocompletion and static analysis.
"""

from typing import List

import pyarrow

# Version string from Cargo.toml
__version__: str

# Exceptions
class ClickHouseError(Exception):
    """Base exception for all clickhouse-arrow errors."""

    ...

class ConnectionError(ClickHouseError):
    """Connection-related errors (network, timeout, etc.)."""

    ...

class QueryError(ClickHouseError):
    """Query execution errors (protocol, parsing, types)."""

    ...

class SerializationError(ClickHouseError):
    """Data serialization/deserialization errors."""

    ...

class ServerError(ClickHouseError):
    """ClickHouse server-side errors."""

    ...

class ConfigurationError(ClickHouseError):
    """Client configuration errors."""

    ...

class ClientBuilder:
    """
    Builder for configuring a ClickHouse client connection.

    Use method chaining to configure connection parameters, then call `build()`
    to create a connected Client instance.

    Example:
        >>> client = (
        ...     ClientBuilder()
        ...     .endpoint("localhost:9000")
        ...     .username("default")
        ...     .build()
        ... )
    """

    def __init__(self) -> None:
        """Create a new ClientBuilder with default configuration."""
        ...

    def endpoint(self, endpoint: str) -> "ClientBuilder":
        """Set the ClickHouse server endpoint (host:port)."""
        ...

    def username(self, username: str) -> "ClientBuilder":
        """Set the username for authentication."""
        ...

    def password(self, password: str) -> "ClientBuilder":
        """Set the password for authentication."""
        ...

    def database(self, database: str) -> "ClientBuilder":
        """Set the default database."""
        ...

    def tls(self, enabled: bool) -> "ClientBuilder":
        """Enable or disable TLS encryption."""
        ...

    def domain(self, domain: str) -> "ClientBuilder":
        """Set the TLS domain for certificate verification."""
        ...

    def cafile(self, path: str) -> "ClientBuilder":
        """Set the CA certificate file path for TLS."""
        ...

    def compression(self, method: str) -> "ClientBuilder":
        """
        Set the compression method.

        Args:
            method: One of "none", "lz4", or "zstd"

        Raises:
            ValueError: If method is not supported
        """
        ...

    def ipv4_only(self, enabled: bool) -> "ClientBuilder":
        """Force IPv4-only address resolution."""
        ...

    def build(self) -> "Client":
        """
        Build and connect the client.

        Returns:
            A connected ClickHouse client

        Raises:
            ConnectionError: If connection fails
            ConfigurationError: If configuration is invalid
        """
        ...

class Client:
    """
    ClickHouse client with Arrow integration.

    Use `ClientBuilder` or `connect()` to create a client instance.
    All methods are synchronous (blocking) from Python's perspective.
    """

    def query(self, query: str) -> List[pyarrow.RecordBatch]:
        """
        Execute a query and return results as PyArrow RecordBatches.

        Args:
            query: SQL query string

        Returns:
            List of PyArrow RecordBatch objects

        Raises:
            QueryError: If query execution fails
            ConnectionError: If connection is lost
        """
        ...

    def insert(self, query: str, batch: pyarrow.RecordBatch) -> None:
        """
        Insert a PyArrow RecordBatch into ClickHouse.

        Args:
            query: INSERT query (e.g., "INSERT INTO table")
            batch: PyArrow RecordBatch containing the data

        Raises:
            SerializationError: If data serialization fails
            QueryError: If insert fails
            ConnectionError: If connection is lost
        """
        ...

    def execute(self, query: str) -> None:
        """
        Execute a query without returning results.

        Use for DDL (CREATE, DROP, ALTER) and DML operations.

        Args:
            query: SQL query string

        Raises:
            QueryError: If execution fails
            ConnectionError: If connection is lost
        """
        ...

    def health_check(self, ping: bool = False) -> None:
        """
        Check connection health.

        Args:
            ping: If True, send a ping packet to verify server responsiveness

        Raises:
            ConnectionError: If health check fails
        """
        ...

    def shutdown(self) -> None:
        """
        Gracefully shutdown the client connection.

        Raises:
            ConnectionError: If shutdown fails
        """
        ...
