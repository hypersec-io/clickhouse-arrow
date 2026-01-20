"""
clickhouse-arrow: High-performance ClickHouse client with Arrow integration.

This package provides Python bindings for the clickhouse-arrow Rust library,
offering native protocol access to ClickHouse with zero-copy PyArrow integration.

Quick Start:
    >>> import clickhouse_arrow
    >>> client = clickhouse_arrow.connect("localhost:9000")
    >>> batches = client.query("SELECT 1 AS value")
    >>> print(batches[0].to_pandas())

For more control over connection settings, use ClientBuilder:
    >>> client = (
    ...     clickhouse_arrow.ClientBuilder()
    ...     .endpoint("localhost:9000")
    ...     .username("default")
    ...     .compression("lz4")
    ...     .build()
    ... )

Architecture Note:
    This package follows the Polars monorepo model where Python bindings
    (py-clickhouse-arrow) live alongside the core Rust library (clickhouse-arrow)
    in the same repository. This enables atomic changes across both APIs and
    natural version synchronisation.

    See: https://github.com/pola-rs/polars
"""

from clickhouse_arrow._internal import (
    Client,
    ClientBuilder,
    ClickHouseError,
    ConfigurationError,
    ConnectionError,
    QueryError,
    SerializationError,
    ServerError,
    __version__,
)

__all__ = [
    # Core classes
    "Client",
    "ClientBuilder",
    # Exceptions
    "ClickHouseError",
    "ConnectionError",
    "QueryError",
    "SerializationError",
    "ServerError",
    "ConfigurationError",
    # Convenience function
    "connect",
    # Metadata
    "__version__",
]


def connect(
    endpoint: str = "localhost:9000",
    username: str = "default",
    password: str = "",
    database: str = "default",
    tls: bool = False,
    compression: str = "lz4",
) -> Client:
    """
    Create a ClickHouse client with common defaults.

    This is a convenience function that wraps ClientBuilder for simple use cases.
    For more control over connection settings, use ClientBuilder directly.

    Args:
        endpoint: ClickHouse server address as "host:port" (default: "localhost:9000")
        username: Authentication username (default: "default")
        password: Authentication password (default: "")
        database: Default database to use (default: "default")
        tls: Enable TLS encryption (default: False)
        compression: Compression method - "none", "lz4", or "zstd" (default: "lz4")

    Returns:
        Connected Client instance ready for queries

    Raises:
        ConnectionError: If connection to server fails
        ConfigurationError: If configuration is invalid

    Example:
        >>> client = clickhouse_arrow.connect("localhost:9000")
        >>> batches = client.query("SELECT version()")
        >>> print(batches[0].to_pandas())

        >>> # With authentication
        >>> client = clickhouse_arrow.connect(
        ...     endpoint="clickhouse.example.com:9440",
        ...     username="admin",
        ...     password="secret",
        ...     tls=True,
        ... )
    """
    return (
        ClientBuilder()
        .endpoint(endpoint)
        .username(username)
        .password(password)
        .database(database)
        .tls(tls)
        .compression(compression)
        .build()
    )
