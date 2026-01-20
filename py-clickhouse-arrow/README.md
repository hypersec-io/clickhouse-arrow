# py-clickhouse-arrow

Python bindings for [clickhouse-arrow](https://github.com/georgeleepatterson/clickhouse-arrow) - a high-performance ClickHouse client with native protocol and Arrow integration.

## Architecture

This package follows the [Polars monorepo model](https://github.com/pola-rs/polars) where Python bindings (`py-polars`) live alongside the core Rust library (`polars`) in the same repository. This enables:

- **Atomic changes** across Rust and Python APIs in single commits
- **Natural version synchronisation** between packages
- **Simpler CI/CD** with unified build and test pipelines

The Rust crate implementing the Python bindings is called `py-clickhouse-arrow` to distinguish from the wrapped Rust crate `clickhouse-arrow`. The Python package is named `clickarrow` (since `clickhouse-arrow` is already taken on PyPI by a different HTTP-based implementation).

## Installation

```bash
pip install clickarrow
```

## Quick Start

```python
import clickhouse_arrow

# Connect with convenience function
client = clickhouse_arrow.connect("localhost:9000")

# Query returns PyArrow RecordBatches
batches = client.query("SELECT * FROM system.numbers LIMIT 10")
for batch in batches:
    print(batch.to_pandas())

# Insert PyArrow data
import pyarrow as pa
batch = pa.RecordBatch.from_pydict({
    "id": pa.array([1, 2, 3], type=pa.uint64()),
    "name": pa.array(["a", "b", "c"]),
})
client.execute("CREATE TABLE test (id UInt64, name String) ENGINE = Memory")
client.insert("INSERT INTO test", batch)
```

## Builder Pattern

For more control over connection settings:

```python
client = (
    clickhouse_arrow.ClientBuilder()
    .endpoint("localhost:9000")
    .username("default")
    .password("")
    .database("my_db")
    .compression("lz4")  # or "zstd" or "none"
    .tls(True)
    .build()
)
```

## Features

- **Native Protocol**: Direct TCP connection to ClickHouse (not HTTP)
- **Zero-Copy Arrow**: Data transfer via PyArrow with no serialisation overhead
- **Compression**: LZ4 (default) and ZSTD support
- **TLS**: Secure connections with certificate verification

## Development

Development uses [uv](https://github.com/astral-sh/uv) for Python environment management and [maturin](https://github.com/PyO3/maturin) for building the Rust extension.

### Setup

```bash
cd py-clickhouse-arrow

# Create virtual environment and install dev dependencies
uv sync --group dev

# Build the extension in development mode
uv run maturin develop

# Run tests
uv run pytest
```

### Building Wheels

```bash
# Build release wheel
uv run maturin build --release

# Build with specific Python version
uv run maturin build --release -i python3.11
```

### Code Quality

```bash
# Format and lint Python code
uv run ruff format .
uv run ruff check .

# Rust formatting and linting (from workspace root)
cargo fmt -p py-clickhouse-arrow
cargo clippy -p py-clickhouse-arrow
```

## API Reference

### Exceptions

All exceptions inherit from `ClickHouseError`:

- `ConnectionError` - Network, timeout, connection issues
- `QueryError` - Protocol, parsing, type errors
- `SerializationError` - Data serialisation failures
- `ServerError` - ClickHouse server exceptions
- `ConfigurationError` - Client configuration issues

### Client Methods

- `query(sql)` → `List[pyarrow.RecordBatch]`
- `insert(sql, batch)` → `None`
- `execute(sql)` → `None`
- `health_check(ping=False)` → `None`
- `shutdown()` → `None`

## License

Apache-2.0
