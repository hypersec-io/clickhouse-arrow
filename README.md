# ClickHouse Arrow Client for Rust

## Crates

The project consists of several crates:

### [`clickhouse-arrow`](./clickhouse-arrow/README.md)

The core client library – native TCP protocol w/ Arrow integration:

- Arrow RecordBatch query/insert
- DDL and DML support
- SIMD-accelerated serialisation (~2.2x null bitmap speedup)
- Buffer pooling (~21% faster allocations)
- Sparse column support (MergeTree optimisation)
- HTTP transport (ArrowStream) for environments requiring HTTP-only

### [`clickhouse-arrow-derive`](./clickhouse-arrow-derive)

The `Row` derive macro for serde-like (de)serialisation of Rust structs to ClickHouse tables.

### [`py-clickhouse-arrow`](./py-clickhouse-arrow)

Python bindings via PyO3/maturin. Sync API for data science workflows:

```python
import clickhouse_arrow
client = clickhouse_arrow.connect("localhost:9000")
batches = client.query("SELECT * FROM table")  # Returns PyArrow RecordBatches
```

See [py-clickhouse-arrow/README.md](./py-clickhouse-arrow/README.md) for details.

## Example Usage

```rust
/// Basic connection example
use clickhouse_arrow::{ArrowFormat, Client, Result};
use clickhouse_arrow::arrow::arrow::util::pretty;
use futures_util::stream::StreamExt;

async fn example() -> Result<(), Box<dyn std::error::Error>> {
    let client = Client::<ArrowFormat>::builder()
        .with_url("http://localhost:9000")
        .with_database("default")
        .with_user("default")
        .build()?;

    // Query execution
    let batches = client
        .query("SELECT number FROM system.numbers LIMIT 10")
        .await?
        .collect::<Vec<_>>()
        .await
        .into_iter()
        .collect::<Result<Vec<_>>>()?;

    // Print RecordBatches
    pretty::print_record_batches(&batches)?;

    Ok(())
}
```

Refer to the e2e tests in [clickhouse-arrow](./clickhouse-arrow/tests/)

## Installation

Add this to your `Cargo.toml`:

```toml
[dependencies]
clickhouse-arrow = "0.4"

# With connection pooling
clickhouse-arrow = { version = "0.4", features = ["pool"] }

# HTTP transport only (no native TCP)
clickhouse-arrow = { version = "0.4", features = ["http"] }
```

## Features

- Native TCP – faster and less CPU than HTTP
- Arrow RecordBatch streaming w/ zero-copy where possible
- SIMD null bitmap expansion (~2.2x)
- Buffer pooling (~21% faster 4KB allocs)
- Vectored I/O (15-25% fewer syscalls)
- Sparse columns (MergeTree optimisation)
- tokio-based async

## Performance

Overall v0.4.x is **~2x faster** than v0.2.x for typical mixed workloads.

Insert throughput improvements:
- Bulk primitives: 40-60% faster
- String batching: 20-35% faster
- Deferred flush: 98% fewer syscalls (batch inserts)
- SIMD null bitmaps: 2.2x faster

```bash
cargo bench --features test-utils
```

## Supported Data Types

Full support for ClickHouse data types including:
- Numeric types (UInt8-256, Int8-256, Float32/64)
- String types (String, FixedString)
- Date/Time types (Date, Date32, DateTime, DateTime64)
- Complex types (Array, Nullable, LowCardinality, Map, Tuple)
- Special types (UUID, IPv4/IPv6, Enum8/16)

## License

Licensed under the Apache License, Version 2.0. See [LICENSE](https://github.com/GeorgeLeePatterson/clickhouse-arrow/blob/main/LICENSE) for details.

## Contributing

We welcome contributions! Please see [CONTRIBUTING.md](https://github.com/GeorgeLeePatterson/clickhouse-arrow/blob/main/CONTRIBUTING.md) for guidelines.

## Releasing

Maintainer setup and release process:

### Setup
```bash
# Initialize development environment
just init-dev
```

### Making Releases
```bash
# Create a new release (patch/minor/major)
cargo release patch
```

Releases are fully automated and include:
- Changelog generation
- GitHub release creation
- Publishing to crates.io

## Acknowledgments

Special thanks to [klickhouse](https://github.com/Protryon/klickhouse), which provided inspiration and some initial code for this project to get started. While `clickhouse-arrow` Native has evolved into a complete rewrite in most areas, while others are essentially the same (`Row` macro), the early foundation benefited greatly from klickhouse's work. Ultimately the design goals are different, with this library focusing on Arrow interoperability and tools to make integrating `ClickHouse` and `Arrow` easier.
