//! Memory allocation profiling benchmarks.
//!
//! These benchmarks focus on memory characteristics:
//! - Arrow batch memory footprint vs wire bytes
//! - Memory efficiency of different data types
//! - Allocation patterns during serialization/deserialization
//! - Buffer reuse effectiveness
#![expect(unused_crate_dependencies)]
// Benchmark code: casts are safe for test data sizes
#![allow(clippy::cast_precision_loss)]
#![allow(clippy::cast_possible_wrap)]
#![allow(clippy::cast_possible_truncation)]
#![allow(clippy::cast_sign_loss)]
#![allow(clippy::cast_lossless)]
#![allow(unused_results)]
#![allow(dead_code)]
#![allow(clippy::slow_vector_initialization)]

mod common;

use std::hint::black_box;
use std::sync::Arc;
use std::time::Duration;

use arrow::array::*;
use arrow::datatypes::*;
use arrow::record_batch::RecordBatch;
use clickhouse_arrow::CompressionMethod;
use clickhouse_arrow::prelude::*;
use clickhouse_arrow::test_utils::{arrow_tests, get_or_create_container};
use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use futures_util::StreamExt;
use tokio::runtime::Runtime;

use self::common::{DISABLE_CLEANUP_ENV, TEST_DB_NAME, init, print_msg};

/// Memory stats for a batch
#[derive(Debug, Clone)]
struct MemoryStats {
    /// Arrow array memory size (actual heap usage)
    arrow_memory_bytes:  usize,
    /// Estimated wire format size
    wire_bytes:          usize,
    /// Number of rows
    rows:                usize,
    /// Number of columns
    columns:             usize,
    /// Bytes per row in Arrow format
    arrow_bytes_per_row: f64,
    /// Bytes per row in wire format
    wire_bytes_per_row:  f64,
    /// Memory amplification factor (Arrow / wire)
    amplification:       f64,
}

impl MemoryStats {
    fn from_batch(batch: &RecordBatch, wire_bytes: usize) -> Self {
        let arrow_memory_bytes = batch.get_array_memory_size();
        let rows = batch.num_rows();
        let columns = batch.num_columns();
        let arrow_bytes_per_row = arrow_memory_bytes as f64 / rows as f64;
        let wire_bytes_per_row = wire_bytes as f64 / rows as f64;
        let amplification =
            if wire_bytes > 0 { arrow_memory_bytes as f64 / wire_bytes as f64 } else { 1.0 };

        Self {
            arrow_memory_bytes,
            wire_bytes,
            rows,
            columns,
            arrow_bytes_per_row,
            wire_bytes_per_row,
            amplification,
        }
    }
}

/// Create a batch with fixed-size types (no variable-length overhead)
fn create_fixed_size_batch(rows: usize) -> (RecordBatch, usize) {
    let schema = Arc::new(Schema::new(vec![
        Field::new("int8", DataType::Int8, false),
        Field::new("int16", DataType::Int16, false),
        Field::new("int32", DataType::Int32, false),
        Field::new("int64", DataType::Int64, false),
        Field::new("float32", DataType::Float32, false),
        Field::new("float64", DataType::Float64, false),
    ]));

    // Wire bytes: 1 + 2 + 4 + 8 + 4 + 8 = 27 bytes/row
    let wire_bytes_per_row = 27;

    let columns: Vec<ArrayRef> = vec![
        Arc::new(Int8Array::from((0..rows).map(|i| (i % 128) as i8).collect::<Vec<_>>())),
        Arc::new(Int16Array::from((0..rows).map(|i| (i % 32768) as i16).collect::<Vec<_>>())),
        Arc::new(Int32Array::from((0..rows).map(|i| i as i32).collect::<Vec<_>>())),
        Arc::new(Int64Array::from((0..rows).map(|i| i as i64).collect::<Vec<_>>())),
        Arc::new(Float32Array::from((0..rows).map(|i| i as f32).collect::<Vec<_>>())),
        Arc::new(Float64Array::from((0..rows).map(|i| i as f64).collect::<Vec<_>>())),
    ];

    let batch = RecordBatch::try_new(schema, columns).unwrap();
    (batch, rows * wire_bytes_per_row)
}

/// Create a batch with variable-length strings of consistent size
fn create_string_batch(rows: usize, string_len: usize) -> (RecordBatch, usize) {
    let schema = Arc::new(Schema::new(vec![Field::new("str", DataType::Binary, false)]));

    // Wire bytes: length prefix (varint ~1-2 bytes) + string data
    let wire_bytes_per_row = 2 + string_len; // approximate

    let pattern: Vec<u8> = (0..string_len).map(|i| b'a' + (i % 26) as u8).collect();
    let columns: Vec<ArrayRef> =
        vec![Arc::new(BinaryArray::from_iter_values((0..rows).map(|_| pattern.as_slice())))];

    let batch = RecordBatch::try_new(schema, columns).unwrap();
    (batch, rows * wire_bytes_per_row)
}

/// Create a batch with variable-length strings of varying sizes
fn create_variable_string_batch(rows: usize) -> (RecordBatch, usize) {
    let schema = Arc::new(Schema::new(vec![Field::new("str", DataType::Binary, false)]));

    // Create strings of varying lengths: 8, 16, 32, 64, 128, 256
    let lengths = [8, 16, 32, 64, 128, 256];
    let strings: Vec<Vec<u8>> = (0..rows)
        .map(|i| {
            let len = lengths[i % lengths.len()];
            (0..len).map(|j| b'a' + (j % 26) as u8).collect()
        })
        .collect();

    let avg_len: usize = strings.iter().map(Vec::len).sum::<usize>() / rows;
    let wire_bytes = rows * (2 + avg_len);

    let columns: Vec<ArrayRef> =
        vec![Arc::new(BinaryArray::from_iter_values(strings.iter().map(Vec::as_slice)))];

    let batch = RecordBatch::try_new(schema, columns).unwrap();
    (batch, wire_bytes)
}

/// Create a batch with nullable columns (tests null bitmap overhead)
fn create_nullable_batch(rows: usize, null_ratio: f64) -> (RecordBatch, usize) {
    let schema = Arc::new(Schema::new(vec![
        Field::new("int64_nullable", DataType::Int64, true),
        Field::new("float64_nullable", DataType::Float64, true),
    ]));

    // Wire bytes: 8 + 8 + null bitmaps (1 bit per value, but sent per-column)
    let wire_bytes_per_row = 16 + 1; // approximate with null overhead

    let int_values: Vec<Option<i64>> = (0..rows)
        .map(|i| if (i as f64 / rows as f64) < null_ratio { None } else { Some(i as i64) })
        .collect();

    let float_values: Vec<Option<f64>> = (0..rows)
        .map(|i| if ((i + 1) as f64 / rows as f64) < null_ratio { None } else { Some(i as f64) })
        .collect();

    let columns: Vec<ArrayRef> =
        vec![Arc::new(Int64Array::from(int_values)), Arc::new(Float64Array::from(float_values))];

    let batch = RecordBatch::try_new(schema, columns).unwrap();
    (batch, rows * wire_bytes_per_row)
}

/// Create a nested array batch
fn create_nested_batch(rows: usize, list_size: usize) -> (RecordBatch, usize) {
    let inner_field = Field::new("item", DataType::Int64, true);
    let list_type = DataType::List(Arc::new(inner_field));
    let schema = Arc::new(Schema::new(vec![Field::new("list_col", list_type, false)]));

    // Wire bytes: offsets (8 bytes per row) + values (8 bytes * list_size per row)
    let wire_bytes_per_row = 8 + list_size * 8;

    let mut list_builder = ListBuilder::new(Int64Builder::new());
    for i in 0..rows {
        let values: Vec<i64> = (0..list_size).map(|j| (i * list_size + j) as i64).collect();
        list_builder.append_value(values.iter().map(|&v| Some(v)));
    }

    let columns: Vec<ArrayRef> = vec![Arc::new(list_builder.finish())];

    let batch = RecordBatch::try_new(schema, columns).unwrap();
    (batch, rows * wire_bytes_per_row)
}

/// Create a dictionary-encoded batch (simulates `LowCardinality`)
fn create_dictionary_batch(
    rows: usize,
    cardinality: usize,
    value_len: usize,
) -> (RecordBatch, usize) {
    let dict_values: Vec<String> = (0..cardinality)
        .map(|i| {
            let base = format!("val_{i:08}");
            base.chars().cycle().take(value_len).collect()
        })
        .collect();

    let schema = Arc::new(Schema::new(vec![Field::new(
        "dict",
        DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8)),
        false,
    )]));

    // Wire bytes: keys (based on cardinality) + dictionary values (once)
    let key_size = if cardinality <= 256 {
        1
    } else if cardinality <= 65536 {
        2
    } else {
        4
    };
    let wire_bytes = rows * key_size + cardinality * value_len;

    let keys: Vec<i32> = (0..rows).map(|i| (i % cardinality) as i32).collect();
    let keys_array = Int32Array::from(keys);
    let values_array = StringArray::from(dict_values);

    let columns: Vec<ArrayRef> = vec![Arc::new(
        DictionaryArray::<Int32Type>::try_new(keys_array, Arc::new(values_array)).unwrap(),
    )];

    let batch = RecordBatch::try_new(schema, columns).unwrap();
    (batch, wire_bytes)
}

/// Print memory stats to stderr for analysis
fn log_memory_stats(label: &str, stats: &MemoryStats) {
    eprintln!(
        "[{label}] rows={}, cols={}, arrow_mem={}KB, wire={}KB, arrow/row={:.1}B, \
         wire/row={:.1}B, amp={:.2}x",
        stats.rows,
        stats.columns,
        stats.arrow_memory_bytes / 1024,
        stats.wire_bytes / 1024,
        stats.arrow_bytes_per_row,
        stats.wire_bytes_per_row,
        stats.amplification
    );
}

/// Benchmark memory efficiency of fixed-size types
fn bench_memory_fixed_types(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    init();

    let ch = rt.block_on(get_or_create_container(None));
    print_msg("Container ready for memory profiling benchmarks");

    let client = rt
        .block_on(
            arrow_tests::setup_test_arrow_client(ch.get_native_url(), &ch.user, &ch.password)
                .with_ipv4_only(true)
                .with_compression(CompressionMethod::None)
                .build::<ArrowFormat>(),
        )
        .expect("client setup");

    rt.block_on(arrow_tests::setup_database(TEST_DB_NAME, &client)).expect("database setup");

    let mut group = c.benchmark_group("MemoryFixedTypes");
    let _ = group.sample_size(20);
    let _ = group.measurement_time(Duration::from_secs(15));

    let row_counts = [10_000, 100_000, 500_000, 1_000_000];

    for &rows in &row_counts {
        let (batch, wire_bytes) = create_fixed_size_batch(rows);
        let stats = MemoryStats::from_batch(&batch, wire_bytes);
        log_memory_stats(&format!("fixed_types_{rows}"), &stats);

        let table = rt
            .block_on(arrow_tests::setup_table(&client, TEST_DB_NAME, &batch.schema()))
            .expect("table setup");
        let query = format!("INSERT INTO {table} FORMAT NATIVE");

        // Track both Arrow memory and wire bytes
        let _ = group.throughput(Throughput::Bytes(stats.arrow_memory_bytes as u64));

        let _ = group.bench_with_input(
            BenchmarkId::new("insert_arrow_mem", rows),
            &(&query, &client, &batch),
            |b, (query, client, batch)| {
                b.to_async(&rt).iter_batched(
                    || (*batch).clone(),
                    |batch| async {
                        let stream = client.insert(*query, batch, None).await.unwrap();
                        drop(black_box(stream));
                    },
                    criterion::BatchSize::SmallInput,
                );
            },
        );
    }

    group.finish();

    if std::env::var(DISABLE_CLEANUP_ENV).is_ok_and(|e| e.eq_ignore_ascii_case("true")) {
        return;
    }
    rt.block_on(ch.shutdown()).expect("shutdown");
}

/// Benchmark memory efficiency of string types with different sizes
fn bench_memory_string_sizes(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    init();

    let ch = rt.block_on(get_or_create_container(None));

    let client = rt
        .block_on(
            arrow_tests::setup_test_arrow_client(ch.get_native_url(), &ch.user, &ch.password)
                .with_ipv4_only(true)
                .with_compression(CompressionMethod::None)
                .build::<ArrowFormat>(),
        )
        .expect("client setup");

    rt.block_on(arrow_tests::setup_database(TEST_DB_NAME, &client)).expect("database setup");

    let mut group = c.benchmark_group("MemoryStringSizes");
    let _ = group.sample_size(20);
    let _ = group.measurement_time(Duration::from_secs(15));

    let rows = 100_000;
    let string_lengths = [8, 32, 128, 512, 2048];

    for &len in &string_lengths {
        let (batch, wire_bytes) = create_string_batch(rows, len);
        let stats = MemoryStats::from_batch(&batch, wire_bytes);
        log_memory_stats(&format!("string_len_{len}"), &stats);

        let table = rt
            .block_on(arrow_tests::setup_table(&client, TEST_DB_NAME, &batch.schema()))
            .expect("table setup");
        let query = format!("INSERT INTO {table} FORMAT NATIVE");

        let _ = group.throughput(Throughput::Bytes(stats.arrow_memory_bytes as u64));

        let _ = group.bench_with_input(
            BenchmarkId::new("insert", format!("len_{len}")),
            &(&query, &client, &batch),
            |b, (query, client, batch)| {
                b.to_async(&rt).iter_batched(
                    || (*batch).clone(),
                    |batch| async {
                        let stream = client.insert(*query, batch, None).await.unwrap();
                        drop(black_box(stream));
                    },
                    criterion::BatchSize::SmallInput,
                );
            },
        );
    }

    group.finish();

    if std::env::var(DISABLE_CLEANUP_ENV).is_ok_and(|e| e.eq_ignore_ascii_case("true")) {
        return;
    }
    rt.block_on(ch.shutdown()).expect("shutdown");
}

/// Benchmark memory overhead of variable-length strings
fn bench_memory_variable_strings(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    init();

    let ch = rt.block_on(get_or_create_container(None));

    let client = rt
        .block_on(
            arrow_tests::setup_test_arrow_client(ch.get_native_url(), &ch.user, &ch.password)
                .with_ipv4_only(true)
                .with_compression(CompressionMethod::None)
                .build::<ArrowFormat>(),
        )
        .expect("client setup");

    rt.block_on(arrow_tests::setup_database(TEST_DB_NAME, &client)).expect("database setup");

    let mut group = c.benchmark_group("MemoryVariableStrings");
    let _ = group.sample_size(20);
    let _ = group.measurement_time(Duration::from_secs(15));

    let row_counts = [10_000, 50_000, 100_000, 500_000];

    for &rows in &row_counts {
        let (batch, wire_bytes) = create_variable_string_batch(rows);
        let stats = MemoryStats::from_batch(&batch, wire_bytes);
        log_memory_stats(&format!("variable_str_{rows}"), &stats);

        let table = rt
            .block_on(arrow_tests::setup_table(&client, TEST_DB_NAME, &batch.schema()))
            .expect("table setup");
        let query = format!("INSERT INTO {table} FORMAT NATIVE");

        let _ = group.throughput(Throughput::Bytes(stats.arrow_memory_bytes as u64));

        let _ = group.bench_with_input(
            BenchmarkId::new("insert", rows),
            &(&query, &client, &batch),
            |b, (query, client, batch)| {
                b.to_async(&rt).iter_batched(
                    || (*batch).clone(),
                    |batch| async {
                        let stream = client.insert(*query, batch, None).await.unwrap();
                        drop(black_box(stream));
                    },
                    criterion::BatchSize::SmallInput,
                );
            },
        );
    }

    group.finish();

    if std::env::var(DISABLE_CLEANUP_ENV).is_ok_and(|e| e.eq_ignore_ascii_case("true")) {
        return;
    }
    rt.block_on(ch.shutdown()).expect("shutdown");
}

/// Benchmark memory overhead of nullable columns
fn bench_memory_nullable(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    init();

    let ch = rt.block_on(get_or_create_container(None));

    let client = rt
        .block_on(
            arrow_tests::setup_test_arrow_client(ch.get_native_url(), &ch.user, &ch.password)
                .with_ipv4_only(true)
                .with_compression(CompressionMethod::None)
                .build::<ArrowFormat>(),
        )
        .expect("client setup");

    rt.block_on(arrow_tests::setup_database(TEST_DB_NAME, &client)).expect("database setup");

    let mut group = c.benchmark_group("MemoryNullable");
    let _ = group.sample_size(20);
    let _ = group.measurement_time(Duration::from_secs(15));

    let rows = 100_000;
    let null_ratios = [0.0, 0.1, 0.25, 0.5, 0.75, 0.9];

    for &ratio in &null_ratios {
        let (batch, wire_bytes) = create_nullable_batch(rows, ratio);
        let stats = MemoryStats::from_batch(&batch, wire_bytes);
        log_memory_stats(&format!("nullable_{:.0}pct", ratio * 100.0), &stats);

        let table = rt
            .block_on(arrow_tests::setup_table(&client, TEST_DB_NAME, &batch.schema()))
            .expect("table setup");
        let query = format!("INSERT INTO {table} FORMAT NATIVE");

        let _ = group.throughput(Throughput::Bytes(stats.arrow_memory_bytes as u64));

        let _ = group.bench_with_input(
            BenchmarkId::new("insert", format!("{:.0}pct_null", ratio * 100.0)),
            &(&query, &client, &batch),
            |b, (query, client, batch)| {
                b.to_async(&rt).iter_batched(
                    || (*batch).clone(),
                    |batch| async {
                        let stream = client.insert(*query, batch, None).await.unwrap();
                        drop(black_box(stream));
                    },
                    criterion::BatchSize::SmallInput,
                );
            },
        );
    }

    group.finish();

    if std::env::var(DISABLE_CLEANUP_ENV).is_ok_and(|e| e.eq_ignore_ascii_case("true")) {
        return;
    }
    rt.block_on(ch.shutdown()).expect("shutdown");
}

/// Benchmark memory overhead of nested types
fn bench_memory_nested(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    init();

    let ch = rt.block_on(get_or_create_container(None));

    let client = rt
        .block_on(
            arrow_tests::setup_test_arrow_client(ch.get_native_url(), &ch.user, &ch.password)
                .with_ipv4_only(true)
                .with_compression(CompressionMethod::None)
                .build::<ArrowFormat>(),
        )
        .expect("client setup");

    rt.block_on(arrow_tests::setup_database(TEST_DB_NAME, &client)).expect("database setup");

    let mut group = c.benchmark_group("MemoryNested");
    let _ = group.sample_size(15);
    let _ = group.measurement_time(Duration::from_secs(20));

    let rows = 50_000;
    let list_sizes = [2, 5, 10, 20, 50];

    for &list_size in &list_sizes {
        let (batch, wire_bytes) = create_nested_batch(rows, list_size);
        let stats = MemoryStats::from_batch(&batch, wire_bytes);
        log_memory_stats(&format!("nested_list_{list_size}"), &stats);

        let table = rt
            .block_on(arrow_tests::setup_table(&client, TEST_DB_NAME, &batch.schema()))
            .expect("table setup");
        let query = format!("INSERT INTO {table} FORMAT NATIVE");

        let _ = group.throughput(Throughput::Bytes(stats.arrow_memory_bytes as u64));

        let _ = group.bench_with_input(
            BenchmarkId::new("insert", format!("list_size_{list_size}")),
            &(&query, &client, &batch),
            |b, (query, client, batch)| {
                b.to_async(&rt).iter_batched(
                    || (*batch).clone(),
                    |batch| async {
                        let stream = client.insert(*query, batch, None).await.unwrap();
                        drop(black_box(stream));
                    },
                    criterion::BatchSize::SmallInput,
                );
            },
        );
    }

    group.finish();

    if std::env::var(DISABLE_CLEANUP_ENV).is_ok_and(|e| e.eq_ignore_ascii_case("true")) {
        return;
    }
    rt.block_on(ch.shutdown()).expect("shutdown");
}

/// Benchmark memory efficiency of dictionary encoding
fn bench_memory_dictionary(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    init();

    let ch = rt.block_on(get_or_create_container(None));
    print_msg("Container ready for dictionary memory benchmarks");

    let client = rt
        .block_on(
            arrow_tests::setup_test_arrow_client(ch.get_native_url(), &ch.user, &ch.password)
                .with_ipv4_only(true)
                .with_compression(CompressionMethod::None)
                .build::<ArrowFormat>(),
        )
        .expect("client setup");

    rt.block_on(arrow_tests::setup_database(TEST_DB_NAME, &client)).expect("database setup");

    let mut group = c.benchmark_group("MemoryDictionary");
    let _ = group.sample_size(15);
    let _ = group.measurement_time(Duration::from_secs(20));

    let rows = 500_000;
    let value_len = 32;
    let cardinalities = [10, 100, 1000, 10_000];

    for &cardinality in &cardinalities {
        let (batch, wire_bytes) = create_dictionary_batch(rows, cardinality, value_len);
        let stats = MemoryStats::from_batch(&batch, wire_bytes);
        log_memory_stats(&format!("dict_card_{cardinality}"), &stats);

        let table = rt
            .block_on(arrow_tests::setup_table(&client, TEST_DB_NAME, &batch.schema()))
            .expect("table setup");
        let query = format!("INSERT INTO {table} FORMAT NATIVE");

        let _ = group.throughput(Throughput::Bytes(stats.arrow_memory_bytes as u64));

        let _ = group.bench_with_input(
            BenchmarkId::new("insert", format!("card_{cardinality}")),
            &(&query, &client, &batch),
            |b, (query, client, batch)| {
                b.to_async(&rt).iter_batched(
                    || (*batch).clone(),
                    |batch| async {
                        let stream = client.insert(*query, batch, None).await.unwrap();
                        drop(black_box(stream));
                    },
                    criterion::BatchSize::SmallInput,
                );
            },
        );
    }

    group.finish();

    if std::env::var(DISABLE_CLEANUP_ENV).is_ok_and(|e| e.eq_ignore_ascii_case("true")) {
        return;
    }
    rt.block_on(ch.shutdown()).expect("shutdown");
}

/// Benchmark query memory allocation patterns
fn bench_memory_query(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    init();

    let ch = rt.block_on(get_or_create_container(None));

    let client = rt
        .block_on(
            arrow_tests::setup_test_arrow_client(ch.get_native_url(), &ch.user, &ch.password)
                .with_ipv4_only(true)
                .with_compression(CompressionMethod::None)
                .build::<ArrowFormat>(),
        )
        .expect("client setup");

    rt.block_on(arrow_tests::setup_database(TEST_DB_NAME, &client)).expect("database setup");

    let mut group = c.benchmark_group("MemoryQuery");
    let _ = group.sample_size(20);
    let _ = group.measurement_time(Duration::from_secs(15));

    let row_counts = [50_000, 100_000, 500_000];

    for &rows in &row_counts {
        // Insert fixed-size data first
        let (insert_batch, wire_bytes) = create_fixed_size_batch(rows);
        let table = rt
            .block_on(arrow_tests::setup_table(&client, TEST_DB_NAME, &insert_batch.schema()))
            .expect("table setup");

        rt.block_on(async {
            let insert_query = format!("INSERT INTO {table} FORMAT NATIVE");
            let mut stream = client.insert(&insert_query, insert_batch, None).await.unwrap();
            while let Some(r) = stream.next().await {
                r.unwrap();
            }
        });

        let _ = group.throughput(Throughput::Bytes(wire_bytes as u64));

        let query = format!("SELECT * FROM {table}");
        let _ = group.bench_with_input(
            BenchmarkId::new("fixed_types", rows),
            &(&query, &client),
            |b, (query, client)| {
                b.to_async(&rt).iter(|| async {
                    let mut stream = client.query(*query, None).await.unwrap();
                    let mut total_memory = 0usize;
                    while let Some(result) = stream.next().await {
                        let batch = result.unwrap();
                        total_memory += batch.get_array_memory_size();
                        drop(black_box(batch));
                    }
                    black_box(total_memory)
                });
            },
        );
    }

    group.finish();

    if std::env::var(DISABLE_CLEANUP_ENV).is_ok_and(|e| e.eq_ignore_ascii_case("true")) {
        return;
    }
    rt.block_on(ch.shutdown()).expect("shutdown");
}

criterion_group!(
    benches,
    bench_memory_fixed_types,
    bench_memory_string_sizes,
    bench_memory_variable_strings,
    bench_memory_nullable,
    bench_memory_nested,
    bench_memory_dictionary,
    bench_memory_query,
);
criterion_main!(benches);
