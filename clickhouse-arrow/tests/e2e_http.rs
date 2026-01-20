//! End-to-end tests for HTTP transport with `ArrowStream` format.

#![allow(unused_crate_dependencies)]

use std::sync::Arc;

use arrow::array::{
    ArrayRef, BooleanArray, Date32Array, Float64Array, Int32Array, Int64Array, RecordBatch,
    StringArray, TimestampMillisecondArray,
};
use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use clickhouse_arrow::http::{HttpClient, HttpOptions};
use clickhouse_arrow::prelude::ClientBuilder;
use clickhouse_arrow::test_utils::ClickHouseContainer;

pub mod common;
pub mod tests;

const TRACING_DIRECTIVES: &[(&str, &str)] =
    &[("testcontainers", "debug"), ("clickhouse_arrow", "debug"), ("reqwest", "debug")];

// Basic HTTP query test
#[cfg(feature = "test-utils")]
e2e_test!(e2e_http_query, test_http_query, TRACING_DIRECTIVES, None);

// HTTP insert test
#[cfg(feature = "test-utils")]
e2e_test!(e2e_http_insert, test_http_insert, TRACING_DIRECTIVES, None);

// HTTP round-trip test
#[cfg(feature = "test-utils")]
e2e_test!(e2e_http_round_trip, test_http_round_trip, TRACING_DIRECTIVES, None);

// HTTP insert_batches test
#[cfg(feature = "test-utils")]
e2e_test!(e2e_http_insert_batches, test_http_insert_batches, TRACING_DIRECTIVES, None);

// HTTP ClientBuilder integration test
#[cfg(feature = "test-utils")]
e2e_test!(e2e_http_builder, test_http_builder, TRACING_DIRECTIVES, None);

// HTTP large data test
#[cfg(feature = "test-utils")]
e2e_test!(e2e_http_large_data, test_http_large_data, TRACING_DIRECTIVES, None);

// HTTP type coverage test
#[cfg(feature = "test-utils")]
e2e_test!(e2e_http_types, test_http_types, TRACING_DIRECTIVES, None);

// HTTP error handling test
#[cfg(feature = "test-utils")]
e2e_test!(e2e_http_errors, test_http_errors, TRACING_DIRECTIVES, None);

/// Create a test schema
fn test_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("name", DataType::Utf8, true),
    ]))
}

/// Create a test `RecordBatch`
fn test_batch() -> RecordBatch {
    let schema = test_schema();
    let id_array = Int64Array::from(vec![1, 2, 3, 4, 5]);
    let name_array =
        StringArray::from(vec![Some("Alice"), Some("Bob"), None, Some("Dave"), Some("Eve")]);

    RecordBatch::try_new(schema, vec![Arc::new(id_array), Arc::new(name_array)]).unwrap()
}

/// Create HTTP client from container
fn create_http_client(ch: &ClickHouseContainer) -> HttpClient {
    let url = format!("http://{}:{}", ch.endpoint, ch.http_port);

    let options = HttpOptions::new(&url)
        .expect("Valid URL")
        .with_credentials(&ch.user, &ch.password)
        .with_database("default");

    HttpClient::new(options).expect("Create HTTP client")
}

/// Test basic HTTP query execution
///
/// # Panics
/// Panics if assertions fail
pub async fn test_http_query(ch: Arc<ClickHouseContainer>) {
    let client = create_http_client(&ch);

    // Simple scalar query
    let batches = client.query("SELECT 1 as value").await.expect("Query should succeed");

    assert_eq!(batches.len(), 1);
    assert_eq!(batches[0].num_rows(), 1);

    eprintln!("HTTP query test passed");
}

/// Test HTTP insert functionality
///
/// # Panics
/// Panics if assertions fail
pub async fn test_http_insert(ch: Arc<ClickHouseContainer>) {
    let client = create_http_client(&ch);

    // Create table via DDL
    client
        .execute(
            "CREATE TABLE IF NOT EXISTS http_test_insert (
                id Int64,
                name Nullable(String)
            ) ENGINE = MergeTree() ORDER BY id",
        )
        .await
        .expect("Create table");

    // Insert test data
    let batch = test_batch();
    client.insert("http_test_insert", batch).await.expect("Insert should succeed");

    // Verify data was inserted
    let batches =
        client.query("SELECT count(*) as cnt FROM http_test_insert").await.expect("Count query");

    assert_eq!(batches.len(), 1);
    let count_col = batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<arrow::array::UInt64Array>()
        .expect("Count column should be UInt64");
    assert_eq!(count_col.value(0), 5);

    // Cleanup
    client.execute("DROP TABLE IF EXISTS http_test_insert").await.expect("Drop table");

    eprintln!("HTTP insert test passed");
}

/// Test HTTP round-trip (insert + query)
///
/// # Panics
/// Panics if assertions fail
pub async fn test_http_round_trip(ch: Arc<ClickHouseContainer>) {
    let client = create_http_client(&ch);

    // Create table
    client
        .execute(
            "CREATE TABLE IF NOT EXISTS http_test_round_trip (
                id Int64,
                name Nullable(String)
            ) ENGINE = MergeTree() ORDER BY id",
        )
        .await
        .expect("Create table");

    // Insert data
    let original_batch = test_batch();
    client
        .insert("http_test_round_trip", original_batch.clone())
        .await
        .expect("Insert should succeed");

    // Query back
    let batches = client
        .query("SELECT id, name FROM http_test_round_trip ORDER BY id")
        .await
        .expect("Query should succeed");

    assert_eq!(batches.len(), 1);
    let result = &batches[0];

    // Verify row count
    assert_eq!(result.num_rows(), original_batch.num_rows());

    // Verify id column
    let result_ids =
        result.column(0).as_any().downcast_ref::<Int64Array>().expect("id column should be Int64");
    let original_ids =
        original_batch.column(0).as_any().downcast_ref::<Int64Array>().expect("id column");

    for i in 0..result.num_rows() {
        assert_eq!(result_ids.value(i), original_ids.value(i), "id mismatch at row {i}");
    }

    // Cleanup
    client.execute("DROP TABLE IF EXISTS http_test_round_trip").await.expect("Drop table");

    eprintln!("HTTP round-trip test passed");
}

/// Test `insert_batches()` with multiple batches
///
/// # Panics
/// Panics if assertions fail
pub async fn test_http_insert_batches(ch: Arc<ClickHouseContainer>) {
    let client = create_http_client(&ch);

    // Create table
    client
        .execute(
            "CREATE TABLE IF NOT EXISTS http_test_batches (
                id Int64,
                name Nullable(String)
            ) ENGINE = MergeTree() ORDER BY id",
        )
        .await
        .expect("Create table");

    // Create multiple batches
    let schema = test_schema();

    let batch1 = RecordBatch::try_new(Arc::clone(&schema), vec![
        Arc::new(Int64Array::from(vec![1, 2, 3])),
        Arc::new(StringArray::from(vec![Some("A"), Some("B"), Some("C")])),
    ])
    .unwrap();

    let batch2 = RecordBatch::try_new(Arc::clone(&schema), vec![
        Arc::new(Int64Array::from(vec![4, 5, 6])),
        Arc::new(StringArray::from(vec![Some("D"), None, Some("F")])),
    ])
    .unwrap();

    let batch3 = RecordBatch::try_new(Arc::clone(&schema), vec![
        Arc::new(Int64Array::from(vec![7, 8, 9, 10])),
        Arc::new(StringArray::from(vec![Some("G"), Some("H"), Some("I"), Some("J")])),
    ])
    .unwrap();

    // Insert all batches at once
    client
        .insert_batches("http_test_batches", vec![batch1, batch2, batch3])
        .await
        .expect("Insert batches should succeed");

    // Verify total count
    let batches =
        client.query("SELECT count(*) as cnt FROM http_test_batches").await.expect("Count query");

    let count_col = batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<arrow::array::UInt64Array>()
        .expect("Count column");
    assert_eq!(count_col.value(0), 10); // 3 + 3 + 4 = 10 rows

    // Cleanup
    client.execute("DROP TABLE IF EXISTS http_test_batches").await.expect("Drop table");

    eprintln!("HTTP insert_batches test passed");
}

/// Test `ClientBuilder.build_http()` integration
///
/// # Panics
/// Panics if assertions fail
pub async fn test_http_builder(ch: Arc<ClickHouseContainer>) {
    let url = format!("http://{}:{}", ch.endpoint, ch.http_port);

    // Build HTTP client via ClientBuilder
    let client = ClientBuilder::new()
        .with_endpoint(&url)
        .with_username(&ch.user)
        .with_database("default")
        .build_http()
        .expect("build_http should succeed");

    // Verify it works
    let batches = client.query("SELECT 42 as answer").await.expect("Query should succeed");

    assert_eq!(batches.len(), 1);
    assert_eq!(batches[0].num_rows(), 1);

    eprintln!("HTTP builder test passed");
}

/// Test large data insertion and retrieval
///
/// # Panics
/// Panics if assertions fail
#[allow(clippy::cast_possible_wrap, clippy::cast_precision_loss, clippy::items_after_statements)]
pub async fn test_http_large_data(ch: Arc<ClickHouseContainer>) {
    let client = create_http_client(&ch);

    // Create table
    client
        .execute(
            "CREATE TABLE IF NOT EXISTS http_test_large (
                id Int64,
                value Float64,
                text String
            ) ENGINE = MergeTree() ORDER BY id",
        )
        .await
        .expect("Create table");

    // Create a large batch (10,000 rows)
    const ROW_COUNT: usize = 10_000;

    let ids: Vec<i64> = (0..ROW_COUNT as i64).collect();
    let values: Vec<f64> = (0..ROW_COUNT).map(|i| i as f64 * 1.5).collect();
    let texts: Vec<String> = (0..ROW_COUNT).map(|i| format!("row_{i:05}")).collect();

    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("value", DataType::Float64, false),
        Field::new("text", DataType::Utf8, false),
    ]));

    let batch = RecordBatch::try_new(schema, vec![
        Arc::new(Int64Array::from(ids)),
        Arc::new(Float64Array::from(values)),
        Arc::new(StringArray::from(texts.iter().map(String::as_str).collect::<Vec<_>>())),
    ])
    .unwrap();

    // Insert large batch
    client.insert("http_test_large", batch).await.expect("Insert large batch");

    // Verify count
    let batches =
        client.query("SELECT count(*) as cnt FROM http_test_large").await.expect("Count query");

    let count_col = batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<arrow::array::UInt64Array>()
        .expect("Count column");
    assert_eq!(count_col.value(0), ROW_COUNT as u64);

    // Query back with limit and verify some values
    let batches = client
        .query("SELECT id, value, text FROM http_test_large ORDER BY id LIMIT 100")
        .await
        .expect("Query large data");

    assert_eq!(batches[0].num_rows(), 100);

    let result_ids = batches[0].column(0).as_any().downcast_ref::<Int64Array>().expect("id column");
    assert_eq!(result_ids.value(0), 0);
    assert_eq!(result_ids.value(99), 99);

    // Cleanup
    client.execute("DROP TABLE IF EXISTS http_test_large").await.expect("Drop table");

    eprintln!("HTTP large data test passed ({ROW_COUNT} rows)");
}

/// Test various `ClickHouse` types
///
/// # Panics
/// Panics if assertions fail
pub async fn test_http_types(ch: Arc<ClickHouseContainer>) {
    let client = create_http_client(&ch);

    // Create table with various types
    client
        .execute(
            "CREATE TABLE IF NOT EXISTS http_test_types (
                id Int32,
                big_id Int64,
                flag UInt8,
                ratio Float64,
                name String,
                optional_name Nullable(String),
                created_date Date,
                created_at DateTime64(3)
            ) ENGINE = MergeTree() ORDER BY id",
        )
        .await
        .expect("Create table");

    // Create batch with various types
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("big_id", DataType::Int64, false),
        Field::new("flag", DataType::Boolean, false),
        Field::new("ratio", DataType::Float64, false),
        Field::new("name", DataType::Utf8, false),
        Field::new("optional_name", DataType::Utf8, true),
        Field::new("created_date", DataType::Date32, false),
        Field::new("created_at", DataType::Timestamp(TimeUnit::Millisecond, None), false),
    ]));

    let arrays: Vec<ArrayRef> = vec![
        Arc::new(Int32Array::from(vec![1, 2, 3])),
        Arc::new(Int64Array::from(vec![100, 200, 300])),
        Arc::new(BooleanArray::from(vec![true, false, true])),
        Arc::new(Float64Array::from(vec![1.5, 2.5, 3.5])),
        Arc::new(StringArray::from(vec!["Alice", "Bob", "Charlie"])),
        Arc::new(StringArray::from(vec![Some("A"), None, Some("C")])),
        Arc::new(Date32Array::from(vec![19000, 19001, 19002])), // Days since epoch
        Arc::new(TimestampMillisecondArray::from(vec![
            1_640_000_000_000_i64, // 2021-12-20
            1_640_100_000_000_i64,
            1_640_200_000_000_i64,
        ])),
    ];

    let batch = RecordBatch::try_new(schema, arrays).unwrap();

    // Insert
    client.insert("http_test_types", batch).await.expect("Insert types");

    // Query back and verify
    let batches = client
        .query(
            "SELECT id, big_id, flag, ratio, name, optional_name FROM http_test_types ORDER BY id",
        )
        .await
        .expect("Query types");

    assert_eq!(batches.len(), 1);
    assert_eq!(batches[0].num_rows(), 3);

    // Verify Int32 column
    let id_col = batches[0].column(0).as_any().downcast_ref::<Int32Array>().expect("id column");
    assert_eq!(id_col.value(0), 1);
    assert_eq!(id_col.value(2), 3);

    // Cleanup
    client.execute("DROP TABLE IF EXISTS http_test_types").await.expect("Drop table");

    eprintln!("HTTP types test passed");
}

/// Test error handling
///
/// # Panics
/// Panics if assertions fail
pub async fn test_http_errors(ch: Arc<ClickHouseContainer>) {
    let client = create_http_client(&ch);

    // Test invalid SQL - should return server error
    let result = client.query("SELECT * FROM nonexistent_table_12345").await;
    assert!(result.is_err(), "Query to nonexistent table should fail");
    let err = result.unwrap_err();
    let err_str = err.to_string();
    assert!(
        err_str.contains("Server error") || err_str.contains("UNKNOWN_TABLE"),
        "Error should mention server error or unknown table: {err_str}"
    );

    // Test invalid DDL
    let result = client.execute("CREATE INVALID SYNTAX").await;
    assert!(result.is_err(), "Invalid DDL should fail");

    // Test insert to nonexistent table
    let batch = test_batch();
    let result = client.insert("nonexistent_table_67890", batch).await;
    assert!(result.is_err(), "Insert to nonexistent table should fail");

    // Test empty batch insert (should succeed)
    client
        .execute(
            "CREATE TABLE IF NOT EXISTS http_test_empty (
                id Int64
            ) ENGINE = MergeTree() ORDER BY id",
        )
        .await
        .expect("Create table");

    let result = client.insert_batches("http_test_empty", vec![]).await;
    assert!(result.is_ok(), "Empty batch insert should succeed");

    // Cleanup
    client.execute("DROP TABLE IF EXISTS http_test_empty").await.expect("Drop table");

    eprintln!("HTTP error handling test passed");
}
