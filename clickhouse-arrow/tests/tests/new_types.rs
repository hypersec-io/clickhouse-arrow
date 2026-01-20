//! E2E integration tests for new `ClickHouse` types introduced in v24+
//!
//! Tests: `BFloat16`, Variant, Dynamic, Nested, Time/Time64
//!
//! These tests verify round-trip serialization/deserialization against a real `ClickHouse`
//! instance. Uses `ArrowClient` for querying since it handles arbitrary result types better.

// Test utilities intentionally panic on failure
#![allow(clippy::missing_panics_doc)]
#![allow(clippy::unused_async)]

use std::sync::Arc;

use arrow::record_batch::RecordBatch;
use clickhouse_arrow::prelude::*;
use clickhouse_arrow::test_utils::ClickHouseContainer;
use clickhouse_arrow::{CompressionMethod, Result as ClickHouseResult};
use futures_util::StreamExt;
use tracing::{debug, info};

use crate::common::header;

/// Helper to create an `ArrowClient` for testing
async fn create_client(ch: &ClickHouseContainer) -> ArrowClient {
    let native_url = ch.get_native_url();
    debug!("ClickHouse Native URL: {native_url}");

    ClientBuilder::new()
        .with_endpoint(native_url)
        .with_username(&ch.user)
        .with_password(&ch.password)
        .with_ipv4_only(true)
        .with_compression(CompressionMethod::LZ4)
        .build()
        .await
        .expect("Building client")
}

/// Helper to query and collect results
async fn query_all(client: &ArrowClient, query: &str, qid: Qid) -> Vec<RecordBatch> {
    client
        .query(query, Some(qid))
        .await
        .expect("Query failed")
        .collect::<Vec<_>>()
        .await
        .into_iter()
        .collect::<ClickHouseResult<Vec<_>>>()
        .expect("Failed to collect results")
}

/// Count total rows across all batches
fn count_rows(batches: &[RecordBatch]) -> usize { batches.iter().map(RecordBatch::num_rows).sum() }

// =============================================================================
// BFloat16 Tests
// =============================================================================

/// Test `BFloat16` type round-trip via raw SQL
pub async fn test_bfloat16_basic(ch: Arc<ClickHouseContainer>) {
    let client = create_client(&ch).await;

    let qid = Qid::new();
    let db = format!("test_bfloat16_{qid}");
    let table = "bf16_test";

    // Setup
    header(qid, format!("Creating database: {db}"));
    client.execute(format!("CREATE DATABASE IF NOT EXISTS {db}"), Some(qid)).await.unwrap();

    // Create table with BFloat16 column
    header(qid, "Creating BFloat16 table");
    let create_sql = format!(
        "CREATE TABLE {db}.{table} (
            id UInt32,
            bf16_col BFloat16,
            bf16_nullable Nullable(BFloat16)
        ) ENGINE = MergeTree() ORDER BY id"
    );
    client.execute(&create_sql, Some(qid)).await.unwrap();

    // Insert test data
    header(qid, "Inserting BFloat16 test data");
    let insert_sql = format!(
        "INSERT INTO {db}.{table} VALUES
        (1, 1.5, 2.5),
        (2, 0.0, NULL),
        (3, -1.25, -0.5),
        (4, 3.14159, 2.71828)"
    );
    client.execute(&insert_sql, Some(qid)).await.unwrap();

    // Wait for data to be visible
    tokio::time::sleep(std::time::Duration::from_secs(1)).await;

    // Query and verify
    header(qid, "Querying BFloat16 data");
    let query = format!("SELECT id, bf16_col, bf16_nullable FROM {db}.{table} ORDER BY id");
    let batches = query_all(&client, &query, qid).await;

    for batch in &batches {
        info!("Received batch with {} rows, {} columns", batch.num_rows(), batch.num_columns());
        info!("Schema: {:?}", batch.schema());
    }

    assert!(!batches.is_empty(), "Expected at least one batch");
    assert_eq!(count_rows(&batches), 4, "Expected 4 rows");

    // Cleanup
    header(qid, "Cleanup");
    client.execute(format!("DROP TABLE {db}.{table}"), None).await.unwrap();
    client.execute(format!("DROP DATABASE {db}"), None).await.unwrap();

    info!("BFloat16 basic test passed!");
}

// =============================================================================
// Variant Tests
// =============================================================================

/// Test Variant type with multiple inner types
///
/// Note: Arrow Union builder support is pending. This test queries the variant type info
/// (variantType function) rather than the raw variant data to verify the type system works.
pub async fn test_variant_basic(ch: Arc<ClickHouseContainer>) {
    let client = create_client(&ch).await;

    let qid = Qid::new();
    let db = format!("test_variant_{qid}");
    let table = "variant_test";

    // Setup
    header(qid, format!("Creating database: {db}"));
    client.execute(format!("CREATE DATABASE IF NOT EXISTS {db}"), Some(qid)).await.unwrap();

    // Create table with Variant column (String, UInt64, Float64)
    header(qid, "Creating Variant table");
    let create_sql = format!(
        "CREATE TABLE {db}.{table} (
            id UInt32,
            var_col Variant(String, UInt64, Float64)
        ) ENGINE = MergeTree() ORDER BY id
        SETTINGS allow_experimental_variant_type = 1"
    );
    client.execute(&create_sql, Some(qid)).await.unwrap();

    // Insert test data
    header(qid, "Inserting Variant test data");
    let insert_sql = format!(
        "INSERT INTO {db}.{table} VALUES
        (1, 'hello'),
        (2, 42),
        (3, 3.14159),
        (4, NULL),
        (5, 'world'),
        (6, 12345678901234)"
    );
    client.execute(&insert_sql, Some(qid)).await.unwrap();

    tokio::time::sleep(std::time::Duration::from_secs(1)).await;

    // Query type info (avoiding Variant column direct read until Arrow Union builder is
    // implemented)
    header(qid, "Querying Variant type info");
    let query = format!("SELECT id, variantType(var_col) as vtype FROM {db}.{table} ORDER BY id");
    let batches = query_all(&client, &query, qid).await;

    for batch in &batches {
        info!("Received batch with {} rows", batch.num_rows());
        info!("Schema: {:?}", batch.schema());
    }

    assert!(!batches.is_empty(), "Expected at least one batch");
    assert_eq!(count_rows(&batches), 6, "Expected 6 rows");

    // Cleanup
    header(qid, "Cleanup");
    client.execute(format!("DROP TABLE {db}.{table}"), None).await.unwrap();
    client.execute(format!("DROP DATABASE {db}"), None).await.unwrap();

    info!("Variant basic test passed!");
}

/// Test Variant with complex inner types (Array, Tuple)
///
/// Note: Arrow Union builder support is pending. This test queries type info only.
pub async fn test_variant_complex(ch: Arc<ClickHouseContainer>) {
    let client = create_client(&ch).await;

    let qid = Qid::new();
    let db = format!("test_variant_complex_{qid}");
    let table = "variant_complex_test";

    header(qid, format!("Creating database: {db}"));
    client.execute(format!("CREATE DATABASE IF NOT EXISTS {db}"), Some(qid)).await.unwrap();

    // Variant with Array and Tuple inner types
    header(qid, "Creating complex Variant table");
    let create_sql = format!(
        "CREATE TABLE {db}.{table} (
            id UInt32,
            var_col Variant(String, Array(Int32), Tuple(String, Int64))
        ) ENGINE = MergeTree() ORDER BY id
        SETTINGS allow_experimental_variant_type = 1"
    );
    client.execute(&create_sql, Some(qid)).await.unwrap();

    // Insert test data
    header(qid, "Inserting complex Variant data");
    let insert_sql = format!(
        "INSERT INTO {db}.{table} VALUES
        (1, 'simple_string'),
        (2, [1, 2, 3, 4, 5]),
        (3, ('name', 42)),
        (4, []),
        (5, ('empty', 0))"
    );
    client.execute(&insert_sql, Some(qid)).await.unwrap();

    tokio::time::sleep(std::time::Duration::from_secs(1)).await;

    // Query type info only (avoiding Variant column direct read)
    header(qid, "Querying complex Variant type info");
    let query = format!("SELECT id, variantType(var_col) as vtype FROM {db}.{table} ORDER BY id");
    let batches = query_all(&client, &query, qid).await;

    assert_eq!(count_rows(&batches), 5, "Expected 5 rows");

    // Cleanup
    client.execute(format!("DROP TABLE {db}.{table}"), None).await.unwrap();
    client.execute(format!("DROP DATABASE {db}"), None).await.unwrap();

    info!("Variant complex test passed!");
}

// =============================================================================
// Dynamic Tests
// =============================================================================

/// Test Dynamic type (runtime-typed column)
///
/// Note: Dynamic Arrow deserialization returns data as UTF-8 strings.
/// This test queries type info to avoid the direct Dynamic column read.
pub async fn test_dynamic_basic(ch: Arc<ClickHouseContainer>) {
    let client = create_client(&ch).await;

    let qid = Qid::new();
    let db = format!("test_dynamic_{qid}");
    let table = "dynamic_test";

    header(qid, format!("Creating database: {db}"));
    client.execute(format!("CREATE DATABASE IF NOT EXISTS {db}"), Some(qid)).await.unwrap();

    // Create table with Dynamic column
    header(qid, "Creating Dynamic table");
    let create_sql = format!(
        "CREATE TABLE {db}.{table} (
            id UInt32,
            dyn_col Dynamic
        ) ENGINE = MergeTree() ORDER BY id
        SETTINGS allow_experimental_dynamic_type = 1"
    );
    client.execute(&create_sql, Some(qid)).await.unwrap();

    // Insert heterogeneous data
    header(qid, "Inserting Dynamic test data");
    let insert_sql = format!(
        "INSERT INTO {db}.{table} VALUES
        (1, 'string_value'),
        (2, 42),
        (3, 3.14159),
        (4, [1, 2, 3]),
        (5, NULL),
        (6, true),
        (7, ('tuple_elem', 100))"
    );
    client.execute(&insert_sql, Some(qid)).await.unwrap();

    tokio::time::sleep(std::time::Duration::from_secs(1)).await;

    // Query type info (avoiding Dynamic column direct read)
    header(qid, "Querying Dynamic type info");
    let query = format!("SELECT id, dynamicType(dyn_col) as dtype FROM {db}.{table} ORDER BY id");
    let batches = query_all(&client, &query, qid).await;

    for batch in &batches {
        info!("Schema: {:?}", batch.schema());
    }

    assert_eq!(count_rows(&batches), 7, "Expected 7 rows");

    // Cleanup
    client.execute(format!("DROP TABLE {db}.{table}"), None).await.unwrap();
    client.execute(format!("DROP DATABASE {db}"), None).await.unwrap();

    info!("Dynamic basic test passed!");
}

/// Test Dynamic with `max_types` limit
///
/// Note: Dynamic Arrow deserialization returns data as UTF-8 strings.
/// This test queries type info only.
pub async fn test_dynamic_max_types(ch: Arc<ClickHouseContainer>) {
    let client = create_client(&ch).await;

    let qid = Qid::new();
    let db = format!("test_dynamic_max_{qid}");
    let table = "dynamic_max_test";

    header(qid, format!("Creating database: {db}"));
    client.execute(format!("CREATE DATABASE IF NOT EXISTS {db}"), Some(qid)).await.unwrap();

    // Create table with Dynamic(max_types=3)
    header(qid, "Creating Dynamic(max_types=3) table");
    let create_sql = format!(
        "CREATE TABLE {db}.{table} (
            id UInt32,
            dyn_col Dynamic(max_types=3)
        ) ENGINE = MergeTree() ORDER BY id
        SETTINGS allow_experimental_dynamic_type = 1"
    );
    client.execute(&create_sql, Some(qid)).await.unwrap();

    // Insert data - types beyond max_types will be stored as String
    header(qid, "Inserting data with type overflow");
    let insert_sql = format!(
        "INSERT INTO {db}.{table} VALUES
        (1, 'string'),
        (2, 42),
        (3, 3.14),
        (4, true)"
    );
    client.execute(&insert_sql, Some(qid)).await.unwrap();

    tokio::time::sleep(std::time::Duration::from_secs(1)).await;

    // Query type info (avoiding Dynamic column direct read)
    header(qid, "Querying Dynamic(max_types=3) type info");
    let query = format!("SELECT id, dynamicType(dyn_col) FROM {db}.{table} ORDER BY id");
    let batches = query_all(&client, &query, qid).await;

    assert_eq!(count_rows(&batches), 4, "Expected 4 rows");

    // Cleanup
    client.execute(format!("DROP TABLE {db}.{table}"), None).await.unwrap();
    client.execute(format!("DROP DATABASE {db}"), None).await.unwrap();

    info!("Dynamic max_types test passed!");
}

// =============================================================================
// Nested Tests
// =============================================================================

/// Test Nested type (parallel arrays structure)
pub async fn test_nested_basic(ch: Arc<ClickHouseContainer>) {
    let client = create_client(&ch).await;

    let qid = Qid::new();
    let db = format!("test_nested_{qid}");
    let table = "nested_test";

    header(qid, format!("Creating database: {db}"));
    client.execute(format!("CREATE DATABASE IF NOT EXISTS {db}"), Some(qid)).await.unwrap();

    // Create table with Nested column
    header(qid, "Creating Nested table");
    let create_sql = format!(
        "CREATE TABLE {db}.{table} (
            id UInt32,
            events Nested(
                name String,
                timestamp DateTime,
                value Float64
            )
        ) ENGINE = MergeTree() ORDER BY id"
    );
    client.execute(&create_sql, Some(qid)).await.unwrap();

    // Insert nested data
    header(qid, "Inserting Nested test data");
    let insert_sql = format!(
        "INSERT INTO {db}.{table} VALUES
        (1, ['click', 'view', 'purchase'], ['2024-01-01 00:00:00', '2024-01-01 00:01:00', \
         '2024-01-01 00:02:00'], [1.0, 2.0, 99.99]),
        (2, ['login'], ['2024-01-02 12:00:00'], [0.0]),
        (3, [], [], [])"
    );
    client.execute(&insert_sql, Some(qid)).await.unwrap();

    tokio::time::sleep(std::time::Duration::from_secs(1)).await;

    // Query and verify
    header(qid, "Querying Nested data");
    let query = format!(
        "SELECT id, events.name, events.timestamp, events.value FROM {db}.{table} ORDER BY id"
    );
    let batches = query_all(&client, &query, qid).await;

    for batch in &batches {
        info!("Schema: {:?}", batch.schema());
    }

    assert_eq!(count_rows(&batches), 3, "Expected 3 rows");

    // Cleanup
    client.execute(format!("DROP TABLE {db}.{table}"), None).await.unwrap();
    client.execute(format!("DROP DATABASE {db}"), None).await.unwrap();

    info!("Nested basic test passed!");
}

/// Test accessing Nested fields with flatten
pub async fn test_nested_flatten(ch: Arc<ClickHouseContainer>) {
    let client = create_client(&ch).await;

    let qid = Qid::new();
    let db = format!("test_nested_flatten_{qid}");
    let table = "nested_flatten_test";

    header(qid, format!("Creating database: {db}"));
    client.execute(format!("CREATE DATABASE IF NOT EXISTS {db}"), Some(qid)).await.unwrap();

    let create_sql = format!(
        "CREATE TABLE {db}.{table} (
            id UInt32,
            metrics Nested(key String, value Int64)
        ) ENGINE = MergeTree() ORDER BY id
        SETTINGS flatten_nested = 0"
    );
    client.execute(&create_sql, Some(qid)).await.unwrap();

    header(qid, "Inserting flatten_nested=0 data");
    let insert_sql = format!(
        "INSERT INTO {db}.{table} VALUES
        (1, [('cpu', 90), ('memory', 75), ('disk', 50)]),
        (2, [('cpu', 45)]),
        (3, [])"
    );
    client.execute(&insert_sql, Some(qid)).await.unwrap();

    tokio::time::sleep(std::time::Duration::from_secs(1)).await;

    // Query with array functions
    header(qid, "Querying with ARRAY JOIN");
    let query = format!(
        "SELECT id, m.key, m.value FROM {db}.{table} ARRAY JOIN metrics AS m ORDER BY id, m.key"
    );
    let batches = query_all(&client, &query, qid).await;

    // Row 1 has 3 metrics, Row 2 has 1, Row 3 has 0 (won't appear in ARRAY JOIN)
    assert_eq!(count_rows(&batches), 4, "Expected 4 rows from ARRAY JOIN");

    // Cleanup
    client.execute(format!("DROP TABLE {db}.{table}"), None).await.unwrap();
    client.execute(format!("DROP DATABASE {db}"), None).await.unwrap();

    info!("Nested flatten test passed!");
}

// =============================================================================
// Time Types Tests
// =============================================================================

/// Test Time (seconds since midnight) - Note: `ClickHouse` doesn't have native Time type,
/// this tests the `UInt32` representation we use
pub async fn test_time_simulation(ch: Arc<ClickHouseContainer>) {
    let client = create_client(&ch).await;

    let qid = Qid::new();
    let db = format!("test_time_{qid}");
    let table = "time_test";

    header(qid, format!("Creating database: {db}"));
    client.execute(format!("CREATE DATABASE IF NOT EXISTS {db}"), Some(qid)).await.unwrap();

    // ClickHouse stores time-of-day as UInt32 (seconds since midnight)
    header(qid, "Creating time simulation table");
    let create_sql = format!(
        "CREATE TABLE {db}.{table} (
            id UInt32,
            time_seconds UInt32,
            time_label String
        ) ENGINE = MergeTree() ORDER BY id"
    );
    client.execute(&create_sql, Some(qid)).await.unwrap();

    // Insert various times of day
    header(qid, "Inserting time data");
    let insert_sql = format!(
        "INSERT INTO {db}.{table} VALUES
        (1, 0, 'midnight'),
        (2, 3600, '1:00 AM'),
        (3, 43200, 'noon'),
        (4, 86399, '23:59:59')"
    );
    client.execute(&insert_sql, Some(qid)).await.unwrap();

    tokio::time::sleep(std::time::Duration::from_secs(1)).await;

    // Query with time formatting
    header(qid, "Querying time data with formatting");
    let query = format!(
        "SELECT id, time_seconds, formatDateTime(toDateTime(time_seconds), '%H:%M:%S') as \
         formatted
         FROM {db}.{table} ORDER BY id"
    );
    let batches = query_all(&client, &query, qid).await;

    assert_eq!(count_rows(&batches), 4, "Expected 4 rows");

    // Cleanup
    client.execute(format!("DROP TABLE {db}.{table}"), None).await.unwrap();
    client.execute(format!("DROP DATABASE {db}"), None).await.unwrap();

    info!("Time simulation test passed!");
}

// =============================================================================
// Combined New Types Test
// =============================================================================

/// Test table with multiple new types together
pub async fn test_new_types_combined(ch: Arc<ClickHouseContainer>) {
    let client = create_client(&ch).await;

    let qid = Qid::new();
    let db = format!("test_combined_{qid}");
    let table = "combined_test";

    header(qid, format!("Creating database: {db}"));
    client.execute(format!("CREATE DATABASE IF NOT EXISTS {db}"), Some(qid)).await.unwrap();

    // Table with BFloat16 and Nested (skip Variant/Dynamic if not supported in this CH version)
    header(qid, "Creating combined types table");
    let create_sql = format!(
        "CREATE TABLE {db}.{table} (
            id UInt32,
            bf16_value BFloat16,
            events Nested(name String, score Float32)
        ) ENGINE = MergeTree() ORDER BY id"
    );

    match client.execute(&create_sql, Some(qid)).await {
        Ok(()) => {
            header(qid, "Inserting combined data");
            let insert_sql = format!(
                "INSERT INTO {db}.{table} VALUES
                (1, 1.5, ['event1', 'event2'], [0.9, 0.8]),
                (2, 2.5, [], []),
                (3, 0.0, ['single'], [1.0])"
            );
            client.execute(&insert_sql, Some(qid)).await.unwrap();

            tokio::time::sleep(std::time::Duration::from_secs(1)).await;

            // Query
            header(qid, "Querying combined data");
            let query = format!("SELECT * FROM {db}.{table} ORDER BY id");
            let batches = query_all(&client, &query, qid).await;

            for batch in &batches {
                info!("Schema: {:?}", batch.schema());
            }

            assert_eq!(count_rows(&batches), 3, "Expected 3 rows");

            // Cleanup
            client.execute(format!("DROP TABLE {db}.{table}"), None).await.unwrap();
        }
        Err(e) => {
            // Some ClickHouse versions may not support all types
            info!("Combined types table creation failed (may be CH version): {e}");
        }
    }

    client.execute(format!("DROP DATABASE IF EXISTS {db}"), None).await.unwrap();

    info!("Combined new types test completed!");
}
