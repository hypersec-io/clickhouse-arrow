//! Tests for EXPLAIN functionality via `query_with_options`.

// Test utilities intentionally panic on failure
#![allow(clippy::missing_panics_doc)]
#![allow(clippy::unused_async)]

use std::sync::Arc;

use clickhouse_arrow::CompressionMethod;
use clickhouse_arrow::prelude::*;
use clickhouse_arrow::test_utils::ClickHouseContainer;
use futures_util::StreamExt;
use tracing::debug;

use super::arrow::{bootstrap, create_schema, drop_schema};
use crate::common::header;

/// Test EXPLAIN AST with parallel query execution.
pub async fn test_explain_ast_parallel(ch: Arc<ClickHouseContainer>) {
    let (client, options) = bootstrap(ch.as_ref(), Some(CompressionMethod::None)).await;

    // Create a simple table for testing
    let schema = simple_schema();
    let batch = simple_batch();
    let (db, table) =
        create_schema(&client, schema, &options).await.expect("Schema creation failed");

    // Insert test data
    drop(
        client
            .insert(&format!("INSERT INTO {db}.{table} FORMAT Native"), batch, None)
            .await
            .expect("Insert failed")
            .collect::<Vec<_>>()
            .await,
    );

    let qid = Qid::new();
    header(qid, "Testing EXPLAIN AST with parallel query");

    // Run query with EXPLAIN AST in parallel
    let query = format!("SELECT * FROM {db}.{table}");
    let explain_opts = ExplainOptions::new()
        .with_operation(ExplainOperation::Ast)
        .with_mode(ExplainMode::Parallel);
    let query_opts = QueryOptions::new().with_explain(explain_opts).with_qid(qid);

    let mut response = client.query_with_options(&query, query_opts).await.expect("Query failed");

    // Verify we got explain results
    assert!(response.has_explain(), "Response should have explain");

    // Collect the main query results
    let mut row_count = 0;
    while let Some(batch_result) = response.next().await {
        let batch = batch_result.expect("Batch should be valid");
        row_count += batch.num_rows();
    }
    assert!(row_count > 0, "Should have received rows from main query");

    // Get the explain result
    let explain_result =
        response.explain().await.expect("Should have explain").expect("Explain should succeed");
    match explain_result {
        ExplainResult::Text(text) => {
            debug!("EXPLAIN AST result:\n{text}");
            assert!(!text.is_empty(), "EXPLAIN AST should return non-empty text");
            // AST output typically contains SelectWithUnionQuery
            assert!(
                text.contains("Select") || text.contains("Query"),
                "EXPLAIN AST output should contain query AST nodes"
            );
        }
        ExplainResult::Arrow(batch) => {
            debug!("EXPLAIN AST returned Arrow batch with {} rows", batch.num_rows());
            assert!(batch.num_rows() > 0, "EXPLAIN AST should have rows");
        }
        #[cfg(feature = "serde")]
        ExplainResult::Json(_) => {
            panic!("EXPLAIN AST should not return JSON format");
        }
    }

    drop_schema(&db, &table, &client).await.expect("Drop table");
    client.shutdown().await.unwrap();
}

/// Test EXPLAIN SYNTAX for query optimization.
pub async fn test_explain_syntax(ch: Arc<ClickHouseContainer>) {
    let (client, _) = bootstrap(ch.as_ref(), Some(CompressionMethod::None)).await;

    let qid = Qid::new();
    header(qid, "Testing EXPLAIN SYNTAX");

    // Run query with EXPLAIN SYNTAX in explain-only mode
    let query = "SELECT 1 + 1";
    let explain_opts = ExplainOptions::new()
        .with_operation(ExplainOperation::Syntax)
        .with_mode(ExplainMode::ExplainOnly);
    let query_opts = QueryOptions::new().with_explain(explain_opts).with_qid(qid);

    let mut response = client.query_with_options(query, query_opts).await.expect("Query failed");

    // Verify we got explain results
    assert!(response.has_explain(), "Response should have explain");

    // Main query should return nothing in ExplainOnly mode
    let mut had_rows = false;
    while let Some(batch_result) = response.next().await {
        let batch = batch_result.expect("Batch should be valid");
        if batch.num_rows() > 0 {
            had_rows = true;
        }
    }
    assert!(!had_rows, "ExplainOnly mode should not return data rows");

    // Get the explain result
    let explain_result =
        response.explain().await.expect("Should have explain").expect("Explain should succeed");
    match explain_result {
        ExplainResult::Text(text) => {
            debug!("EXPLAIN SYNTAX result:\n{text}");
            assert!(!text.is_empty(), "EXPLAIN SYNTAX should return non-empty text");
            // SYNTAX output shows optimized SQL
            assert!(text.contains("SELECT"), "EXPLAIN SYNTAX should contain SELECT");
        }
        ExplainResult::Arrow(batch) => {
            debug!("EXPLAIN SYNTAX returned Arrow batch with {} rows", batch.num_rows());
            assert!(batch.num_rows() > 0, "EXPLAIN SYNTAX should have rows");
        }
        #[cfg(feature = "serde")]
        ExplainResult::Json(_) => {
            panic!("EXPLAIN SYNTAX should not return JSON format");
        }
    }

    client.shutdown().await.unwrap();
}

/// Test EXPLAIN PLAN for query execution plan.
pub async fn test_explain_plan(ch: Arc<ClickHouseContainer>) {
    let (client, options) = bootstrap(ch.as_ref(), Some(CompressionMethod::None)).await;

    // Create a simple table for testing
    let schema = simple_schema();
    let batch = simple_batch();
    let (db, table) =
        create_schema(&client, schema, &options).await.expect("Schema creation failed");

    drop(
        client
            .insert(&format!("INSERT INTO {db}.{table} FORMAT Native"), batch, None)
            .await
            .expect("Insert failed")
            .collect::<Vec<_>>()
            .await,
    );

    let qid = Qid::new();
    header(qid, "Testing EXPLAIN PLAN");

    // Run query with EXPLAIN PLAN
    let query = format!("SELECT id, name FROM {db}.{table} WHERE id > 0");
    let explain_opts = ExplainOptions::new()
        .with_operation(ExplainOperation::Plan)
        .with_format(ExplainFormat::Text)
        .with_mode(ExplainMode::ExplainOnly);
    let query_opts = QueryOptions::new().with_explain(explain_opts).with_qid(qid);

    let mut response = client.query_with_options(&query, query_opts).await.expect("Query failed");

    // Get the explain result
    let explain_result =
        response.explain().await.expect("Should have explain").expect("Explain should succeed");
    match explain_result {
        ExplainResult::Text(text) => {
            debug!("EXPLAIN PLAN result:\n{text}");
            assert!(!text.is_empty(), "EXPLAIN PLAN should return non-empty text");
            // PLAN output typically contains ReadFromMergeTree or Expression
            assert!(
                text.contains("Read") || text.contains("Expression") || text.contains("Filter"),
                "EXPLAIN PLAN should contain plan nodes"
            );
        }
        ExplainResult::Arrow(batch) => {
            debug!("EXPLAIN PLAN returned Arrow batch with {} rows", batch.num_rows());
            assert!(batch.num_rows() > 0, "EXPLAIN PLAN should have rows");
        }
        #[cfg(feature = "serde")]
        ExplainResult::Json(_) => {
            panic!("Should not return JSON when Text format requested");
        }
    }

    // Consume the stream
    while response.next().await.is_some() {}

    drop_schema(&db, &table, &client).await.expect("Drop table");
    client.shutdown().await.unwrap();
}

/// Test EXPLAIN PIPELINE for query execution pipeline.
pub async fn test_explain_pipeline(ch: Arc<ClickHouseContainer>) {
    let (client, options) = bootstrap(ch.as_ref(), Some(CompressionMethod::None)).await;

    // Create a simple table for testing
    let schema = simple_schema();
    let batch = simple_batch();
    let (db, table) =
        create_schema(&client, schema, &options).await.expect("Schema creation failed");

    drop(
        client
            .insert(&format!("INSERT INTO {db}.{table} FORMAT Native"), batch, None)
            .await
            .expect("Insert failed")
            .collect::<Vec<_>>()
            .await,
    );

    let qid = Qid::new();
    header(qid, "Testing EXPLAIN PIPELINE");

    // Run query with EXPLAIN PIPELINE
    let query = format!("SELECT * FROM {db}.{table}");
    let explain_opts = ExplainOptions::new()
        .with_operation(ExplainOperation::Pipeline)
        .with_mode(ExplainMode::ExplainOnly);
    let query_opts = QueryOptions::new().with_explain(explain_opts).with_qid(qid);

    let mut response = client.query_with_options(&query, query_opts).await.expect("Query failed");

    let explain_result =
        response.explain().await.expect("Should have explain").expect("Explain should succeed");
    match explain_result {
        ExplainResult::Text(text) => {
            debug!("EXPLAIN PIPELINE result:\n{text}");
            assert!(!text.is_empty(), "EXPLAIN PIPELINE should return non-empty text");
        }
        ExplainResult::Arrow(batch) => {
            debug!("EXPLAIN PIPELINE returned Arrow batch with {} rows", batch.num_rows());
            assert!(batch.num_rows() > 0, "EXPLAIN PIPELINE should have rows");
        }
        #[cfg(feature = "serde")]
        ExplainResult::Json(_) => {
            panic!("EXPLAIN PIPELINE should not return JSON format");
        }
    }

    // Consume the stream
    while response.next().await.is_some() {}

    drop_schema(&db, &table, &client).await.expect("Drop table");
    client.shutdown().await.unwrap();
}

/// Test EXPLAIN ESTIMATE for query cost estimation.
pub async fn test_explain_estimate(ch: Arc<ClickHouseContainer>) {
    let (client, options) = bootstrap(ch.as_ref(), Some(CompressionMethod::None)).await;

    // Create a simple table for testing
    let schema = simple_schema();
    let batch = simple_batch();
    let (db, table) =
        create_schema(&client, schema, &options).await.expect("Schema creation failed");

    drop(
        client
            .insert(&format!("INSERT INTO {db}.{table} FORMAT Native"), batch, None)
            .await
            .expect("Insert failed")
            .collect::<Vec<_>>()
            .await,
    );

    let qid = Qid::new();
    header(qid, "Testing EXPLAIN ESTIMATE");

    // Run query with EXPLAIN ESTIMATE - returns structured data
    let query = format!("SELECT * FROM {db}.{table}");
    let explain_opts = ExplainOptions::new()
        .with_operation(ExplainOperation::Estimate)
        .with_format(ExplainFormat::Arrow) // Arrow format for structured output
        .with_mode(ExplainMode::ExplainOnly);
    let query_opts = QueryOptions::new().with_explain(explain_opts).with_qid(qid);

    let mut response = client.query_with_options(&query, query_opts).await.expect("Query failed");

    let explain_result =
        response.explain().await.expect("Should have explain").expect("Explain should succeed");
    match explain_result {
        ExplainResult::Arrow(batch) => {
            debug!("EXPLAIN ESTIMATE returned Arrow batch with {} rows", batch.num_rows());
            // ESTIMATE returns structured data with database, table, parts, rows, marks columns
            assert!(batch.num_columns() >= 1, "ESTIMATE should have columns");
            debug!("EXPLAIN ESTIMATE schema: {:?}", batch.schema());
        }
        ExplainResult::Text(text) => {
            debug!("EXPLAIN ESTIMATE result (text):\n{text}");
            // Some formats might return text
        }
        #[cfg(feature = "serde")]
        ExplainResult::Json(json) => {
            debug!("EXPLAIN ESTIMATE result (json): {json:?}");
        }
    }

    // Consume the stream
    while response.next().await.is_some() {}

    drop_schema(&db, &table, &client).await.expect("Drop table");
    client.shutdown().await.unwrap();
}

/// Test `query_with_options` without explain (just params and qid).
pub async fn test_query_options_no_explain(ch: Arc<ClickHouseContainer>) {
    let (client, options) = bootstrap(ch.as_ref(), Some(CompressionMethod::None)).await;

    // Create a simple table for testing
    let schema = simple_schema();
    let batch = simple_batch();
    let (db, table) =
        create_schema(&client, schema, &options).await.expect("Schema creation failed");

    drop(
        client
            .insert(&format!("INSERT INTO {db}.{table} FORMAT Native"), batch, None)
            .await
            .expect("Insert failed")
            .collect::<Vec<_>>()
            .await,
    );

    let qid = Qid::new();
    header(qid, "Testing query_with_options without explain");

    // Run query with just qid, no explain
    let query = format!("SELECT * FROM {db}.{table}");
    let query_opts = QueryOptions::new().with_qid(qid);

    let mut response = client.query_with_options(&query, query_opts).await.expect("Query failed");

    // Should not have explain
    assert!(!response.has_explain(), "Response should not have explain when not requested");

    // Should have data
    let mut row_count = 0;
    while let Some(batch_result) = response.next().await {
        let batch = batch_result.expect("Batch should be valid");
        row_count += batch.num_rows();
    }
    assert!(row_count > 0, "Should have received rows");

    // explain() should return None
    let explain = response.explain().await;
    assert!(explain.is_none(), "explain() should return None when not requested");

    drop_schema(&db, &table, &client).await.expect("Drop table");
    client.shutdown().await.unwrap();
}

// Helper function to create a simple schema for testing
fn simple_schema() -> Arc<arrow::datatypes::Schema> {
    use arrow::datatypes::{DataType, Field, Schema};
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("name", DataType::Utf8, true),
    ]))
}

// Helper function to create a simple test batch
fn simple_batch() -> arrow::record_batch::RecordBatch {
    use arrow::array::{Int32Array, StringArray};
    use arrow::record_batch::RecordBatch;

    let schema = simple_schema();
    RecordBatch::try_new(schema, vec![
        Arc::new(Int32Array::from(vec![1, 2, 3, 4, 5])),
        Arc::new(StringArray::from(vec![
            Some("alice"),
            Some("bob"),
            Some("charlie"),
            None,
            Some("eve"),
        ])),
    ])
    .unwrap()
}
