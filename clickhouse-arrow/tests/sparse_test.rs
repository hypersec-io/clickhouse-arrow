//! Sparse serialisation tests (fixes #96).
//!
//! Sparse is a ClickHouse optimisation for columns w/ many defaults – stores only
//! non-default values + their positions. MergeTree tables primarily.

use arrow::array::{Array, AsArray};
use clickhouse_arrow::prelude::*;
use clickhouse_arrow::test_utils::{get_or_create_container, init_tracing};
use futures_util::StreamExt;

#[tokio::test]
async fn test_sparse_column_mergetree() {
    init_tracing(None);

    let ch = get_or_create_container(None).await;
    let client = Client::<ArrowFormat>::builder()
        .with_endpoint(ch.get_native_url())
        .with_username(&ch.user)
        .with_password(&ch.password)
        .build()
        .await
        .expect("Failed to create client");

    // Create a MergeTree table with sparse-friendly columns
    // MergeTree is where sparse serialization actually matters in production
    client
        .execute("DROP TABLE IF EXISTS sparse_mergetree_test", None)
        .await
        .expect("Failed to drop table");

    client
        .execute(
            r#"
            CREATE TABLE sparse_mergetree_test (
                id UInt64,
                event_time DateTime DEFAULT now(),
                value Int64,
                sparse_int Int64 DEFAULT 0,
                sparse_string String DEFAULT ''
            ) ENGINE = MergeTree()
            ORDER BY (id, event_time)
            "#,
            None,
        )
        .await
        .expect("Failed to create table");

    // Insert data with many default values (90%+ zeros/empty strings)
    // This ratio should trigger sparse serialization when enabled
    let mut insert_sql = String::from(
        "INSERT INTO sparse_mergetree_test (id, value, sparse_int, sparse_string) VALUES ",
    );
    let mut values = Vec::new();
    for i in 0..1000 {
        // Only 5% of rows have non-default sparse values
        // Use (i + 1) * 100 so that i=0 produces 100 (non-zero)
        let sparse_int = if i % 20 == 0 { (i + 1) * 100 } else { 0 };
        let sparse_string = if i % 25 == 0 { format!("'value_{i}'") } else { "''".to_string() };
        values.push(format!("({i}, {}, {sparse_int}, {sparse_string})", i * 10));
    }
    insert_sql.push_str(&values.join(", "));

    client.execute(&insert_sql, None).await.expect("Failed to insert data");

    // Force a merge to ensure data is written to parts (sparse is determined per-part)
    client
        .execute("OPTIMIZE TABLE sparse_mergetree_test FINAL", None)
        .await
        .expect("Failed to optimize table");

    // Query the data back
    let mut stream = Client::<ArrowFormat>::query(
        &client,
        "SELECT id, value, sparse_int, sparse_string FROM sparse_mergetree_test ORDER BY id",
        None,
    )
    .await
    .expect("Failed to query");

    let mut total_rows = 0;
    let mut non_zero_sparse_int = 0;
    let mut non_empty_sparse_string = 0;

    while let Some(result) = stream.next().await {
        let batch = result.expect("Failed to get batch");
        total_rows += batch.num_rows();

        // Count non-default values to verify data integrity
        let sparse_int_col =
            batch.column_by_name("sparse_int").expect("sparse_int column not found");
        let sparse_int_array = sparse_int_col.as_primitive::<arrow::datatypes::Int64Type>();
        for i in 0..sparse_int_array.len() {
            if sparse_int_array.value(i) != 0 {
                non_zero_sparse_int += 1;
            }
        }

        let sparse_string_col =
            batch.column_by_name("sparse_string").expect("sparse_string column not found");
        let sparse_string_array = sparse_string_col
            .as_any()
            .downcast_ref::<arrow::array::StringArray>()
            .or_else(|| {
                sparse_string_col
                    .as_any()
                    .downcast_ref::<arrow::array::LargeStringArray>()
                    .map(|_| panic!("LargeStringArray not expected"))
            })
            .or_else(|| {
                // Handle Binary type (ClickHouse String -> Arrow Binary by default)
                None
            });

        if let Some(arr) = sparse_string_array {
            for i in 0..arr.len() {
                if !arr.value(i).is_empty() {
                    non_empty_sparse_string += 1;
                }
            }
        } else {
            // If it's binary, handle that
            if let Some(arr) =
                sparse_string_col.as_any().downcast_ref::<arrow::array::BinaryArray>()
            {
                for i in 0..arr.len() {
                    if !arr.value(i).is_empty() {
                        non_empty_sparse_string += 1;
                    }
                }
            }
        }
    }

    assert_eq!(total_rows, 1000, "Expected 1000 rows");
    assert_eq!(non_zero_sparse_int, 50, "Expected 50 non-zero sparse_int values (every 20th row)");
    assert_eq!(
        non_empty_sparse_string, 40,
        "Expected 40 non-empty sparse_string values (every 25th row)"
    );

    // Cleanup
    client.execute("DROP TABLE IF EXISTS sparse_mergetree_test", None).await.unwrap();

    println!("MergeTree sparse column test passed!");
    println!("  Total rows: {total_rows}");
    println!("  Non-zero sparse_int: {non_zero_sparse_int}");
    println!("  Non-empty sparse_string: {non_empty_sparse_string}");
}

#[tokio::test]
async fn test_query_system_numbers_sparse() {
    init_tracing(None);

    let ch = get_or_create_container(None).await;
    let client = Client::<ArrowFormat>::builder()
        .with_endpoint(ch.get_native_url())
        .with_username(&ch.user)
        .with_password(&ch.password)
        .build()
        .await
        .expect("Failed to create client");

    // Query system.numbers - this tests basic functionality
    // The result column may or may not be sparse depending on the server version and settings
    let mut stream =
        Client::<ArrowFormat>::query(&client, "SELECT number FROM system.numbers LIMIT 1000", None)
            .await
            .expect("Failed to query");

    let mut total_rows = 0;
    while let Some(result) = stream.next().await {
        let batch = result.expect("Failed to get batch");
        total_rows += batch.num_rows();
    }

    assert_eq!(total_rows, 1000);
    println!("System numbers query test passed with {} rows", total_rows);
}

/// Wide variety of data types – integers, floats, strings, dates.
#[tokio::test]
async fn test_sparse_wide_variety_types() {
    init_tracing(None);

    let ch = get_or_create_container(None).await;
    let client = Client::<ArrowFormat>::builder()
        .with_endpoint(ch.get_native_url())
        .with_username(&ch.user)
        .with_password(&ch.password)
        .build()
        .await
        .expect("Failed to create client");

    // Create table with many different column types that can be sparse
    client
        .execute("DROP TABLE IF EXISTS sparse_variety_test", None)
        .await
        .expect("Failed to drop table");

    client
        .execute(
            r#"
            CREATE TABLE sparse_variety_test (
                id UInt64,
                -- Integer types
                sparse_int8 Int8 DEFAULT 0,
                sparse_int16 Int16 DEFAULT 0,
                sparse_int32 Int32 DEFAULT 0,
                sparse_int64 Int64 DEFAULT 0,
                sparse_uint8 UInt8 DEFAULT 0,
                sparse_uint16 UInt16 DEFAULT 0,
                sparse_uint32 UInt32 DEFAULT 0,
                sparse_uint64 UInt64 DEFAULT 0,
                -- Float types
                sparse_float32 Float32 DEFAULT 0,
                sparse_float64 Float64 DEFAULT 0,
                -- String types
                sparse_string String DEFAULT '',
                sparse_fixed FixedString(8) DEFAULT '',
                -- Date types
                sparse_date Date DEFAULT '1970-01-01',
                sparse_datetime DateTime DEFAULT '1970-01-01 00:00:00'
            ) ENGINE = MergeTree()
            ORDER BY id
            "#,
            None,
        )
        .await
        .expect("Failed to create table");

    // Insert 2000 rows with 95% default values (varying sparsity per column)
    let mut insert_sql = String::from(
        "INSERT INTO sparse_variety_test (id, sparse_int8, sparse_int16, sparse_int32, \
         sparse_int64, sparse_uint8, sparse_uint16, sparse_uint32, sparse_uint64, sparse_float32, \
         sparse_float64, sparse_string, sparse_fixed, sparse_date, sparse_datetime) VALUES ",
    );

    let mut values = Vec::new();
    for i in 0..2000 {
        // Different sparsity patterns for different columns
        let int8 = if i % 10 == 0 { ((i % 127) + 1) as i8 } else { 0 };
        let int16 = if i % 15 == 0 { (i + 1) as i16 } else { 0 };
        let int32 = if i % 20 == 0 { (i + 1) as i32 } else { 0 };
        let int64 = if i % 25 == 0 { (i + 1) as i64 } else { 0 };
        let uint8 = if i % 30 == 0 { ((i % 255) + 1) as u8 } else { 0 };
        let uint16 = if i % 35 == 0 { (i + 1) as u16 } else { 0 };
        let uint32 = if i % 40 == 0 { (i + 1) as u32 } else { 0 };
        let uint64 = if i % 45 == 0 { (i + 1) as u64 } else { 0 };
        let float32 = if i % 50 == 0 { (i as f32) + 0.5 } else { 0.0 };
        let float64 = if i % 55 == 0 { (i as f64) + 0.5 } else { 0.0 };
        let string = if i % 60 == 0 { format!("'str_{i}'") } else { "''".to_string() };
        let fixed = if i % 65 == 0 { format!("'fix{:04}'", i % 10000) } else { "''".to_string() };
        let date = if i % 70 == 0 {
            format!("'2024-01-{:02}'", (i % 28) + 1)
        } else {
            "'1970-01-01'".to_string()
        };
        let datetime = if i % 75 == 0 {
            format!("'2024-01-01 {:02}:00:00'", i % 24)
        } else {
            "'1970-01-01 00:00:00'".to_string()
        };

        values.push(format!(
            "({i}, {int8}, {int16}, {int32}, {int64}, {uint8}, {uint16}, {uint32}, {uint64}, \
             {float32}, {float64}, {string}, {fixed}, {date}, {datetime})"
        ));
    }
    insert_sql.push_str(&values.join(", "));

    client.execute(&insert_sql, None).await.expect("Failed to insert data");

    // Force merge to trigger sparse serialization
    client
        .execute("OPTIMIZE TABLE sparse_variety_test FINAL", None)
        .await
        .expect("Failed to optimize table");

    // Query all columns back
    let mut stream = Client::<ArrowFormat>::query(
        &client,
        "SELECT * FROM sparse_variety_test ORDER BY id",
        None,
    )
    .await
    .expect("Failed to query");

    let mut total_rows = 0;
    let mut counts = std::collections::HashMap::new();

    while let Some(result) = stream.next().await {
        let batch = result.expect("Failed to get batch");
        total_rows += batch.num_rows();

        // Count non-default values for each column
        for col_idx in 1..batch.num_columns() {
            // skip id column
            let col = batch.column(col_idx);
            let col_name = batch.schema().field(col_idx).name().clone();
            let count = counts.entry(col_name.clone()).or_insert(0usize);

            // Check for non-default values based on type
            match col.data_type() {
                arrow::datatypes::DataType::Int8 => {
                    let arr = col.as_primitive::<arrow::datatypes::Int8Type>();
                    for i in 0..arr.len() {
                        if arr.value(i) != 0 {
                            *count += 1;
                        }
                    }
                }
                arrow::datatypes::DataType::Int16 => {
                    let arr = col.as_primitive::<arrow::datatypes::Int16Type>();
                    for i in 0..arr.len() {
                        if arr.value(i) != 0 {
                            *count += 1;
                        }
                    }
                }
                arrow::datatypes::DataType::Int32 => {
                    let arr = col.as_primitive::<arrow::datatypes::Int32Type>();
                    for i in 0..arr.len() {
                        if arr.value(i) != 0 {
                            *count += 1;
                        }
                    }
                }
                arrow::datatypes::DataType::Int64 => {
                    let arr = col.as_primitive::<arrow::datatypes::Int64Type>();
                    for i in 0..arr.len() {
                        if arr.value(i) != 0 {
                            *count += 1;
                        }
                    }
                }
                arrow::datatypes::DataType::UInt8 => {
                    let arr = col.as_primitive::<arrow::datatypes::UInt8Type>();
                    for i in 0..arr.len() {
                        if arr.value(i) != 0 {
                            *count += 1;
                        }
                    }
                }
                arrow::datatypes::DataType::UInt16 => {
                    let arr = col.as_primitive::<arrow::datatypes::UInt16Type>();
                    for i in 0..arr.len() {
                        if arr.value(i) != 0 {
                            *count += 1;
                        }
                    }
                }
                arrow::datatypes::DataType::UInt32 => {
                    let arr = col.as_primitive::<arrow::datatypes::UInt32Type>();
                    for i in 0..arr.len() {
                        if arr.value(i) != 0 {
                            *count += 1;
                        }
                    }
                }
                arrow::datatypes::DataType::UInt64 => {
                    let arr = col.as_primitive::<arrow::datatypes::UInt64Type>();
                    for i in 0..arr.len() {
                        if arr.value(i) != 0 {
                            *count += 1;
                        }
                    }
                }
                arrow::datatypes::DataType::Date32 => {
                    // Date32 stores days since epoch as i32
                    let arr = col.as_primitive::<arrow::datatypes::Date32Type>();
                    for i in 0..arr.len() {
                        if arr.value(i) != 0 {
                            *count += 1;
                        }
                    }
                }
                arrow::datatypes::DataType::Timestamp(_, _) => {
                    // DateTime is stored as UInt32 seconds since epoch
                    let arr = col.as_primitive::<arrow::datatypes::TimestampSecondType>();
                    for i in 0..arr.len() {
                        if arr.value(i) != 0 {
                            *count += 1;
                        }
                    }
                }
                arrow::datatypes::DataType::Float32 => {
                    let arr = col.as_primitive::<arrow::datatypes::Float32Type>();
                    for i in 0..arr.len() {
                        if arr.value(i) != 0.0 {
                            *count += 1;
                        }
                    }
                }
                arrow::datatypes::DataType::Float64 => {
                    let arr = col.as_primitive::<arrow::datatypes::Float64Type>();
                    for i in 0..arr.len() {
                        if arr.value(i) != 0.0 {
                            *count += 1;
                        }
                    }
                }
                arrow::datatypes::DataType::Binary
                | arrow::datatypes::DataType::FixedSizeBinary(_) => {
                    if let Some(arr) = col.as_any().downcast_ref::<arrow::array::BinaryArray>() {
                        for i in 0..arr.len() {
                            if !arr.value(i).is_empty() {
                                *count += 1;
                            }
                        }
                    } else if let Some(arr) =
                        col.as_any().downcast_ref::<arrow::array::FixedSizeBinaryArray>()
                    {
                        for i in 0..arr.len() {
                            // Check if not all zeros
                            if arr.value(i).iter().any(|&b| b != 0) {
                                *count += 1;
                            }
                        }
                    }
                }
                arrow::datatypes::DataType::Utf8 => {
                    if let Some(arr) = col.as_any().downcast_ref::<arrow::array::StringArray>() {
                        for i in 0..arr.len() {
                            if !arr.value(i).is_empty() {
                                *count += 1;
                            }
                        }
                    }
                }
                _ => {}
            }
        }
    }

    // Verify counts
    assert_eq!(total_rows, 2000, "Expected 2000 rows");

    // Expected counts based on sparsity patterns
    // For n rows, count of non-defaults = ceil(n / interval)
    // But since we use i % interval == 0 starting from 0, it's (n-1)/interval + 1
    let expected = [
        ("sparse_int8", 200),    // every 10th: 0, 10, 20... 1990 = 200
        ("sparse_int16", 134),   // every 15th: 0, 15, 30... 1995 = 134
        ("sparse_int32", 100),   // every 20th: 0, 20, 40... 1980 = 100
        ("sparse_int64", 80),    // every 25th: 0, 25, 50... 1975 = 80
        ("sparse_uint8", 67),    // every 30th
        ("sparse_uint16", 58),   // every 35th
        ("sparse_uint32", 50),   // every 40th
        ("sparse_uint64", 45),   // every 45th
        ("sparse_float32", 40),  // every 50th
        ("sparse_float64", 37),  // every 55th
        ("sparse_string", 34),   // every 60th
        ("sparse_fixed", 31),    // every 65th
        ("sparse_date", 29),     // every 70th
        ("sparse_datetime", 27), // every 75th
    ];

    println!("\nSparse variety test results:");
    println!("  Total rows: {total_rows}");
    for (col_name, expected_count) in expected {
        let actual = counts.get(col_name).copied().unwrap_or(0);
        println!("  {col_name}: {actual} non-default (expected ~{expected_count})");
        // Allow some tolerance for edge cases
        assert!(
            actual >= expected_count - 2 && actual <= expected_count + 2,
            "{col_name}: expected ~{expected_count} but got {actual}"
        );
    }

    // Cleanup
    client.execute("DROP TABLE IF EXISTS sparse_variety_test", None).await.unwrap();

    println!("Wide variety sparse test passed!");
}

/// Edge cases: first/last row non-default, consecutive, mixed dense/sparse.
#[tokio::test]
async fn test_sparse_edge_cases() {
    init_tracing(None);

    let ch = get_or_create_container(None).await;
    let client = Client::<ArrowFormat>::builder()
        .with_endpoint(ch.get_native_url())
        .with_username(&ch.user)
        .with_password(&ch.password)
        .build()
        .await
        .expect("Failed to create client");

    client
        .execute("DROP TABLE IF EXISTS sparse_edge_test", None)
        .await
        .expect("Failed to drop table");

    // Simplified test with fewer columns to debug sparse handling
    client
        .execute(
            r#"
            CREATE TABLE sparse_edge_test (
                id UInt64,
                -- Consecutive non-defaults at start (rows 0-9)
                consecutive_start Int64 DEFAULT 0,
                -- Consecutive non-defaults at end (last 10 rows)
                consecutive_end Int64 DEFAULT 0,
                -- Dense column (many non-defaults - won't be sparse)
                dense_col Int64 DEFAULT 0,
                -- Pattern: every 100th row (5% non-default)
                sparse_5pct Int64 DEFAULT 0
            ) ENGINE = MergeTree()
            ORDER BY id
            "#,
            None,
        )
        .await
        .expect("Failed to create table");

    // Insert 10000 rows with specific patterns
    let mut values = Vec::new();
    let total_rows = 10000usize;

    for i in 0..total_rows {
        // Consecutive non-defaults at start (rows 0-9)
        let consecutive_start = if i < 10 { (i + 1) as i64 } else { 0 };

        // Consecutive non-defaults at end (last 10 rows)
        let consecutive_end = if i >= total_rows - 10 { (i + 1) as i64 } else { 0 };

        // Dense: 30% non-default values - likely won't be sparse
        let dense_col = if i % 3 == 0 { (i + 1) as i64 } else { 0 };

        // 5% non-default - every 20th row
        let sparse_5pct = if i % 20 == 0 { (i + 1) as i64 } else { 0 };

        values.push(format!(
            "({i}, {consecutive_start}, {consecutive_end}, {dense_col}, {sparse_5pct})"
        ));
    }

    // Insert in batches to avoid SQL size limits
    for chunk in values.chunks(1000) {
        let insert_sql = format!(
            "INSERT INTO sparse_edge_test (id, consecutive_start, consecutive_end, dense_col, \
             sparse_5pct) VALUES {}",
            chunk.join(", ")
        );
        client.execute(&insert_sql, None).await.expect("Failed to insert batch");
    }

    // Force merge
    client
        .execute("OPTIMIZE TABLE sparse_edge_test FINAL", None)
        .await
        .expect("Failed to optimize table");

    // Query and verify
    let mut stream =
        Client::<ArrowFormat>::query(&client, "SELECT * FROM sparse_edge_test ORDER BY id", None)
            .await
            .expect("Failed to query");

    let mut total_read = 0;
    let mut counts = std::collections::HashMap::new();

    while let Some(result) = stream.next().await {
        let batch = result.expect("Failed to get batch");
        total_read += batch.num_rows();

        for col_idx in 1..batch.num_columns() {
            let col = batch.column(col_idx);
            let col_name = batch.schema().field(col_idx).name().clone();
            let count = counts.entry(col_name.clone()).or_insert(0usize);

            let arr = col.as_primitive::<arrow::datatypes::Int64Type>();
            for i in 0..arr.len() {
                let val = arr.value(i);
                if val != 0 {
                    *count += 1;
                }
            }
        }
    }

    assert_eq!(total_read, total_rows, "Expected {total_rows} rows");

    println!("\nSparse edge cases test results:");
    println!("  Total rows: {total_read}");
    println!(
        "  consecutive_start: {} (expected 10)",
        counts.get("consecutive_start").unwrap_or(&0)
    );
    println!("  consecutive_end: {} (expected 10)", counts.get("consecutive_end").unwrap_or(&0));
    println!("  dense_col: {} (expected ~3334)", counts.get("dense_col").unwrap_or(&0));
    println!("  sparse_5pct: {} (expected 500)", counts.get("sparse_5pct").unwrap_or(&0));

    // Verify counts
    assert_eq!(*counts.get("consecutive_start").unwrap_or(&0), 10);
    assert_eq!(*counts.get("consecutive_end").unwrap_or(&0), 10);

    // Dense column: ceil(10000/3) = 3334
    let dense_count = *counts.get("dense_col").unwrap_or(&0);
    assert!(dense_count >= 3333 && dense_count <= 3335, "Dense count: {dense_count}");

    // 5% sparse: every 20th row = 500
    let sparse_count = *counts.get("sparse_5pct").unwrap_or(&0);
    assert_eq!(sparse_count, 500, "sparse_5pct count: {sparse_count}");

    // Cleanup
    client.execute("DROP TABLE IF EXISTS sparse_edge_test", None).await.unwrap();

    println!("Edge cases sparse test passed!");
}

/// Nullable columns w/ sparse – can have both NULL and default values.
#[tokio::test]
async fn test_sparse_nullable_columns() {
    init_tracing(None);

    let ch = get_or_create_container(None).await;
    let client = Client::<ArrowFormat>::builder()
        .with_endpoint(ch.get_native_url())
        .with_username(&ch.user)
        .with_password(&ch.password)
        .build()
        .await
        .expect("Failed to create client");

    client
        .execute("DROP TABLE IF EXISTS sparse_nullable_test", None)
        .await
        .expect("Failed to drop table");

    client
        .execute(
            r#"
            CREATE TABLE sparse_nullable_test (
                id UInt64,
                -- Nullable int with sparse values
                nullable_int Nullable(Int64),
                -- Nullable string
                nullable_str Nullable(String),
                -- Nullable float
                nullable_float Nullable(Float64)
            ) ENGINE = MergeTree()
            ORDER BY id
            "#,
            None,
        )
        .await
        .expect("Failed to create table");

    // Insert rows with mix of NULLs, defaults, and actual values
    let mut values = Vec::new();
    for i in 0..5000 {
        let int_val = match i % 100 {
            0 => "NULL".to_string(),              // 1% NULL
            1..=5 => format!("{}", (i + 1) * 10), // 5% non-null values
            _ => "NULL".to_string(),              // rest NULL (sparse storage for nullables)
        };

        let str_val = match i % 50 {
            0 => "NULL".to_string(),
            1 => format!("'str_{i}'"),
            _ => "NULL".to_string(),
        };

        let float_val = match i % 75 {
            0 => "NULL".to_string(),
            1 => format!("{}.5", i),
            _ => "NULL".to_string(),
        };

        values.push(format!("({i}, {int_val}, {str_val}, {float_val})"));
    }

    for chunk in values.chunks(1000) {
        let insert_sql = format!(
            "INSERT INTO sparse_nullable_test (id, nullable_int, nullable_str, nullable_float) \
             VALUES {}",
            chunk.join(", ")
        );
        client.execute(&insert_sql, None).await.expect("Failed to insert batch");
    }

    client
        .execute("OPTIMIZE TABLE sparse_nullable_test FINAL", None)
        .await
        .expect("Failed to optimize table");

    // Query and verify
    let mut stream = Client::<ArrowFormat>::query(
        &client,
        "SELECT * FROM sparse_nullable_test ORDER BY id",
        None,
    )
    .await
    .expect("Failed to query");

    let mut total_read = 0;
    let mut null_counts = std::collections::HashMap::new();
    let mut non_null_counts = std::collections::HashMap::new();

    while let Some(result) = stream.next().await {
        let batch = result.expect("Failed to get batch");
        total_read += batch.num_rows();

        for col_idx in 1..batch.num_columns() {
            let col = batch.column(col_idx);
            let col_name = batch.schema().field(col_idx).name().clone();
            let null_count = null_counts.entry(col_name.clone()).or_insert(0usize);
            let non_null_count = non_null_counts.entry(col_name.clone()).or_insert(0usize);

            for i in 0..col.len() {
                if col.is_null(i) {
                    *null_count += 1;
                } else {
                    *non_null_count += 1;
                }
            }
        }
    }

    assert_eq!(total_read, 5000, "Expected 5000 rows");

    println!("\nSparse nullable test results:");
    println!("  Total rows: {total_read}");

    for col in ["nullable_int", "nullable_str", "nullable_float"] {
        let nulls = null_counts.get(col).copied().unwrap_or(0);
        let non_nulls = non_null_counts.get(col).copied().unwrap_or(0);
        println!("  {col}: {nulls} NULLs, {non_nulls} non-null values");
    }

    // Verify we got data back correctly
    let int_non_nulls = non_null_counts.get("nullable_int").copied().unwrap_or(0);
    assert!(
        int_non_nulls >= 250 && int_non_nulls <= 350,
        "nullable_int non-null count: {int_non_nulls}"
    );

    // Cleanup
    client.execute("DROP TABLE IF EXISTS sparse_nullable_test", None).await.unwrap();

    println!("Nullable sparse test passed!");
}

/// Large-scale (100k rows) – verifies memory efficiency at scale.
#[tokio::test]
async fn test_sparse_large_scale() {
    init_tracing(None);

    let ch = get_or_create_container(None).await;
    let client = Client::<ArrowFormat>::builder()
        .with_endpoint(ch.get_native_url())
        .with_username(&ch.user)
        .with_password(&ch.password)
        .build()
        .await
        .expect("Failed to create client");

    client
        .execute("DROP TABLE IF EXISTS sparse_large_test", None)
        .await
        .expect("Failed to drop table");

    client
        .execute(
            r#"
            CREATE TABLE sparse_large_test (
                id UInt64,
                -- 99.9% sparse (only 100 non-defaults in 100k rows)
                ultra_sparse Int64 DEFAULT 0,
                -- Multiple sparse columns
                sparse_a Int64 DEFAULT 0,
                sparse_b Int64 DEFAULT 0,
                sparse_c String DEFAULT ''
            ) ENGINE = MergeTree()
            ORDER BY id
            "#,
            None,
        )
        .await
        .expect("Failed to create table");

    // Generate 100k rows
    let total_rows = 100_000usize;
    let mut values = Vec::with_capacity(total_rows);

    for i in 0..total_rows {
        // Ultra sparse: every 1000th row
        let ultra = if i % 1000 == 0 { (i + 1) as i64 } else { 0 };

        // Sparse A: every 500th row
        let a = if i % 500 == 0 { (i + 1) as i64 } else { 0 };

        // Sparse B: every 250th row
        let b = if i % 250 == 0 { (i + 1) as i64 } else { 0 };

        // Sparse C: every 200th row
        let c = if i % 200 == 0 { format!("'val_{i}'") } else { "''".to_string() };

        values.push(format!("({i}, {ultra}, {a}, {b}, {c})"));
    }

    // Insert in larger batches for efficiency
    for chunk in values.chunks(5000) {
        let insert_sql = format!(
            "INSERT INTO sparse_large_test (id, ultra_sparse, sparse_a, sparse_b, sparse_c) \
             VALUES {}",
            chunk.join(", ")
        );
        client.execute(&insert_sql, None).await.expect("Failed to insert batch");
    }

    client
        .execute("OPTIMIZE TABLE sparse_large_test FINAL", None)
        .await
        .expect("Failed to optimize table");

    // Query and verify
    let mut stream =
        Client::<ArrowFormat>::query(&client, "SELECT * FROM sparse_large_test ORDER BY id", None)
            .await
            .expect("Failed to query");

    let mut total_read = 0;
    let mut counts = std::collections::HashMap::new();

    while let Some(result) = stream.next().await {
        let batch = result.expect("Failed to get batch");
        total_read += batch.num_rows();

        // Count non-defaults
        for col_idx in 1..batch.num_columns() {
            let col = batch.column(col_idx);
            let col_name = batch.schema().field(col_idx).name().clone();
            let count = counts.entry(col_name.clone()).or_insert(0usize);

            if col_name == "sparse_c" {
                // String/Binary column
                if let Some(arr) = col.as_any().downcast_ref::<arrow::array::BinaryArray>() {
                    for i in 0..arr.len() {
                        if !arr.value(i).is_empty() {
                            *count += 1;
                        }
                    }
                } else if let Some(arr) = col.as_any().downcast_ref::<arrow::array::StringArray>() {
                    for i in 0..arr.len() {
                        if !arr.value(i).is_empty() {
                            *count += 1;
                        }
                    }
                }
            } else {
                // Int64 columns
                let arr = col.as_primitive::<arrow::datatypes::Int64Type>();
                for i in 0..arr.len() {
                    if arr.value(i) != 0 {
                        *count += 1;
                    }
                }
            }
        }
    }

    assert_eq!(total_read, total_rows, "Expected {total_rows} rows");

    println!("\nLarge-scale sparse test results:");
    println!("  Total rows: {total_read}");
    println!("  ultra_sparse: {} (expected 100)", counts.get("ultra_sparse").unwrap_or(&0));
    println!("  sparse_a: {} (expected 200)", counts.get("sparse_a").unwrap_or(&0));
    println!("  sparse_b: {} (expected 400)", counts.get("sparse_b").unwrap_or(&0));
    println!("  sparse_c: {} (expected 500)", counts.get("sparse_c").unwrap_or(&0));

    // Verify counts
    assert_eq!(*counts.get("ultra_sparse").unwrap_or(&0), 100);
    assert_eq!(*counts.get("sparse_a").unwrap_or(&0), 200);
    assert_eq!(*counts.get("sparse_b").unwrap_or(&0), 400);
    assert_eq!(*counts.get("sparse_c").unwrap_or(&0), 500);

    // Cleanup
    client.execute("DROP TABLE IF EXISTS sparse_large_test", None).await.unwrap();

    println!("Large-scale sparse test passed!");
}
