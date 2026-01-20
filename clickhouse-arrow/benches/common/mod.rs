use clickhouse_arrow::prelude::*;
use clickhouse_arrow::test_utils::{ClickHouseContainer, init_tracing};
use clickhouse_arrow::{CompressionMethod, Row, Uuid};
use serde::{Deserialize, Serialize};

#[allow(unused)]
pub(crate) const DISABLE_CLEANUP_ENV: &str = "DISABLE_CLEANUP";
#[allow(unused)]
pub(crate) const TEST_DB_NAME: &str = "benchmark_test";
#[allow(unused)]
pub(crate) const DEFAULT_INSERT_SAMPLE_SIZE: usize = 50;

#[derive(Row, Clone, Serialize, Deserialize)]
pub(crate) struct ClickHouseNativeRow {
    id:    String,
    name:  String,
    value: f64,
    ts:    DateTime64<3>,
}

pub(crate) fn init() {
    if let Ok(l) = std::env::var("RUST_LOG")
        && !l.is_empty()
    {
        // Add directives here
        init_tracing(Some(&[/*("tokio", "error")*/]));
    }
}

pub(crate) fn print_msg(msg: impl std::fmt::Display) {
    eprintln!("\n--------\n{msg}\n--------\n\n");
}

#[allow(unused)]
pub(crate) async fn setup_clickhouse_native(
    ch: &'static ClickHouseContainer,
) -> Result<NativeClient> {
    Client::<NativeFormat>::builder()
        .with_endpoint(ch.get_native_url())
        .with_username(&ch.user)
        .with_password(&ch.password)
        .with_compression(CompressionMethod::None)
        .build()
        .await
}

// Helper function to create test rows for native format
#[allow(unused)]
#[expect(clippy::cast_precision_loss)]
#[expect(clippy::cast_possible_wrap)]
pub(crate) fn create_test_native_rows(rows: usize) -> Vec<ClickHouseNativeRow> {
    (0..rows)
        .map(|i| ClickHouseNativeRow {
            id:    Uuid::new_v4().to_string(),
            name:  format!("name{i}"),
            value: i as f64,
            ts:    DateTime64::<3>::try_from(
                chrono::DateTime::<chrono::Utc>::from_timestamp(i as i64 * 1000, 0).unwrap(),
            )
            .unwrap(),
        })
        .collect()
}
