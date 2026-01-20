//! Query result limits and truncation handling.
//!
//! This module provides mechanisms to limit query results by:
//! - Maximum memory size (Arrow array memory footprint)
//! - Maximum row count
//! - Maximum number of batches
//!
//! When limits are exceeded, results are truncated and a status indicator
//! is provided to inform the caller that the results were cropped.

use std::pin::Pin;
use std::task::{Context, Poll};

use arrow::record_batch::RecordBatch;
use futures_util::Stream;
use pin_project::pin_project;

use crate::Result;

/// Reason why query results were truncated.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TruncationReason {
    /// Results exceeded the maximum memory limit.
    MemoryLimit,
    /// Results exceeded the maximum row count.
    RowLimit,
    /// Results exceeded the maximum batch count.
    BatchLimit,
}

impl std::fmt::Display for TruncationReason {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            TruncationReason::MemoryLimit => write!(f, "memory limit exceeded"),
            TruncationReason::RowLimit => write!(f, "row limit exceeded"),
            TruncationReason::BatchLimit => write!(f, "batch limit exceeded"),
        }
    }
}

/// Statistics about the limited query results.
#[derive(Debug, Clone, Copy, Default)]
pub struct QueryStats {
    /// Total rows returned (after truncation).
    pub rows_returned:     u64,
    /// Total batches returned (after truncation).
    pub batches_returned:  u64,
    /// Total memory consumed by returned batches (bytes).
    pub memory_bytes:      usize,
    /// Whether results were truncated.
    pub truncated:         bool,
    /// Reason for truncation, if any.
    pub truncation_reason: Option<TruncationReason>,
}

impl QueryStats {
    /// Returns true if the results were truncated for any reason.
    #[must_use]
    pub fn is_truncated(&self) -> bool { self.truncated }

    /// Returns a human-readable summary of the stats.
    #[must_use]
    pub fn summary(&self) -> String {
        let truncation = if self.truncated {
            format!(
                " (TRUNCATED: {})",
                self.truncation_reason.map_or("unknown".to_string(), |r| r.to_string())
            )
        } else {
            String::new()
        };
        format!(
            "{} rows, {} batches, {} bytes{}",
            self.rows_returned, self.batches_returned, self.memory_bytes, truncation
        )
    }
}

/// Configuration for query result limits.
///
/// All limits are optional. When a limit is reached, the stream stops
/// yielding results and marks the response as truncated.
#[derive(Debug, Clone, Copy, Default)]
pub struct QueryLimits {
    /// Maximum total memory (in bytes) for all returned batches.
    /// Uses `RecordBatch::get_array_memory_size()` for measurement.
    pub max_memory_bytes: Option<usize>,

    /// Maximum total number of rows across all batches.
    pub max_rows: Option<u64>,

    /// Maximum number of batches to return.
    pub max_batches: Option<u64>,
}

impl QueryLimits {
    /// Create a new `QueryLimits` with no limits set.
    #[must_use]
    pub fn none() -> Self { Self::default() }

    /// Set the maximum memory limit in bytes.
    #[must_use]
    pub fn with_max_memory(mut self, bytes: usize) -> Self {
        self.max_memory_bytes = Some(bytes);
        self
    }

    /// Set the maximum memory limit using human-readable units.
    #[must_use]
    pub fn with_max_memory_mb(self, mb: usize) -> Self { self.with_max_memory(mb * 1024 * 1024) }

    /// Set the maximum memory limit using human-readable units.
    #[must_use]
    pub fn with_max_memory_gb(self, gb: usize) -> Self {
        self.with_max_memory(gb * 1024 * 1024 * 1024)
    }

    /// Set the maximum row count.
    #[must_use]
    pub fn with_max_rows(mut self, rows: u64) -> Self {
        self.max_rows = Some(rows);
        self
    }

    /// Set the maximum batch count.
    #[must_use]
    pub fn with_max_batches(mut self, batches: u64) -> Self {
        self.max_batches = Some(batches);
        self
    }

    /// Returns true if any limits are configured.
    #[must_use]
    pub fn has_limits(&self) -> bool {
        self.max_memory_bytes.is_some() || self.max_rows.is_some() || self.max_batches.is_some()
    }
}

/// Internal state for tracking limits during streaming.
#[derive(Debug, Default)]
struct LimitState {
    total_rows:        u64,
    total_batches:     u64,
    total_memory:      usize,
    truncated:         bool,
    truncation_reason: Option<TruncationReason>,
}

impl LimitState {
    fn to_stats(&self) -> QueryStats {
        QueryStats {
            rows_returned:     self.total_rows,
            batches_returned:  self.total_batches,
            memory_bytes:      self.total_memory,
            truncated:         self.truncated,
            truncation_reason: self.truncation_reason,
        }
    }
}

/// A stream wrapper that enforces query limits on `RecordBatch` results.
///
/// When a limit is exceeded, the stream stops yielding items and the
/// `stats()` method will indicate truncation.
#[pin_project]
pub struct LimitedStream<S> {
    #[pin]
    inner:   S,
    limits:  QueryLimits,
    state:   LimitState,
    /// Whether we've already stopped due to limits
    stopped: bool,
}

impl<S> LimitedStream<S>
where
    S: Stream<Item = Result<RecordBatch>>,
{
    /// Create a new limited stream wrapping the inner stream.
    pub fn new(inner: S, limits: QueryLimits) -> Self {
        Self { inner, limits, state: LimitState::default(), stopped: false }
    }

    /// Get the current statistics (can be called during or after streaming).
    pub fn stats(&self) -> QueryStats { self.state.to_stats() }
}

impl<S> Stream for LimitedStream<S>
where
    S: Stream<Item = Result<RecordBatch>>,
{
    type Item = Result<RecordBatch>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.project();

        // If we've already stopped due to limits, return None
        if *this.stopped {
            return Poll::Ready(None);
        }

        match this.inner.poll_next(cx) {
            Poll::Ready(Some(Ok(batch))) => {
                // Check if this batch would exceed limits
                if let Some(reason) = this.state.truncation_reason.or_else(|| {
                    let batch_rows = batch.num_rows() as u64;
                    let batch_memory = batch.get_array_memory_size();

                    // Check row limit
                    if let Some(max_rows) = this.limits.max_rows
                        && this.state.total_rows + batch_rows > max_rows
                    {
                        return Some(TruncationReason::RowLimit);
                    }

                    // Check memory limit
                    if let Some(max_memory) = this.limits.max_memory_bytes
                        && this.state.total_memory + batch_memory > max_memory
                    {
                        return Some(TruncationReason::MemoryLimit);
                    }

                    // Check batch limit
                    if let Some(max_batches) = this.limits.max_batches
                        && this.state.total_batches + 1 > max_batches
                    {
                        return Some(TruncationReason::BatchLimit);
                    }

                    None
                }) {
                    // Limit exceeded - mark as truncated and stop
                    this.state.truncated = true;
                    this.state.truncation_reason = Some(reason);
                    *this.stopped = true;
                    return Poll::Ready(None);
                }

                // Accept the batch and update stats
                this.state.total_rows += batch.num_rows() as u64;
                this.state.total_batches += 1;
                this.state.total_memory += batch.get_array_memory_size();

                Poll::Ready(Some(Ok(batch)))
            }
            Poll::Ready(Some(Err(e))) => Poll::Ready(Some(Err(e))),
            Poll::Ready(None) => Poll::Ready(None),
            Poll::Pending => Poll::Pending,
        }
    }
}

/// Response wrapper that includes both the stream and access to stats.
#[pin_project]
pub struct LimitedResponse<S> {
    #[pin]
    stream: LimitedStream<S>,
}

impl<S> LimitedResponse<S>
where
    S: Stream<Item = Result<RecordBatch>>,
{
    /// Create a new limited response.
    pub fn new(inner: S, limits: QueryLimits) -> Self {
        Self { stream: LimitedStream::new(inner, limits) }
    }

    /// Get the current statistics.
    ///
    /// This can be called at any time, including during streaming.
    /// The stats will reflect all batches received so far.
    pub fn stats(&self) -> QueryStats { self.stream.stats() }

    /// Returns true if results were truncated.
    pub fn is_truncated(&self) -> bool { self.stream.state.truncated }

    /// Returns the truncation reason if results were truncated.
    pub fn truncation_reason(&self) -> Option<TruncationReason> {
        self.stream.state.truncation_reason
    }
}

impl<S> Stream for LimitedResponse<S>
where
    S: Stream<Item = Result<RecordBatch>>,
{
    type Item = Result<RecordBatch>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.project().stream.poll_next(cx)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::array::Int64Array;
    use arrow::datatypes::{DataType, Field, Schema};
    use futures_util::StreamExt;

    use super::*;

    #[allow(clippy::cast_possible_wrap)] // Test data, rows << i64::MAX
    fn create_test_batch(rows: usize) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
        let array = Int64Array::from((0..rows).map(|i| i as i64).collect::<Vec<_>>());
        RecordBatch::try_new(schema, vec![Arc::new(array)]).unwrap()
    }

    #[tokio::test]
    async fn test_no_limits() {
        let batches = vec![
            Ok(create_test_batch(100)),
            Ok(create_test_batch(100)),
            Ok(create_test_batch(100)),
        ];
        let stream = futures_util::stream::iter(batches);
        let mut limited = LimitedResponse::new(stream, QueryLimits::none());

        let mut count = 0;
        while let Some(result) = limited.next().await {
            let _batch = result.unwrap();
            count += 1;
        }

        assert_eq!(count, 3);
        assert!(!limited.is_truncated());
        assert_eq!(limited.stats().rows_returned, 300);
        assert_eq!(limited.stats().batches_returned, 3);
    }

    #[tokio::test]
    async fn test_row_limit() {
        let batches = vec![
            Ok(create_test_batch(100)),
            Ok(create_test_batch(100)),
            Ok(create_test_batch(100)),
        ];
        let stream = futures_util::stream::iter(batches);
        let limits = QueryLimits::none().with_max_rows(150);
        let mut limited = LimitedResponse::new(stream, limits);

        let mut count = 0;
        while let Some(result) = limited.next().await {
            let _batch = result.unwrap();
            count += 1;
        }

        // Should get first batch (100 rows) but not second (would be 200 total)
        assert_eq!(count, 1);
        assert!(limited.is_truncated());
        assert_eq!(limited.truncation_reason(), Some(TruncationReason::RowLimit));
        assert_eq!(limited.stats().rows_returned, 100);
    }

    #[tokio::test]
    async fn test_batch_limit() {
        let batches = vec![
            Ok(create_test_batch(100)),
            Ok(create_test_batch(100)),
            Ok(create_test_batch(100)),
        ];
        let stream = futures_util::stream::iter(batches);
        let limits = QueryLimits::none().with_max_batches(2);
        let mut limited = LimitedResponse::new(stream, limits);

        let mut count = 0;
        while let Some(result) = limited.next().await {
            let _batch = result.unwrap();
            count += 1;
        }

        assert_eq!(count, 2);
        assert!(limited.is_truncated());
        assert_eq!(limited.truncation_reason(), Some(TruncationReason::BatchLimit));
        assert_eq!(limited.stats().batches_returned, 2);
    }

    #[tokio::test]
    async fn test_memory_limit() {
        let batches = vec![
            Ok(create_test_batch(1000)),
            Ok(create_test_batch(1000)),
            Ok(create_test_batch(1000)),
        ];
        let stream = futures_util::stream::iter(batches);

        // Get memory size of one batch
        let sample_batch = create_test_batch(1000);
        let batch_memory = sample_batch.get_array_memory_size();

        // Set limit to allow ~1.5 batches worth of memory
        let limits = QueryLimits::none().with_max_memory(batch_memory + batch_memory / 2);
        let mut limited = LimitedResponse::new(stream, limits);

        let mut count = 0;
        while let Some(result) = limited.next().await {
            let _batch = result.unwrap();
            count += 1;
        }

        // Should get first batch but not second
        assert_eq!(count, 1);
        assert!(limited.is_truncated());
        assert_eq!(limited.truncation_reason(), Some(TruncationReason::MemoryLimit));
    }

    #[tokio::test]
    async fn test_stats_summary() {
        let stats = QueryStats {
            rows_returned:     1000,
            batches_returned:  5,
            memory_bytes:      8192,
            truncated:         true,
            truncation_reason: Some(TruncationReason::RowLimit),
        };

        let summary = stats.summary();
        assert!(summary.contains("1000 rows"));
        assert!(summary.contains("5 batches"));
        assert!(summary.contains("8192 bytes"));
        assert!(summary.contains("TRUNCATED"));
        assert!(summary.contains("row limit exceeded"));
    }

    #[tokio::test]
    async fn test_query_limits_builder() {
        let limits =
            QueryLimits::none().with_max_rows(1000).with_max_memory_mb(10).with_max_batches(5);

        assert_eq!(limits.max_rows, Some(1000));
        assert_eq!(limits.max_memory_bytes, Some(10 * 1024 * 1024));
        assert_eq!(limits.max_batches, Some(5));
        assert!(limits.has_limits());

        let no_limits = QueryLimits::none();
        assert!(!no_limits.has_limits());
    }

    // ========================================================================
    // Edge case tests
    // ========================================================================

    #[tokio::test]
    async fn test_zero_row_limit_truncates_immediately() {
        let batches = vec![Ok(create_test_batch(100)), Ok(create_test_batch(100))];
        let stream = futures_util::stream::iter(batches);
        let limits = QueryLimits::none().with_max_rows(0);
        let mut limited = LimitedResponse::new(stream, limits);

        // Zero limit should truncate immediately (return no batches)
        let mut count = 0;
        while let Some(result) = limited.next().await {
            let _batch = result.unwrap();
            count += 1;
        }

        assert_eq!(count, 0, "Zero row limit should return no batches");
        assert!(limited.is_truncated());
        assert_eq!(limited.truncation_reason(), Some(TruncationReason::RowLimit));
        assert_eq!(limited.stats().rows_returned, 0);
    }

    #[tokio::test]
    async fn test_zero_batch_limit_truncates_immediately() {
        let batches = vec![Ok(create_test_batch(100))];
        let stream = futures_util::stream::iter(batches);
        let limits = QueryLimits::none().with_max_batches(0);
        let mut limited = LimitedResponse::new(stream, limits);

        let mut count = 0;
        while let Some(result) = limited.next().await {
            let _batch = result.unwrap();
            count += 1;
        }

        assert_eq!(count, 0, "Zero batch limit should return no batches");
        assert!(limited.is_truncated());
        assert_eq!(limited.truncation_reason(), Some(TruncationReason::BatchLimit));
    }

    #[tokio::test]
    async fn test_zero_memory_limit_truncates_immediately() {
        let batches = vec![Ok(create_test_batch(100))];
        let stream = futures_util::stream::iter(batches);
        let limits = QueryLimits::none().with_max_memory(0);
        let mut limited = LimitedResponse::new(stream, limits);

        let mut count = 0;
        while let Some(result) = limited.next().await {
            let _batch = result.unwrap();
            count += 1;
        }

        assert_eq!(count, 0, "Zero memory limit should return no batches");
        assert!(limited.is_truncated());
        assert_eq!(limited.truncation_reason(), Some(TruncationReason::MemoryLimit));
    }

    #[tokio::test]
    async fn test_empty_stream_with_limits() {
        let batches: Vec<Result<RecordBatch>> = vec![];
        let stream = futures_util::stream::iter(batches);
        let limits = QueryLimits::none().with_max_rows(100);
        let mut limited = LimitedResponse::new(stream, limits);

        let mut count = 0;
        while let Some(result) = limited.next().await {
            let _batch = result.unwrap();
            count += 1;
        }

        assert_eq!(count, 0);
        assert!(!limited.is_truncated(), "Empty stream should not be truncated");
        assert_eq!(limited.stats().rows_returned, 0);
        assert_eq!(limited.stats().batches_returned, 0);
    }

    #[tokio::test]
    async fn test_exact_row_limit_boundary() {
        // Test when data exactly matches the limit
        let batches = vec![Ok(create_test_batch(50)), Ok(create_test_batch(50))];
        let stream = futures_util::stream::iter(batches);
        let limits = QueryLimits::none().with_max_rows(100);
        let mut limited = LimitedResponse::new(stream, limits);

        let mut count = 0;
        while let Some(result) = limited.next().await {
            let _batch = result.unwrap();
            count += 1;
        }

        assert_eq!(count, 2, "Should get exactly 2 batches");
        assert!(!limited.is_truncated(), "Should not be truncated when exactly at limit");
        assert_eq!(limited.stats().rows_returned, 100);
    }

    #[tokio::test]
    async fn test_exact_batch_limit_boundary() {
        let batches =
            vec![Ok(create_test_batch(50)), Ok(create_test_batch(50)), Ok(create_test_batch(50))];
        let stream = futures_util::stream::iter(batches);
        let limits = QueryLimits::none().with_max_batches(2);
        let mut limited = LimitedResponse::new(stream, limits);

        let mut count = 0;
        while let Some(result) = limited.next().await {
            let _batch = result.unwrap();
            count += 1;
        }

        assert_eq!(count, 2);
        assert!(limited.is_truncated(), "Should be truncated when there's more data");
        assert_eq!(limited.stats().batches_returned, 2);
    }

    #[tokio::test]
    async fn test_error_propagation_through_limits() {
        let batches: Vec<Result<RecordBatch>> = vec![
            Ok(create_test_batch(50)),
            Err(crate::Error::Protocol("test error".into())),
            Ok(create_test_batch(50)),
        ];
        let stream = futures_util::stream::iter(batches);
        let limits = QueryLimits::none().with_max_rows(1000);
        let mut limited = LimitedResponse::new(stream, limits);

        let first = limited.next().await;
        assert!(first.is_some());
        assert!(first.unwrap().is_ok());

        let second = limited.next().await;
        assert!(second.is_some());
        assert!(second.unwrap().is_err(), "Error should propagate through limits");
    }

    #[tokio::test]
    async fn test_very_large_limit_values() {
        let batches = vec![Ok(create_test_batch(100))];
        let stream = futures_util::stream::iter(batches);
        let limits = QueryLimits::none()
            .with_max_rows(u64::MAX)
            .with_max_memory(usize::MAX)
            .with_max_batches(u64::MAX);
        let mut limited = LimitedResponse::new(stream, limits);

        let mut count = 0;
        while let Some(result) = limited.next().await {
            let _batch = result.unwrap();
            count += 1;
        }

        assert_eq!(count, 1);
        assert!(!limited.is_truncated());
    }

    #[tokio::test]
    async fn test_one_row_limit() {
        let batches = vec![Ok(create_test_batch(10))];
        let stream = futures_util::stream::iter(batches);
        let limits = QueryLimits::none().with_max_rows(1);
        let mut limited = LimitedResponse::new(stream, limits);

        let mut count = 0;
        while let Some(result) = limited.next().await {
            let _batch = result.unwrap();
            count += 1;
        }

        // First batch has 10 rows, exceeds limit of 1
        assert_eq!(count, 0, "No full batches should be returned for 1 row limit");
        assert!(limited.is_truncated());
    }

    #[tokio::test]
    async fn test_one_batch_limit() {
        let batches = vec![Ok(create_test_batch(10)), Ok(create_test_batch(10))];
        let stream = futures_util::stream::iter(batches);
        let limits = QueryLimits::none().with_max_batches(1);
        let mut limited = LimitedResponse::new(stream, limits);

        let mut count = 0;
        while let Some(result) = limited.next().await {
            let _batch = result.unwrap();
            count += 1;
        }

        assert_eq!(count, 1);
        assert!(limited.is_truncated());
        assert_eq!(limited.truncation_reason(), Some(TruncationReason::BatchLimit));
    }

    #[tokio::test]
    async fn test_multiple_limits_row_hits_first() {
        let batches = vec![
            Ok(create_test_batch(100)),
            Ok(create_test_batch(100)),
            Ok(create_test_batch(100)),
        ];
        let stream = futures_util::stream::iter(batches);
        // Row limit will trigger first (150 < 300)
        let limits = QueryLimits::none().with_max_rows(150).with_max_batches(10);
        let mut limited = LimitedResponse::new(stream, limits);

        let mut count = 0;
        while let Some(result) = limited.next().await {
            let _batch = result.unwrap();
            count += 1;
        }

        assert_eq!(count, 1);
        assert!(limited.is_truncated());
        assert_eq!(limited.truncation_reason(), Some(TruncationReason::RowLimit));
    }

    #[tokio::test]
    async fn test_multiple_limits_batch_hits_first() {
        let batches =
            vec![Ok(create_test_batch(10)), Ok(create_test_batch(10)), Ok(create_test_batch(10))];
        let stream = futures_util::stream::iter(batches);
        // Batch limit will trigger first (2 batches = 20 rows < 100 row limit)
        let limits = QueryLimits::none().with_max_rows(100).with_max_batches(2);
        let mut limited = LimitedResponse::new(stream, limits);

        let mut count = 0;
        while let Some(result) = limited.next().await {
            let _batch = result.unwrap();
            count += 1;
        }

        assert_eq!(count, 2);
        assert!(limited.is_truncated());
        assert_eq!(limited.truncation_reason(), Some(TruncationReason::BatchLimit));
    }
}
