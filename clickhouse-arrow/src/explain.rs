//! EXPLAIN query support for analyzing query execution.
//!
//! This module provides types and utilities for running EXPLAIN queries
//! alongside regular queries, allowing developers to analyze query plans,
//! AST, pipelines, and estimates.
//!
//! # Example
//!
//! ```rust,ignore
//! use clickhouse_arrow::prelude::*;
//! use clickhouse_arrow::explain::{ExplainOptions, ExplainOperation};
//!
//! let opts = QueryOptions::new()
//!     .with_explain(ExplainOptions::plan());
//!
//! let mut response = client.query_with_options("SELECT * FROM users", opts).await?;
//!
//! // Consume query results as normal
//! while let Some(batch) = response.next().await {
//!     let batch = batch?;
//!     println!("Received {} rows", batch.num_rows());
//! }
//!
//! // Get explain results (blocks until parallel explain completes)
//! if let Some(explain) = response.explain().await {
//!     println!("{}", explain);
//! }
//! ```

use std::fmt;

use arrow::record_batch::RecordBatch;

use crate::limits::QueryLimits;
use crate::query::{Qid, QueryParams};

/// Type of EXPLAIN operation to run.
///
/// Each operation provides different insights into query execution:
/// - `Ast`: Shows the parsed Abstract Syntax Tree
/// - `Syntax`: Shows the normalized/optimized SQL query
/// - `Plan`: Shows the query execution plan
/// - `Pipeline`: Shows the processor execution pipeline
/// - `Estimate`: Shows estimated rows/bytes to be read
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Hash)]
pub enum ExplainOperation {
    /// Abstract Syntax Tree - shows query structure after parsing.
    Ast,
    /// Normalized SQL - shows query after AST-level optimizations.
    Syntax,
    /// Query execution plan - shows operations like `ReadFromStorage`, Filter, etc.
    #[default]
    Plan,
    /// Processor pipeline - shows actual execution threads and processors.
    Pipeline,
    /// I/O estimate - shows estimated rows, parts, and marks to read.
    Estimate,
}

impl ExplainOperation {
    /// Returns the SQL keyword for this operation.
    #[must_use]
    pub fn as_sql(&self) -> &'static str {
        match self {
            ExplainOperation::Ast => "AST",
            ExplainOperation::Syntax => "SYNTAX",
            ExplainOperation::Plan => "PLAN",
            ExplainOperation::Pipeline => "PIPELINE",
            ExplainOperation::Estimate => "ESTIMATE",
        }
    }

    /// Returns true if this operation supports JSON output format.
    #[must_use]
    pub fn supports_json(&self) -> bool { matches!(self, ExplainOperation::Plan) }

    /// Returns true if this operation returns tabular data (suitable for Arrow).
    #[must_use]
    pub fn is_tabular(&self) -> bool { matches!(self, ExplainOperation::Estimate) }
}

impl fmt::Display for ExplainOperation {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result { write!(f, "{}", self.as_sql()) }
}

/// Output format for EXPLAIN results.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Hash)]
pub enum ExplainFormat {
    /// Automatically choose the best format based on operation.
    /// - `Estimate` → Arrow (tabular data)
    /// - `Plan` with json requested → Json
    /// - All others → Text
    #[default]
    Auto,
    /// Plain text output (works with all operations).
    Text,
    /// JSON output (only valid for `Plan` operation).
    Json,
    /// Arrow `RecordBatch` output (only valid for `Estimate` operation).
    Arrow,
}

impl ExplainFormat {
    /// Resolve `Auto` to the actual format based on the operation.
    #[must_use]
    pub fn resolve(self, operation: ExplainOperation) -> ExplainFormat {
        match self {
            ExplainFormat::Auto => {
                if operation.is_tabular() {
                    ExplainFormat::Arrow
                } else {
                    ExplainFormat::Text
                }
            }
            other => other,
        }
    }
}

/// Execution mode for EXPLAIN.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Hash)]
pub enum ExplainMode {
    /// Run EXPLAIN in parallel with the actual query (default).
    /// The query executes normally while EXPLAIN runs concurrently.
    #[default]
    Parallel,
    /// Only run EXPLAIN, do not execute the query.
    /// The response stream will be empty.
    ExplainOnly,
}

/// Configuration for EXPLAIN queries.
///
/// # Example
///
/// ```rust,ignore
/// use clickhouse_arrow::explain::{ExplainOptions, ExplainOperation, ExplainFormat};
///
/// // Simple: just get the execution plan
/// let opts = ExplainOptions::plan();
///
/// // With JSON output
/// let opts = ExplainOptions::plan().with_json();
///
/// // Only explain, don't run the query
/// let opts = ExplainOptions::plan().explain_only();
///
/// // Full configuration
/// let opts = ExplainOptions::new()
///     .with_operation(ExplainOperation::Pipeline)
///     .with_format(ExplainFormat::Text);
/// ```
#[derive(Debug, Clone, Copy, Default)]
pub struct ExplainOptions {
    /// The type of explain to run.
    pub operation: ExplainOperation,
    /// The output format.
    pub format:    ExplainFormat,
    /// The execution mode.
    pub mode:      ExplainMode,
}

impl ExplainOptions {
    /// Create new explain options with defaults (Plan, Auto format, Parallel mode).
    #[must_use]
    pub fn new() -> Self { Self::default() }

    /// Create explain options for AST output.
    #[must_use]
    pub fn ast() -> Self { Self { operation: ExplainOperation::Ast, ..Default::default() } }

    /// Create explain options for SYNTAX output.
    #[must_use]
    pub fn syntax() -> Self { Self { operation: ExplainOperation::Syntax, ..Default::default() } }

    /// Create explain options for PLAN output.
    #[must_use]
    pub fn plan() -> Self { Self { operation: ExplainOperation::Plan, ..Default::default() } }

    /// Create explain options for PIPELINE output.
    #[must_use]
    pub fn pipeline() -> Self {
        Self { operation: ExplainOperation::Pipeline, ..Default::default() }
    }

    /// Create explain options for ESTIMATE output.
    #[must_use]
    pub fn estimate() -> Self {
        Self { operation: ExplainOperation::Estimate, ..Default::default() }
    }

    /// Set the explain operation.
    #[must_use]
    pub fn with_operation(mut self, operation: ExplainOperation) -> Self {
        self.operation = operation;
        self
    }

    /// Set the output format.
    #[must_use]
    pub fn with_format(mut self, format: ExplainFormat) -> Self {
        self.format = format;
        self
    }

    /// Request JSON output (only valid for Plan).
    #[must_use]
    pub fn with_json(mut self) -> Self {
        self.format = ExplainFormat::Json;
        self
    }

    /// Set the execution mode.
    #[must_use]
    pub fn with_mode(mut self, mode: ExplainMode) -> Self {
        self.mode = mode;
        self
    }

    /// Set mode to `ExplainOnly` (don't execute the query).
    #[must_use]
    pub fn explain_only(mut self) -> Self {
        self.mode = ExplainMode::ExplainOnly;
        self
    }

    /// Build the EXPLAIN SQL prefix for a query.
    ///
    /// Returns a string like "EXPLAIN PLAN" or "EXPLAIN PLAN json=1".
    #[must_use]
    pub fn build_prefix(&self) -> String {
        let resolved_format = self.format.resolve(self.operation);
        let json_suffix =
            if resolved_format == ExplainFormat::Json && self.operation.supports_json() {
                " json=1"
            } else {
                ""
            };
        format!("EXPLAIN {}{}", self.operation.as_sql(), json_suffix)
    }
}

/// Result of an EXPLAIN query.
///
/// The variant depends on the `ExplainFormat` used:
/// - `Text`: Most operations (AST, SYNTAX, PLAN, PIPELINE)
/// - `Json`: PLAN with json=1
/// - `Arrow`: ESTIMATE (tabular data)
#[derive(Debug, Clone)]
pub enum ExplainResult {
    /// Text output (tree structure, SQL, etc.).
    Text(String),
    /// JSON output (structured plan data).
    #[cfg(feature = "serde")]
    Json(serde_json::Value),
    /// Arrow `RecordBatch` output (ESTIMATE tabular data).
    Arrow(RecordBatch),
}

impl ExplainResult {
    /// Get the result as text, if it is text.
    #[must_use]
    pub fn as_text(&self) -> Option<&str> {
        match self {
            ExplainResult::Text(s) => Some(s),
            _ => None,
        }
    }

    /// Get the result as JSON, if it is JSON.
    #[cfg(feature = "serde")]
    #[must_use]
    pub fn as_json(&self) -> Option<&serde_json::Value> {
        match self {
            ExplainResult::Json(v) => Some(v),
            _ => None,
        }
    }

    /// Get the result as Arrow `RecordBatch`, if it is Arrow.
    #[must_use]
    pub fn as_arrow(&self) -> Option<&RecordBatch> {
        match self {
            ExplainResult::Arrow(b) => Some(b),
            _ => None,
        }
    }

    /// Check if the result is text.
    #[must_use]
    pub fn is_text(&self) -> bool { matches!(self, ExplainResult::Text(_)) }

    /// Check if the result is JSON.
    #[cfg(feature = "serde")]
    #[must_use]
    pub fn is_json(&self) -> bool { matches!(self, ExplainResult::Json(_)) }

    /// Check if the result is Arrow.
    #[must_use]
    pub fn is_arrow(&self) -> bool { matches!(self, ExplainResult::Arrow(_)) }
}

impl fmt::Display for ExplainResult {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ExplainResult::Text(s) => write!(f, "{s}"),
            #[cfg(feature = "serde")]
            ExplainResult::Json(v) => {
                if let Ok(pretty) = serde_json::to_string_pretty(v) {
                    write!(f, "{pretty}")
                } else {
                    write!(f, "{v}")
                }
            }
            ExplainResult::Arrow(batch) => {
                write!(f, "RecordBatch({} rows, {} columns)", batch.num_rows(), batch.num_columns())
            }
        }
    }
}

/// Row structure for EXPLAIN ESTIMATE results.
///
/// This provides typed access to the tabular data returned by `EXPLAIN ESTIMATE`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExplainEstimateRow {
    /// Database name.
    pub database: String,
    /// Table name.
    pub table:    String,
    /// Number of parts to read.
    pub parts:    u64,
    /// Estimated number of rows.
    pub rows:     u64,
    /// Number of marks to read.
    pub marks:    u64,
}

impl ExplainEstimateRow {
    /// Parse estimate rows from an Arrow `RecordBatch`.
    ///
    /// # Errors
    /// Returns an error if the batch doesn't have the expected schema.
    pub fn from_batch(batch: &RecordBatch) -> crate::Result<Vec<Self>> {
        use arrow::array::{AsArray, StringArray};

        let database_col = batch
            .column_by_name("database")
            .ok_or_else(|| crate::Error::DeserializeError("Missing 'database' column".into()))?;
        let table_col = batch
            .column_by_name("table")
            .ok_or_else(|| crate::Error::DeserializeError("Missing 'table' column".into()))?;
        let parts_col = batch
            .column_by_name("parts")
            .ok_or_else(|| crate::Error::DeserializeError("Missing 'parts' column".into()))?;
        let rows_col = batch
            .column_by_name("rows")
            .ok_or_else(|| crate::Error::DeserializeError("Missing 'rows' column".into()))?;
        let marks_col = batch
            .column_by_name("marks")
            .ok_or_else(|| crate::Error::DeserializeError("Missing 'marks' column".into()))?;

        let databases = database_col.as_any().downcast_ref::<StringArray>().ok_or_else(|| {
            crate::Error::DeserializeError("'database' column is not a string array".into())
        })?;
        let tables = table_col.as_any().downcast_ref::<StringArray>().ok_or_else(|| {
            crate::Error::DeserializeError("'table' column is not a string array".into())
        })?;
        let parts =
            parts_col.as_primitive_opt::<arrow::datatypes::UInt64Type>().ok_or_else(|| {
                crate::Error::DeserializeError("'parts' column is not a UInt64 array".into())
            })?;
        let rows =
            rows_col.as_primitive_opt::<arrow::datatypes::UInt64Type>().ok_or_else(|| {
                crate::Error::DeserializeError("'rows' column is not a UInt64 array".into())
            })?;
        let marks =
            marks_col.as_primitive_opt::<arrow::datatypes::UInt64Type>().ok_or_else(|| {
                crate::Error::DeserializeError("'marks' column is not a UInt64 array".into())
            })?;

        let mut result = Vec::with_capacity(batch.num_rows());
        for i in 0..batch.num_rows() {
            result.push(ExplainEstimateRow {
                database: databases.value(i).to_string(),
                table:    tables.value(i).to_string(),
                parts:    parts.value(i),
                rows:     rows.value(i),
                marks:    marks.value(i),
            });
        }

        Ok(result)
    }
}

/// Unified query options for configuring query execution.
///
/// This builder allows combining multiple optional features:
/// - Query parameters
/// - Result limits (memory, rows, batches)
/// - EXPLAIN execution
/// - Query ID
///
/// # Example
///
/// ```rust,ignore
/// use clickhouse_arrow::prelude::*;
/// use clickhouse_arrow::explain::{ExplainOptions, QueryOptions};
///
/// // Simple query with explain
/// let opts = QueryOptions::new()
///     .with_explain(ExplainOptions::plan());
///
/// // Full configuration
/// let opts = QueryOptions::new()
///     .with_params(vec![("id", ParamValue::from(42))].into())
///     .with_limits(QueryLimits::none().with_max_rows(1000))
///     .with_explain(ExplainOptions::plan().with_json())
///     .with_qid(Qid::new());
/// ```
#[derive(Debug, Clone, Default)]
pub struct QueryOptions {
    /// Query parameters for parameterized queries.
    pub params:  Option<QueryParams>,
    /// Result limits (memory, rows, batches).
    pub limits:  Option<QueryLimits>,
    /// EXPLAIN configuration.
    pub explain: Option<ExplainOptions>,
    /// Query ID for tracking and debugging.
    pub qid:     Option<Qid>,
}

impl QueryOptions {
    /// Create new query options with defaults.
    #[must_use]
    pub fn new() -> Self { Self::default() }

    /// Set query parameters.
    #[must_use]
    pub fn with_params(mut self, params: impl Into<QueryParams>) -> Self {
        self.params = Some(params.into());
        self
    }

    /// Set result limits.
    #[must_use]
    pub fn with_limits(mut self, limits: QueryLimits) -> Self {
        self.limits = Some(limits);
        self
    }

    /// Set EXPLAIN options.
    #[must_use]
    pub fn with_explain(mut self, explain: ExplainOptions) -> Self {
        self.explain = Some(explain);
        self
    }

    /// Set query ID.
    #[must_use]
    pub fn with_qid(mut self, qid: Qid) -> Self {
        self.qid = Some(qid);
        self
    }

    /// Check if any options are set.
    #[must_use]
    pub fn has_options(&self) -> bool {
        self.params.is_some()
            || self.limits.is_some()
            || self.explain.is_some()
            || self.qid.is_some()
    }

    /// Check if explain is configured.
    #[must_use]
    pub fn has_explain(&self) -> bool { self.explain.is_some() }

    /// Check if this is explain-only mode.
    #[must_use]
    pub fn is_explain_only(&self) -> bool {
        self.explain.as_ref().is_some_and(|e| e.mode == ExplainMode::ExplainOnly)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_explain_operation_sql() {
        assert_eq!(ExplainOperation::Ast.as_sql(), "AST");
        assert_eq!(ExplainOperation::Syntax.as_sql(), "SYNTAX");
        assert_eq!(ExplainOperation::Plan.as_sql(), "PLAN");
        assert_eq!(ExplainOperation::Pipeline.as_sql(), "PIPELINE");
        assert_eq!(ExplainOperation::Estimate.as_sql(), "ESTIMATE");
    }

    #[test]
    fn test_explain_operation_supports_json() {
        assert!(!ExplainOperation::Ast.supports_json());
        assert!(!ExplainOperation::Syntax.supports_json());
        assert!(ExplainOperation::Plan.supports_json());
        assert!(!ExplainOperation::Pipeline.supports_json());
        assert!(!ExplainOperation::Estimate.supports_json());
    }

    #[test]
    fn test_explain_operation_is_tabular() {
        assert!(!ExplainOperation::Ast.is_tabular());
        assert!(!ExplainOperation::Syntax.is_tabular());
        assert!(!ExplainOperation::Plan.is_tabular());
        assert!(!ExplainOperation::Pipeline.is_tabular());
        assert!(ExplainOperation::Estimate.is_tabular());
    }

    #[test]
    fn test_explain_format_resolve() {
        // Auto resolves to Text for most operations
        assert_eq!(ExplainFormat::Auto.resolve(ExplainOperation::Ast), ExplainFormat::Text);
        assert_eq!(ExplainFormat::Auto.resolve(ExplainOperation::Plan), ExplainFormat::Text);

        // Auto resolves to Arrow for Estimate
        assert_eq!(ExplainFormat::Auto.resolve(ExplainOperation::Estimate), ExplainFormat::Arrow);

        // Explicit formats stay as-is
        assert_eq!(ExplainFormat::Json.resolve(ExplainOperation::Plan), ExplainFormat::Json);
    }

    #[test]
    fn test_explain_options_builder() {
        let opts = ExplainOptions::plan().with_json().explain_only();

        assert_eq!(opts.operation, ExplainOperation::Plan);
        assert_eq!(opts.format, ExplainFormat::Json);
        assert_eq!(opts.mode, ExplainMode::ExplainOnly);
    }

    #[test]
    fn test_explain_options_build_prefix() {
        assert_eq!(ExplainOptions::ast().build_prefix(), "EXPLAIN AST");
        assert_eq!(ExplainOptions::syntax().build_prefix(), "EXPLAIN SYNTAX");
        assert_eq!(ExplainOptions::plan().build_prefix(), "EXPLAIN PLAN");
        assert_eq!(ExplainOptions::plan().with_json().build_prefix(), "EXPLAIN PLAN json=1");
        assert_eq!(ExplainOptions::pipeline().build_prefix(), "EXPLAIN PIPELINE");
        assert_eq!(ExplainOptions::estimate().build_prefix(), "EXPLAIN ESTIMATE");
    }

    #[test]
    fn test_query_options_builder() {
        let opts = QueryOptions::new()
            .with_limits(QueryLimits::none().with_max_rows(100))
            .with_explain(ExplainOptions::plan());

        assert!(opts.has_options());
        assert!(opts.has_explain());
        assert!(!opts.is_explain_only());

        let explain_only = QueryOptions::new().with_explain(ExplainOptions::plan().explain_only());

        assert!(explain_only.is_explain_only());
    }

    #[test]
    fn test_explain_result_display() {
        let text = ExplainResult::Text("Expression\n  ReadFromStorage".to_string());
        assert!(text.to_string().contains("Expression"));
        assert!(text.is_text());
        assert!(!text.is_arrow());
    }
}
