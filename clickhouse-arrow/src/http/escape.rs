//! SQL escaping utilities for HTTP queries.
//!
//! This module provides functions to safely escape SQL values and identifiers
//! for use in `ClickHouse` queries over HTTP. The escaping rules follow the
//! `ClickHouse` SQL syntax and match the patterns used by the official
//! `clickhouse-rs` client and `ClickHouse` Connect Python library.
//!
//! # Special Characters
//!
//! The following 5 characters require escaping in string values:
//! - `\` (backslash) → `\\`
//! - `'` (single quote) → `\'`
//! - `` ` `` (backtick) → `` \` ``
//! - `\t` (tab) → `\\t`
//! - `\n` (newline) → `\\n`
//!
//! # Example
//!
//! ```rust,ignore
//! use clickhouse_arrow::http::escape::{escape_string, escape_identifier};
//!
//! // Escape a string value for use in a query
//! let safe_value = escape_string("O'Brien");
//! assert_eq!(safe_value, "O\\'Brien");
//!
//! // Escape an identifier (table/column name)
//! let safe_id = escape_identifier("my-table");
//! assert_eq!(safe_id, "`my-table`");
//! ```

use std::borrow::Cow;

/// Escape a string value for use in a SQL query.
///
/// Escapes special characters that could break SQL syntax or enable injection.
/// The result should be used within single quotes in the query.
///
/// # Example
///
/// ```rust,ignore
/// let query = format!("SELECT * FROM users WHERE name = '{}'", escape_string("O'Brien"));
/// // Result: SELECT * FROM users WHERE name = 'O\'Brien'
/// ```
#[must_use]
pub fn escape_string(s: &str) -> Cow<'_, str> {
    // Fast path: check if any escaping is needed
    if !s.bytes().any(|b| matches!(b, b'\\' | b'\'' | b'`' | b'\t' | b'\n')) {
        return Cow::Borrowed(s);
    }

    // Slow path: escape characters
    let mut result = String::with_capacity(s.len() + 8);
    for c in s.chars() {
        match c {
            '\\' => result.push_str("\\\\"),
            '\'' => result.push_str("\\'"),
            '`' => result.push_str("\\`"),
            '\t' => result.push_str("\\t"),
            '\n' => result.push_str("\\n"),
            _ => result.push(c),
        }
    }
    Cow::Owned(result)
}

/// Escape an identifier (table name, column name) for use in a SQL query.
///
/// Wraps the identifier in backticks and escapes any backticks within.
/// This is the safest way to use dynamic identifiers in queries.
///
/// # Example
///
/// ```rust,ignore
/// let query = format!("SELECT * FROM {}", escape_identifier("my-table"));
/// // Result: SELECT * FROM `my-table`
/// ```
#[must_use]
pub fn escape_identifier(s: &str) -> String {
    // Escape any backticks in the identifier
    let escaped = s.replace('`', "\\`");
    format!("`{escaped}`")
}

/// Check if a string is a valid unquoted identifier.
///
/// Valid identifiers start with a letter or underscore and contain only
/// letters, digits, and underscores. If this returns `true`, the identifier
/// can be used directly without escaping.
#[must_use]
pub fn is_valid_identifier(s: &str) -> bool {
    if s.is_empty() {
        return false;
    }

    let mut chars = s.chars();

    // First character must be letter or underscore
    match chars.next() {
        Some(c) if c.is_ascii_alphabetic() || c == '_' => {}
        _ => return false,
    }

    // Remaining characters must be alphanumeric or underscore
    chars.all(|c| c.is_ascii_alphanumeric() || c == '_')
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_escape_string_no_special_chars() {
        let result = escape_string("hello world");
        assert!(matches!(result, Cow::Borrowed(_)));
        assert_eq!(result, "hello world");
    }

    #[test]
    fn test_escape_string_single_quote() {
        assert_eq!(escape_string("O'Brien"), "O\\'Brien");
    }

    #[test]
    fn test_escape_string_backslash() {
        assert_eq!(escape_string("path\\to\\file"), "path\\\\to\\\\file");
    }

    #[test]
    fn test_escape_string_backtick() {
        assert_eq!(escape_string("value`here"), "value\\`here");
    }

    #[test]
    fn test_escape_string_tab() {
        assert_eq!(escape_string("col1\tcol2"), "col1\\tcol2");
    }

    #[test]
    fn test_escape_string_newline() {
        assert_eq!(escape_string("line1\nline2"), "line1\\nline2");
    }

    #[test]
    fn test_escape_string_multiple_special() {
        assert_eq!(
            escape_string("it's a 'test'\nwith\\special"),
            "it\\'s a \\'test\\'\\nwith\\\\special"
        );
    }

    #[test]
    fn test_escape_identifier_simple() {
        assert_eq!(escape_identifier("my_table"), "`my_table`");
    }

    #[test]
    fn test_escape_identifier_with_dash() {
        assert_eq!(escape_identifier("my-table"), "`my-table`");
    }

    #[test]
    fn test_escape_identifier_with_backtick() {
        assert_eq!(escape_identifier("my`table"), "`my\\`table`");
    }

    #[test]
    fn test_is_valid_identifier_valid() {
        assert!(is_valid_identifier("my_table"));
        assert!(is_valid_identifier("_private"));
        assert!(is_valid_identifier("Table123"));
        assert!(is_valid_identifier("a"));
    }

    #[test]
    fn test_is_valid_identifier_invalid() {
        assert!(!is_valid_identifier(""));
        assert!(!is_valid_identifier("123abc")); // starts with digit
        assert!(!is_valid_identifier("my-table")); // contains dash
        assert!(!is_valid_identifier("my table")); // contains space
        assert!(!is_valid_identifier("my`table")); // contains backtick
    }
}
