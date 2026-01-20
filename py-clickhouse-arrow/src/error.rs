// Project:   py-clickhouse-arrow
// File:      error.rs
// Purpose:   Python exception hierarchy mapping from Rust errors
// Language:  Rust
//
// License:   Apache-2.0
// Copyright: (c) 2026 HyperSec

//! Python exception hierarchy for clickhouse-arrow errors.
//!
//! Maps the Rust `Error` enum to a Python exception hierarchy:
//!
//! ```text
//! ClickHouseError (base)
//! ├── ConnectionError      - Network, timeout, connection issues
//! ├── QueryError           - Protocol, parsing, type errors
//! ├── SerializationError   - Data serialization failures
//! ├── ServerError          - ClickHouse server exceptions
//! └── ConfigurationError   - Client configuration issues
//! ```

use pyo3::create_exception;
use pyo3::exceptions::PyException;
use pyo3::prelude::*;

// Base exception for all clickhouse-arrow errors
create_exception!(clickhouse_arrow, ClickHouseError, PyException);

// Connection-related errors
create_exception!(clickhouse_arrow, ConnectionError, ClickHouseError);

// Query execution errors
create_exception!(clickhouse_arrow, QueryError, ClickHouseError);

// Data serialisation errors
create_exception!(clickhouse_arrow, SerializationError, ClickHouseError);

// Server-side errors from ClickHouse
create_exception!(clickhouse_arrow, ServerError, ClickHouseError);

// Configuration and setup errors
create_exception!(clickhouse_arrow, ConfigurationError, ClickHouseError);

/// Wrapper type to allow implementing From trait (orphan rules workaround).
pub(crate) struct ClickHouseErrorWrapper(clickhouse_arrow::Error);

impl From<clickhouse_arrow::Error> for ClickHouseErrorWrapper {
    fn from(err: clickhouse_arrow::Error) -> Self {
        Self(err)
    }
}

impl From<ClickHouseErrorWrapper> for PyErr {
    fn from(wrapper: ClickHouseErrorWrapper) -> PyErr {
        use clickhouse_arrow::Error;

        let err = wrapper.0;
        let msg = err.to_string();

        match err {
            // Connection errors
            Error::Io(_)
            | Error::ConnectionTimeout(_)
            | Error::ConnectionGone(_)
            | Error::InternalChannelError
            | Error::ChannelClosed
            | Error::OutgoingTimeout(_)
            | Error::StartupError
            | Error::Network(_) => ConnectionError::new_err(msg),

            // Query/protocol errors
            Error::Protocol(_)
            | Error::TypeParseError(_)
            | Error::DeserializeError(_)
            | Error::DeserializeErrorWithColumn(_, _)
            | Error::UnexpectedType(_)
            | Error::UnexpectedTypeWithColumn(_, _)
            | Error::TypeConversion(_)
            | Error::DoubleFetch
            | Error::OutOfBounds
            | Error::Utf8(_)
            | Error::FromUtf8(_)
            | Error::DateTime(_)
            | Error::BytesRead(_)
            | Error::ArrowDeserialize(_)
            | Error::ArrowTypeMismatch { .. }
            | Error::ArrowUnsupportedType(_)
            | Error::Arrow(_) => QueryError::new_err(msg),

            // Serialisation errors
            Error::SerializeError(_)
            | Error::ArrowSerialize(_)
            | Error::InsertArrowRetry(_) => SerializationError::new_err(msg),

            // Server errors
            Error::ServerException(_) | Error::Server(_) => ServerError::new_err(msg),

            // Configuration errors
            Error::MissingConnectionInformation
            | Error::MalformedConnectionInformation(_)
            | Error::InvalidDnsName(_)
            | Error::MissingField(_)
            | Error::DuplicateField(_)
            | Error::UnsupportedSettingType(_)
            | Error::UnsupportedFieldType(_)
            | Error::UndefinedSchemas
            | Error::UndefinedTables { .. }
            | Error::SchemaConfig(_)
            | Error::DDLMalformed(_)
            | Error::InsufficientDDLScope(_)
            | Error::Configuration(_) => ConfigurationError::new_err(msg),

            // Generic/other errors
            Error::Client(_)
            | Error::External(_)
            | Error::Unknown(_)
            | Error::Unimplemented(_) => ClickHouseError::new_err(msg),

            // Catch-all for future variants (Error is #[non_exhaustive])
            _ => ClickHouseError::new_err(msg),
        }
    }
}

/// Convert a clickhouse-arrow Result to a PyResult.
pub(crate) fn to_py_result<T>(result: Result<T, clickhouse_arrow::Error>) -> PyResult<T> {
    result.map_err(|e| ClickHouseErrorWrapper(e).into())
}

/// Register exception types with the Python module.
pub(crate) fn register_exceptions(py: Python<'_>, m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add("ClickHouseError", py.get_type::<ClickHouseError>())?;
    m.add("ConnectionError", py.get_type::<ConnectionError>())?;
    m.add("QueryError", py.get_type::<QueryError>())?;
    m.add("SerializationError", py.get_type::<SerializationError>())?;
    m.add("ServerError", py.get_type::<ServerError>())?;
    m.add("ConfigurationError", py.get_type::<ConfigurationError>())?;
    Ok(())
}
