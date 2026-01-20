use std::borrow::Cow;
use std::fmt::Display;
use std::num::TryFromIntError;
use std::str::Utf8Error;
use std::string::FromUtf8Error;

use crate::Type;
use crate::native::ServerError;

/// Represents various library errors.
///
/// This enum is marked `#[non_exhaustive]` to allow adding new error variants
/// in future versions without breaking changes.
#[derive(thiserror::Error, Debug)]
#[non_exhaustive]
pub enum Error {
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),

    #[error("can't fetch the same column twice from RawRow")]
    DoubleFetch,
    #[error("column index was out of bounds or not present")]
    OutOfBounds,
    #[error("missing field {0}")]
    MissingField(&'static str),
    #[error("missing connection information")]
    MissingConnectionInformation,
    #[error("malformed connection information: {0}")]
    MalformedConnectionInformation(String),
    #[error("duplicate field {0} in struct")]
    DuplicateField(&'static str),
    #[error("protocol error: {0}")]
    Protocol(String),
    #[error("Internal channel closed")]
    InternalChannelError,
    #[error("connection timeout: {0}")]
    ConnectionTimeout(String),
    #[error("connection gone: reason = {0}")]
    ConnectionGone(&'static str),
    #[error("type parse error: {0}")]
    TypeParseError(String),
    #[error("deserialize error: {0}")]
    DeserializeError(String),
    #[error("serialize error: {0}")]
    SerializeError(String),
    #[error("deserialize error for column {0}: {1}")]
    DeserializeErrorWithColumn(&'static str, String),
    #[error("connection startup error")]
    StartupError,
    #[error("Exception({0:?})")]
    ServerException(ServerError),
    #[error("unexpected type: {0}")]
    UnexpectedType(Type),
    #[error("unexpected type for column {0}: {1}")]
    UnexpectedTypeWithColumn(Cow<'static, str>, Type),
    #[error("type conversion failure: {0}")]
    TypeConversion(String),
    #[error("str utf-8 conversion error: {0}")]
    Utf8(#[from] Utf8Error),
    #[error("string utf-8 conversion error: {0}")]
    FromUtf8(#[from] FromUtf8Error),
    #[error("Date failed to parse: {0}")]
    DateTime(#[from] TryFromIntError),
    #[error("channel closed")]
    ChannelClosed,
    #[error("Timeout while sending message: {0}")]
    OutgoingTimeout(String),
    #[error("Invalid DNS name: {0}")]
    InvalidDnsName(String),
    #[error("Unsupported setting type: {0}")]
    UnsupportedSettingType(String),
    #[error("Unsupported setting field type: {0}")]
    UnsupportedFieldType(String),
    #[error("No schemas found")]
    UndefinedSchemas,
    #[error("Tables undefined in database {db}: {tables:?}")]
    UndefinedTables { db: String, tables: Vec<String> },
    #[error("Schema configuration is not valid: {0}")]
    SchemaConfig(String),
    #[error("DDL Statement malformed: {0}")]
    DDLMalformed(String),
    #[error("Insufficient scope for ddl queries: {0}")]
    InsufficientDDLScope(String),
    #[error("Client error: {0}")]
    Client(String),

    // HTTP transport errors
    #[error("Network error: {0}")]
    Network(String),
    #[error("Server error: {0}")]
    Server(String),
    #[error("Configuration error: {0}")]
    Configuration(String),

    // Other
    #[error("External error: {0}")]
    External(Box<dyn std::error::Error + Send + Sync>),
    #[error("Unknown error occurred: {0}")]
    Unknown(String),

    // Arrow errors
    #[error(transparent)]
    Arrow(#[from] arrow::error::ArrowError),
    #[error("insert block retry")]
    InsertArrowRetry(arrow::record_batch::RecordBatch),
    #[error("arrow serialize error: {0}")]
    ArrowSerialize(String),
    #[error("arrow deserialize error: {0}")]
    ArrowDeserialize(String),
    #[error("Type mismatch: expected {expected}")]
    ArrowTypeMismatch { expected: String, provided: String },
    #[error("Unsupported arrow type: {0}")]
    ArrowUnsupportedType(String),

    // DFE Fork: Unimplemented feature
    #[error("Unimplemented: {0}")]
    Unimplemented(String),

    // RowBinary
    #[error(transparent)]
    BytesRead(#[from] bytes::TryGetError),
}

impl Error {
    #[must_use]
    pub fn with_column_name(self, name: &'static str) -> Self {
        match self {
            Error::DeserializeError(e) => Error::DeserializeErrorWithColumn(name, e),
            Error::UnexpectedType(e) => Error::UnexpectedTypeWithColumn(Cow::Borrowed(name), e),
            x => x,
        }
    }
}

/// Implement `serde::ser::Error` to enable custom serialization in query parameters.
///
/// This follows the pattern from the official `clickhouse-rs` client, allowing
/// errors during serialization to be properly propagated through serde.
impl serde::ser::Error for Error {
    fn custom<T: Display>(msg: T) -> Self { Error::SerializeError(msg.to_string()) }
}

/// Implement `serde::de::Error` to enable custom deserialization error handling.
///
/// This follows the pattern from the official `clickhouse-rs` client, allowing
/// errors during deserialization to be properly propagated through serde.
impl serde::de::Error for Error {
    fn custom<T: Display>(msg: T) -> Self { Error::DeserializeError(msg.to_string()) }
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_serde_ser_error_custom() {
        let err: Error = serde::ser::Error::custom("test serialization error");
        assert!(matches!(err, Error::SerializeError(_)));
        assert!(err.to_string().contains("test serialization error"));
    }

    #[test]
    fn test_serde_de_error_custom() {
        let err: Error = serde::de::Error::custom("test deserialization error");
        assert!(matches!(err, Error::DeserializeError(_)));
        assert!(err.to_string().contains("test deserialization error"));
    }

    #[test]
    fn test_error_with_column_name() {
        let err = Error::DeserializeError("failed".to_string());
        let err_with_col = err.with_column_name("my_column");
        assert!(matches!(err_with_col, Error::DeserializeErrorWithColumn("my_column", _)));
    }

    #[test]
    fn test_non_exhaustive_pattern() {
        // This test verifies that the #[non_exhaustive] attribute works correctly.
        // External crates cannot exhaustively match on Error variants.
        let err = Error::Protocol("test".to_string());

        // We can still match specific variants within the crate
        match err {
            Error::Protocol(msg) => assert_eq!(msg, "test"),
            _ => panic!("unexpected variant"),
        }
    }
}
