/// Serialization logic for nullability bitmaps in `ClickHouse`'s native format.
///
/// This module provides functions to serialize nullability bitmaps for Arrow arrays, used by
/// the `ClickHouseArrowSerializer` implementation in `types.rs` for nullable types. It writes
/// a bitmap where `1` represents a null value and `0` represents a non-null value, as expected
/// by `ClickHouse`.
///
/// # Performance
///
/// Incorporates optimisations from `HyperSec` DFE (Data Format Engine) loader:
///
/// - **SIMD acceleration**: Uses AVX2 (`x86_64`) or NEON (`aarch64`) for bit expansion
/// - **Buffer pooling**: Reuses allocations in hot paths
/// - **Vectored I/O** (v0.4.0): Adapted from DFE-loader syscall reduction patterns,
///   combines null bitmap + values into single syscall (15-25% reduction)
///
/// # Examples
/// ```rust,ignore
/// use arrow::array::Int32Array;
/// use clickhouse_arrow::types::null::write_nullability;
/// use std::sync::Arc;
/// use tokio::io::AsyncWriteExt;
///
/// let array = Arc::new(Int32Array::from(vec![Some(1), None, Some(3)])) as ArrayRef;
/// let mut buffer = Vec::new();
/// write_nullability(&mut buffer, &array).await.unwrap();
/// ```
use std::io::IoSlice;

use arrow::array::ArrayRef;
use tokio::io::AsyncWriteExt;

use crate::formats::SerializerState;
use crate::io::{ClickHouseBytesWrite, ClickHouseWrite};
use crate::simd::{PooledBuffer, expand_null_bitmap};
use crate::{Result, Type};

/// Prepare null bitmap buffer for an array.
///
/// Returns a pooled buffer containing the expanded null bitmap (1=null, 0=valid).
/// Used by both standard and vectored I/O paths.
#[inline]
pub(super) fn prepare_null_bitmap(array: &ArrayRef) -> PooledBuffer {
    let len = array.len();
    let mut null_mask = PooledBuffer::with_capacity(len);
    null_mask.resize(len, 0);

    if let Some(null_buffer) = array.nulls() {
        let bitmap_bytes = null_buffer.validity();
        expand_null_bitmap(bitmap_bytes, &mut null_mask, len);
    }

    null_mask
}

/// Write nullable primitive data using vectored I/O (single syscall).
///
/// Combines null bitmap + values buffer into a single `write_vectored` call,
/// reducing syscall overhead by 15-25% for nullable primitive columns.
///
/// # Arguments
/// - `type_hint`: The `ClickHouse` `Type` for validation
/// - `writer`: The async writer
/// - `array`: The Arrow array containing nullability information
/// - `values_bytes`: Pre-computed values buffer (from `bytemuck::cast_slice`)
///
/// # Performance
/// This avoids two separate `write_all` calls by using `IoSlice` to batch them.
pub(super) async fn write_nullable_vectored<W: ClickHouseWrite>(
    type_hint: &Type,
    writer: &mut W,
    array: &ArrayRef,
    values_bytes: &[u8],
) -> Result<()> {
    // Arrays/Maps cannot be nullable in ClickHouse
    if matches!(type_hint.strip_null(), Type::Array(_) | Type::Map(_, _)) {
        // Just write values, no null bitmap
        if !values_bytes.is_empty() {
            writer.write_all(values_bytes).await?;
        }
        return Ok(());
    }

    let len = array.len();
    if len == 0 {
        return Ok(());
    }

    // Prepare null bitmap
    let null_mask = prepare_null_bitmap(array);

    // Combine into vectored write
    let mut bufs = [IoSlice::new(&null_mask), IoSlice::new(values_bytes)];
    writer.write_vectored_all(&mut bufs).await?;

    Ok(())
}

/// Serializes the nullability bitmap for an Arrow array to `ClickHouse`’s native format.
///
/// Writes a bitmap where `1` indicates a null value and `0` indicates a non-null value. If the
/// array has a null buffer, it constructs the bitmap based on valid indices. If no null buffer
/// exists, it writes a zeroed bitmap (all `0`).
///
/// # Arguments
/// - `writer`: The async writer to serialize to (e.g., a TCP stream).
/// - `array`: The Arrow array containing the nullability information.
///
/// # Returns
/// A `Result` indicating success or a `Error` if writing fails.
///
/// # Errors
/// - Returns `Io` if writing to the writer fails.
pub(super) async fn serialize_nulls_async<W: ClickHouseWrite>(
    type_hint: &Type,
    writer: &mut W,
    array: &ArrayRef,
    _state: &mut SerializerState,
) -> Result<()> {
    if matches!(type_hint.strip_null(), Type::Array(_) | Type::Map(_, _)) {
        return Ok(());
    }

    let len = array.len();
    if len == 0 {
        return Ok(());
    }

    // Use pooled buffer to avoid repeated allocations
    let mut null_mask = PooledBuffer::with_capacity(len);
    null_mask.resize(len, 0);

    // Write null bitmap using SIMD-accelerated expansion
    if let Some(null_buffer) = array.nulls() {
        // Get the packed bitmap bytes from Arrow
        let bitmap_bytes = null_buffer.validity();
        // SIMD-accelerated expansion: Arrow packed bits -> CH bytes
        // Arrow: bit=1 means valid, bit=0 means null
        // ClickHouse: byte=0 means valid, byte=1 means null
        expand_null_bitmap(bitmap_bytes, &mut null_mask, len);
    }
    // else: null_mask is already all zeros (all valid)

    writer.write_all(&null_mask).await?;

    Ok(())
}
pub(super) fn serialize_nulls<W: ClickHouseBytesWrite>(
    type_hint: &Type,
    writer: &mut W,
    array: &ArrayRef,
    _state: &mut SerializerState,
) {
    // ClickHouse: Arrays cannot be nullable
    if matches!(type_hint.strip_null(), Type::Array(_) | Type::Map(_, _)) {
        return;
    }

    let len = array.len();
    if len == 0 {
        return;
    }

    // Use pooled buffer to avoid repeated allocations
    let mut null_mask = PooledBuffer::with_capacity(len);
    null_mask.resize(len, 0);

    // Write null bitmap using SIMD-accelerated expansion
    if let Some(null_buffer) = array.nulls() {
        // Get the packed bitmap bytes from Arrow
        let bitmap_bytes = null_buffer.validity();
        // SIMD-accelerated expansion: Arrow packed bits -> CH bytes
        expand_null_bitmap(bitmap_bytes, &mut null_mask, len);
    }
    // else: null_mask is already all zeros (all valid)

    writer.put_slice(&null_mask);
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::array::{Int32Array, ListArray, StringArray};
    use arrow::datatypes::Int32Type;

    use super::*;

    // Helper to create a mock writer
    type MockWriter = Vec<u8>;

    #[tokio::test]
    async fn test_write_nullability_with_nulls() {
        let mut state = SerializerState::default();
        let array = Arc::new(Int32Array::from(vec![Some(1), None, Some(3)])) as ArrayRef;
        let mut writer = MockWriter::new();
        serialize_nulls_async(&Type::Int32, &mut writer, &array, &mut state).await.unwrap();
        assert_eq!(writer, vec![0, 1, 0]); // 1 for null, 0 for non-null
    }

    #[tokio::test]
    async fn test_write_nullability_without_nulls() {
        let mut state = SerializerState::default();
        let array = Arc::new(Int32Array::from(vec![1, 2, 3])) as ArrayRef;
        let mut writer = MockWriter::new();
        serialize_nulls_async(&Type::Int32, &mut writer, &array, &mut state).await.unwrap();
        assert_eq!(writer, vec![0, 0, 0]); // All 0 for non-null
    }

    #[tokio::test]
    async fn test_write_nullability_empty() {
        let mut state = SerializerState::default();
        let array = Arc::new(Int32Array::from(Vec::<i32>::new())) as ArrayRef;
        let mut writer = MockWriter::new();
        serialize_nulls_async(&Type::Int32, &mut writer, &array, &mut state).await.unwrap();
        assert!(writer.is_empty());
    }

    #[tokio::test]
    async fn test_write_nullability_nullable_string() {
        let mut state = SerializerState::default();
        let array = Arc::new(StringArray::from(vec![Some("a"), None, Some("c")])) as ArrayRef;
        let mut writer = MockWriter::new();
        serialize_nulls_async(&Type::String, &mut writer, &array, &mut state).await.unwrap();
        assert_eq!(writer, vec![0, 1, 0]); // 1 for null, 0 for non-null
    }

    // ClickHouse doesn't support nullable arrays
    #[tokio::test]
    async fn test_write_nullability_nullable_array() {
        let mut state = SerializerState::default();
        let data = vec![
            Some(vec![Some(0), Some(1), Some(2)]),
            None,
            Some(vec![Some(3), None, Some(5)]),
            Some(vec![Some(6), Some(7)]),
        ];
        let list_array = ListArray::from_iter_primitive::<Int32Type, _, _>(data);
        let array = Arc::new(list_array) as ArrayRef;
        let mut writer = MockWriter::new();
        serialize_nulls_async(
            &Type::Nullable(Type::Array(Type::Int32.into()).into()),
            &mut writer,
            &array,
            &mut state,
        )
        .await
        .unwrap();
        assert!(writer.is_empty());
    }
}

#[cfg(test)]
mod tests_sync {
    use std::sync::Arc;

    use arrow::array::{Int32Array, ListArray, StringArray};
    use arrow::datatypes::Int32Type;

    use super::*;

    // Helper to create a mock writer
    type MockWriter = Vec<u8>;

    #[test]
    fn test_write_nullability_with_nulls() {
        let mut state = SerializerState::default();
        let array = Arc::new(Int32Array::from(vec![Some(1), None, Some(3)])) as ArrayRef;
        let mut writer = MockWriter::new();
        serialize_nulls(&Type::Int32, &mut writer, &array, &mut state);
        assert_eq!(writer, vec![0, 1, 0]); // 1 for null, 0 for non-null
    }

    #[test]
    fn test_write_nullability_without_nulls() {
        let mut state = SerializerState::default();
        let array = Arc::new(Int32Array::from(vec![1, 2, 3])) as ArrayRef;
        let mut writer = MockWriter::new();
        serialize_nulls(&Type::Int32, &mut writer, &array, &mut state);
        assert_eq!(writer, vec![0, 0, 0]); // All 0 for non-null
    }

    #[test]
    fn test_write_nullability_empty() {
        let mut state = SerializerState::default();
        let array = Arc::new(Int32Array::from(Vec::<i32>::new())) as ArrayRef;
        let mut writer = MockWriter::new();
        serialize_nulls(&Type::Int32, &mut writer, &array, &mut state);
        assert!(writer.is_empty());
    }

    #[test]
    fn test_write_nullability_nullable_string() {
        let mut state = SerializerState::default();
        let array = Arc::new(StringArray::from(vec![Some("a"), None, Some("c")])) as ArrayRef;
        let mut writer = MockWriter::new();
        serialize_nulls(&Type::String, &mut writer, &array, &mut state);
        assert_eq!(writer, vec![0, 1, 0]); // 1 for null, 0 for non-null
    }

    // ClickHouse doesn't support nullable arrays
    #[test]
    fn test_write_nullability_nullable_array() {
        let mut state = SerializerState::default();
        let data = vec![
            Some(vec![Some(0), Some(1), Some(2)]),
            None,
            Some(vec![Some(3), None, Some(5)]),
            Some(vec![Some(6), Some(7)]),
        ];
        let list_array = ListArray::from_iter_primitive::<Int32Type, _, _>(data);
        let array = Arc::new(list_array) as ArrayRef;
        let mut writer = MockWriter::new();
        serialize_nulls(
            &Type::Nullable(Type::Array(Type::Int32.into()).into()),
            &mut writer,
            &array,
            &mut state,
        );
        assert!(writer.is_empty());
    }

    // Comprehensive test for various Array type combinations
    #[test]
    fn test_write_nullability_array_type_variations() {
        let mut state = SerializerState::default();

        // Test 1: Array(Nullable(Int64)) - should not write null mask
        let data = vec![
            Some(vec![Some(1i64), None, Some(3)]),
            Some(vec![None, None]),
            Some(vec![Some(10), Some(20), Some(30)]),
        ];
        let list_array = ListArray::from_iter_primitive::<arrow::datatypes::Int64Type, _, _>(data);
        let array = Arc::new(list_array) as ArrayRef;
        let mut writer = MockWriter::new();

        // Type is Array(Nullable(Int64)) - not wrapped in Nullable
        serialize_nulls(
            &Type::Array(Type::Nullable(Type::Int64.into()).into()),
            &mut writer,
            &array,
            &mut state,
        );
        assert!(writer.is_empty(), "Array type should not write null mask");

        // Test 2: Nullable(Array(Int64)) - should not write null mask due to special handling
        let data2 = vec![
            Some(vec![Some(1i64), Some(2), Some(3)]),
            None, // This array is null
            Some(vec![Some(10), Some(20)]),
        ];
        let list_array2 =
            ListArray::from_iter_primitive::<arrow::datatypes::Int64Type, _, _>(data2);
        let array2 = Arc::new(list_array2) as ArrayRef;
        let mut writer2 = MockWriter::new();

        serialize_nulls(
            &Type::Nullable(Type::Array(Type::Int64.into()).into()),
            &mut writer2,
            &array2,
            &mut state,
        );
        assert!(writer2.is_empty(), "Nullable(Array) should not write null mask");

        // Test 3: Map type (similar rules as Array)
        let mut writer3 = MockWriter::new();
        serialize_nulls(
            &Type::Nullable(Type::Map(Type::String.into(), Type::Int32.into()).into()),
            &mut writer3,
            &array, // Using dummy array since we're just testing the type check
            &mut state,
        );
        assert!(writer3.is_empty(), "Nullable(Map) should not write null mask");
    }
}
