//! Sparse serialisation for ClickHouse native protocol (fixes #96).
//!
//! Optimisation for columns w/ many defaults – only non-default values are stored
//! along with their positions. Wire format:
//!
//! 1. Offsets: VarUInt group sizes (count of defaults before each non-default)
//!    - Final group has `END_OF_GRANULE_FLAG` (2^62) ORed in
//! 2. Values: Only the non-default values
//!
//! Example: `[0, 0, 5, 0, 3, 0, 0, 0]` → offsets [2, 1, 3|END], values [5, 3]

use std::sync::Arc;

use arrow::array::*;
use arrow::datatypes::*;

use crate::Result;
use crate::io::ClickHouseRead;

/// End-of-granule marker (bit 62). When set, this is the final VarUInt in the offsets stream.
pub(crate) const END_OF_GRANULE_FLAG: u64 = 1 << 62;

/// State for sparse deserialisation across multiple reads.
#[derive(Debug, Default, Clone)]
pub(crate) struct SparseDeserializeState {
    /// Trailing defaults from previous read that haven't been consumed yet.
    pub num_trailing_defaults:    u64,
    /// Non-default value pending after the trailing defaults.
    pub has_value_after_defaults: bool,
}

/// Read sparse offsets from stream. Returns positions of non-default values.
///
/// Must loop until END_OF_GRANULE_FLAG – can't stop early even if we have enough
/// rows, or the stream will be misaligned for the next column.
pub(crate) async fn read_sparse_offsets<R: ClickHouseRead>(
    reader: &mut R,
    num_rows: usize,
    state: &mut SparseDeserializeState,
) -> Result<Vec<usize>> {
    let mut offsets = Vec::new();
    let mut current_position: u64 = 0;

    // Handle any state carried over from previous read
    if state.num_trailing_defaults > 0 {
        current_position += state.num_trailing_defaults;
        state.num_trailing_defaults = 0;
    }
    if state.has_value_after_defaults {
        if (current_position as usize) < num_rows {
            offsets.push(current_position as usize);
        }
        current_position += 1;
        state.has_value_after_defaults = false;
    }

    // Read offset groups until we hit END_OF_GRANULE
    // The format is: [group_size, group_size, ..., group_size | END_OF_GRANULE_FLAG]
    // Each group_size represents the count of defaults before a non-default value
    // The final group has END_OF_GRANULE_FLAG set and represents trailing defaults
    loop {
        let group_size = reader.read_var_uint().await?;

        // Check if this is the end of granule
        let is_end_of_granule = (group_size & END_OF_GRANULE_FLAG) != 0;
        let actual_group_size = group_size & !END_OF_GRANULE_FLAG;

        // Move past the default values
        current_position += actual_group_size;

        if is_end_of_granule {
            // Store trailing defaults for potential next read
            if current_position > num_rows as u64 {
                state.num_trailing_defaults = current_position - num_rows as u64;
            }
            break;
        }

        // There's a non-default value at current_position
        if (current_position as usize) < num_rows {
            offsets.push(current_position as usize);
            current_position += 1;
        } else {
            // Non-default value is beyond our window, save state
            state.has_value_after_defaults = true;
            // We still need to read remaining VarUInts until END_OF_GRANULE
            // For now, just continue looping - the END_OF_GRANULE will terminate
        }
    }

    Ok(offsets)
}

/// Sync version of read_sparse_offsets for bytes::Buf readers.
pub(crate) fn read_sparse_offsets_sync<R: crate::io::ClickHouseBytesRead>(
    reader: &mut R,
    num_rows: usize,
    state: &mut SparseDeserializeState,
) -> Result<Vec<usize>> {
    let mut offsets = Vec::new();
    let mut current_position: u64 = 0;

    // Handle any state carried over from previous read
    if state.num_trailing_defaults > 0 {
        current_position += state.num_trailing_defaults;
        state.num_trailing_defaults = 0;
    }
    if state.has_value_after_defaults {
        if (current_position as usize) < num_rows {
            offsets.push(current_position as usize);
        }
        current_position += 1;
        state.has_value_after_defaults = false;
    }

    // Read offset groups until we hit END_OF_GRANULE
    // The format is: [group_size, group_size, ..., group_size | END_OF_GRANULE_FLAG]
    // Each group_size represents the count of defaults before a non-default value
    // The final group has END_OF_GRANULE_FLAG set and represents trailing defaults
    loop {
        let group_size = reader.try_get_var_uint()?;

        // Check if this is the end of granule
        let is_end_of_granule = (group_size & END_OF_GRANULE_FLAG) != 0;
        let actual_group_size = group_size & !END_OF_GRANULE_FLAG;

        // Move past the default values
        current_position += actual_group_size;

        if is_end_of_granule {
            // Store trailing defaults for potential next read
            if current_position > num_rows as u64 {
                state.num_trailing_defaults = current_position - num_rows as u64;
            }
            break;
        }

        // There's a non-default value at current_position
        if (current_position as usize) < num_rows {
            offsets.push(current_position as usize);
            current_position += 1;
        } else {
            // Non-default value is beyond our window, save state
            state.has_value_after_defaults = true;
            // We still need to read remaining VarUInts until END_OF_GRANULE
            // For now, just continue looping - the END_OF_GRANULE will terminate
        }
    }

    Ok(offsets)
}

/// Expand sparse array to full size, filling non-offset positions with defaults.
pub(crate) fn expand_sparse_array(
    sparse_array: &ArrayRef,
    offsets: &[usize],
    total_rows: usize,
) -> Result<ArrayRef> {
    assert_eq!(sparse_array.len(), offsets.len(), "Sparse array length must match offsets length");

    let data_type = sparse_array.data_type();

    // Handle each data type
    let result: ArrayRef = match data_type {
        DataType::Int8 => expand_primitive::<Int8Type>(sparse_array, offsets, total_rows),
        DataType::Int16 => expand_primitive::<Int16Type>(sparse_array, offsets, total_rows),
        DataType::Int32 => expand_primitive::<Int32Type>(sparse_array, offsets, total_rows),
        DataType::Int64 => expand_primitive::<Int64Type>(sparse_array, offsets, total_rows),
        DataType::UInt8 => expand_primitive::<UInt8Type>(sparse_array, offsets, total_rows),
        DataType::UInt16 => expand_primitive::<UInt16Type>(sparse_array, offsets, total_rows),
        DataType::UInt32 => expand_primitive::<UInt32Type>(sparse_array, offsets, total_rows),
        DataType::UInt64 => expand_primitive::<UInt64Type>(sparse_array, offsets, total_rows),
        DataType::Float32 => expand_primitive::<Float32Type>(sparse_array, offsets, total_rows),
        DataType::Float64 => expand_primitive::<Float64Type>(sparse_array, offsets, total_rows),
        DataType::Date32 => expand_primitive::<Date32Type>(sparse_array, offsets, total_rows),
        DataType::Date64 => expand_primitive::<Date64Type>(sparse_array, offsets, total_rows),
        DataType::Timestamp(TimeUnit::Second, _) => {
            expand_primitive::<TimestampSecondType>(sparse_array, offsets, total_rows)
        }
        DataType::Timestamp(TimeUnit::Millisecond, _) => {
            expand_primitive::<TimestampMillisecondType>(sparse_array, offsets, total_rows)
        }
        DataType::Timestamp(TimeUnit::Microsecond, _) => {
            expand_primitive::<TimestampMicrosecondType>(sparse_array, offsets, total_rows)
        }
        DataType::Timestamp(TimeUnit::Nanosecond, _) => {
            expand_primitive::<TimestampNanosecondType>(sparse_array, offsets, total_rows)
        }
        DataType::Decimal128(_, _) => {
            expand_primitive::<Decimal128Type>(sparse_array, offsets, total_rows)
        }
        DataType::Decimal256(_, _) => {
            expand_primitive::<Decimal256Type>(sparse_array, offsets, total_rows)
        }
        DataType::Utf8 => expand_string::<i32>(sparse_array, offsets, total_rows),
        DataType::LargeUtf8 => expand_string::<i64>(sparse_array, offsets, total_rows),
        DataType::Binary => expand_binary::<i32>(sparse_array, offsets, total_rows),
        DataType::LargeBinary => expand_binary::<i64>(sparse_array, offsets, total_rows),
        DataType::Boolean => expand_boolean(sparse_array, offsets, total_rows),
        DataType::FixedSizeBinary(size) => {
            expand_fixed_size_binary(sparse_array, offsets, total_rows, *size)
        }
        _ => {
            return Err(crate::Error::Unimplemented(format!(
                "Sparse expansion not implemented for type: {:?}",
                data_type
            )));
        }
    };

    Ok(result)
}

fn expand_primitive<T: ArrowPrimitiveType>(
    sparse_array: &ArrayRef,
    offsets: &[usize],
    total_rows: usize,
) -> ArrayRef
where
    T::Native: Default,
{
    let sparse = sparse_array.as_primitive::<T>();
    let mut builder = PrimitiveBuilder::<T>::with_capacity(total_rows);

    let mut offset_idx = 0;
    for row in 0..total_rows {
        if offset_idx < offsets.len() && offsets[offset_idx] == row {
            if sparse.is_null(offset_idx) {
                builder.append_null();
            } else {
                builder.append_value(sparse.value(offset_idx));
            }
            offset_idx += 1;
        } else {
            // Default value for the type
            builder.append_value(T::Native::default());
        }
    }

    Arc::new(builder.finish())
}

fn expand_string<O: OffsetSizeTrait>(
    sparse_array: &ArrayRef,
    offsets: &[usize],
    total_rows: usize,
) -> ArrayRef {
    let sparse = sparse_array.as_any().downcast_ref::<GenericStringArray<O>>().unwrap();
    let mut builder =
        GenericStringBuilder::<O>::with_capacity(total_rows, sparse.value_data().len());

    let mut offset_idx = 0;
    for row in 0..total_rows {
        if offset_idx < offsets.len() && offsets[offset_idx] == row {
            if sparse.is_null(offset_idx) {
                builder.append_null();
            } else {
                builder.append_value(sparse.value(offset_idx));
            }
            offset_idx += 1;
        } else {
            // Default is empty string
            builder.append_value("");
        }
    }

    Arc::new(builder.finish())
}

fn expand_binary<O: OffsetSizeTrait>(
    sparse_array: &ArrayRef,
    offsets: &[usize],
    total_rows: usize,
) -> ArrayRef {
    let sparse = sparse_array.as_any().downcast_ref::<GenericBinaryArray<O>>().unwrap();
    let mut builder =
        GenericBinaryBuilder::<O>::with_capacity(total_rows, sparse.value_data().len());

    let mut offset_idx = 0;
    for row in 0..total_rows {
        if offset_idx < offsets.len() && offsets[offset_idx] == row {
            if sparse.is_null(offset_idx) {
                builder.append_null();
            } else {
                builder.append_value(sparse.value(offset_idx));
            }
            offset_idx += 1;
        } else {
            // Default is empty bytes
            builder.append_value(b"");
        }
    }

    Arc::new(builder.finish())
}

fn expand_boolean(sparse_array: &ArrayRef, offsets: &[usize], total_rows: usize) -> ArrayRef {
    let sparse = sparse_array.as_boolean();
    let mut builder = BooleanBuilder::with_capacity(total_rows);

    let mut offset_idx = 0;
    for row in 0..total_rows {
        if offset_idx < offsets.len() && offsets[offset_idx] == row {
            if sparse.is_null(offset_idx) {
                builder.append_null();
            } else {
                builder.append_value(sparse.value(offset_idx));
            }
            offset_idx += 1;
        } else {
            // Default is false
            builder.append_value(false);
        }
    }

    Arc::new(builder.finish())
}

fn expand_fixed_size_binary(
    sparse_array: &ArrayRef,
    offsets: &[usize],
    total_rows: usize,
    size: i32,
) -> ArrayRef {
    let sparse = sparse_array.as_fixed_size_binary();
    let mut builder = FixedSizeBinaryBuilder::with_capacity(total_rows, size);
    let default_value = vec![0u8; size as usize];

    let mut offset_idx = 0;
    for row in 0..total_rows {
        if offset_idx < offsets.len() && offsets[offset_idx] == row {
            if sparse.is_null(offset_idx) {
                builder.append_null();
            } else {
                builder.append_value(sparse.value(offset_idx)).unwrap();
            }
            offset_idx += 1;
        } else {
            // Default is zeros
            builder.append_value(&default_value).unwrap();
        }
    }

    Arc::new(builder.finish())
}

#[cfg(test)]
mod tests {
    use bytes::Bytes;

    use super::*;

    fn encode_var_uint(value: u64) -> Vec<u8> {
        let mut result = Vec::new();
        let mut v = value;
        loop {
            let byte = (v & 0x7f) as u8;
            v >>= 7;
            if v == 0 {
                result.push(byte);
                break;
            }
            result.push(byte | 0x80);
        }
        result
    }

    #[test]
    fn test_read_sparse_offsets_simple() {
        // Column: [default, default, value, default, value, default, default, default]
        // Positions of non-defaults: [2, 4]
        // Encoded: group_size=2, group_size=1, group_size=(3 | END_OF_GRANULE_FLAG)
        let mut data = Vec::new();
        data.extend(encode_var_uint(2)); // 2 defaults before first value
        data.extend(encode_var_uint(1)); // 1 default before second value
        data.extend(encode_var_uint(3 | END_OF_GRANULE_FLAG)); // 3 trailing defaults

        let mut bytes = Bytes::from(data);
        let mut state = SparseDeserializeState::default();
        let offsets = read_sparse_offsets_sync(&mut bytes, 8, &mut state).unwrap();

        assert_eq!(offsets, vec![2, 4]);
    }

    #[test]
    fn test_read_sparse_offsets_all_defaults() {
        // Column: [default, default, default, default]
        // No non-default values
        // Encoded: group_size=(4 | END_OF_GRANULE_FLAG)
        let mut data = Vec::new();
        data.extend(encode_var_uint(4 | END_OF_GRANULE_FLAG));

        let mut bytes = Bytes::from(data);
        let mut state = SparseDeserializeState::default();
        let offsets = read_sparse_offsets_sync(&mut bytes, 4, &mut state).unwrap();

        assert!(offsets.is_empty());
    }

    #[test]
    fn test_read_sparse_offsets_no_defaults() {
        // Column: [value, value, value]
        // All non-default values
        // Each VarUInt without END_OF_GRANULE_FLAG = "N defaults followed by a value"
        // Final VarUInt with END_OF_GRANULE_FLAG = "N trailing defaults (no value)"
        // Encoded: group_size=0 (value), group_size=0 (value), group_size=0 (value),
        //          group_size=(0 | END_OF_GRANULE_FLAG) (no trailing defaults)
        let mut data = Vec::new();
        data.extend(encode_var_uint(0)); // 0 defaults before first value at position 0
        data.extend(encode_var_uint(0)); // 0 defaults before second value at position 1
        data.extend(encode_var_uint(0)); // 0 defaults before third value at position 2
        data.extend(encode_var_uint(END_OF_GRANULE_FLAG)); // 0 trailing defaults

        let mut bytes = Bytes::from(data);
        let mut state = SparseDeserializeState::default();
        let offsets = read_sparse_offsets_sync(&mut bytes, 3, &mut state).unwrap();

        assert_eq!(offsets, vec![0, 1, 2]);
    }

    #[test]
    fn test_read_sparse_offsets_first_is_value() {
        // Column: [value, default, default, value]
        // Positions: [0, 3]
        // Encoded: group_size=0, group_size=2, group_size=(0 | END_OF_GRANULE_FLAG)
        let mut data = Vec::new();
        data.extend(encode_var_uint(0)); // 0 defaults before first value
        data.extend(encode_var_uint(2)); // 2 defaults before second value
        data.extend(encode_var_uint(END_OF_GRANULE_FLAG)); // 0 trailing defaults

        let mut bytes = Bytes::from(data);
        let mut state = SparseDeserializeState::default();
        let offsets = read_sparse_offsets_sync(&mut bytes, 4, &mut state).unwrap();

        assert_eq!(offsets, vec![0, 3]);
    }

    #[test]
    fn test_expand_sparse_int64_array() {
        // Sparse values at positions [1, 3]: values [10, 30]
        // Total rows: 5
        // Expected: [0, 10, 0, 30, 0]
        let sparse_values = Int64Array::from(vec![10i64, 30i64]);
        let sparse_array: ArrayRef = Arc::new(sparse_values);
        let offsets = vec![1, 3];
        let total_rows = 5;

        let expanded = expand_sparse_array(&sparse_array, &offsets, total_rows).unwrap();
        let expanded_i64 = expanded.as_primitive::<Int64Type>();

        assert_eq!(expanded_i64.len(), 5);
        assert_eq!(expanded_i64.value(0), 0);
        assert_eq!(expanded_i64.value(1), 10);
        assert_eq!(expanded_i64.value(2), 0);
        assert_eq!(expanded_i64.value(3), 30);
        assert_eq!(expanded_i64.value(4), 0);
    }

    #[test]
    fn test_expand_sparse_string_array() {
        // Sparse values at positions [0, 2]: values ["hello", "world"]
        // Total rows: 4
        // Expected: ["hello", "", "world", ""]
        let sparse_values = StringArray::from(vec!["hello", "world"]);
        let sparse_array: ArrayRef = Arc::new(sparse_values);
        let offsets = vec![0, 2];
        let total_rows = 4;

        let expanded = expand_sparse_array(&sparse_array, &offsets, total_rows).unwrap();
        let expanded_str = expanded.as_any().downcast_ref::<StringArray>().unwrap();

        assert_eq!(expanded_str.len(), 4);
        assert_eq!(expanded_str.value(0), "hello");
        assert_eq!(expanded_str.value(1), "");
        assert_eq!(expanded_str.value(2), "world");
        assert_eq!(expanded_str.value(3), "");
    }

    #[test]
    fn test_expand_sparse_all_values() {
        // All positions have non-default values
        let sparse_values = Int32Array::from(vec![1i32, 2, 3]);
        let sparse_array: ArrayRef = Arc::new(sparse_values);
        let offsets = vec![0, 1, 2];
        let total_rows = 3;

        let expanded = expand_sparse_array(&sparse_array, &offsets, total_rows).unwrap();
        let expanded_i32 = expanded.as_primitive::<Int32Type>();

        assert_eq!(expanded_i32.len(), 3);
        assert_eq!(expanded_i32.value(0), 1);
        assert_eq!(expanded_i32.value(1), 2);
        assert_eq!(expanded_i32.value(2), 3);
    }

    #[test]
    fn test_expand_sparse_empty() {
        // No non-default values
        let sparse_values = Int32Array::from(Vec::<i32>::new());
        let sparse_array: ArrayRef = Arc::new(sparse_values);
        let offsets: Vec<usize> = vec![];
        let total_rows = 5;

        let expanded = expand_sparse_array(&sparse_array, &offsets, total_rows).unwrap();
        let expanded_i32 = expanded.as_primitive::<Int32Type>();

        assert_eq!(expanded_i32.len(), 5);
        for i in 0..5 {
            assert_eq!(expanded_i32.value(i), 0);
        }
    }
}
