//! `ArrowStream` IPC format helpers.
//!
//! Uses the `arrow-ipc` crate to serialize and deserialize Arrow data
//! in `ClickHouse`'s `ArrowStream` format.

use std::io::Cursor;

use arrow::array::RecordBatch;
use arrow::ipc::reader::StreamReader;
use arrow::ipc::writer::StreamWriter;
use bytes::Bytes;

use crate::Error;
use crate::errors::Result;

/// Serialize a `RecordBatch` to `ArrowStream` IPC format.
pub(super) fn serialize_batch(batch: &RecordBatch) -> Result<Bytes> {
    let schema = batch.schema();
    let mut buffer = Vec::with_capacity(batch.get_array_memory_size());

    let mut writer = StreamWriter::try_new(&mut buffer, &schema)
        .map_err(|e| Error::ArrowSerialize(format!("Failed to create ArrowStream writer: {e}")))?;

    writer
        .write(batch)
        .map_err(|e| Error::ArrowSerialize(format!("Failed to write batch to ArrowStream: {e}")))?;

    writer
        .finish()
        .map_err(|e| Error::ArrowSerialize(format!("Failed to finish ArrowStream: {e}")))?;

    Ok(Bytes::from(buffer))
}

/// Deserialize `ArrowStream` IPC format to `RecordBatch`es.
pub(super) fn deserialize_batches(data: Bytes) -> Result<Vec<RecordBatch>> {
    if data.is_empty() {
        return Ok(Vec::new());
    }

    let cursor = Cursor::new(data);
    let reader = StreamReader::try_new(cursor, None).map_err(|e| {
        Error::ArrowDeserialize(format!("Failed to create ArrowStream reader: {e}"))
    })?;

    let mut batches = Vec::new();
    for batch_result in reader {
        let batch = batch_result.map_err(|e| {
            Error::ArrowDeserialize(format!("Failed to read batch from ArrowStream: {e}"))
        })?;
        batches.push(batch);
    }

    Ok(batches)
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::array::{Array, Float64Array, Int32Array, Int64Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};

    use super::*;

    fn create_test_schema() -> Arc<Schema> {
        Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, true),
        ]))
    }

    fn create_test_batch() -> RecordBatch {
        let schema = create_test_schema();
        let id_array = Int64Array::from(vec![1, 2, 3]);
        let name_array = StringArray::from(vec![Some("Alice"), Some("Bob"), None]);

        RecordBatch::try_new(schema, vec![Arc::new(id_array), Arc::new(name_array)]).unwrap()
    }

    #[test]
    fn test_round_trip() {
        let original = create_test_batch();
        let serialized = serialize_batch(&original).unwrap();
        let deserialized = deserialize_batches(serialized).unwrap();

        assert_eq!(deserialized.len(), 1);
        assert_eq!(deserialized[0], original);
    }

    #[test]
    fn test_empty_data() {
        let result = deserialize_batches(Bytes::new()).unwrap();
        assert!(result.is_empty());
    }

    #[test]
    fn test_nullable_values() {
        let schema = Arc::new(Schema::new(vec![Field::new("value", DataType::Utf8, true)]));

        let array = StringArray::from(vec![Some("a"), None, Some("c"), None, None]);
        let batch = RecordBatch::try_new(schema, vec![Arc::new(array)]).unwrap();

        let serialized = serialize_batch(&batch).unwrap();
        let deserialized = deserialize_batches(serialized).unwrap();

        assert_eq!(deserialized.len(), 1);
        assert_eq!(deserialized[0].num_rows(), 5);

        let result_col = deserialized[0].column(0).as_any().downcast_ref::<StringArray>().unwrap();
        assert!(result_col.is_valid(0));
        assert!(result_col.is_null(1));
        assert!(result_col.is_valid(2));
        assert!(result_col.is_null(3));
        assert!(result_col.is_null(4));
    }

    #[test]
    fn test_various_types() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("int32_col", DataType::Int32, false),
            Field::new("int64_col", DataType::Int64, false),
            Field::new("float64_col", DataType::Float64, false),
            Field::new("string_col", DataType::Utf8, false),
        ]));

        let batch = RecordBatch::try_new(schema, vec![
            Arc::new(Int32Array::from(vec![1, 2, 3])),
            Arc::new(Int64Array::from(vec![100, 200, 300])),
            Arc::new(Float64Array::from(vec![1.5, 2.5, 3.5])),
            Arc::new(StringArray::from(vec!["a", "b", "c"])),
        ])
        .unwrap();

        let serialized = serialize_batch(&batch).unwrap();
        let deserialized = deserialize_batches(serialized).unwrap();

        assert_eq!(deserialized.len(), 1);
        assert_eq!(deserialized[0].num_rows(), 3);
        assert_eq!(deserialized[0].num_columns(), 4);
    }

    #[test]
    #[allow(clippy::cast_possible_wrap, clippy::cast_precision_loss)]
    fn test_large_batch() {
        const ROW_COUNT: usize = 10_000;

        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("value", DataType::Float64, false),
        ]));

        let ids: Vec<i64> = (0..ROW_COUNT as i64).collect();
        let values: Vec<f64> = (0..ROW_COUNT).map(|i| i as f64 * 0.5).collect();

        let batch = RecordBatch::try_new(schema, vec![
            Arc::new(Int64Array::from(ids)),
            Arc::new(Float64Array::from(values)),
        ])
        .unwrap();

        let serialized = serialize_batch(&batch).unwrap();
        assert!(!serialized.is_empty());

        let deserialized = deserialize_batches(serialized).unwrap();
        assert_eq!(deserialized.len(), 1);
        assert_eq!(deserialized[0].num_rows(), ROW_COUNT);
    }

    #[test]
    fn test_serialized_bytes_not_empty() {
        let batch = create_test_batch();
        let serialized = serialize_batch(&batch).unwrap();

        // ArrowStream format should produce non-trivial output
        assert!(serialized.len() > 50, "Serialized data too small: {} bytes", serialized.len());
    }
}
