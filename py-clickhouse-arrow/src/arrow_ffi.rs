// Project:   py-clickhouse-arrow
// File:      arrow_ffi.rs
// Purpose:   Arrow C Data Interface for zero-copy PyArrow interop
// Language:  Rust
//
// License:   Apache-2.0
// Copyright: (c) 2026 HyperSec

//! Arrow FFI bridge for zero-copy data transfer between Rust and Python.
//!
//! Uses the Arrow C Data Interface to exchange `RecordBatch` data with PyArrow.
//!
//! ## References
//!
//! - [Arrow C Data Interface](https://arrow.apache.org/docs/format/CDataInterface.html)
//! - [arrow-rs FFI](https://docs.rs/arrow/latest/arrow/ffi/index.html)

use arrow::array::{Array, RecordBatch, StructArray};
use arrow::ffi::{FFI_ArrowArray, FFI_ArrowSchema};
use pyo3::exceptions::PyTypeError;
use pyo3::prelude::*;

/// Error type for Arrow FFI operations.
#[derive(Debug, thiserror::Error)]
pub(crate) enum ArrowFfiError {
    #[error("Arrow error: {0}")]
    Arrow(#[from] arrow::error::ArrowError),

    #[error("PyArrow import failed: {0}")]
    PyArrowImport(String),

    #[error("FFI conversion failed: {0}")]
    #[expect(dead_code)]
    FfiConversion(String),
}

impl From<ArrowFfiError> for PyErr {
    fn from(err: ArrowFfiError) -> PyErr {
        PyTypeError::new_err(err.to_string())
    }
}

/// Export a `RecordBatch` to a PyArrow `RecordBatch` via the C Data Interface.
///
/// Uses PyArrow's `RecordBatch._import_from_c(array_ptr, schema_ptr)` method.
pub(crate) fn record_batch_to_pyarrow(py: Python<'_>, batch: &RecordBatch) -> PyResult<PyObject> {
    // Import PyArrow
    let pyarrow = py.import("pyarrow").map_err(|e| {
        ArrowFfiError::PyArrowImport(format!("Failed to import pyarrow: {e}"))
    })?;

    // Convert RecordBatch to StructArray for FFI export
    let struct_array: StructArray = batch.clone().into();

    // Export to FFI structs
    let (ffi_array, ffi_schema) =
        arrow::ffi::to_ffi(&struct_array.to_data()).map_err(ArrowFfiError::Arrow)?;

    // Box and get raw pointers for PyArrow
    let array_ptr = Box::into_raw(Box::new(ffi_array)) as usize;
    let schema_ptr = Box::into_raw(Box::new(ffi_schema)) as usize;

    // Use PyArrow's RecordBatch._import_from_c(array_ptr, schema_ptr)
    // This takes ownership of the FFI structs
    let pa_record_batch = pyarrow.getattr("RecordBatch")?;
    let result = pa_record_batch.call_method1("_import_from_c", (array_ptr, schema_ptr))?;

    Ok(result.into())
}

/// Import a `RecordBatch` from a PyArrow object via the C Data Interface.
///
/// Uses PyArrow's `_export_to_c(array_ptr, schema_ptr)` method.
pub(crate) fn record_batch_from_pyarrow(_py: Python<'_>, obj: &Bound<'_, PyAny>) -> PyResult<RecordBatch> {
    // Allocate FFI structs for PyArrow to write into
    let mut ffi_array = FFI_ArrowArray::empty();
    let mut ffi_schema = FFI_ArrowSchema::empty();

    // Get raw pointers as integers for PyArrow
    let array_ptr = &mut ffi_array as *mut FFI_ArrowArray as usize;
    let schema_ptr = &mut ffi_schema as *mut FFI_ArrowSchema as usize;

    // Call PyArrow's _export_to_c(array_ptr, schema_ptr)
    drop(obj.call_method1("_export_to_c", (array_ptr, schema_ptr))?);

    // Convert to Arrow data
    // SAFETY: PyArrow has written valid FFI structs to our pointers
    let array_data = unsafe {
        arrow::ffi::from_ffi(ffi_array, &ffi_schema).map_err(ArrowFfiError::Arrow)?
    };
    let struct_array = StructArray::from(array_data);

    Ok(RecordBatch::from(struct_array))
}
