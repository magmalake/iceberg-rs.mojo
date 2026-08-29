//! Record batches: the opaque `Batch` handle, column materialisation, the Arrow
//! C Data Interface bridge, and the column-wise batch builder used for appends.
//!
//! Two ways out of Rust:
//!
//! * **Opaque handle + typed copies** (`ib_batch_get_i64` / `_f64` / `_str`).
//!   Every column is `arrow::compute::cast`-ed to the requested width first, so
//!   int32/int64/date/timestamp all land in the same `i64` path and float32/64 in
//!   the same `f64` path. This is what the Mojo helper uses: no Arrow layout
//!   knowledge is needed on the Mojo side.
//! * **Arrow C Data Interface** (`ib_batch_export` / `ib_batch_import`). The batch
//!   is exported as a struct array — one `ArrowArray` + one `ArrowSchema` whose
//!   children are the columns — which is exactly what marrow's `c_data.mojo`
//!   consumes. Ownership transfers to the caller: call the struct's own `release`
//!   callback (or `ib_arrow_release`) when done.

use std::ffi::c_char;
use std::os::raw::c_void;
use std::ptr;
use std::sync::Arc;

use arrow_array::ffi::{FFI_ArrowArray, FFI_ArrowSchema, from_ffi, to_ffi};
use arrow_array::{Array, ArrayRef, BooleanArray, Float64Array, Int64Array, RecordBatch, StringArray, StructArray};
use arrow_schema::DataType;

use crate::{cstr, out_string, set_err};

pub(crate) struct BatchHandle {
    pub(crate) batch: RecordBatch,
}

pub(crate) fn as_batch<'a>(p: *mut c_void) -> Option<&'a BatchHandle> {
    if p.is_null() {
        set_err("batch handle is null");
        return None;
    }
    Some(unsafe { &*(p as *const BatchHandle) })
}

pub(crate) fn boxed_batch(batch: RecordBatch) -> *mut c_void {
    Box::into_raw(Box::new(BatchHandle { batch })) as *mut c_void
}

/// Release a batch handle.
///
/// # Safety
/// `b` must come from this library and must not be reused.
#[no_mangle]
pub unsafe extern "C" fn ib_batch_free(b: *mut c_void) {
    if !b.is_null() {
        drop(unsafe { Box::from_raw(b as *mut BatchHandle) });
    }
}

/// Rows in the batch, or -1 on error.
#[no_mangle]
pub extern "C" fn ib_batch_num_rows(b: *mut c_void) -> i64 {
    match as_batch(b) {
        Some(h) => h.batch.num_rows() as i64,
        None => -1,
    }
}

/// Columns in the batch, or -1 on error.
#[no_mangle]
pub extern "C" fn ib_batch_num_columns(b: *mut c_void) -> i32 {
    match as_batch(b) {
        Some(h) => h.batch.num_columns() as i32,
        None => -1,
    }
}

/// JSON array of `{"name": …, "type": …}` describing the batch's columns, in
/// order; `type` is the Arrow type's display form. Caller frees with `ib_string_free`.
#[no_mangle]
pub extern "C" fn ib_batch_schema_json(b: *mut c_void) -> *mut c_char {
    let Some(h) = as_batch(b) else {
        return ptr::null_mut();
    };
    let cols: Vec<serde_json::Value> = h
        .batch
        .schema()
        .fields()
        .iter()
        .map(|f| {
            serde_json::json!({
                "name": f.name(),
                "type": format!("{}", f.data_type()),
                "nullable": f.is_nullable(),
            })
        })
        .collect();
    out_string(serde_json::to_string(&cols).unwrap_or_else(|_| "[]".into()))
}

// ── typed column reads ───────────────────────────────────────────────────────

fn column<'a>(h: &'a BatchHandle, name: &str, what: &str) -> Option<&'a ArrayRef> {
    match h.batch.column_by_name(name) {
        Some(c) => Some(c),
        None => {
            set_err(format!("{what}: no column named '{name}' in the batch"));
            None
        }
    }
}

fn cast_to(col: &ArrayRef, to: &DataType, what: &str) -> Option<ArrayRef> {
    if col.data_type() == to {
        return Some(col.clone());
    }
    match arrow::compute::kernels::cast::cast(col, to) {
        Ok(a) => Some(a),
        Err(e) => {
            set_err(format!(
                "{what}: cannot read a {} column as {to}: {e}",
                col.data_type()
            ));
            None
        }
    }
}

/// Write a column's validity bytes (1 = value present) if the caller asked for them.
fn write_validity(arr: &dyn Array, out_valid: *mut u8, n: usize) {
    if out_valid.is_null() {
        return;
    }
    let dst = unsafe { std::slice::from_raw_parts_mut(out_valid, n) };
    for (i, slot) in dst.iter_mut().enumerate() {
        *slot = u8::from(arr.is_valid(i));
    }
}

/// Copy up to `cap` values of column `name` into `out` as `i64`, casting from
/// int8/16/32/64, boolean, date and timestamp columns. `out_valid` may be NULL;
/// otherwise it takes one byte per row (1 = non-null). Returns the number of
/// values written, or -1 on error.
///
/// # Safety
/// `out` must have room for `cap` `i64`s, `out_valid` for `cap` bytes.
#[no_mangle]
pub unsafe extern "C" fn ib_batch_get_i64(
    b: *mut c_void,
    name: *const c_char,
    out: *mut i64,
    out_valid: *mut u8,
    cap: usize,
) -> i64 {
    let what = "ib_batch_get_i64";
    let Some(h) = as_batch(b) else { return -1 };
    let Some(name) = cstr(name, what) else { return -1 };
    let Some(col) = column(h, name, what) else {
        return -1;
    };
    if out.is_null() {
        set_err(format!("{what}: null out pointer"));
        return -1;
    }
    let Some(casted) = cast_to(col, &DataType::Int64, what) else {
        return -1;
    };
    let arr = casted.as_any().downcast_ref::<Int64Array>().unwrap();
    let n = arr.len().min(cap);
    let dst = unsafe { std::slice::from_raw_parts_mut(out, n) };
    for (i, slot) in dst.iter_mut().enumerate() {
        // Nulls read back as 0; `out_valid` is how the caller tells them apart.
        *slot = if arr.is_null(i) { 0 } else { arr.value(i) };
    }
    write_validity(arr, out_valid, n);
    n as i64
}

/// Same as `ib_batch_get_i64` but as `f64` (float32/64, and any numeric column).
///
/// # Safety
/// `out` must have room for `cap` `f64`s, `out_valid` for `cap` bytes.
#[no_mangle]
pub unsafe extern "C" fn ib_batch_get_f64(
    b: *mut c_void,
    name: *const c_char,
    out: *mut f64,
    out_valid: *mut u8,
    cap: usize,
) -> i64 {
    let what = "ib_batch_get_f64";
    let Some(h) = as_batch(b) else { return -1 };
    let Some(name) = cstr(name, what) else { return -1 };
    let Some(col) = column(h, name, what) else {
        return -1;
    };
    if out.is_null() {
        set_err(format!("{what}: null out pointer"));
        return -1;
    }
    let Some(casted) = cast_to(col, &DataType::Float64, what) else {
        return -1;
    };
    let arr = casted.as_any().downcast_ref::<Float64Array>().unwrap();
    let n = arr.len().min(cap);
    let dst = unsafe { std::slice::from_raw_parts_mut(out, n) };
    for (i, slot) in dst.iter_mut().enumerate() {
        *slot = if arr.is_null(i) { 0.0 } else { arr.value(i) };
    }
    write_validity(arr, out_valid, n);
    n as i64
}

/// Same as `ib_batch_get_i64` but as booleans (one byte per row, 0/1).
///
/// # Safety
/// `out` must have room for `cap` bytes, `out_valid` likewise.
#[no_mangle]
pub unsafe extern "C" fn ib_batch_get_bool(
    b: *mut c_void,
    name: *const c_char,
    out: *mut u8,
    out_valid: *mut u8,
    cap: usize,
) -> i64 {
    let what = "ib_batch_get_bool";
    let Some(h) = as_batch(b) else { return -1 };
    let Some(name) = cstr(name, what) else { return -1 };
    let Some(col) = column(h, name, what) else {
        return -1;
    };
    if out.is_null() {
        set_err(format!("{what}: null out pointer"));
        return -1;
    }
    let Some(casted) = cast_to(col, &DataType::Boolean, what) else {
        return -1;
    };
    let arr = casted.as_any().downcast_ref::<BooleanArray>().unwrap();
    let n = arr.len().min(cap);
    let dst = unsafe { std::slice::from_raw_parts_mut(out, n) };
    for (i, slot) in dst.iter_mut().enumerate() {
        *slot = u8::from(!arr.is_null(i) && arr.value(i));
    }
    write_validity(arr, out_valid, n);
    n as i64
}

fn utf8_column(h: &BatchHandle, name: &str, what: &str) -> Option<ArrayRef> {
    let col = column(h, name, what)?;
    cast_to(col, &DataType::Utf8, what)
}

/// Total UTF-8 byte length of string column `name` (nulls contribute 0), or -1 on
/// error. Call this to size the buffer for `ib_batch_get_str`.
#[no_mangle]
pub extern "C" fn ib_batch_utf8_size(b: *mut c_void, name: *const c_char) -> i64 {
    let what = "ib_batch_utf8_size";
    let Some(h) = as_batch(b) else { return -1 };
    let Some(name) = cstr(name, what) else { return -1 };
    let Some(casted) = utf8_column(h, name, what) else {
        return -1;
    };
    let arr = casted.as_any().downcast_ref::<StringArray>().unwrap();
    let mut total = 0i64;
    for i in 0..arr.len() {
        if !arr.is_null(i) {
            total += arr.value(i).len() as i64;
        }
    }
    total
}

/// Copy string column `name` out as a packed UTF-8 blob plus offsets.
///
/// `out_offsets` receives `rows + 1` `i64`s (so value `i` is
/// `bytes[offsets[i]..offsets[i+1]]`), `out_bytes` receives `ib_batch_utf8_size`
/// bytes, and `out_valid` (may be NULL) one byte per row. Returns the number of
/// rows written, -1 on error, or -2 when `bytes_cap` is too small.
///
/// # Safety
/// The three buffers must be sized as described above.
#[no_mangle]
pub unsafe extern "C" fn ib_batch_get_str(
    b: *mut c_void,
    name: *const c_char,
    out_offsets: *mut i64,
    out_bytes: *mut u8,
    bytes_cap: usize,
    out_valid: *mut u8,
    cap: usize,
) -> i64 {
    let what = "ib_batch_get_str";
    let Some(h) = as_batch(b) else { return -1 };
    let Some(name) = cstr(name, what) else { return -1 };
    if out_offsets.is_null() || out_bytes.is_null() {
        set_err(format!("{what}: null out pointer"));
        return -1;
    }
    let Some(casted) = utf8_column(h, name, what) else {
        return -1;
    };
    let arr = casted.as_any().downcast_ref::<StringArray>().unwrap();
    let n = arr.len().min(cap);

    let mut needed = 0usize;
    for i in 0..n {
        if !arr.is_null(i) {
            needed += arr.value(i).len();
        }
    }
    if needed > bytes_cap {
        set_err(format!(
            "{what}: byte buffer too small: need {needed}, have {bytes_cap}"
        ));
        return -2;
    }

    let offs = unsafe { std::slice::from_raw_parts_mut(out_offsets, n + 1) };
    let bytes = unsafe { std::slice::from_raw_parts_mut(out_bytes, needed) };
    let mut pos = 0usize;
    offs[0] = 0;
    for i in 0..n {
        if !arr.is_null(i) {
            let s = arr.value(i).as_bytes();
            bytes[pos..pos + s.len()].copy_from_slice(s);
            pos += s.len();
        }
        offs[i + 1] = pos as i64;
    }
    write_validity(arr, out_valid, n);
    n as i64
}

// ── Arrow C Data Interface ───────────────────────────────────────────────────

/// `sizeof(struct ArrowArray)` — 80 bytes on every 64-bit ABI. Mojo allocates
/// this many bytes for the out-param of `ib_batch_export` / `ib_scan_next_batch`.
#[no_mangle]
pub extern "C" fn ib_arrow_array_size() -> usize {
    std::mem::size_of::<FFI_ArrowArray>()
}

/// `sizeof(struct ArrowSchema)` — 72 bytes on every 64-bit ABI.
#[no_mangle]
pub extern "C" fn ib_arrow_schema_size() -> usize {
    std::mem::size_of::<FFI_ArrowSchema>()
}

/// Export `b` through the Arrow C Data Interface as a **struct array**: the batch's
/// columns become the struct's children. `out_array` and `out_schema` must point at
/// `ib_arrow_array_size()` / `ib_arrow_schema_size()` bytes of writable, 8-byte
/// aligned memory. Ownership of the exported data moves to the caller — release it
/// with `ib_arrow_release` (or by calling the structs' own `release` callbacks).
/// 0 on success, -1 on error.
///
/// # Safety
/// The out-params must be correctly sized and not already hold a live export.
#[no_mangle]
pub unsafe extern "C" fn ib_batch_export(
    b: *mut c_void,
    out_array: *mut c_void,
    out_schema: *mut c_void,
) -> i32 {
    let Some(h) = as_batch(b) else { return -1 };
    if out_array.is_null() || out_schema.is_null() {
        set_err("ib_batch_export: null out pointer");
        return -1;
    }
    export_batch(&h.batch, out_array, out_schema)
}

pub(crate) fn export_batch(
    batch: &RecordBatch,
    out_array: *mut c_void,
    out_schema: *mut c_void,
) -> i32 {
    let struct_array = StructArray::from(batch.clone());
    match to_ffi(&struct_array.into_data()) {
        Ok((array, schema)) => {
            unsafe {
                ptr::write(out_array as *mut FFI_ArrowArray, array);
                ptr::write(out_schema as *mut FFI_ArrowSchema, schema);
            }
            0
        }
        Err(e) => {
            set_err(format!("arrow export: {e}"));
            -1
        }
    }
}

/// Import an Arrow C Data Interface struct-array pair into an opaque batch handle.
/// **Consumes** the pair either way — success or failure — so the caller must
/// never release the structs itself afterwards. Returns a batch handle, or NULL
/// on error.
///
/// # Safety
/// The pointers must reference a live, correctly-formed `ArrowArray`/`ArrowSchema`
/// describing a struct array with no top-level nulls.
#[no_mangle]
pub unsafe extern "C" fn ib_batch_import(
    array: *mut c_void,
    schema: *mut c_void,
) -> *mut c_void {
    match unsafe { import_batch(array, schema) } {
        Some(b) => boxed_batch(b),
        None => ptr::null_mut(),
    }
}

pub(crate) unsafe fn import_batch(array: *mut c_void, schema: *mut c_void) -> Option<RecordBatch> {
    if array.is_null() || schema.is_null() {
        set_err("arrow import: null pointer");
        return None;
    }
    // Move the ArrowArray out of the caller's memory (taking ownership of its
    // release callback) but only *borrow* the schema, as `from_ffi` requires.
    let ffi_array = unsafe { ptr::read(array as *mut FFI_ArrowArray) };
    let ffi_schema = unsafe { &*(schema as *const FFI_ArrowSchema) };
    let data = match unsafe { from_ffi(ffi_array, ffi_schema) } {
        Ok(d) => d,
        Err(e) => {
            set_err(format!("arrow import: {e}"));
            return None;
        }
    };
    if !matches!(data.data_type(), DataType::Struct(_)) {
        set_err(format!(
            "arrow import: expected a struct array (one child per column), got {}",
            data.data_type()
        ));
        return None;
    }
    let struct_array = StructArray::from(data);
    if struct_array.null_count() > 0 {
        set_err("arrow import: the top-level struct array must not contain nulls");
        return None;
    }
    // Consume the caller's schema too: from_ffi copied what it needed.
    unsafe { release_schema(schema) };
    Some(RecordBatch::from(struct_array))
}

unsafe fn release_schema(schema: *mut c_void) {
    let s = unsafe { ptr::read(schema as *mut FFI_ArrowSchema) };
    drop(s);
    // Leave an empty (released) struct behind so a double release is a no-op.
    unsafe { ptr::write(schema as *mut FFI_ArrowSchema, FFI_ArrowSchema::empty()) };
}

/// Release an `ArrowArray`/`ArrowSchema` pair produced by `ib_batch_export` /
/// `ib_scan_next_batch`. Either pointer may be NULL. Safe to call twice.
///
/// # Safety
/// The pointers must reference structs this library exported.
#[no_mangle]
pub unsafe extern "C" fn ib_arrow_release(array: *mut c_void, schema: *mut c_void) {
    if !array.is_null() {
        let a = unsafe { ptr::read(array as *mut FFI_ArrowArray) };
        drop(a);
        unsafe { ptr::write(array as *mut FFI_ArrowArray, FFI_ArrowArray::empty()) };
    }
    if !schema.is_null() {
        unsafe { release_schema(schema) };
    }
}

/// Shared by the scan and the appender: build a `RecordBatch` handle from an
/// `ArrayRef` column set. Kept here so `write.rs` and `scan.rs` agree on the
/// error strings.
pub(crate) fn batch_from_columns(
    schema: Arc<arrow_schema::Schema>,
    columns: Vec<ArrayRef>,
) -> Result<RecordBatch, String> {
    RecordBatch::try_new(schema, columns).map_err(|e| format!("building record batch: {e}"))
}
