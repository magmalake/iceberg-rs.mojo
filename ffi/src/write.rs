//! The write path: a column-wise batch builder, staged appends, and commit.
//!
//! iceberg-rust 0.10 can only **append** (fast-append snapshots); there is no
//! overwrite/merge-on-read writer. `ib_table_append*` writes Parquet data files
//! immediately — through the table's own `FileIO`, location generator and
//! writer properties — and stages the resulting `DataFile`s on the table handle.
//! `ib_table_commit` turns the whole staged set into exactly one new snapshot.
//!
//! Partitioned tables go through `RecordBatchPartitionSplitter` +
//! `FanoutWriter`, so an unsorted batch spanning several partitions is split into
//! one data file per partition value.

use std::collections::HashMap;
use std::ffi::c_char;
use std::os::raw::c_void;
use std::ptr;
use std::sync::Arc;

use arrow_array::{
    ArrayRef, BooleanArray, Float64Array, Int64Array, RecordBatch, StringArray,
};
use arrow_schema::Schema as ArrowSchema;
use iceberg::arrow::{RecordBatchPartitionSplitter, schema_to_arrow_schema};
use iceberg::spec::{DataFile, DataFileFormat};
use iceberg::table::Table;
use iceberg::transaction::{ApplyTransactionAction, Transaction};
use iceberg::writer::base_writer::data_file_writer::DataFileWriterBuilder;
use iceberg::writer::file_writer::ParquetWriterBuilder;
use iceberg::writer::file_writer::location_generator::{
    DefaultFileNameGenerator, DefaultLocationGenerator,
};
use iceberg::writer::file_writer::rolling_writer::RollingFileWriterBuilder;
use iceberg::writer::partitioning::PartitioningWriter;
use iceberg::writer::partitioning::fanout_writer::FanoutWriter;
use iceberg::writer::{IcebergWriter, IcebergWriterBuilder};

use crate::batch::{as_batch, import_batch};
use crate::table::{as_table_mut, TableHandle};
use crate::{cstr, rt, set_err};

// ── batch builder ────────────────────────────────────────────────────────────

/// Accumulates columns by name against a table's Arrow schema. Each `ib_batch_builder_*`
/// call takes the caller's raw buffer, casts it to the column's declared type, and
/// stores the result; `build` assembles them in schema order.
pub(crate) struct BuilderHandle {
    schema: Arc<ArrowSchema>,
    columns: HashMap<String, ArrayRef>,
    rows: Option<usize>,
}

fn as_builder<'a>(p: *mut c_void) -> Option<&'a mut BuilderHandle> {
    if p.is_null() {
        set_err("batch builder handle is null");
        return None;
    }
    Some(unsafe { &mut *(p as *mut BuilderHandle) })
}

/// Start a batch builder bound to `table`'s current schema.
/// Returns an opaque builder handle, or NULL on error.
#[no_mangle]
pub extern "C" fn ib_batch_builder_new(table: *mut c_void) -> *mut c_void {
    let Some(h) = as_table_mut(table) else {
        return ptr::null_mut();
    };
    match table_arrow_schema(&h.table) {
        Ok(schema) => Box::into_raw(Box::new(BuilderHandle {
            schema,
            columns: HashMap::new(),
            rows: None,
        })) as *mut c_void,
        Err(e) => {
            set_err(format!("ib_batch_builder_new: {e}"));
            ptr::null_mut()
        }
    }
}

/// Release a batch builder handle.
///
/// # Safety
/// `b` must come from `ib_batch_builder_new` and must not be reused.
#[no_mangle]
pub unsafe extern "C" fn ib_batch_builder_free(b: *mut c_void) {
    if !b.is_null() {
        drop(unsafe { Box::from_raw(b as *mut BuilderHandle) });
    }
}

/// The Arrow schema the table writes with — `schema_to_arrow_schema` carries the
/// `PARQUET:field_id` metadata the Iceberg Parquet writer needs on every field.
fn table_arrow_schema(table: &Table) -> Result<Arc<ArrowSchema>, String> {
    schema_to_arrow_schema(table.metadata().current_schema())
        .map(Arc::new)
        .map_err(|e| format!("converting the table schema to Arrow: {e}"))
}

fn validity_slice<'a>(valid: *const u8, n: usize) -> Option<&'a [u8]> {
    if valid.is_null() {
        None
    } else {
        Some(unsafe { std::slice::from_raw_parts(valid, n) })
    }
}

fn add_column(
    b: &mut BuilderHandle,
    name: &str,
    array: ArrayRef,
    what: &str,
) -> i32 {
    let Some(field) = b.schema.fields().iter().find(|f| f.name() == name).cloned() else {
        set_err(format!("{what}: no column named '{name}' in the table schema"));
        return -1;
    };
    let n = array.len();
    if let Some(rows) = b.rows {
        if rows != n {
            set_err(format!(
                "{what}: column '{name}' has {n} rows but the batch already has {rows}"
            ));
            return -1;
        }
    }
    let casted = if array.data_type() == field.data_type() {
        array
    } else {
        match arrow::compute::kernels::cast::cast(&array, field.data_type()) {
            Ok(a) => a,
            Err(e) => {
                set_err(format!(
                    "{what}: column '{name}' is {} in the table; cannot convert the supplied {}: {e}",
                    field.data_type(),
                    array.data_type()
                ));
                return -1;
            }
        }
    };
    b.rows = Some(n);
    b.columns.insert(name.to_string(), casted);
    0
}

/// Supply an integer column. Accepts any column the table declares as an integer,
/// date, time or timestamp type — the values are cast to the declared type, so a
/// `timestamp` column takes microseconds since the epoch and a `date` column takes
/// days. `valid` may be NULL (all values present), else one byte per row.
/// 0 on success, -1 on error.
///
/// # Safety
/// `values` must point at `n` `i64`s and `valid`, when non-NULL, at `n` bytes.
#[no_mangle]
pub unsafe extern "C" fn ib_batch_builder_int(
    b: *mut c_void,
    name: *const c_char,
    values: *const i64,
    n: usize,
    valid: *const u8,
) -> i32 {
    let what = "ib_batch_builder_int";
    let Some(b) = as_builder(b) else { return -1 };
    let Some(name) = cstr(name, what) else { return -1 };
    if values.is_null() && n > 0 {
        set_err(format!("{what}: null values pointer"));
        return -1;
    }
    let vals = unsafe { std::slice::from_raw_parts(values, n) };
    let valid = validity_slice(valid, n);
    let arr: Int64Array = (0..n)
        .map(|i| match valid {
            Some(v) if v[i] == 0 => None,
            _ => Some(vals[i]),
        })
        .collect();
    add_column(b, name, Arc::new(arr), what)
}

/// Supply a floating-point column (float or double in the table schema).
/// 0 on success, -1 on error.
///
/// # Safety
/// `values` must point at `n` `f64`s and `valid`, when non-NULL, at `n` bytes.
#[no_mangle]
pub unsafe extern "C" fn ib_batch_builder_float(
    b: *mut c_void,
    name: *const c_char,
    values: *const f64,
    n: usize,
    valid: *const u8,
) -> i32 {
    let what = "ib_batch_builder_float";
    let Some(b) = as_builder(b) else { return -1 };
    let Some(name) = cstr(name, what) else { return -1 };
    if values.is_null() && n > 0 {
        set_err(format!("{what}: null values pointer"));
        return -1;
    }
    let vals = unsafe { std::slice::from_raw_parts(values, n) };
    let valid = validity_slice(valid, n);
    let arr: Float64Array = (0..n)
        .map(|i| match valid {
            Some(v) if v[i] == 0 => None,
            _ => Some(vals[i]),
        })
        .collect();
    add_column(b, name, Arc::new(arr), what)
}

/// Supply a boolean column, one byte per row (0/1). 0 on success, -1 on error.
///
/// # Safety
/// `values` must point at `n` bytes and `valid`, when non-NULL, at `n` bytes.
#[no_mangle]
pub unsafe extern "C" fn ib_batch_builder_bool(
    b: *mut c_void,
    name: *const c_char,
    values: *const u8,
    n: usize,
    valid: *const u8,
) -> i32 {
    let what = "ib_batch_builder_bool";
    let Some(b) = as_builder(b) else { return -1 };
    let Some(name) = cstr(name, what) else { return -1 };
    if values.is_null() && n > 0 {
        set_err(format!("{what}: null values pointer"));
        return -1;
    }
    let vals = unsafe { std::slice::from_raw_parts(values, n) };
    let valid = validity_slice(valid, n);
    let arr: BooleanArray = (0..n)
        .map(|i| match valid {
            Some(v) if v[i] == 0 => None,
            _ => Some(vals[i] != 0),
        })
        .collect();
    add_column(b, name, Arc::new(arr), what)
}

/// Supply a string column as a packed UTF-8 blob plus `n + 1` offsets — the same
/// shape `ib_batch_get_str` hands back, so a read/write round-trip needs no
/// re-encoding. 0 on success, -1 on error.
///
/// # Safety
/// `offsets` must point at `n + 1` `i64`s, `bytes` at `offsets[n]` bytes, and
/// `valid`, when non-NULL, at `n` bytes.
#[no_mangle]
pub unsafe extern "C" fn ib_batch_builder_str(
    b: *mut c_void,
    name: *const c_char,
    offsets: *const i64,
    n: usize,
    bytes: *const u8,
    valid: *const u8,
) -> i32 {
    let what = "ib_batch_builder_str";
    let Some(b) = as_builder(b) else { return -1 };
    let Some(name) = cstr(name, what) else { return -1 };
    if offsets.is_null() && n > 0 {
        set_err(format!("{what}: null offsets pointer"));
        return -1;
    }
    let offs = unsafe { std::slice::from_raw_parts(offsets, n + 1) };
    let total = offs[n].max(0) as usize;
    if bytes.is_null() && total > 0 {
        set_err(format!("{what}: null bytes pointer"));
        return -1;
    }
    let blob = if total == 0 {
        &[][..]
    } else {
        unsafe { std::slice::from_raw_parts(bytes, total) }
    };
    let valid = validity_slice(valid, n);

    let mut values: Vec<Option<&str>> = Vec::with_capacity(n);
    for i in 0..n {
        if matches!(valid, Some(v) if v[i] == 0) {
            values.push(None);
            continue;
        }
        let (s, e) = (offs[i].max(0) as usize, offs[i + 1].max(0) as usize);
        if e < s || e > blob.len() {
            set_err(format!("{what}: offsets[{i}]..[{}] out of range", i + 1));
            return -1;
        }
        match std::str::from_utf8(&blob[s..e]) {
            Ok(v) => values.push(Some(v)),
            Err(_) => {
                set_err(format!("{what}: row {i} is not valid UTF-8"));
                return -1;
            }
        }
    }
    let arr = StringArray::from(values);
    add_column(b, name, Arc::new(arr), what)
}

/// Assemble the staged columns into an opaque batch handle. Columns not supplied
/// must be nullable in the table schema; they are filled with nulls. The builder
/// stays usable (and keeps its columns) — free it with `ib_batch_builder_free`.
/// Returns a batch handle, or NULL on error.
#[no_mangle]
pub extern "C" fn ib_batch_builder_build(b: *mut c_void) -> *mut c_void {
    let what = "ib_batch_builder_build";
    let Some(b) = as_builder(b) else {
        return ptr::null_mut();
    };
    let Some(rows) = b.rows else {
        set_err(format!("{what}: no columns were supplied"));
        return ptr::null_mut();
    };
    let mut columns: Vec<ArrayRef> = Vec::with_capacity(b.schema.fields().len());
    for field in b.schema.fields() {
        match b.columns.get(field.name()) {
            Some(a) => columns.push(a.clone()),
            None => {
                if !field.is_nullable() {
                    set_err(format!(
                        "{what}: column '{}' is required but was not supplied",
                        field.name()
                    ));
                    return ptr::null_mut();
                }
                columns.push(arrow_array::new_null_array(field.data_type(), rows));
            }
        }
    }
    match crate::batch::batch_from_columns(b.schema.clone(), columns) {
        Ok(batch) => crate::batch::boxed_batch(batch),
        Err(e) => {
            set_err(format!("{what}: {e}"));
            ptr::null_mut()
        }
    }
}

// ── append + commit ──────────────────────────────────────────────────────────

/// Coerce an incoming batch onto the table's Arrow schema (names and order must
/// match; types are cast). This is what stamps the `PARQUET:field_id` metadata on
/// every field, which the Iceberg Parquet writer requires.
fn conform(batch: &RecordBatch, schema: &Arc<ArrowSchema>) -> Result<RecordBatch, String> {
    let mut columns: Vec<ArrayRef> = Vec::with_capacity(schema.fields().len());
    for field in schema.fields() {
        let col = batch.column_by_name(field.name()).ok_or_else(|| {
            format!(
                "the batch has no column '{}' (table columns: {})",
                field.name(),
                schema
                    .fields()
                    .iter()
                    .map(|f| f.name().as_str())
                    .collect::<Vec<_>>()
                    .join(", ")
            )
        })?;
        if col.data_type() == field.data_type() {
            columns.push(col.clone());
        } else {
            let casted = arrow::compute::kernels::cast::cast(col, field.data_type())
                .map_err(|e| format!("column '{}': {e}", field.name()))?;
            columns.push(casted);
        }
    }
    crate::batch::batch_from_columns(schema.clone(), columns)
}

fn write_data_files(table: &Table, batches: Vec<RecordBatch>) -> Result<Vec<DataFile>, String> {
    let schema = table.metadata().current_schema().clone();
    let table_props = table
        .metadata()
        .table_properties()
        .map_err(|e| format!("reading table properties: {e}"))?;
    let location_generator = DefaultLocationGenerator::new(table.metadata())
        .map_err(|e| format!("location generator: {e}"))?;
    let file_name_generator = DefaultFileNameGenerator::new(
        "iceberg-rs-mojo".to_string(),
        Some(uuid::Uuid::new_v4().to_string()),
        DataFileFormat::Parquet,
    );
    // Honour the table's own write.parquet.* properties (compression, page size,
    // row-group size) rather than hardcoding defaults.
    let parquet_writer_builder = ParquetWriterBuilder::from_table_properties(&table_props, schema.clone());
    let rolling = RollingFileWriterBuilder::new_with_default_file_size(
        parquet_writer_builder,
        table.file_io().clone(),
        location_generator,
        file_name_generator,
    );
    let data_file_writer_builder = DataFileWriterBuilder::new(rolling);

    let spec = table.metadata().default_partition_spec().clone();
    rt().block_on(async move {
        if spec.is_unpartitioned() {
            let mut writer = data_file_writer_builder
                .build(None)
                .await
                .map_err(|e| format!("opening the data file writer: {e}"))?;
            for b in batches {
                writer
                    .write(b)
                    .await
                    .map_err(|e| format!("writing a batch: {e}"))?;
            }
            writer
                .close()
                .await
                .map_err(|e| format!("closing the data file writer: {e}"))
        } else {
            let splitter =
                RecordBatchPartitionSplitter::try_new_with_computed_values(schema.clone(), spec)
                    .map_err(|e| format!("partition splitter: {e}"))?;
            let mut fanout = FanoutWriter::new(data_file_writer_builder);
            for b in batches {
                for (key, part) in splitter
                    .split(&b)
                    .map_err(|e| format!("splitting a batch by partition: {e}"))?
                {
                    fanout
                        .write(key, part)
                        .await
                        .map_err(|e| format!("writing a partition: {e}"))?;
                }
            }
            fanout
                .close()
                .await
                .map_err(|e| format!("closing the partition writer: {e}"))
        }
    })
}

fn stage(h: &mut TableHandle, batch: RecordBatch, what: &str) -> i32 {
    let schema = match table_arrow_schema(&h.table) {
        Ok(s) => s,
        Err(e) => {
            set_err(format!("{what}: {e}"));
            return -1;
        }
    };
    let conformed = match conform(&batch, &schema) {
        Ok(b) => b,
        Err(e) => {
            set_err(format!("{what}: {e}"));
            return -1;
        }
    };
    match write_data_files(&h.table, vec![conformed]) {
        Ok(files) => {
            h.pending.extend(files);
            0
        }
        Err(e) => {
            set_err(format!("{what}: {e}"));
            -1
        }
    }
}

/// Stage a batch for append: writes the Parquet data file(s) now and remembers
/// them until `ib_table_commit`. 0 on success, -1 on error.
#[no_mangle]
pub extern "C" fn ib_table_append(table: *mut c_void, batch: *mut c_void) -> i32 {
    let what = "ib_table_append";
    let Some(h) = as_table_mut(table) else {
        return -1;
    };
    let Some(b) = as_batch(batch) else { return -1 };
    let batch = b.batch.clone();
    stage(h, batch, what)
}

/// Same as `ib_table_append` but takes an Arrow C Data Interface struct-array
/// pair, which it **consumes**. 0 on success, -1 on error.
///
/// # Safety
/// The pointers must reference a live, correctly-formed `ArrowArray`/`ArrowSchema`.
#[no_mangle]
pub unsafe extern "C" fn ib_table_append_batch(
    table: *mut c_void,
    array: *mut c_void,
    schema: *mut c_void,
) -> i32 {
    let what = "ib_table_append_batch";
    let Some(h) = as_table_mut(table) else {
        return -1;
    };
    let Some(batch) = (unsafe { import_batch(array, schema) }) else {
        return -1;
    };
    stage(h, batch, what)
}

/// Number of data files staged but not yet committed, or -1 on error.
#[no_mangle]
pub extern "C" fn ib_table_pending_files(table: *mut c_void) -> i64 {
    match as_table_mut(table) {
        Some(h) => h.pending.len() as i64,
        None => -1,
    }
}

/// Commit every staged append as one new snapshot and refresh the handle.
/// Returns the new current snapshot id; with nothing staged this is a no-op that
/// returns the existing snapshot id (0 when the table has none). -1 on error.
#[no_mangle]
pub extern "C" fn ib_table_commit(table: *mut c_void) -> i64 {
    let Some(h) = as_table_mut(table) else {
        return -1;
    };
    if h.pending.is_empty() {
        return h.table.metadata().current_snapshot_id().unwrap_or(0);
    }
    let files = std::mem::take(&mut h.pending);
    let tx = Transaction::new(&h.table);
    let action = tx.fast_append().add_data_files(files.clone());
    let tx = match action.apply(tx) {
        Ok(t) => t,
        Err(e) => {
            h.pending = files; // keep the staged files so the caller can retry
            set_err(format!("ib_table_commit: {e}"));
            return -1;
        }
    };
    let catalog = h.catalog.clone();
    match rt().block_on(tx.commit(catalog.as_ref())) {
        Ok(t) => {
            h.table = t;
            h.table.metadata().current_snapshot_id().unwrap_or(0)
        }
        Err(e) => {
            h.pending = files;
            set_err(format!("ib_table_commit: {e}"));
            -1
        }
    }
}

/// Drop every staged (uncommitted) append. The Parquet files already written are
/// left behind as orphans. 0 on success, -1 on error.
#[no_mangle]
pub extern "C" fn ib_table_rollback(table: *mut c_void) -> i32 {
    match as_table_mut(table) {
        Some(h) => {
            h.pending.clear();
            0
        }
        None => -1,
    }
}
