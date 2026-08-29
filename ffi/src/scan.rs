//! Table scans: planning (`ib_scan_plan_files_json`) and streaming record
//! batches out (`ib_scan_next` / `ib_scan_next_batch`).
//!
//! `TableScan` and its `ArrowRecordBatchStream` are `'static` — they own a clone
//! of the table metadata and a `FileIO` — so the handle can outlive the borrow of
//! the table it was built from. The stream is created lazily on the first
//! `next` so `plan_files` can be called on a fresh scan without paying for reads.

use std::ffi::c_char;
use std::os::raw::c_void;
use std::ptr;

use futures::StreamExt;
use iceberg::scan::{ArrowRecordBatchStream, TableScan};

use crate::batch::{boxed_batch, export_batch};
use crate::filter::parse_filter;
use crate::table::as_table;
use crate::{cstr_opt, out_string, rt, set_err};

pub(crate) struct ScanHandle {
    scan: TableScan,
    stream: Option<ArrowRecordBatchStream>,
}

fn as_scan<'a>(p: *mut c_void) -> Option<&'a mut ScanHandle> {
    if p.is_null() {
        set_err("scan handle is null");
        return None;
    }
    Some(unsafe { &mut *(p as *mut ScanHandle) })
}

/// Build a scan over `table`.
///
/// * `columns_csv` — NULL or empty for all columns, else a comma-separated
///   projection (`"id,name"`; surrounding whitespace is trimmed).
/// * `snapshot_id` — 0 for the current snapshot, else a specific snapshot id.
/// * `filter_json` — NULL for no filter, else the JSON filter DSL (see `filter.rs`).
///
/// Returns an opaque scan handle, or NULL on error.
#[no_mangle]
pub extern "C" fn ib_scan_new(
    table: *mut c_void,
    columns_csv: *const c_char,
    snapshot_id: i64,
    filter_json: *const c_char,
) -> *mut c_void {
    let Some(h) = as_table(table) else {
        return ptr::null_mut();
    };
    let Ok(columns_csv) = cstr_opt(columns_csv, "ib_scan_new: columns_csv") else {
        return ptr::null_mut();
    };
    let Ok(filter_json) = cstr_opt(filter_json, "ib_scan_new: filter_json") else {
        return ptr::null_mut();
    };

    let mut builder = h.table.scan();
    match columns_csv.map(str::trim).filter(|s| !s.is_empty()) {
        Some(csv) => {
            let cols: Vec<String> = csv
                .split(',')
                .map(|c| c.trim().to_string())
                .filter(|c| !c.is_empty())
                .collect();
            builder = builder.select(cols);
        }
        None => builder = builder.select_all(),
    }
    if snapshot_id != 0 {
        builder = builder.snapshot_id(snapshot_id);
    }
    if let Some(json) = filter_json.map(str::trim).filter(|s| !s.is_empty()) {
        match parse_filter(json, h.table.metadata().current_schema()) {
            Ok(p) => builder = builder.with_filter(p),
            Err(e) => {
                set_err(format!("ib_scan_new: {e}"));
                return ptr::null_mut();
            }
        }
    }

    match builder.build() {
        Ok(scan) => Box::into_raw(Box::new(ScanHandle { scan, stream: None })) as *mut c_void,
        Err(e) => {
            set_err(format!("ib_scan_new: {e}"));
            ptr::null_mut()
        }
    }
}

/// Release a scan handle.
///
/// # Safety
/// `s` must come from `ib_scan_new` and must not be reused.
#[no_mangle]
pub unsafe extern "C" fn ib_scan_free(s: *mut c_void) {
    if !s.is_null() {
        drop(unsafe { Box::from_raw(s as *mut ScanHandle) });
    }
}

/// JSON array of the file scan tasks this scan will read:
/// `{"data-file-path", "file-format", "record-count", "file-size-in-bytes",
///   "start", "length", "deletes":[{"file-path","file-type","equality-ids"}]}`.
///
/// This is the parity oracle's view of *planning* — which files and deletes
/// Iceberg selected — independent of the data actually read. Caller frees with
/// `ib_string_free`.
#[no_mangle]
pub extern "C" fn ib_scan_plan_files_json(s: *mut c_void) -> *mut c_char {
    let Some(h) = as_scan(s) else {
        return ptr::null_mut();
    };
    let res = rt().block_on(async {
        let mut stream = h.scan.plan_files().await?;
        let mut out: Vec<serde_json::Value> = Vec::new();
        while let Some(task) = stream.next().await {
            let task = task?;
            let deletes: Vec<serde_json::Value> = task
                .deletes
                .iter()
                .map(|d| {
                    serde_json::json!({
                        "file-path": d.file_path,
                        "file-size-in-bytes": d.file_size_in_bytes,
                        "file-type": format!("{:?}", d.file_type),
                        "partition-spec-id": d.partition_spec_id,
                        "equality-ids": d.equality_ids,
                    })
                })
                .collect();
            out.push(serde_json::json!({
                "data-file-path": task.data_file_path,
                "file-format": format!("{}", task.data_file_format),
                "record-count": task.record_count,
                "file-size-in-bytes": task.file_size_in_bytes,
                "start": task.start,
                "length": task.length,
                "project-field-ids": task.project_field_ids,
                "deletes": deletes,
            }));
        }
        Ok::<_, iceberg::Error>(out)
    });
    match res {
        Ok(tasks) => out_string(serde_json::to_string(&tasks).unwrap_or_else(|_| "[]".into())),
        Err(e) => {
            set_err(format!("ib_scan_plan_files_json: {e}"));
            ptr::null_mut()
        }
    }
}

fn ensure_stream(h: &mut ScanHandle) -> Result<(), String> {
    if h.stream.is_none() {
        let stream = rt()
            .block_on(h.scan.to_arrow())
            .map_err(|e| format!("starting the scan: {e}"))?;
        h.stream = Some(stream);
    }
    Ok(())
}

fn next_batch(h: &mut ScanHandle) -> Result<Option<arrow_array::RecordBatch>, String> {
    ensure_stream(h)?;
    let stream = h.stream.as_mut().unwrap();
    match rt().block_on(stream.next()) {
        None => Ok(None),
        Some(Ok(b)) => Ok(Some(b)),
        Some(Err(e)) => Err(format!("reading a batch: {e}")),
    }
}

/// Pull the next record batch as an opaque batch handle.
/// Returns 1 and stores a handle in `*out_batch`, 0 at end of stream, -1 on error.
///
/// # Safety
/// `out_batch` must point at a writable `void*`.
#[no_mangle]
pub unsafe extern "C" fn ib_scan_next(s: *mut c_void, out_batch: *mut *mut c_void) -> i32 {
    let Some(h) = as_scan(s) else { return -1 };
    if out_batch.is_null() {
        set_err("ib_scan_next: null out_batch");
        return -1;
    }
    match next_batch(h) {
        Ok(None) => 0,
        Ok(Some(b)) => {
            unsafe { *out_batch = boxed_batch(b) };
            1
        }
        Err(e) => {
            set_err(format!("ib_scan_next: {e}"));
            -1
        }
    }
}

/// Pull the next record batch straight through the Arrow C Data Interface.
/// `out_array` / `out_schema` must point at `ib_arrow_array_size()` /
/// `ib_arrow_schema_size()` bytes of writable, 8-byte aligned memory; on a
/// return of 1 they hold an exported struct array that the caller must release
/// (`ib_arrow_release`). Returns 1 for a batch, 0 at end of stream, -1 on error.
///
/// # Safety
/// The out-params must be correctly sized and not already hold a live export.
#[no_mangle]
pub unsafe extern "C" fn ib_scan_next_batch(
    s: *mut c_void,
    out_array: *mut c_void,
    out_schema: *mut c_void,
) -> i32 {
    let Some(h) = as_scan(s) else { return -1 };
    if out_array.is_null() || out_schema.is_null() {
        set_err("ib_scan_next_batch: null out pointer");
        return -1;
    }
    match next_batch(h) {
        Ok(None) => 0,
        Ok(Some(b)) => {
            if export_batch(&b, out_array, out_schema) == 0 {
                1
            } else {
                -1
            }
        }
        Err(e) => {
            set_err(format!("ib_scan_next_batch: {e}"));
            -1
        }
    }
}
