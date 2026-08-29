//! The table handle plus everything that reads table *metadata*.
//!
//! A `TableHandle` owns the loaded `iceberg::table::Table`, an `Arc<dyn Catalog>`
//! (so it can refresh and commit on its own), and the data files staged by
//! `ib_table_append*` but not yet committed.

use std::ffi::c_char;
use std::os::raw::c_void;
use std::ptr;
use std::sync::Arc;

use iceberg::spec::DataFile;
use iceberg::table::Table;
use iceberg::Catalog;

use crate::{out_string, rt, set_err};

pub(crate) struct TableHandle {
    pub(crate) catalog: Arc<dyn Catalog>,
    pub(crate) table: Table,
    /// Data files written by `ib_table_append*` and waiting for `ib_table_commit`.
    pub(crate) pending: Vec<DataFile>,
}

impl TableHandle {
    pub(crate) fn boxed(catalog: Arc<dyn Catalog>, table: Table) -> *mut c_void {
        Box::into_raw(Box::new(TableHandle {
            catalog,
            table,
            pending: Vec::new(),
        })) as *mut c_void
    }
}

pub(crate) fn as_table<'a>(p: *mut c_void) -> Option<&'a TableHandle> {
    if p.is_null() {
        set_err("table handle is null");
        return None;
    }
    Some(unsafe { &*(p as *const TableHandle) })
}

pub(crate) fn as_table_mut<'a>(p: *mut c_void) -> Option<&'a mut TableHandle> {
    if p.is_null() {
        set_err("table handle is null");
        return None;
    }
    Some(unsafe { &mut *(p as *mut TableHandle) })
}

/// Release a table handle. Uncommitted appends are discarded.
///
/// # Safety
/// `t` must come from `ib_table_load` / `ib_table_create` and not be reused.
#[no_mangle]
pub unsafe extern "C" fn ib_table_free(t: *mut c_void) {
    if !t.is_null() {
        drop(unsafe { Box::from_raw(t as *mut TableHandle) });
    }
}

/// Re-load the table from its catalog, picking up snapshots committed elsewhere.
/// Staged (uncommitted) appends are kept. 0 on success, -1 on error.
#[no_mangle]
pub extern "C" fn ib_table_refresh(t: *mut c_void) -> i32 {
    let Some(h) = as_table_mut(t) else { return -1 };
    let ident = h.table.identifier().clone();
    match rt().block_on(h.catalog.load_table(&ident)) {
        Ok(t) => {
            h.table = t;
            0
        }
        Err(e) => {
            set_err(format!("ib_table_refresh: {e}"));
            -1
        }
    }
}

/// The table's full current `metadata.json` text. Caller frees with `ib_string_free`.
#[no_mangle]
pub extern "C" fn ib_table_metadata_json(t: *mut c_void) -> *mut c_char {
    let Some(h) = as_table(t) else {
        return ptr::null_mut();
    };
    match serde_json::to_string_pretty(h.table.metadata()) {
        Ok(s) => out_string(s),
        Err(e) => {
            set_err(format!("ib_table_metadata_json: {e}"));
            ptr::null_mut()
        }
    }
}

/// The table's current schema as Iceberg metadata JSON. Caller frees.
#[no_mangle]
pub extern "C" fn ib_table_schema_json(t: *mut c_void) -> *mut c_char {
    let Some(h) = as_table(t) else {
        return ptr::null_mut();
    };
    match serde_json::to_string(h.table.metadata().current_schema().as_ref()) {
        Ok(s) => out_string(s),
        Err(e) => {
            set_err(format!("ib_table_schema_json: {e}"));
            ptr::null_mut()
        }
    }
}

/// The table's default partition spec as JSON. Caller frees.
#[no_mangle]
pub extern "C" fn ib_table_partition_spec_json(t: *mut c_void) -> *mut c_char {
    let Some(h) = as_table(t) else {
        return ptr::null_mut();
    };
    match serde_json::to_string(h.table.metadata().default_partition_spec().as_ref()) {
        Ok(s) => out_string(s),
        Err(e) => {
            set_err(format!("ib_table_partition_spec_json: {e}"));
            ptr::null_mut()
        }
    }
}

/// The table's base location. Caller frees.
#[no_mangle]
pub extern "C" fn ib_table_location(t: *mut c_void) -> *mut c_char {
    let Some(h) = as_table(t) else {
        return ptr::null_mut();
    };
    out_string(h.table.metadata().location().to_string())
}

/// Current snapshot id, `0` when the table has no snapshot yet, `-1` on error.
#[no_mangle]
pub extern "C" fn ib_table_current_snapshot_id(t: *mut c_void) -> i64 {
    let Some(h) = as_table(t) else { return -1 };
    h.table.metadata().current_snapshot_id().unwrap_or(0)
}

/// JSON array of the table's snapshots, oldest first, each
/// `{"snapshot-id", "parent-snapshot-id", "sequence-number", "timestamp-ms",
///   "manifest-list", "schema-id", "operation", "summary"}`.
/// Built by hand rather than via serde so the shape is stable for the binding.
/// Caller frees with `ib_string_free`.
#[no_mangle]
pub extern "C" fn ib_table_snapshots_json(t: *mut c_void) -> *mut c_char {
    let Some(h) = as_table(t) else {
        return ptr::null_mut();
    };
    let mut snaps: Vec<&Arc<iceberg::spec::Snapshot>> = h.table.metadata().snapshots().collect();
    snaps.sort_by_key(|s| s.sequence_number());
    let arr: Vec<serde_json::Value> = snaps
        .into_iter()
        .map(|s| {
            let summary = s.summary();
            serde_json::json!({
                "snapshot-id": s.snapshot_id(),
                "parent-snapshot-id": s.parent_snapshot_id(),
                "sequence-number": s.sequence_number(),
                "timestamp-ms": s.timestamp_ms(),
                "manifest-list": s.manifest_list(),
                "schema-id": s.schema_id(),
                "operation": summary.operation.as_str(),
                "summary": summary.additional_properties,
            })
        })
        .collect();
    out_string(serde_json::to_string(&arr).unwrap_or_else(|_| "[]".into()))
}
