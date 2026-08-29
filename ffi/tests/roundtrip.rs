//! End-to-end smoke test of the C ABI, driven from Rust: it calls exactly the
//! `extern "C"` entry points the Mojo binding dlopens, so a failure here is a
//! shim bug rather than a Mojo binding bug.

use std::ffi::{CStr, CString, c_char};
use std::os::raw::c_void;

use icebergrsmojo::*;

fn c(s: &str) -> CString {
    CString::new(s).unwrap()
}

fn take_string(p: *mut c_char) -> String {
    assert!(!p.is_null(), "null string: {}", last_error());
    let s = unsafe { CStr::from_ptr(p) }.to_string_lossy().into_owned();
    unsafe { ib_string_free(p) };
    s
}

fn last_error() -> String {
    unsafe { CStr::from_ptr(ib_last_error()) }
        .to_string_lossy()
        .into_owned()
}

const SCHEMA: &str = r#"{
  "type": "struct",
  "schema-id": 0,
  "fields": [
    {"id": 1, "name": "id",     "required": true,  "type": "long"},
    {"id": 2, "name": "region", "required": true,  "type": "string"},
    {"id": 3, "name": "count",  "required": false, "type": "int"},
    {"id": 4, "name": "amount", "required": false, "type": "double"},
    {"id": 5, "name": "ts",     "required": false, "type": "timestamp"},
    {"id": 6, "name": "ok",     "required": false, "type": "boolean"}
  ]
}"#;

const SPEC: &str = r#"{"spec-id":0,"fields":[{"source-id":2,"name":"region","transform":"identity"}]}"#;

struct Cols {
    ids: Vec<i64>,
    regions: Vec<&'static str>,
    counts: Vec<i64>,
    amounts: Vec<f64>,
    ts: Vec<i64>,
    ok: Vec<u8>,
}

fn append(table: *mut c_void, cols: &Cols) {
    let n = cols.ids.len();
    let b = ib_batch_builder_new(table);
    assert!(!b.is_null(), "{}", last_error());
    unsafe {
        assert_eq!(
            ib_batch_builder_int(b, c("id").as_ptr(), cols.ids.as_ptr(), n, std::ptr::null()),
            0,
            "{}",
            last_error()
        );
        let mut blob = Vec::new();
        let mut offs = vec![0i64];
        for r in &cols.regions {
            blob.extend_from_slice(r.as_bytes());
            offs.push(blob.len() as i64);
        }
        assert_eq!(
            ib_batch_builder_str(
                b,
                c("region").as_ptr(),
                offs.as_ptr(),
                n,
                blob.as_ptr(),
                std::ptr::null()
            ),
            0,
            "{}",
            last_error()
        );
        assert_eq!(
            ib_batch_builder_int(
                b,
                c("count").as_ptr(),
                cols.counts.as_ptr(),
                n,
                std::ptr::null()
            ),
            0,
            "{}",
            last_error()
        );
        assert_eq!(
            ib_batch_builder_float(
                b,
                c("amount").as_ptr(),
                cols.amounts.as_ptr(),
                n,
                std::ptr::null()
            ),
            0,
            "{}",
            last_error()
        );
        assert_eq!(
            ib_batch_builder_int(b, c("ts").as_ptr(), cols.ts.as_ptr(), n, std::ptr::null()),
            0,
            "{}",
            last_error()
        );
        assert_eq!(
            ib_batch_builder_bool(b, c("ok").as_ptr(), cols.ok.as_ptr(), n, std::ptr::null()),
            0,
            "{}",
            last_error()
        );
    }
    let batch = ib_batch_builder_build(b);
    assert!(!batch.is_null(), "{}", last_error());
    assert_eq!(ib_table_append(table, batch), 0, "{}", last_error());
    unsafe {
        ib_batch_free(batch);
        ib_batch_builder_free(b);
    }
}

fn scan_ids(table: *mut c_void, columns: Option<&str>, filter: Option<&str>) -> Vec<i64> {
    let cols = columns.map(c);
    let filt = filter.map(c);
    let scan = ib_scan_new(
        table,
        cols.as_ref().map_or(std::ptr::null(), |s| s.as_ptr()),
        0,
        filt.as_ref().map_or(std::ptr::null(), |s| s.as_ptr()),
    );
    assert!(!scan.is_null(), "{}", last_error());
    let mut ids = Vec::new();
    loop {
        let mut batch: *mut c_void = std::ptr::null_mut();
        let rc = unsafe { ib_scan_next(scan, &mut batch) };
        assert!(rc >= 0, "{}", last_error());
        if rc == 0 {
            break;
        }
        let rows = ib_batch_num_rows(batch) as usize;
        let mut buf = vec![0i64; rows];
        let got = unsafe {
            ib_batch_get_i64(
                batch,
                c("id").as_ptr(),
                buf.as_mut_ptr(),
                std::ptr::null_mut(),
                rows,
            )
        };
        assert_eq!(got as usize, rows, "{}", last_error());
        ids.extend(buf);
        unsafe { ib_batch_free(batch) };
    }
    unsafe { ib_scan_free(scan) };
    ids.sort();
    ids
}

#[test]
fn sql_catalog_roundtrip() {
    let dir = std::env::temp_dir().join(format!("ib-rs-mojo-test-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(dir.join("warehouse")).unwrap();

    let db = format!("sqlite:{}?mode=rwc", dir.join("catalog.db").display());
    let warehouse = format!("file://{}", dir.join("warehouse").display());

    assert_eq!(
        unsafe { CStr::from_ptr(ib_version()) }.to_str().unwrap(),
        "0.10.1"
    );

    let cat = ib_catalog_sql_new(c(&db).as_ptr(), c(&warehouse).as_ptr());
    assert!(!cat.is_null(), "{}", last_error());

    assert_eq!(
        ib_create_namespace(cat, c("sales").as_ptr(), std::ptr::null()),
        0,
        "{}",
        last_error()
    );
    assert_eq!(ib_namespace_exists(cat, c("sales").as_ptr()), 1);
    let ns = take_string(ib_list_namespaces(cat, std::ptr::null()));
    assert_eq!(ns, r#"["sales"]"#);

    let table = ib_table_create(
        cat,
        c("sales").as_ptr(),
        c("orders").as_ptr(),
        c(SCHEMA).as_ptr(),
        c(SPEC).as_ptr(),
        std::ptr::null(),
    );
    assert!(!table.is_null(), "{}", last_error());
    let tables = take_string(ib_list_tables(cat, c("sales").as_ptr()));
    assert_eq!(tables, r#"["orders"]"#);

    // Two commits -> two snapshots.
    append(
        table,
        &Cols {
            ids: vec![1, 2, 3],
            regions: vec!["eu", "us", "eu"],
            counts: vec![10, 20, 30],
            amounts: vec![1.5, 2.5, 3.5],
            ts: vec![1_700_000_000_000_000, 1_700_000_001_000_000, 1_700_000_002_000_000],
            ok: vec![1, 0, 1],
        },
    );
    assert_eq!(ib_table_pending_files(table), 2, "identity partition splits eu/us");
    let s1 = ib_table_commit(table);
    assert!(s1 > 0, "{}", last_error());

    append(
        table,
        &Cols {
            ids: vec![4, 5],
            regions: vec!["us", "apac"],
            counts: vec![40, 50],
            amounts: vec![4.5, 5.5],
            ts: vec![1_700_000_003_000_000, 1_700_000_004_000_000],
            ok: vec![0, 1],
        },
    );
    let s2 = ib_table_commit(table);
    assert!(s2 > 0 && s2 != s1, "{}", last_error());
    assert_eq!(ib_table_current_snapshot_id(table), s2);

    let snaps: serde_json::Value =
        serde_json::from_str(&take_string(ib_table_snapshots_json(table))).unwrap();
    assert_eq!(snaps.as_array().unwrap().len(), 2);

    let meta: serde_json::Value =
        serde_json::from_str(&take_string(ib_table_metadata_json(table))).unwrap();
    assert_eq!(meta["format-version"], 2);

    assert_eq!(scan_ids(table, None, None), vec![1, 2, 3, 4, 5]);
    assert_eq!(scan_ids(table, Some("id,region"), None), vec![1, 2, 3, 4, 5]);
    assert_eq!(
        scan_ids(table, None, Some(r#"["=","region","eu"]"#)),
        vec![1, 3]
    );
    assert_eq!(
        scan_ids(table, None, Some(r#"["and",[">","id",2],["!=","region","apac"]]"#)),
        vec![3, 4]
    );
    assert_eq!(
        scan_ids(table, None, Some(r#"["in","region",["us","apac"]]"#)),
        vec![2, 4, 5]
    );

    // Planning JSON must list the data files (3 partitions across 2 snapshots).
    let scan = ib_scan_new(table, std::ptr::null(), 0, std::ptr::null());
    let plan: serde_json::Value =
        serde_json::from_str(&take_string(ib_scan_plan_files_json(scan))).unwrap();
    assert_eq!(plan.as_array().unwrap().len(), 4);
    assert_eq!(plan[0]["file-format"], "parquet");
    unsafe { ib_scan_free(scan) };

    // Arrow C Data Interface round-trip: export a scanned batch and re-import it.
    let scan = ib_scan_new(table, std::ptr::null(), 0, std::ptr::null());
    let mut arr = vec![0u8; ib_arrow_array_size()];
    let mut sch = vec![0u8; ib_arrow_schema_size()];
    let rc = unsafe {
        ib_scan_next_batch(
            scan,
            arr.as_mut_ptr() as *mut c_void,
            sch.as_mut_ptr() as *mut c_void,
        )
    };
    assert_eq!(rc, 1, "{}", last_error());
    // Release the exported pair the way the Mojo binding's ArrowCData destructor
    // does — this is the path that segfaulted when the buffers were freed early.
    unsafe {
        ib_arrow_release(
            arr.as_mut_ptr() as *mut c_void,
            sch.as_mut_ptr() as *mut c_void,
        )
    };
    unsafe { ib_scan_free(scan) };

    // Then export again from a fresh scan and import it back, to prove the
    // round-trip through the C Data Interface.
    let scan = ib_scan_new(table, std::ptr::null(), 0, std::ptr::null());
    assert_eq!(
        unsafe {
            ib_scan_next_batch(
                scan,
                arr.as_mut_ptr() as *mut c_void,
                sch.as_mut_ptr() as *mut c_void,
            )
        },
        1,
        "{}",
        last_error()
    );
    let imported = unsafe {
        ib_batch_import(arr.as_mut_ptr() as *mut c_void, sch.as_mut_ptr() as *mut c_void)
    };
    assert!(!imported.is_null(), "{}", last_error());
    assert!(ib_batch_num_rows(imported) > 0);
    assert_eq!(ib_batch_num_columns(imported), 6);
    unsafe {
        ib_batch_free(imported);
        ib_scan_free(scan);
    }

    // String column materialisation.
    let filter = c(r#"["=","id",5]"#);
    let scan = ib_scan_new(table, std::ptr::null(), 0, filter.as_ptr());
    let mut batch: *mut c_void = std::ptr::null_mut();
    assert_eq!(unsafe { ib_scan_next(scan, &mut batch) }, 1, "{}", last_error());
    let rows = ib_batch_num_rows(batch) as usize;
    assert_eq!(rows, 1);
    let need = ib_batch_utf8_size(batch, c("region").as_ptr());
    assert_eq!(need, 4);
    let mut offs = vec![0i64; rows + 1];
    let mut bytes = vec![0u8; need as usize];
    let got = unsafe {
        ib_batch_get_str(
            batch,
            c("region").as_ptr(),
            offs.as_mut_ptr(),
            bytes.as_mut_ptr(),
            bytes.len(),
            std::ptr::null_mut(),
            rows,
        )
    };
    assert_eq!(got, 1, "{}", last_error());
    assert_eq!(std::str::from_utf8(&bytes).unwrap(), "apac");
    unsafe {
        ib_batch_free(batch);
        ib_scan_free(scan);
        ib_table_free(table);
        ib_catalog_free(cat);
    }
    let _ = std::fs::remove_dir_all(&dir);
}
