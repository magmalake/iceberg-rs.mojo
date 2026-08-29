//! Catalog handles: REST and SQL (sqlite/postgres/mysql), plus namespace and
//! table listing/creation/drop.
//!
//! A `CatalogHandle` is an `Arc<dyn Catalog>` behind a `*mut c_void`. Tables keep
//! their own clone of the `Arc`, so a table stays usable after `ib_catalog_free`
//! — but freeing the catalog first is still discouraged.

use std::collections::HashMap;
use std::ffi::c_char;
use std::os::raw::c_void;
use std::ptr;
use std::sync::Arc;

use iceberg::io::StorageFactory;
use iceberg::spec::{Schema, UnboundPartitionSpec};
use iceberg::{Catalog, CatalogBuilder, NamespaceIdent, TableCreation, TableIdent};
use iceberg_catalog_rest::{
    REST_CATALOG_PROP_URI, REST_CATALOG_PROP_WAREHOUSE, RestCatalogBuilder,
};
use iceberg_catalog_sql::{
    SQL_CATALOG_PROP_BIND_STYLE, SQL_CATALOG_PROP_URI, SQL_CATALOG_PROP_WAREHOUSE, SqlBindStyle,
    SqlCatalogBuilder,
};
use iceberg_storage_opendal::OpenDalResolvingStorageFactory;

use crate::table::TableHandle;
use crate::{cstr, cstr_opt, out_string, rt, set_err};

pub(crate) struct CatalogHandle {
    pub(crate) inner: Arc<dyn Catalog>,
}

pub(crate) fn as_catalog<'a>(p: *mut c_void) -> Option<&'a CatalogHandle> {
    if p.is_null() {
        set_err("catalog handle is null");
        return None;
    }
    Some(unsafe { &*(p as *const CatalogHandle) })
}

/// The FileIO factory every catalog is built with. iceberg core only ships
/// `file://` and `memory://`; the OpenDAL resolving factory adds s3/gcs/azure/oss
/// by URL scheme, which is why `iceberg-storage-opendal` is a hard dependency.
fn storage_factory() -> Arc<dyn StorageFactory> {
    Arc::new(OpenDalResolvingStorageFactory::new())
}

fn props_from_json(json: Option<&str>, what: &str) -> Result<HashMap<String, String>, ()> {
    let Some(json) = json else {
        return Ok(HashMap::new());
    };
    let trimmed = json.trim();
    if trimmed.is_empty() {
        return Ok(HashMap::new());
    }
    match serde_json::from_str::<HashMap<String, String>>(trimmed) {
        Ok(m) => Ok(m),
        Err(e) => {
            set_err(format!("{what}: props JSON must be an object of strings: {e}"));
            Err(())
        }
    }
}

fn boxed(inner: Arc<dyn Catalog>) -> *mut c_void {
    Box::into_raw(Box::new(CatalogHandle { inner })) as *mut c_void
}

// ── constructors ─────────────────────────────────────────────────────────────

/// Open an Iceberg **REST** catalog at `uri`.
///
/// `warehouse` may be NULL. `props_json` may be NULL or a JSON object of extra
/// properties passed straight through to the REST client — this is where OAuth2
/// settings (`credential`, `oauth2-server-uri`, `scope`, `token`) and any
/// `header.*` entries go. Returns an opaque catalog handle, or NULL on error.
#[no_mangle]
pub extern "C" fn ib_catalog_rest_new(
    uri: *const c_char,
    warehouse: *const c_char,
    props_json: *const c_char,
) -> *mut c_void {
    let Some(uri) = cstr(uri, "ib_catalog_rest_new: uri") else {
        return ptr::null_mut();
    };
    let Ok(warehouse) = cstr_opt(warehouse, "ib_catalog_rest_new: warehouse") else {
        return ptr::null_mut();
    };
    let Ok(props_json) = cstr_opt(props_json, "ib_catalog_rest_new: props_json") else {
        return ptr::null_mut();
    };
    let Ok(mut props) = props_from_json(props_json, "ib_catalog_rest_new") else {
        return ptr::null_mut();
    };
    props.insert(REST_CATALOG_PROP_URI.to_string(), uri.to_string());
    if let Some(w) = warehouse {
        if !w.is_empty() {
            props.insert(REST_CATALOG_PROP_WAREHOUSE.to_string(), w.to_string());
        }
    }

    let res = rt().block_on(async {
        RestCatalogBuilder::default()
            .with_storage_factory(storage_factory())
            .load("rest", props)
            .await
    });
    match res {
        Ok(c) => boxed(Arc::new(c)),
        Err(e) => {
            set_err(format!("ib_catalog_rest_new: {e}"));
            ptr::null_mut()
        }
    }
}

/// Open an Iceberg **SQL** catalog. `uri` is an sqlx connection string
/// (`sqlite:///abs/path/catalog.db`, `postgres://…`, `mysql://…`) and
/// `warehouse` is the root location for table data (`file:///…`, `s3://…`).
///
/// The sqlite flavour is the zero-infrastructure local option and is what the
/// test-suite uses. Returns an opaque catalog handle, or NULL on error.
#[no_mangle]
pub extern "C" fn ib_catalog_sql_new(
    uri: *const c_char,
    warehouse: *const c_char,
) -> *mut c_void {
    let Some(uri) = cstr(uri, "ib_catalog_sql_new: uri") else {
        return ptr::null_mut();
    };
    let Some(warehouse) = cstr(warehouse, "ib_catalog_sql_new: warehouse") else {
        return ptr::null_mut();
    };
    // `?` placeholders for sqlite/mysql, `$1…` for postgres.
    let bind_style = if uri.starts_with("postgres") {
        SqlBindStyle::DollarNumeric
    } else {
        SqlBindStyle::QMark
    };
    let props = HashMap::from_iter([
        (SQL_CATALOG_PROP_URI.to_string(), uri.to_string()),
        (
            SQL_CATALOG_PROP_WAREHOUSE.to_string(),
            warehouse.to_string(),
        ),
        (
            SQL_CATALOG_PROP_BIND_STYLE.to_string(),
            bind_style.to_string(),
        ),
    ]);
    let res = rt().block_on(async {
        SqlCatalogBuilder::default()
            .with_storage_factory(storage_factory())
            .load("sql", props)
            .await
    });
    match res {
        Ok(c) => boxed(Arc::new(c)),
        Err(e) => {
            set_err(format!("ib_catalog_sql_new: {e}"));
            ptr::null_mut()
        }
    }
}

/// Release a catalog handle.
///
/// # Safety
/// `cat` must come from `ib_catalog_*_new` and must not be used afterwards.
#[no_mangle]
pub unsafe extern "C" fn ib_catalog_free(cat: *mut c_void) {
    if !cat.is_null() {
        drop(unsafe { Box::from_raw(cat as *mut CatalogHandle) });
    }
}

// ── namespaces ───────────────────────────────────────────────────────────────

fn ns_ident(s: &str, what: &str) -> Option<NamespaceIdent> {
    // Multi-level namespaces are dotted, matching the REST spec's `a.b.c`.
    let parts: Vec<String> = s.split('.').map(|p| p.to_string()).collect();
    match NamespaceIdent::from_vec(parts) {
        Ok(n) => Some(n),
        Err(e) => {
            set_err(format!("{what}: bad namespace '{s}': {e}"));
            None
        }
    }
}

/// JSON array of the namespaces under `parent` (NULL for top level), each a
/// dotted string. Caller frees with `ib_string_free`; NULL on error.
#[no_mangle]
pub extern "C" fn ib_list_namespaces(cat: *mut c_void, parent: *const c_char) -> *mut c_char {
    let Some(cat) = as_catalog(cat) else {
        return ptr::null_mut();
    };
    let Ok(parent) = cstr_opt(parent, "ib_list_namespaces: parent") else {
        return ptr::null_mut();
    };
    let parent = match parent {
        Some(p) if !p.is_empty() => match ns_ident(p, "ib_list_namespaces") {
            Some(n) => Some(n),
            None => return ptr::null_mut(),
        },
        _ => None,
    };
    match rt().block_on(cat.inner.list_namespaces(parent.as_ref())) {
        Ok(ns) => {
            let names: Vec<String> = ns.into_iter().map(|n| n.inner().join(".")).collect();
            out_string(serde_json::to_string(&names).unwrap_or_else(|_| "[]".into()))
        }
        Err(e) => {
            set_err(format!("ib_list_namespaces: {e}"));
            ptr::null_mut()
        }
    }
}

/// Create namespace `ns` (dotted). `props_json` may be NULL. 0 on success, -1 on error.
#[no_mangle]
pub extern "C" fn ib_create_namespace(
    cat: *mut c_void,
    ns: *const c_char,
    props_json: *const c_char,
) -> i32 {
    let Some(cat) = as_catalog(cat) else { return -1 };
    let Some(ns) = cstr(ns, "ib_create_namespace: ns") else {
        return -1;
    };
    let Some(ident) = ns_ident(ns, "ib_create_namespace") else {
        return -1;
    };
    let Ok(props_json) = cstr_opt(props_json, "ib_create_namespace: props_json") else {
        return -1;
    };
    let Ok(props) = props_from_json(props_json, "ib_create_namespace") else {
        return -1;
    };
    match rt().block_on(cat.inner.create_namespace(&ident, props)) {
        Ok(_) => 0,
        Err(e) => {
            set_err(format!("ib_create_namespace: {e}"));
            -1
        }
    }
}

/// 1 if the namespace exists, 0 if not, -1 on error.
#[no_mangle]
pub extern "C" fn ib_namespace_exists(cat: *mut c_void, ns: *const c_char) -> i32 {
    let Some(cat) = as_catalog(cat) else { return -1 };
    let Some(ns) = cstr(ns, "ib_namespace_exists: ns") else {
        return -1;
    };
    let Some(ident) = ns_ident(ns, "ib_namespace_exists") else {
        return -1;
    };
    match rt().block_on(cat.inner.namespace_exists(&ident)) {
        Ok(true) => 1,
        Ok(false) => 0,
        Err(e) => {
            set_err(format!("ib_namespace_exists: {e}"));
            -1
        }
    }
}

/// Drop namespace `ns`. 0 on success, -1 on error.
#[no_mangle]
pub extern "C" fn ib_drop_namespace(cat: *mut c_void, ns: *const c_char) -> i32 {
    let Some(cat) = as_catalog(cat) else { return -1 };
    let Some(ns) = cstr(ns, "ib_drop_namespace: ns") else {
        return -1;
    };
    let Some(ident) = ns_ident(ns, "ib_drop_namespace") else {
        return -1;
    };
    match rt().block_on(cat.inner.drop_namespace(&ident)) {
        Ok(_) => 0,
        Err(e) => {
            set_err(format!("ib_drop_namespace: {e}"));
            -1
        }
    }
}

// ── tables ───────────────────────────────────────────────────────────────────

/// JSON array of table names in `ns`. Caller frees with `ib_string_free`.
#[no_mangle]
pub extern "C" fn ib_list_tables(cat: *mut c_void, ns: *const c_char) -> *mut c_char {
    let Some(cat) = as_catalog(cat) else {
        return ptr::null_mut();
    };
    let Some(ns) = cstr(ns, "ib_list_tables: ns") else {
        return ptr::null_mut();
    };
    let Some(ident) = ns_ident(ns, "ib_list_tables") else {
        return ptr::null_mut();
    };
    match rt().block_on(cat.inner.list_tables(&ident)) {
        Ok(ts) => {
            let names: Vec<String> = ts.into_iter().map(|t| t.name().to_string()).collect();
            out_string(serde_json::to_string(&names).unwrap_or_else(|_| "[]".into()))
        }
        Err(e) => {
            set_err(format!("ib_list_tables: {e}"));
            ptr::null_mut()
        }
    }
}

/// Load table `ns.name`. Returns an opaque table handle, or NULL on error.
#[no_mangle]
pub extern "C" fn ib_table_load(
    cat: *mut c_void,
    ns: *const c_char,
    name: *const c_char,
) -> *mut c_void {
    let Some(cat) = as_catalog(cat) else {
        return ptr::null_mut();
    };
    let Some(ns) = cstr(ns, "ib_table_load: ns") else {
        return ptr::null_mut();
    };
    let Some(name) = cstr(name, "ib_table_load: name") else {
        return ptr::null_mut();
    };
    let Some(nsi) = ns_ident(ns, "ib_table_load") else {
        return ptr::null_mut();
    };
    let ident = TableIdent::new(nsi, name.to_string());
    match rt().block_on(cat.inner.load_table(&ident)) {
        Ok(t) => TableHandle::boxed(cat.inner.clone(), t),
        Err(e) => {
            set_err(format!("ib_table_load: {e}"));
            ptr::null_mut()
        }
    }
}

/// Create table `ns.name`.
///
/// `schema_json` is Iceberg's metadata-JSON schema form, e.g.
/// `{"type":"struct","schema-id":0,"fields":[{"id":1,"name":"id","required":true,"type":"long"}]}`.
/// `partition_spec_json` may be NULL, or an unbound partition spec, e.g.
/// `{"spec-id":0,"fields":[{"source-id":1,"name":"id","transform":"identity"}]}`.
/// `props_json` may be NULL. Returns an opaque table handle, or NULL on error.
#[no_mangle]
pub extern "C" fn ib_table_create(
    cat: *mut c_void,
    ns: *const c_char,
    name: *const c_char,
    schema_json: *const c_char,
    partition_spec_json: *const c_char,
    props_json: *const c_char,
) -> *mut c_void {
    let Some(cat) = as_catalog(cat) else {
        return ptr::null_mut();
    };
    let Some(ns) = cstr(ns, "ib_table_create: ns") else {
        return ptr::null_mut();
    };
    let Some(name) = cstr(name, "ib_table_create: name") else {
        return ptr::null_mut();
    };
    let Some(schema_json) = cstr(schema_json, "ib_table_create: schema_json") else {
        return ptr::null_mut();
    };
    let Ok(spec_json) = cstr_opt(partition_spec_json, "ib_table_create: partition_spec_json")
    else {
        return ptr::null_mut();
    };
    let Ok(props_json) = cstr_opt(props_json, "ib_table_create: props_json") else {
        return ptr::null_mut();
    };
    let Ok(props) = props_from_json(props_json, "ib_table_create") else {
        return ptr::null_mut();
    };
    let Some(nsi) = ns_ident(ns, "ib_table_create") else {
        return ptr::null_mut();
    };

    let schema: Schema = match serde_json::from_str(schema_json) {
        Ok(s) => s,
        Err(e) => {
            set_err(format!("ib_table_create: schema JSON: {e}"));
            return ptr::null_mut();
        }
    };

    // TypedBuilder changes the builder's type with every setter, so the optional
    // partition spec has to be resolved *before* the chain and fed through the
    // generated `*_opt` fallback setter.
    let spec: Option<UnboundPartitionSpec> =
        match spec_json.filter(|s| !s.trim().is_empty()) {
            Some(spec_json) => match serde_json::from_str(spec_json) {
                Ok(s) => Some(s),
                Err(e) => {
                    set_err(format!("ib_table_create: partition spec JSON: {e}"));
                    return ptr::null_mut();
                }
            },
            None => None,
        };
    let creation = TableCreation::builder()
        .name(name.to_string())
        .schema(schema)
        .partition_spec_opt(spec)
        .properties(props)
        .build();

    match rt().block_on(cat.inner.create_table(&nsi, creation)) {
        Ok(t) => TableHandle::boxed(cat.inner.clone(), t),
        Err(e) => {
            set_err(format!("ib_table_create: {e}"));
            ptr::null_mut()
        }
    }
}

/// 1 if the table exists, 0 if not, -1 on error.
#[no_mangle]
pub extern "C" fn ib_table_exists(
    cat: *mut c_void,
    ns: *const c_char,
    name: *const c_char,
) -> i32 {
    let Some(cat) = as_catalog(cat) else { return -1 };
    let Some(ns) = cstr(ns, "ib_table_exists: ns") else {
        return -1;
    };
    let Some(name) = cstr(name, "ib_table_exists: name") else {
        return -1;
    };
    let Some(nsi) = ns_ident(ns, "ib_table_exists") else {
        return -1;
    };
    let ident = TableIdent::new(nsi, name.to_string());
    match rt().block_on(cat.inner.table_exists(&ident)) {
        Ok(true) => 1,
        Ok(false) => 0,
        Err(e) => {
            set_err(format!("ib_table_exists: {e}"));
            -1
        }
    }
}

/// Drop table `ns.name` from the catalog (metadata only; data files stay).
/// 0 on success, -1 on error.
#[no_mangle]
pub extern "C" fn ib_table_drop(
    cat: *mut c_void,
    ns: *const c_char,
    name: *const c_char,
) -> i32 {
    let Some(cat) = as_catalog(cat) else { return -1 };
    let Some(ns) = cstr(ns, "ib_table_drop: ns") else {
        return -1;
    };
    let Some(name) = cstr(name, "ib_table_drop: name") else {
        return -1;
    };
    let Some(nsi) = ns_ident(ns, "ib_table_drop") else {
        return -1;
    };
    let ident = TableIdent::new(nsi, name.to_string());
    match rt().block_on(cat.inner.drop_table(&ident)) {
        Ok(_) => 0,
        Err(e) => {
            set_err(format!("ib_table_drop: {e}"));
            -1
        }
    }
}
