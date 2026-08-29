"""`iceberg_rs` — Apache Iceberg for Mojo, over a Rust cdylib wrapping iceberg-rust.

Mirrors lancedb.mojo's FFI pattern: one relocatable library
(`ffi/src/*.rs` -> `$CONDA_PREFIX/lib/libicebergrsmojo.{dylib,so}`, built by the
`iceberg-shim` pixi package) loaded through an `OwnedDLHandle`. The handle is
passed as a BORROWED `read` param to every worker so Mojo's ASAP destruction
can't `dlclose` the library mid-call (the flare gotcha) — even though each struct
also owns a handle as a field, the borrow keeps it pinned across the C call.

Nothing but integers, raw buffers and UTF-8 bytes crosses FFI. Iceberg handles
(catalog, table, scan, batch) are opaque `Int` pointers; results that don't fit
in a register come back as heap C strings which `_take_string` copies out through
`ib_string_len` / `ib_string_copy` and frees — so no Mojo code ever dereferences
a foreign pointer.

    from iceberg_rs import Catalog

    var cat = Catalog.sql("sqlite:/tmp/cat.db?mode=rwc", "file:///tmp/warehouse")
    cat.create_namespace("sales")
    var t = cat.create_table("sales", "orders", SCHEMA_JSON, SPEC_JSON)

    var b = t.builder()
    b.int_col("id", [Int64(1), Int64(2)])
    b.str_col("region", ["eu", "us"])
    t.append(b.build())
    var snapshot = t.commit()

    var scan = t.scan(columns="id,region", filter='["=","region","eu"]')
    for batch in scan.batches():
        print(batch.int_col("id"))
"""

from std.os import getenv
from std.sys.info import CompilationTarget
from std.ffi import OwnedDLHandle, c_int, c_char


# ── library loading ───────────────────────────────────────────────────────────


def _find_lib() -> String:
    """Path to libicebergrsmojo: `$CONDA_PREFIX/lib` (installed by the
    `iceberg-shim` pixi package), else `ffi/target/release` for a bare checkout.
    """
    var ext = String("dylib") if CompilationTarget.is_macos() else String("so")
    var prefix = getenv("CONDA_PREFIX", "")
    if prefix == "":
        return String("ffi/target/release/libicebergrsmojo.") + ext
    return prefix + "/lib/libicebergrsmojo." + ext


def _open() raises -> OwnedDLHandle:
    """Open the shim. dlopen refcounts, so every handle is cheap after the first.
    """
    return OwnedDLHandle(_find_lib())


def _cstr(s: String) -> List[UInt8]:
    """A NUL-terminated byte buffer for `s`, to pass as a C `const char*`."""
    var b = List[UInt8]()
    var src = s.as_bytes()
    for i in range(len(src)):
        b.append(src[i])
    b.append(0)
    return b^


def _bytes_to_string(imm buf: List[UInt8]) -> String:
    """Decode a UTF-8 byte buffer into a `String`."""
    var out = String("")
    var i = 0
    var n = len(buf)
    while i < n:
        var b = Int(buf[i])
        if b < 0x80:
            out += chr(b)
            i += 1
        elif b < 0xE0:
            # 2-byte sequence
            if i + 1 >= n:
                break
            out += chr(((b & 0x1F) << 6) | (Int(buf[i + 1]) & 0x3F))
            i += 2
        elif b < 0xF0:
            if i + 2 >= n:
                break
            out += chr(
                ((b & 0x0F) << 12)
                | ((Int(buf[i + 1]) & 0x3F) << 6)
                | (Int(buf[i + 2]) & 0x3F)
            )
            i += 3
        else:
            if i + 3 >= n:
                break
            out += chr(
                ((b & 0x07) << 18)
                | ((Int(buf[i + 1]) & 0x3F) << 12)
                | ((Int(buf[i + 2]) & 0x3F) << 6)
                | (Int(buf[i + 3]) & 0x3F)
            )
            i += 4
    return out^


def _read_string(imm lib: OwnedDLHandle, p: Int) raises -> String:
    """Copy the C string at `p` into a Mojo `String` without freeing it."""
    if p == 0:
        return String("")
    var len_fn = lib.get_function[Int64]("ib_string_len")
    var n = Int(len_fn(p))
    if n <= 0:
        return String("")
    var buf = List[UInt8](capacity=n)
    buf.resize(n, 0)
    var copy_fn = lib.get_function[Int64]("ib_string_copy")
    _ = copy_fn(p, Int(buf.unsafe_ptr()), n)
    return _bytes_to_string(buf)


def _take_string(imm lib: OwnedDLHandle, p: Int) raises -> String:
    """Copy the C string at `p` into a Mojo `String`, then free the C string."""
    var s = _read_string(lib, p)
    var free_fn = lib.get_function[NoneType]("ib_string_free")
    free_fn(p)
    return s^


def _last_error(imm lib: OwnedDLHandle) -> String:
    """The shim's thread-local last-error message (never freed — it's static).
    """
    try:
        var err_fn = lib.get_function[Int]("ib_last_error")
        return _read_string(lib, err_fn())
    except:
        return String("<no ib_last_error symbol>")


def _fail(imm lib: OwnedDLHandle, what: String) raises:
    raise Error(what + ": " + _last_error(lib))


def version() raises -> String:
    """The version of the `iceberg` Rust crate the shim was built against."""
    var lib = _open()
    var f = lib.get_function[Int]("ib_version")
    return _read_string(lib, f())


# ── Batch: one Arrow record batch, materialised column by column ──────────────


struct Batch(Movable):
    """One Arrow record batch owned by the Rust side.

    Columns are copied out on demand and cast on the way: `int_col` reads any
    integer, date, time, timestamp or boolean column as `Int64` (timestamps in
    microseconds since the epoch, dates in days), `float_col` reads any numeric
    column as `Float64`, `str_col` reads any column as UTF-8.
    """

    var lib: OwnedDLHandle
    var ptr: Int

    def __init__(out self, var lib: OwnedDLHandle, ptr: Int):
        self.lib = lib^
        self.ptr = ptr

    def __deinit__(deinit self):
        try:
            if self.ptr != 0:
                var f = self.lib.get_function[NoneType]("ib_batch_free")
                f(self.ptr)
        except:
            pass

    def num_rows(self) raises -> Int:
        var f = self.lib.get_function[Int64]("ib_batch_num_rows")
        var n = Int(f(self.ptr))
        if n < 0:
            _fail(self.lib, "Batch.num_rows")
        return n

    def num_columns(self) raises -> Int:
        var f = self.lib.get_function[c_int]("ib_batch_num_columns")
        var n = Int(f(self.ptr))
        if n < 0:
            _fail(self.lib, "Batch.num_columns")
        return n

    def schema_json(self) raises -> String:
        """JSON array of `{"name", "type", "nullable"}` for the batch's columns.
        """
        var f = self.lib.get_function[Int]("ib_batch_schema_json")
        var p = f(self.ptr)
        if p == 0:
            _fail(self.lib, "Batch.schema_json")
        return _take_string(self.lib, p)

    def int_col(self, name: String) raises -> List[Int64]:
        """Column `name` as `Int64`. Nulls read back as 0 — use `validity` to
        tell them apart."""
        return _get_i64(self.lib, self.ptr, name, self.num_rows())

    def float_col(self, name: String) raises -> List[Float64]:
        """Column `name` as `Float64`. Nulls read back as 0.0."""
        return _get_f64(self.lib, self.ptr, name, self.num_rows())

    def bool_col(self, name: String) raises -> List[Bool]:
        """Column `name` as `Bool`. Nulls read back as False."""
        return _get_bool(self.lib, self.ptr, name, self.num_rows())

    def str_col(self, name: String) raises -> List[String]:
        """Column `name` as UTF-8 strings. Nulls read back as empty strings."""
        return _get_str(self.lib, self.ptr, name, self.num_rows())

    def validity(self, name: String) raises -> List[Bool]:
        """One flag per row of column `name`: True when the value is present."""
        return _get_validity(self.lib, self.ptr, name, self.num_rows())

    def export_c_data(self) raises -> ArrowCData:
        """Export this batch through the Arrow C Data Interface as a struct array
        (one child per column) — the shape marrow's `c_data.mojo` imports."""
        var out = ArrowCData(_open(), self.lib)
        var f = self.lib.get_function[c_int]("ib_batch_export")
        if Int(f(self.ptr, out.array_ptr(), out.schema_ptr())) != 0:
            _fail(self.lib, "Batch.export_c_data")
        out.live = True
        return out^


# ── ArrowCData: a live ArrowArray/ArrowSchema pair owned by Mojo ──────────────


struct ArrowCData(Movable):
    """Storage for one Arrow C Data Interface struct pair.

    The two structs live in `Int64`-backed Mojo buffers (guaranteed 8-byte
    aligned, sized from the shim's own `sizeof`), so a consumer such as marrow can
    read them straight from `array_ptr()` / `schema_ptr()`. Releasing is automatic:
    the destructor calls the structs' `release` callbacks through `ib_arrow_release`.
    """

    var lib: OwnedDLHandle
    var array_buf: List[Int64]
    var schema_buf: List[Int64]
    var live: Bool

    def __init__(
        out self, var lib: OwnedDLHandle, imm sizer: OwnedDLHandle
    ) raises:
        var asz_fn = sizer.get_function[Int]("ib_arrow_array_size")
        var ssz_fn = sizer.get_function[Int]("ib_arrow_schema_size")
        var awords = (asz_fn() + 7) // 8
        var swords = (ssz_fn() + 7) // 8
        self.lib = lib^
        self.array_buf = List[Int64](capacity=awords)
        self.array_buf.resize(awords, 0)
        self.schema_buf = List[Int64](capacity=swords)
        self.schema_buf.resize(swords, 0)
        self.live = False

    def __deinit__(deinit self):
        try:
            if self.live:
                # Go through a worker that BORROWS both buffers: taking
                # `.unsafe_ptr()` inline would be their last use, and Mojo's
                # ASAP destruction would free them underneath the C call — the
                # release callback would then read freed memory and abort in
                # malloc. Borrowed params pin them for the call's duration.
                _release_c_data(self.lib, self.array_buf, self.schema_buf)
        except:
            pass

    def array_ptr(self) -> Int:
        """Address of the `ArrowArray` struct."""
        return Int(self.array_buf.unsafe_ptr())

    def schema_ptr(self) -> Int:
        """Address of the `ArrowSchema` struct."""
        return Int(self.schema_buf.unsafe_ptr())


# ── Scan ──────────────────────────────────────────────────────────────────────


struct Scan(Movable):
    """A planned table scan. Pull batches with `next()` until it returns False,
    or inspect the plan with `plan_files()` without reading any data."""

    var lib: OwnedDLHandle
    var ptr: Int

    def __init__(out self, var lib: OwnedDLHandle, ptr: Int):
        self.lib = lib^
        self.ptr = ptr

    def __deinit__(deinit self):
        try:
            if self.ptr != 0:
                var f = self.lib.get_function[NoneType]("ib_scan_free")
                f(self.ptr)
        except:
            pass

    def plan_files(self) raises -> String:
        """JSON array of the file scan tasks: data file path, format, record
        count, byte range, and any delete files that apply to each."""
        var f = self.lib.get_function[Int]("ib_scan_plan_files_json")
        var p = f(self.ptr)
        if p == 0:
            _fail(self.lib, "Scan.plan_files")
        return _take_string(self.lib, p)

    def next(mut self) raises -> Optional[Batch]:
        """The next record batch, or `None` at the end of the stream."""
        var out = List[Int64](capacity=1)
        out.resize(1, 0)
        var f = self.lib.get_function[c_int]("ib_scan_next")
        var rc = Int(f(self.ptr, Int(out.unsafe_ptr())))
        if rc < 0:
            _fail(self.lib, "Scan.next")
        if rc == 0:
            return None
        return Batch(_open(), Int(out[0]))

    def next_c_data(mut self) raises -> Optional[ArrowCData]:
        """The next record batch straight through the Arrow C Data Interface,
        skipping the opaque handle entirely."""
        var out = ArrowCData(_open(), self.lib)
        var f = self.lib.get_function[c_int]("ib_scan_next_batch")
        var rc = Int(f(self.ptr, out.array_ptr(), out.schema_ptr()))
        if rc < 0:
            _fail(self.lib, "Scan.next_c_data")
        if rc == 0:
            return None
        out.live = True
        return out^

    def batches(mut self) raises -> List[Batch]:
        """Drain the scan into a list of batches. Convenient for the small
        results a parity check produces; stream with `next()` for large tables.
        """
        var out = List[Batch]()
        while True:
            var b = self.next()
            if not b:
                break
            out.append(b.take())
        return out^


# ── BatchBuilder ──────────────────────────────────────────────────────────────


struct BatchBuilder(Movable):
    """Builds one record batch column by column against a table's schema.

    Each `*_col` call names a column of the table; the values are cast to the
    column's declared type on the Rust side, so `int_col` feeds `int`, `long`,
    `date` (days) and `timestamp` (microseconds) columns alike. Columns you leave
    out must be optional in the schema; they are filled with nulls.
    """

    var lib: OwnedDLHandle
    var ptr: Int

    def __init__(out self, var lib: OwnedDLHandle, ptr: Int):
        self.lib = lib^
        self.ptr = ptr

    def __deinit__(deinit self):
        try:
            if self.ptr != 0:
                var f = self.lib.get_function[NoneType]("ib_batch_builder_free")
                f(self.ptr)
        except:
            pass

    def int_col(mut self, name: String, imm values: List[Int64]) raises:
        _builder_int(self.lib, self.ptr, name, values, List[UInt8]())

    def int_col(
        mut self, name: String, imm values: List[Int64], imm valid: List[Bool]
    ) raises:
        _builder_int(self.lib, self.ptr, name, values, _valid_bytes(valid))

    def float_col(mut self, name: String, imm values: List[Float64]) raises:
        _builder_float(self.lib, self.ptr, name, values, List[UInt8]())

    def float_col(
        mut self,
        name: String,
        imm values: List[Float64],
        imm valid: List[Bool],
    ) raises:
        _builder_float(self.lib, self.ptr, name, values, _valid_bytes(valid))

    def bool_col(mut self, name: String, imm values: List[Bool]) raises:
        _builder_bool(self.lib, self.ptr, name, values, List[UInt8]())

    def bool_col(
        mut self, name: String, imm values: List[Bool], imm valid: List[Bool]
    ) raises:
        _builder_bool(self.lib, self.ptr, name, values, _valid_bytes(valid))

    def str_col(mut self, name: String, imm values: List[String]) raises:
        _builder_str(self.lib, self.ptr, name, values, List[UInt8]())

    def str_col(
        mut self, name: String, imm values: List[String], imm valid: List[Bool]
    ) raises:
        _builder_str(self.lib, self.ptr, name, values, _valid_bytes(valid))

    def build(self) raises -> Batch:
        """Assemble the staged columns into a `Batch`."""
        var f = self.lib.get_function[Int]("ib_batch_builder_build")
        var p = f(self.ptr)
        if p == 0:
            _fail(self.lib, "BatchBuilder.build")
        return Batch(_open(), p)


# ── Table ─────────────────────────────────────────────────────────────────────


struct Table(Movable):
    """A loaded Iceberg table: metadata, scans, and append/commit.

    iceberg-rust 0.10 is append-only. `append` writes Parquet data files right
    away and stages them; `commit` turns everything staged into exactly one new
    snapshot and returns its id.
    """

    var lib: OwnedDLHandle
    var ptr: Int

    def __init__(out self, var lib: OwnedDLHandle, ptr: Int):
        self.lib = lib^
        self.ptr = ptr

    def __deinit__(deinit self):
        try:
            if self.ptr != 0:
                var f = self.lib.get_function[NoneType]("ib_table_free")
                f(self.ptr)
        except:
            pass

    def refresh(mut self) raises:
        """Re-load from the catalog, picking up snapshots committed elsewhere.
        """
        var f = self.lib.get_function[c_int]("ib_table_refresh")
        if Int(f(self.ptr)) != 0:
            _fail(self.lib, "Table.refresh")

    def metadata_json(self) raises -> String:
        """The table's full current `metadata.json` text."""
        var f = self.lib.get_function[Int]("ib_table_metadata_json")
        var p = f(self.ptr)
        if p == 0:
            _fail(self.lib, "Table.metadata_json")
        return _take_string(self.lib, p)

    def schema_json(self) raises -> String:
        """The current schema in Iceberg metadata JSON form."""
        var f = self.lib.get_function[Int]("ib_table_schema_json")
        var p = f(self.ptr)
        if p == 0:
            _fail(self.lib, "Table.schema_json")
        return _take_string(self.lib, p)

    def partition_spec_json(self) raises -> String:
        """The default partition spec as JSON."""
        var f = self.lib.get_function[Int]("ib_table_partition_spec_json")
        var p = f(self.ptr)
        if p == 0:
            _fail(self.lib, "Table.partition_spec_json")
        return _take_string(self.lib, p)

    def snapshots_json(self) raises -> String:
        """JSON array of the table's snapshots, oldest first."""
        var f = self.lib.get_function[Int]("ib_table_snapshots_json")
        var p = f(self.ptr)
        if p == 0:
            _fail(self.lib, "Table.snapshots_json")
        return _take_string(self.lib, p)

    def location(self) raises -> String:
        """The table's base location."""
        var f = self.lib.get_function[Int]("ib_table_location")
        var p = f(self.ptr)
        if p == 0:
            _fail(self.lib, "Table.location")
        return _take_string(self.lib, p)

    def current_snapshot_id(self) raises -> Int64:
        """The current snapshot id, or 0 when the table has no snapshot yet."""
        var f = self.lib.get_function[Int64]("ib_table_current_snapshot_id")
        var s = f(self.ptr)
        if s < 0:
            _fail(self.lib, "Table.current_snapshot_id")
        return s

    def builder(self) raises -> BatchBuilder:
        """A `BatchBuilder` bound to this table's schema."""
        var f = self.lib.get_function[Int]("ib_batch_builder_new")
        var p = f(self.ptr)
        if p == 0:
            _fail(self.lib, "Table.builder")
        return BatchBuilder(_open(), p)

    def append(mut self, imm batch: Batch) raises:
        """Stage `batch` for append: writes its Parquet data file(s) now and
        remembers them until `commit`."""
        var f = self.lib.get_function[c_int]("ib_table_append")
        if Int(f(self.ptr, batch.ptr)) != 0:
            _fail(self.lib, "Table.append")

    def append_c_data(mut self, mut data: ArrowCData) raises:
        """Stage an Arrow C Data Interface struct pair for append. Consumes the
        pair — `data` is inert afterwards."""
        var f = self.lib.get_function[c_int]("ib_table_append_batch")
        var rc = Int(f(self.ptr, data.array_ptr(), data.schema_ptr()))
        data.live = False  # the shim took ownership either way
        if rc != 0:
            _fail(self.lib, "Table.append_c_data")

    def pending_files(self) raises -> Int:
        """Data files staged but not yet committed."""
        var f = self.lib.get_function[Int64]("ib_table_pending_files")
        var n = Int(f(self.ptr))
        if n < 0:
            _fail(self.lib, "Table.pending_files")
        return n

    def commit(mut self) raises -> Int64:
        """Commit every staged append as one new snapshot; returns its id.
        With nothing staged this is a no-op returning the current snapshot id.
        """
        var f = self.lib.get_function[Int64]("ib_table_commit")
        var s = f(self.ptr)
        if s < 0:
            _fail(self.lib, "Table.commit")
        return s

    def rollback(mut self) raises:
        """Drop every staged (uncommitted) append."""
        var f = self.lib.get_function[c_int]("ib_table_rollback")
        if Int(f(self.ptr)) != 0:
            _fail(self.lib, "Table.rollback")

    def scan(self) raises -> Scan:
        """Scan every column of the current snapshot."""
        return self.scan(String(""), Int64(0), String(""))

    def scan(self, columns: String) raises -> Scan:
        """Scan a comma-separated column projection of the current snapshot."""
        return self.scan(columns, Int64(0), String(""))

    def scan(self, columns: String, filter: String) raises -> Scan:
        """Scan a projection of the current snapshot under a filter (see the
        filter DSL in the README)."""
        return self.scan(columns, Int64(0), filter)

    def scan(
        self, columns: String, snapshot_id: Int64, filter: String
    ) raises -> Scan:
        """Scan `columns` (empty for all) of `snapshot_id` (0 for current) under
        `filter` (empty for none)."""
        return _scan_new(self.lib, self.ptr, columns, snapshot_id, filter)


# ── Catalog ───────────────────────────────────────────────────────────────────


struct Catalog(Movable):
    """An Iceberg catalog: REST, or SQL over sqlite/postgres/mysql."""

    var lib: OwnedDLHandle
    var ptr: Int

    def __init__(out self, var lib: OwnedDLHandle, ptr: Int):
        self.lib = lib^
        self.ptr = ptr

    def __deinit__(deinit self):
        try:
            if self.ptr != 0:
                var f = self.lib.get_function[NoneType]("ib_catalog_free")
                f(self.ptr)
        except:
            pass

    @staticmethod
    def sql(uri: String, warehouse: String) raises -> Catalog:
        """A SQL catalog. `uri` is an sqlx connection string
        (`sqlite:/abs/path/catalog.db?mode=rwc`, `postgres://…`, `mysql://…`) and
        `warehouse` is the data root (`file:///…`, `s3://…`). The sqlite flavour
        needs no infrastructure at all — it is what the test-suite uses."""
        var lib = _open()
        var uri_c = _cstr(uri)
        var wh_c = _cstr(warehouse)
        var f = lib.get_function[Int]("ib_catalog_sql_new")
        var p = f(Int(uri_c.unsafe_ptr()), Int(wh_c.unsafe_ptr()))
        _ = uri_c^  # keep both buffers mapped across the C call
        _ = wh_c^
        if p == 0:
            _fail(lib, "Catalog.sql")
        return Catalog(lib^, p)

    @staticmethod
    def rest(uri: String) raises -> Catalog:
        """A REST catalog with no warehouse hint and no extra properties."""
        return Catalog.rest(uri, String(""), String(""))

    @staticmethod
    def rest(
        uri: String, warehouse: String, props_json: String
    ) raises -> Catalog:
        """A REST catalog. `props_json` is a JSON object of extra properties
        passed to the REST client — OAuth2 (`credential`, `oauth2-server-uri`,
        `scope`, `token`) and `header.*` entries go here. See
        `examples/rest_polaris.md`."""
        var lib = _open()
        var uri_c = _cstr(uri)
        var wh_c = _cstr(warehouse)
        var props_c = _cstr(props_json)
        var f = lib.get_function[Int]("ib_catalog_rest_new")
        var p = f(
            Int(uri_c.unsafe_ptr()),
            0 if warehouse.byte_length() == 0 else Int(wh_c.unsafe_ptr()),
            0 if props_json.byte_length() == 0 else Int(props_c.unsafe_ptr()),
        )
        _ = uri_c^
        _ = wh_c^
        _ = props_c^
        if p == 0:
            _fail(lib, "Catalog.rest")
        return Catalog(lib^, p)

    def list_namespaces(self) raises -> String:
        """JSON array of top-level namespaces, each a dotted string."""
        return self.list_namespaces(String(""))

    def list_namespaces(self, parent: String) raises -> String:
        """JSON array of the namespaces under `parent` (empty for top level)."""
        return _str_call1(self.lib, "ib_list_namespaces", self.ptr, parent)

    def create_namespace(mut self, ns: String) raises:
        self.create_namespace(ns, String(""))

    def create_namespace(mut self, ns: String, props_json: String) raises:
        _int_call2(
            self.lib,
            "ib_create_namespace",
            self.ptr,
            ns,
            props_json,
            "Catalog.create_namespace",
        )

    def namespace_exists(self, ns: String) raises -> Bool:
        return (
            _int_call1(
                self.lib,
                "ib_namespace_exists",
                self.ptr,
                ns,
                "Catalog.namespace_exists",
            )
            == 1
        )

    def drop_namespace(mut self, ns: String) raises:
        _ = _int_call1(
            self.lib,
            "ib_drop_namespace",
            self.ptr,
            ns,
            "Catalog.drop_namespace",
        )

    def list_tables(self, ns: String) raises -> String:
        """JSON array of the table names in `ns`."""
        return _str_call1(self.lib, "ib_list_tables", self.ptr, ns)

    def table_exists(self, ns: String, name: String) raises -> Bool:
        return (
            _int_call2i(
                self.lib,
                "ib_table_exists",
                self.ptr,
                ns,
                name,
                "Catalog.table_exists",
            )
            == 1
        )

    def load_table(self, ns: String, name: String) raises -> Table:
        return _table_call(self.lib, "ib_table_load", self.ptr, ns, name)

    def drop_table(mut self, ns: String, name: String) raises:
        _ = _int_call2i(
            self.lib, "ib_table_drop", self.ptr, ns, name, "Catalog.drop_table"
        )

    def create_table(
        mut self, ns: String, name: String, schema_json: String
    ) raises -> Table:
        """Create an unpartitioned table. `schema_json` is Iceberg's metadata
        JSON schema form."""
        return self.create_table(ns, name, schema_json, String(""), String(""))

    def create_table(
        mut self,
        ns: String,
        name: String,
        schema_json: String,
        partition_spec_json: String,
    ) raises -> Table:
        """Create a table with a partition spec (unbound partition spec JSON).
        """
        return self.create_table(
            ns, name, schema_json, partition_spec_json, String("")
        )

    def create_table(
        mut self,
        ns: String,
        name: String,
        schema_json: String,
        partition_spec_json: String,
        props_json: String,
    ) raises -> Table:
        return _table_create(
            self.lib,
            self.ptr,
            ns,
            name,
            schema_json,
            partition_spec_json,
            props_json,
        )


# ── borrowed-handle workers (lib stays mapped across the C call) ──────────────


def _release_c_data(
    imm lib: OwnedDLHandle, imm arr: List[Int64], imm sch: List[Int64]
) raises:
    """Release an exported ArrowArray/ArrowSchema pair, with both buffers held
    alive by the borrow for the whole C call."""
    var f = lib.get_function[NoneType]("ib_arrow_release")
    f(Int(arr.unsafe_ptr()), Int(sch.unsafe_ptr()))


def _valid_bytes(imm valid: List[Bool]) -> List[UInt8]:
    var b = List[UInt8](capacity=len(valid))
    for i in range(len(valid)):
        b.append(UInt8(1) if valid[i] else UInt8(0))
    return b^


def _str_call1(
    imm lib: OwnedDLHandle, sym: String, handle: Int, arg: String
) raises -> String:
    var a = _cstr(arg)
    var f = lib.get_function[Int](sym)
    var p = f(handle, 0 if arg.byte_length() == 0 else Int(a.unsafe_ptr()))
    _ = a^
    if p == 0:
        _fail(lib, sym)
    return _take_string(lib, p)


def _int_call1(
    imm lib: OwnedDLHandle, sym: String, handle: Int, arg: String, what: String
) raises -> Int:
    var a = _cstr(arg)
    var f = lib.get_function[c_int](sym)
    var rc = Int(f(handle, Int(a.unsafe_ptr())))
    _ = a^
    if rc < 0:
        _fail(lib, what)
    return rc


def _int_call2(
    imm lib: OwnedDLHandle,
    sym: String,
    handle: Int,
    arg: String,
    arg2: String,
    what: String,
) raises:
    """Two-argument call where the second argument is optional (empty -> NULL).
    """
    var a = _cstr(arg)
    var b = _cstr(arg2)
    var f = lib.get_function[c_int](sym)
    var rc = Int(
        f(
            handle,
            Int(a.unsafe_ptr()),
            0 if arg2.byte_length() == 0 else Int(b.unsafe_ptr()),
        )
    )
    _ = a^
    _ = b^
    if rc < 0:
        _fail(lib, what)


def _int_call2i(
    imm lib: OwnedDLHandle,
    sym: String,
    handle: Int,
    arg: String,
    arg2: String,
    what: String,
) raises -> Int:
    """Two-argument call where both arguments are required."""
    var a = _cstr(arg)
    var b = _cstr(arg2)
    var f = lib.get_function[c_int](sym)
    var rc = Int(f(handle, Int(a.unsafe_ptr()), Int(b.unsafe_ptr())))
    _ = a^
    _ = b^
    if rc < 0:
        _fail(lib, what)
    return rc


def _table_call(
    imm lib: OwnedDLHandle, sym: String, handle: Int, ns: String, name: String
) raises -> Table:
    var a = _cstr(ns)
    var b = _cstr(name)
    var f = lib.get_function[Int](sym)
    var p = f(handle, Int(a.unsafe_ptr()), Int(b.unsafe_ptr()))
    _ = a^
    _ = b^
    if p == 0:
        _fail(lib, sym)
    return Table(_open(), p)


def _table_create(
    imm lib: OwnedDLHandle,
    handle: Int,
    ns: String,
    name: String,
    schema_json: String,
    spec_json: String,
    props_json: String,
) raises -> Table:
    var a = _cstr(ns)
    var b = _cstr(name)
    var c = _cstr(schema_json)
    var d = _cstr(spec_json)
    var e = _cstr(props_json)
    var f = lib.get_function[Int]("ib_table_create")
    var p = f(
        handle,
        Int(a.unsafe_ptr()),
        Int(b.unsafe_ptr()),
        Int(c.unsafe_ptr()),
        0 if spec_json.byte_length() == 0 else Int(d.unsafe_ptr()),
        0 if props_json.byte_length() == 0 else Int(e.unsafe_ptr()),
    )
    _ = a^
    _ = b^
    _ = c^
    _ = d^
    _ = e^
    if p == 0:
        _fail(lib, "Catalog.create_table")
    return Table(_open(), p)


def _scan_new(
    imm lib: OwnedDLHandle,
    table: Int,
    columns: String,
    snapshot_id: Int64,
    filter: String,
) raises -> Scan:
    var c = _cstr(columns)
    var fi = _cstr(filter)
    var f = lib.get_function[Int]("ib_scan_new")
    var p = f(
        table,
        0 if columns.byte_length() == 0 else Int(c.unsafe_ptr()),
        snapshot_id,
        0 if filter.byte_length() == 0 else Int(fi.unsafe_ptr()),
    )
    _ = c^
    _ = fi^
    if p == 0:
        _fail(lib, "Table.scan")
    return Scan(_open(), p)


def _get_i64(
    imm lib: OwnedDLHandle, batch: Int, name: String, rows: Int
) raises -> List[Int64]:
    var n = _cstr(name)
    var out = List[Int64](capacity=rows)
    out.resize(rows, 0)
    var f = lib.get_function[Int64]("ib_batch_get_i64")
    var got = Int(f(batch, Int(n.unsafe_ptr()), Int(out.unsafe_ptr()), 0, rows))
    _ = n^
    if got < 0:
        _fail(lib, "Batch.int_col")
    out.resize(got, 0)
    return out^


def _get_f64(
    imm lib: OwnedDLHandle, batch: Int, name: String, rows: Int
) raises -> List[Float64]:
    var n = _cstr(name)
    var out = List[Float64](capacity=rows)
    out.resize(rows, 0.0)
    var f = lib.get_function[Int64]("ib_batch_get_f64")
    var got = Int(f(batch, Int(n.unsafe_ptr()), Int(out.unsafe_ptr()), 0, rows))
    _ = n^
    if got < 0:
        _fail(lib, "Batch.float_col")
    out.resize(got, 0.0)
    return out^


def _get_bool(
    imm lib: OwnedDLHandle, batch: Int, name: String, rows: Int
) raises -> List[Bool]:
    var n = _cstr(name)
    var raw = List[UInt8](capacity=rows)
    raw.resize(rows, 0)
    var f = lib.get_function[Int64]("ib_batch_get_bool")
    var got = Int(f(batch, Int(n.unsafe_ptr()), Int(raw.unsafe_ptr()), 0, rows))
    _ = n^
    if got < 0:
        _fail(lib, "Batch.bool_col")
    var out = List[Bool]()
    for i in range(got):
        out.append(raw[i] != 0)
    return out^


def _get_validity(
    imm lib: OwnedDLHandle, batch: Int, name: String, rows: Int
) raises -> List[Bool]:
    var n = _cstr(name)
    var vals = List[Int64](capacity=rows)
    vals.resize(rows, 0)
    var raw = List[UInt8](capacity=rows)
    raw.resize(rows, 0)
    var f = lib.get_function[Int64]("ib_batch_get_i64")
    var got = Int(
        f(
            batch,
            Int(n.unsafe_ptr()),
            Int(vals.unsafe_ptr()),
            Int(raw.unsafe_ptr()),
            rows,
        )
    )
    _ = n^
    # `vals` is only ever touched through `.unsafe_ptr()`, so that call would be
    # its last use and Mojo would free it while the shim is still writing values
    # into it — heap corruption, not a clean crash. Keep it alive explicitly.
    _ = vals^
    if got < 0:
        _fail(lib, "Batch.validity")
    var out = List[Bool]()
    for i in range(got):
        out.append(raw[i] != 0)
    return out^


def _get_str(
    imm lib: OwnedDLHandle, batch: Int, name: String, rows: Int
) raises -> List[String]:
    var n = _cstr(name)
    var size_fn = lib.get_function[Int64]("ib_batch_utf8_size")
    var total = Int(size_fn(batch, Int(n.unsafe_ptr())))
    if total < 0:
        _ = n^
        _fail(lib, "Batch.str_col")
    var offs = List[Int64](capacity=rows + 1)
    offs.resize(rows + 1, 0)
    var blob = List[UInt8](capacity=total + 1)
    blob.resize(
        total + 1, 0
    )  # +1 so the pointer is never null for an empty column
    var f = lib.get_function[Int64]("ib_batch_get_str")
    var got = Int(
        f(
            batch,
            Int(n.unsafe_ptr()),
            Int(offs.unsafe_ptr()),
            Int(blob.unsafe_ptr()),
            total,
            0,
            rows,
        )
    )
    _ = n^
    if got < 0:
        _fail(lib, "Batch.str_col")
    var out = List[String]()
    for i in range(got):
        var start = Int(offs[i])
        var end = Int(offs[i + 1])
        var piece = List[UInt8](capacity=end - start)
        for j in range(start, end):
            piece.append(blob[j])
        out.append(_bytes_to_string(piece))
    return out^


def _builder_int(
    imm lib: OwnedDLHandle,
    builder: Int,
    name: String,
    imm values: List[Int64],
    imm valid: List[UInt8],
) raises:
    var n = _cstr(name)
    var f = lib.get_function[c_int]("ib_batch_builder_int")
    var rc = Int(
        f(
            builder,
            Int(n.unsafe_ptr()),
            Int(values.unsafe_ptr()),
            len(values),
            0 if len(valid) == 0 else Int(valid.unsafe_ptr()),
        )
    )
    _ = n^
    if rc != 0:
        _fail(lib, "BatchBuilder.int_col")


def _builder_float(
    imm lib: OwnedDLHandle,
    builder: Int,
    name: String,
    imm values: List[Float64],
    imm valid: List[UInt8],
) raises:
    var n = _cstr(name)
    var f = lib.get_function[c_int]("ib_batch_builder_float")
    var rc = Int(
        f(
            builder,
            Int(n.unsafe_ptr()),
            Int(values.unsafe_ptr()),
            len(values),
            0 if len(valid) == 0 else Int(valid.unsafe_ptr()),
        )
    )
    _ = n^
    if rc != 0:
        _fail(lib, "BatchBuilder.float_col")


def _builder_bool(
    imm lib: OwnedDLHandle,
    builder: Int,
    name: String,
    imm values: List[Bool],
    imm valid: List[UInt8],
) raises:
    var n = _cstr(name)
    var raw = _valid_bytes(values)
    var f = lib.get_function[c_int]("ib_batch_builder_bool")
    var rc = Int(
        f(
            builder,
            Int(n.unsafe_ptr()),
            Int(raw.unsafe_ptr()),
            len(values),
            0 if len(valid) == 0 else Int(valid.unsafe_ptr()),
        )
    )
    _ = n^
    _ = raw^
    if rc != 0:
        _fail(lib, "BatchBuilder.bool_col")


def _builder_str(
    imm lib: OwnedDLHandle,
    builder: Int,
    name: String,
    imm values: List[String],
    imm valid: List[UInt8],
) raises:
    var n = _cstr(name)
    # Pack the strings the way `ib_batch_get_str` hands them back: one UTF-8 blob
    # plus len+1 offsets. No per-row allocation crosses FFI.
    var offs = List[Int64](capacity=len(values) + 1)
    var blob = List[UInt8]()
    offs.append(0)
    for i in range(len(values)):
        var bs = values[i].as_bytes()
        for j in range(len(bs)):
            blob.append(bs[j])
        offs.append(Int64(len(blob)))
    blob.append(0)  # never hand the shim a null pointer for an all-empty column
    var f = lib.get_function[c_int]("ib_batch_builder_str")
    var rc = Int(
        f(
            builder,
            Int(n.unsafe_ptr()),
            Int(offs.unsafe_ptr()),
            len(values),
            Int(blob.unsafe_ptr()),
            0 if len(valid) == 0 else Int(valid.unsafe_ptr()),
        )
    )
    _ = n^
    _ = offs^
    _ = blob^
    if rc != 0:
        _fail(lib, "BatchBuilder.str_col")
