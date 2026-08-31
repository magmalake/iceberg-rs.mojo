# iceberg-rs.mojo

[![mojoshelf](https://mojoshelf.org/badge/iceberg-rs-mojo.svg)](https://mojoshelf.org/tins/iceberg-rs-mojo) [![mojo nightly](https://mojoshelf.org/badge/iceberg-rs-mojo/nightly.svg)](https://mojoshelf.org/tins/iceberg-rs-mojo)

> Part of [**magmalake**](https://magmalake.org) — data lake building blocks in Mojo.

**Apache Iceberg for Mojo**, over a thin Rust `cdylib` that wraps
[iceberg-rust](https://github.com/apache/iceberg-rust) behind a C ABI. Open a
catalog, create and load tables, scan them (with column projection, snapshot
selection and pushdown filters), and append record batches.

It exists for two reasons:

1. **A bridge until the native magmalake stack lands.** Mojo has no Iceberg
   reader today. This gives you working reads and appends now, against real
   catalogs and real object storage, while the native Mojo implementation
   (hashes, zstd/lz4/snappy, roaring bitmaps, Parquet, Avro, manifests) is built
   out piece by piece.
2. **A parity oracle.** Every native component can be checked against what
   iceberg-rust produces for the same table — and, through the PyIceberg
   cross-check below, against the reference Python implementation too.

## Design

iceberg-rust is async (tokio) and speaks Arrow. This repo exposes it to Mojo the
same way this author's other Rust-cdylib bindings do:

- **`ffi/`** — a Rust `cdylib` (`ffi/src/*.rs`) exporting 56 `extern "C"`
  functions (`ib_catalog_sql_new`, `ib_table_create`, `ib_scan_next`,
  `ib_table_commit`, …). Each blocks on one shared multi-thread tokio runtime, so
  the boundary is **synchronous**. Handles are opaque `*mut c_void`; strings are
  UTF-8 with explicit lengths; every allocation has a free function; failures are
  signalled by a null pointer or negative return with the message available from
  `ib_last_error()`. It is a pixi package (`recipe.yaml` + `pixi.toml` +
  `Cargo.toml`) that installs `lib/libicebergrsmojo.{dylib,so}` into
  `$CONDA_PREFIX` — no manual build step.
- **`src/iceberg_rs/`** — the Mojo API (`Catalog`, `Table`, `Scan`, `Batch`,
  `BatchBuilder`, `ArrowCData`), loaded through an `OwnedDLHandle`. The handle is
  passed as a **borrowed `read` param** to every worker so Mojo's ASAP
  destruction can't `dlclose` the library mid-call (the flare gotcha) — and, for
  the same reason, so can't free a buffer the C side is still writing into.

Record batches cross the boundary two ways:

- **Opaque handle + typed copies** — `Batch.int_col` / `float_col` / `bool_col` /
  `str_col` / `validity`. Columns are `arrow::compute::cast`-ed on the Rust side
  first, so `int`, `long`, `date` and `timestamp` all arrive as `List[Int64]`
  (timestamps in microseconds, dates in days) and `float`/`double` as
  `List[Float64]`. No Arrow layout knowledge on the Mojo side.
- **Arrow C Data Interface** — `Scan.next_c_data()`, `Batch.export_c_data()`,
  `Table.append_c_data()`. A batch is exported as a struct array (one
  `ArrowArray` + one `ArrowSchema`, children = columns): exactly what
  [marrow](https://github.com/kszucs/marrow)'s `c_data.mojo` imports. `ArrowCData`
  owns 8-byte-aligned storage sized from the shim's own `sizeof`, and releases
  the pair in its destructor.

  *Status:* the C Data Interface **structs and their lifecycle are implemented and
  tested end to end** (export → release, export → import → read back). Importing
  them into marrow arrays is not wired up in this repo — marrow currently pins
  Mojo `1.0.0b2` and this repo targets stable `1.0.0` — so the materialising
  helpers use the typed-copy path instead. `ArrowCData.array_ptr()` /
  `schema_ptr()` are the integration points when marrow catches up.

## Prerequisites

- [pixi](https://pixi.sh) — pins Mojo `1.0.0` (the `default` environment) and
  builds the shim. A `nightly` environment tracks the Modular nightly; the same
  sources compile and pass there too (verified on `1.1.0.dev2026082905`), but
  CI lets that job fail — stable `1.0.0` is the contract.
- [Rust](https://rustup.rs) — `cargo` for local shim iteration and
  `pixi run test-ffi`. (The pixi package build uses conda's own `rust`.)
- [uv](https://docs.astral.sh/uv/) — only for the PyIceberg cross-check.

The first build pulls arrow, parquet, opendal and the iceberg crates — 472
crates in `ffi/Cargo.lock`; expect a couple of minutes locally and rather longer
on a CI runner. Later builds are cached.

## Use

```sh
pixi run test        # Mojo round-trip gate
pixi run pyiceberg   # …then read the same table back with PyIceberg
pixi run test-ffi    # the Rust shim's own C-ABI test
```

```mojo
from iceberg_rs import Catalog

def main() raises:
    var cat = Catalog.sql(
        "sqlite:/tmp/demo/catalog.db?mode=rwc", "file:///tmp/demo/warehouse"
    )
    cat.create_namespace("sales")

    var schema = String(
        '{"type":"struct","schema-id":0,"fields":['
        '{"id":1,"name":"id","required":true,"type":"long"},'
        '{"id":2,"name":"region","required":true,"type":"string"},'
        '{"id":3,"name":"amount","required":false,"type":"double"}]}'
    )
    var spec = String(
        '{"spec-id":0,"fields":'
        '[{"source-id":2,"name":"region","transform":"identity"}]}'
    )
    var t = cat.create_table("sales", "orders", schema, spec)

    var b = t.builder()
    b.int_col("id", [Int64(1), Int64(2)])
    b.str_col("region", ["eu", "us"])
    b.float_col("amount", [1.5, 2.5])
    t.append(b.build())
    print("snapshot", t.commit())

    var scan = t.scan("id,amount", '[">","amount",2.0]')
    while True:
        var maybe = scan.next()
        if not maybe:
            break
        var batch = maybe.take()
        print(batch.int_col("id"), batch.float_col("amount"))
```

Consume it with `-I ../iceberg-rs.mojo/src` plus the installed
shim (no link flags; it is dlopened at runtime), or as a pixi source dependency
(`iceberg-rs-mojo`, import `from iceberg_rs import …`).

## API

### `Catalog`

| method | notes |
|---|---|
| `Catalog.sql(uri, warehouse)` | SQL catalog over sqlite/postgres/mysql. `uri` is an sqlx connection string (`sqlite:/abs/path/catalog.db?mode=rwc`); `warehouse` is the data root (`file:///…`, `s3://…`). Zero infrastructure — what the tests use. |
| `Catalog.rest(uri[, warehouse, props_json])` | Iceberg REST catalog. `props_json` carries OAuth2 and `header.*` settings — see [`examples/rest_polaris.md`](examples/rest_polaris.md). |
| `list_namespaces([parent])` → JSON | dotted namespace names |
| `create_namespace(ns[, props_json])`, `drop_namespace(ns)`, `namespace_exists(ns)` | |
| `list_tables(ns)` → JSON, `table_exists(ns, name)` | |
| `create_table(ns, name, schema_json[, spec_json, props_json])` → `Table` | `schema_json` is Iceberg's metadata-JSON schema form; `spec_json` an unbound partition spec |
| `load_table(ns, name)` → `Table`, `drop_table(ns, name)` | |

### `Table`

| method | notes |
|---|---|
| `metadata_json()` | the full current `metadata.json` text |
| `schema_json()`, `partition_spec_json()`, `location()` | |
| `snapshots_json()` | oldest first: id, parent, sequence number, timestamp, manifest list, operation, summary |
| `current_snapshot_id()` | `0` when the table has no snapshot yet |
| `refresh()` | re-load from the catalog |
| `builder()` → `BatchBuilder` | bound to the table's schema |
| `append(batch)` / `append_c_data(data)` | writes the Parquet data file(s) now, stages them |
| `pending_files()`, `rollback()` | staged-but-uncommitted state |
| `commit()` → `Int64` | one new snapshot from everything staged |
| `scan([columns[, filter]])`, `scan(columns, snapshot_id, filter)` → `Scan` | empty string = "all columns" / "no filter"; `snapshot_id` 0 = current |

### `Scan` / `Batch` / `BatchBuilder`

| method | notes |
|---|---|
| `Scan.plan_files()` → JSON | the file scan tasks: path, format, record count, byte range, applicable delete files |
| `Scan.next()` → `Optional[Batch]`, `Scan.batches()` | |
| `Scan.next_c_data()` → `Optional[ArrowCData]` | Arrow C Data Interface |
| `Batch.num_rows()`, `num_columns()`, `schema_json()` | |
| `Batch.int_col(name)`, `float_col`, `bool_col`, `str_col`, `validity` | nulls read back as 0 / 0.0 / False / `""`; `validity` distinguishes them |
| `Batch.export_c_data()` → `ArrowCData` | |
| `BatchBuilder.int_col/float_col/bool_col/str_col(name, values[, valid])` | values are cast to the column's declared type |
| `BatchBuilder.build()` → `Batch` | omitted columns must be optional; they are filled with nulls |

## Filter DSL

Filters are a tiny JSON S-expression, parsed on the Rust side into an
`iceberg::expr::Predicate` and pushed down into manifest, row-group and page
pruning. The first element of each array is the operator:

```jsonc
["=",  "col", <literal>]     ["!=", "col", <literal>]
["<",  "col", <literal>]     ["<=", "col", <literal>]
[">",  "col", <literal>]     [">=", "col", <literal>]
["is-null",     "col"]       ["not-null", "col"]
["is-nan",      "col"]       ["not-nan",  "col"]
["starts-with", "col", "prefix"]
["in",     "col", [<literal>, …]]
["not-in", "col", [<literal>, …]]
["and", <filter>, <filter>, …]
["or",  <filter>, <filter>, …]
["not", <filter>]
["true"]   ["false"]
```

Literals are plain JSON scalars and are **typed against the table schema**, so
`["=", "id", 3]` on a `long` column produces `Datum::long(3)` rather than
`Datum::int(3)` — a mistyped datum silently matches nothing, which is why this
happens on the Rust side rather than in a string the caller writes. Dates, times
and timestamps accept either an integer (days / microseconds since the epoch) or
an ISO-8601 string; decimals accept a string. Only primitive columns are
filterable; `binary` and `fixed` are not supported.

```mojo
var scan = t.scan("", '["and",[">","id",2],["in","region",["eu","us"]]]')
```

## What iceberg-rust 0.10 can and cannot do

Everything below is an upstream limit, not a limit of this binding.

**Reads**

- ✅ Data files, **positional deletes** and **equality deletes** are applied.
- ❌ **Deletion vectors are not wired into scans.** A v3 table whose deletes live
  in Puffin deletion vectors will read back rows that should be gone. Do not use
  this bridge as an oracle for DV-bearing tables.
- ✅ Column projection, snapshot selection (time travel by id), predicate
  pushdown to manifests, row groups and page indexes.

**Writes**

- ✅ **Append only** — fast-append snapshots via the Parquet data-file writer,
  honouring the table's own `write.parquet.*` properties. Partitioned tables go
  through the fanout writer, so one unsorted batch becomes one data file per
  partition value.
- ❌ No overwrite, no merge-on-read, no row-level deletes, no compaction, no
  snapshot expiry from this binding, no encrypted writes.
- ❌ Schema evolution is not exposed (iceberg-rust has the action; this shim
  doesn't surface it yet).

**Catalogs**

- ✅ **SQL** (sqlite / postgres / mysql) and **REST**.
- ❌ **No SigV4 request signing** on the REST catalog — so AWS Glue's REST
  endpoint and S3 Tables need a signing proxy. The supported cloud path is
  **vended credentials** (`header.X-Iceberg-Access-Delegation: vended-credentials`),
  or ambient credentials in the process environment.
- Glue and Hive catalogs exist upstream (`iceberg-catalog-glue`) but are not
  wired into this shim.

**Storage**

- Core iceberg-rust only ships `file://` and `memory://`. This shim always
  installs `iceberg-storage-opendal`'s resolving factory, which adds `s3://`,
  `s3a://`, `gs://`/`gcs://`, `oss://`, `abfss://`/`abfs://`/`wasbs://` and
  `hf://` by URL scheme.

## Cross-implementation check (PyIceberg)

`pixi run pyiceberg` writes a table with this binding and then reads it back with
**PyIceberg 0.11.1 / PyArrow 25.0.1** in a throwaway `uv` venv. It runs in CI on
both platforms; result on macOS-arm64, 2026-08-29:

```
PyIceberg loaded the table via SqlCatalog (shared sqlite catalog)
  pyiceberg schema: table {
    1: id: required long
    2: region: required string
    3: count: optional int
    4: amount: optional double
    5: ts: optional timestamp
    6: ok: optional boolean
  }
  snapshots: 2
  rows: 5
  filtered (region == 'eu'): [1, 3]
PASS: PyIceberg reads the same 5 rows, 2 snapshots, identity partition
CROSS-CHECK-PATH: SqlCatalog (shared sqlite catalog)
```

That is the strong form of the check: PyIceberg opened **the same sqlite catalog
file**, not just the metadata JSON — so the JDBC catalog rows, the
`metadata.json`, the manifest list, the manifests, the Parquet files and the
identity partition values that iceberg-rust wrote are all readable by the
reference implementation, and its own `region == 'eu'` predicate pushdown selects
the same two rows this binding's filter DSL does. Every column type round-trips
exactly, including `timestamp` microseconds and the `int`/`long` distinction.
(The script falls back to `StaticTable.from_metadata` if a future PyIceberg
changes the catalog table layout; it did not need to.)

## Repo layout

```
ffi/          Rust cdylib + pixi package (recipe.yaml, Cargo.toml)
  src/        lib.rs, catalog.rs, table.rs, filter.rs, scan.rs, batch.rs, write.rs
  tests/      roundtrip.rs — the C ABI's own end-to-end test
src/iceberg_rs/__init__.mojo    the Mojo binding
test/         iceberg_test.mojo, pyiceberg_check.{sh,py}
examples/     rest_polaris.md
```

**Gotcha when hacking on `ffi/`:** pixi caches the built source package under
`.pixi/artifacts-v0/`. If a change to `ffi/src/*.rs` doesn't show up in the
installed dylib, `rm -rf .pixi/artifacts-v0 .pixi/bld && pixi install`.

## What the native stack should replicate first

In dependency order, judged by what this bridge had to lean on hardest:

1. **Parquet read** for primitive + string columns with row-group and page-index
   statistics — every scan bottoms out here, and it is the first place a native
   implementation can be diffed against `ib_scan_next` batch-for-batch.
2. **Manifest and manifest-list read** (Avro). `ib_scan_plan_files_json` is
   deliberately shaped as the parity target: the exact set of data files, their
   record counts, and the delete files that apply.
3. **`metadata.json` parse + snapshot chain.** `ib_table_metadata_json` and
   `ib_table_snapshots_json` give a byte-exact reference.
4. **Predicate binding and evaluation.** The filter DSL here maps 1:1 onto
   `iceberg::expr::Predicate`; a native binder can be checked by comparing
   planned files for the same filter.
5. **Fast-append write path** last — it needs Parquet write, manifest write and
   the snapshot summary, and it is the only part where being wrong corrupts a
   table rather than just returning wrong rows.

## Licence

Apache-2.0 (see `LICENSE` and `NOTICE`). iceberg-rust is Apache-2.0 too; this
repo links it into the FFI shim and ships no Apache Iceberg source.
