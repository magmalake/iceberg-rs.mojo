"""Round-trip gate for the iceberg_rs binding.

Creates a sqlite SQL catalog over a local warehouse, makes a partitioned table
with int/long/double/string/timestamp/bool columns, appends two batches built
from Mojo-supplied columns, then reads them back with and without a filter and a
column projection. Also checks the snapshot list, the metadata JSON, the scan
plan, and the Arrow C Data Interface export.

It finishes by writing `build/pyiceberg_env.sh`, which `test/pyiceberg_check.sh`
sources to read the very same table with PyIceberg — the cross-implementation
check that is the whole point of this bridge.

Run via `pixi run test`.
"""

from std.os import getenv
from iceberg_rs import Catalog, Table, Batch, version


comptime SCHEMA = String(
    '{"type":"struct","schema-id":0,"fields":['
    '{"id":1,"name":"id","required":true,"type":"long"},'
    '{"id":2,"name":"region","required":true,"type":"string"},'
    '{"id":3,"name":"count","required":false,"type":"int"},'
    '{"id":4,"name":"amount","required":false,"type":"double"},'
    '{"id":5,"name":"ts","required":false,"type":"timestamp"},'
    '{"id":6,"name":"ok","required":false,"type":"boolean"}]}'
)

comptime SPEC = String(
    '{"spec-id":0,"fields":['
    '{"source-id":2,"name":"region","transform":"identity"}]}'
)


def check(cond: Bool, what: String) raises:
    if not cond:
        raise Error("FAIL: " + what)
    print("  ok:", what)


def check_eq_int(got: Int, want: Int, what: String) raises:
    if got != want:
        raise Error(
            "FAIL: " + what + ": got " + String(got) + ", want " + String(want)
        )
    print("  ok:", what, "=", got)


def contains(haystack: String, needle: String) -> Bool:
    return haystack.find(needle) >= 0


def append_rows(
    mut t: Table,
    imm ids: List[Int64],
    imm regions: List[String],
    imm counts: List[Int64],
    imm amounts: List[Float64],
    imm ts: List[Int64],
    imm ok: List[Bool],
) raises -> Int64:
    var b = t.builder()
    b.int_col("id", ids)
    b.str_col("region", regions)
    b.int_col("count", counts)
    b.float_col("amount", amounts)
    b.int_col("ts", ts)
    b.bool_col("ok", ok)
    var batch = b.build()
    check_eq_int(batch.num_rows(), len(ids), "batch rows")
    t.append(batch)
    return t.commit()


def collect_ids(
    mut t: Table, columns: String, filter: String
) raises -> List[Int64]:
    var scan = t.scan(columns, filter)
    var out = List[Int64]()
    while True:
        var maybe = scan.next()
        if not maybe:
            break
        var batch = maybe.take()
        var ids = batch.int_col("id")
        for i in range(len(ids)):
            out.append(ids[i])
    # Deterministic order: the scan visits partitions in manifest order.
    for i in range(len(out)):
        for j in range(i + 1, len(out)):
            if out[j] < out[i]:
                var tmp = out[i]
                out[i] = out[j]
                out[j] = tmp
    return out^


def main() raises:
    print("iceberg crate version:", version())

    var root = getenv("ICEBERG_RS_TEST_DIR", "/tmp/iceberg-rs-mojo-test")
    var db_uri = String("sqlite:") + root + "/catalog.db?mode=rwc"
    var warehouse = String("file://") + root + "/warehouse"
    print("catalog:", db_uri)
    print("warehouse:", warehouse)

    var cat = Catalog.sql(db_uri, warehouse)

    print("-- namespaces --")
    cat.create_namespace("sales")
    check(cat.namespace_exists("sales"), "namespace sales exists")
    var ns = cat.list_namespaces()
    check(contains(ns, "sales"), "list_namespaces contains sales: " + ns)

    print("-- create table --")
    var t = cat.create_table("sales", "orders", SCHEMA, SPEC)
    var tables = cat.list_tables("sales")
    check(contains(tables, "orders"), "list_tables contains orders: " + tables)
    check(cat.table_exists("sales", "orders"), "table_exists")
    check_eq_int(
        Int(t.current_snapshot_id()), 0, "no snapshot before the first commit"
    )

    print("-- append batch 1 (3 rows, 2 partitions) --")
    var s1 = append_rows(
        t,
        [Int64(1), Int64(2), Int64(3)],
        ["eu", "us", "eu"],
        [Int64(10), Int64(20), Int64(30)],
        [1.5, 2.5, 3.5],
        [
            Int64(1700000000000000),
            Int64(1700000001000000),
            Int64(1700000002000000),
        ],
        [True, False, True],
    )
    check(s1 != 0, "commit 1 returned a snapshot id: " + String(s1))

    print("-- append batch 2 (2 rows) --")
    var s2 = append_rows(
        t,
        [Int64(4), Int64(5)],
        ["us", "apac"],
        [Int64(40), Int64(50)],
        [4.5, 5.5],
        [Int64(1700000003000000), Int64(1700000004000000)],
        [False, True],
    )
    check(
        s2 != 0 and s2 != s1,
        "commit 2 returned a new snapshot id: " + String(s2),
    )
    check_eq_int(
        Int(t.current_snapshot_id()), Int(s2), "current snapshot is the second"
    )

    print("-- metadata --")
    var meta = t.metadata_json()
    check(contains(meta, '"format-version": 2'), "metadata JSON is a v2 table")
    check(contains(meta, "orders"), "metadata JSON mentions the table")
    var schema_json = t.schema_json()
    check(
        contains(schema_json, '"name":"region"'),
        "schema JSON has the region column",
    )
    var spec_json = t.partition_spec_json()
    check(contains(spec_json, "identity"), "partition spec is identity")

    var snaps = t.snapshots_json()
    var n_snaps = 0
    var idx = snaps.find('"snapshot-id"')
    while idx >= 0:
        n_snaps += 1
        idx = snaps.find('"snapshot-id"', idx + 1)
    check_eq_int(n_snaps, 2, "snapshot count")

    print("-- scan: all rows --")
    var all_ids = collect_ids(t, "", "")
    check_eq_int(len(all_ids), 5, "row count over both snapshots")
    for i in range(5):
        check_eq_int(Int(all_ids[i]), i + 1, "id[" + String(i) + "]")

    print("-- scan: values --")
    var scan = t.scan()
    var seen = 0
    while True:
        var maybe = scan.next()
        if not maybe:
            break
        var batch = maybe.take()
        var ids = batch.int_col("id")
        var regions = batch.str_col("region")
        var counts = batch.int_col("count")
        var amounts = batch.float_col("amount")
        var tss = batch.int_col("ts")
        var oks = batch.bool_col("ok")
        for i in range(len(ids)):
            var id = Int(ids[i])
            seen += 1
            check_eq_int(Int(counts[i]), id * 10, "count for id " + String(id))
            var want_amount = Float64(id) + 0.5
            if amounts[i] != want_amount:
                raise Error(
                    "FAIL: amount for id "
                    + String(id)
                    + ": got "
                    + String(amounts[i])
                    + ", want "
                    + String(want_amount)
                )
            var want_ts = Int64(1700000000000000) + Int64(id - 1) * Int64(
                1000000
            )
            if tss[i] != want_ts:
                raise Error(
                    "FAIL: ts for id "
                    + String(id)
                    + ": got "
                    + String(tss[i])
                    + ", want "
                    + String(want_ts)
                )
            var want_ok = (id % 2) == 1
            if oks[i] != want_ok:
                raise Error("FAIL: ok for id " + String(id))
            if id == 1 or id == 3:
                check(regions[i] == "eu", "region for id " + String(id))
            elif id == 2 or id == 4:
                check(regions[i] == "us", "region for id " + String(id))
            else:
                check(regions[i] == "apac", "region for id " + String(id))
    check_eq_int(seen, 5, "rows visited with values")

    print("-- scan: filter --")
    var eu = collect_ids(t, "", '["=","region","eu"]')
    check_eq_int(len(eu), 2, "eu row count")
    check_eq_int(Int(eu[0]), 1, "eu id[0]")
    check_eq_int(Int(eu[1]), 3, "eu id[1]")

    var big = collect_ids(t, "", '["and",[">","id",2],["!=","region","apac"]]')
    check_eq_int(len(big), 2, "and-filter row count")
    check_eq_int(Int(big[0]), 3, "and-filter id[0]")
    check_eq_int(Int(big[1]), 4, "and-filter id[1]")

    var in_set = collect_ids(t, "", '["in","region",["us","apac"]]')
    check_eq_int(len(in_set), 3, "in-filter row count")

    var not_null = collect_ids(t, "", '["not-null","amount"]')
    check_eq_int(len(not_null), 5, "not-null row count")

    print("-- scan: projection --")
    var proj = t.scan("id,region", "")
    var proj_cols = 0
    var proj_rows = 0
    while True:
        var maybe = proj.next()
        if not maybe:
            break
        var batch = maybe.take()
        proj_cols = batch.num_columns()
        proj_rows += batch.num_rows()
    check_eq_int(proj_cols, 2, "projected column count")
    check_eq_int(proj_rows, 5, "projected row count")

    print("-- scan: plan files --")
    var plan_scan = t.scan()
    var plan = plan_scan.plan_files()
    var n_files = 0
    var pidx = plan.find('"data-file-path"')
    while pidx >= 0:
        n_files += 1
        pidx = plan.find('"data-file-path"', pidx + 1)
    check_eq_int(
        n_files,
        4,
        "planned data files (eu/us from batch 1, us/apac from batch 2)",
    )
    check(contains(plan, "parquet"), "plan reports the parquet format")

    print("-- Arrow C Data Interface --")
    var c_scan = t.scan()
    var maybe_c = c_scan.next_c_data()
    check(Bool(maybe_c), "next_c_data yielded a batch")
    var c_data = maybe_c.take()
    check(c_data.array_ptr() != 0, "ArrowArray pointer is non-null")
    check(c_data.schema_ptr() != 0, "ArrowSchema pointer is non-null")
    _ = c_data^

    var e_scan = t.scan()
    var e_maybe = e_scan.next()
    check(Bool(e_maybe), "scan yielded a batch to export")
    var e_batch = e_maybe.take()
    var exported = e_batch.export_c_data()
    check(exported.array_ptr() != 0, "exported ArrowArray pointer is non-null")
    _ = exported^

    # A second, unpartitioned table exercises nulls, validity and batches().
    # It is deliberately separate so the PyIceberg cross-check keeps looking at
    # exactly the five rows above.
    print("-- nulls, validity, batches() --")
    var nt = cat.create_table("sales", "nullable", SCHEMA)
    var nb = nt.builder()
    nb.int_col("id", [Int64(7), Int64(8), Int64(9)])
    nb.str_col("region", ["eu", "us", "eu"])
    nb.float_col("amount", [1.0, 2.0, 3.0], [True, False, True])
    var nbatch = nb.build()
    nt.append(nbatch)
    check_eq_int(
        nt.pending_files(), 1, "one data file for an unpartitioned table"
    )
    _ = nt.commit()

    var nscan = nt.scan()
    var all_batches = nscan.batches()
    var null_rows = 0
    var nulls_seen = 0
    for i in range(len(all_batches)):
        var valid = all_batches[i].validity("amount")
        var amounts = all_batches[i].float_col("amount")
        var counts = all_batches[i].validity("count")
        null_rows += len(valid)
        for j in range(len(valid)):
            if not valid[j]:
                nulls_seen += 1
                if amounts[j] != 0.0:
                    raise Error("FAIL: a null amount should read back as 0.0")
            # `count` was never supplied, so every row of it must be null.
            if counts[j]:
                raise Error(
                    "FAIL: unsupplied column 'count' should be all-null"
                )
    check_eq_int(null_rows, 3, "batches() row count")
    check_eq_int(nulls_seen, 1, "null amounts")

    # Hand the PyIceberg cross-check the coordinates of what we just wrote.
    with open("build/pyiceberg_env.sh", "w") as f:
        f.write(
            String("export IB_DB='")
            + root
            + "/catalog.db'\nexport IB_WAREHOUSE='"
            + root
            + "/warehouse'\n"
        )

    print("PASS: iceberg_rs round-trip (5 rows, 2 snapshots, 4 data files)")
