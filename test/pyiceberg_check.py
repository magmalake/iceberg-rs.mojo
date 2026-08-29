"""Read the table iceberg-rs.mojo just wrote, using PyIceberg, and assert parity.

Two paths are tried, in order:

1. `SqlCatalog` over the very same sqlite file the Mojo test used. This exercises
   catalog interop as well as data interop, but only works if iceberg-rust and
   PyIceberg agree on the JDBC catalog table layout.
2. `StaticTable` over the newest `metadata.json` under the warehouse. This still
   checks that PyIceberg can parse iceberg-rust's metadata, manifests and Parquet
   files — the part that actually matters for the parity oracle.

Which path ran is printed, and recorded in the README.
"""

import glob
import os
import sys
from datetime import datetime, timedelta

EPOCH = datetime(1970, 1, 1)

DB = os.environ["IB_DB"]
WAREHOUSE = os.environ["IB_WAREHOUSE"]

EXPECTED = {
    1: ("eu", 10, 1.5, True),
    2: ("us", 20, 2.5, False),
    3: ("eu", 30, 3.5, True),
    4: ("us", 40, 4.5, False),
    5: ("apac", 50, 5.5, True),
}


def newest_metadata() -> str:
    files = glob.glob(os.path.join(WAREHOUSE, "**", "metadata", "*.metadata.json"), recursive=True)
    if not files:
        raise SystemExit(f"no metadata.json under {WAREHOUSE}")
    return max(files, key=os.path.getmtime)


def load():
    try:
        from pyiceberg.catalog.sql import SqlCatalog

        catalog = SqlCatalog(
            "sql",
            **{"uri": f"sqlite:///{DB}", "warehouse": f"file://{WAREHOUSE}"},
        )
        table = catalog.load_table("sales.orders")
        return table, "SqlCatalog (shared sqlite catalog)"
    except Exception as exc:  # noqa: BLE001 - any catalog-layout mismatch falls back
        print(f"  SqlCatalog path unavailable ({type(exc).__name__}: {exc})")
        from pyiceberg.table import StaticTable

        path = newest_metadata()
        print(f"  falling back to StaticTable on {path}")
        return StaticTable.from_metadata(f"file://{path}"), "StaticTable (metadata.json)"


def main() -> int:
    table, how = load()
    print(f"PyIceberg loaded the table via {how}")
    print(f"  pyiceberg schema: {table.schema()}")

    snapshots = list(table.metadata.snapshots)
    print(f"  snapshots: {len(snapshots)}")
    assert len(snapshots) == 2, f"expected 2 snapshots, got {len(snapshots)}"

    spec = table.spec()
    assert len(spec.fields) == 1, f"expected 1 partition field, got {spec.fields}"
    assert str(spec.fields[0].transform) == "identity", spec.fields[0]

    arrow = table.scan().to_arrow()
    rows = arrow.to_pylist()
    print(f"  rows: {len(rows)}")
    assert len(rows) == 5, f"expected 5 rows, got {len(rows)}"

    by_id = {r["id"]: r for r in rows}
    assert sorted(by_id) == [1, 2, 3, 4, 5], sorted(by_id)
    for ident, (region, count, amount, ok) in EXPECTED.items():
        row = by_id[ident]
        assert row["region"] == region, (ident, row)
        assert row["count"] == count, (ident, row)
        assert abs(row["amount"] - amount) < 1e-12, (ident, row)
        assert row["ok"] is ok, (ident, row)
        # An Iceberg `timestamp` is timezone-naive UTC; PyIceberg hands it back
        # as a naive datetime, so pin the epoch explicitly rather than letting
        # .timestamp() reinterpret it in the local zone.
        ts = row["ts"]
        if isinstance(ts, int):
            micros = ts
        else:
            micros = (ts - EPOCH) // timedelta(microseconds=1)
        want = 1_700_000_000_000_000 + (ident - 1) * 1_000_000
        assert micros == want, (ident, micros, want)

    filtered = table.scan(row_filter="region == 'eu'").to_arrow().to_pylist()
    ids = sorted(r["id"] for r in filtered)
    assert ids == [1, 3], ids
    print(f"  filtered (region == 'eu'): {ids}")

    print("PASS: PyIceberg reads the same 5 rows, 2 snapshots, identity partition")
    print(f"CROSS-CHECK-PATH: {how}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
