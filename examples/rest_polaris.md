# Pointing at a REST catalog (Polaris, Unity, Lakekeeper, Nessie, Gravitino, …)

The sqlite SQL catalog in the test-suite needs no infrastructure, but production
Iceberg usually sits behind an **Iceberg REST catalog**. `Catalog.rest` speaks the
REST spec through `iceberg-catalog-rest` 0.10.

There is no live test for this — it needs a running server — so treat this as the
recipe, not a verified transcript.

## The shape of the call

```mojo
from iceberg_rs import Catalog

var props = String(
    '{'
      '"credential": "<client-id>:<client-secret>",'
      '"scope": "PRINCIPAL_ROLE:ALL",'
      '"header.X-Iceberg-Access-Delegation": "vended-credentials"'
    '}'
)
var cat = Catalog.rest(
    "https://polaris.example.com/api/catalog",
    "my_catalog",          # the warehouse / catalog name the server knows
    props,
)

var t = cat.load_table("sales", "orders")
var scan = t.scan("id,amount", '[">","amount",100]')
```

`props_json` is a JSON object handed straight to the REST client. The keys that
matter:

| key | meaning |
|---|---|
| `credential` | `client_id:client_secret` for the OAuth2 client-credentials flow |
| `token` | a bearer token, if you already have one (skips the token exchange) |
| `oauth2-server-uri` | token endpoint, when it isn't the catalog's own `/v1/oauth/tokens` |
| `scope` | OAuth2 scope; Polaris wants `PRINCIPAL_ROLE:ALL` |
| `warehouse` | also settable as the second argument to `Catalog.rest` |
| `header.<Name>` | any extra HTTP header, verbatim |

## Object storage and credentials

Two things have to be true for reads and writes to work:

1. **The scheme must be supported.** iceberg-rust's core `FileIO` only ships
   `file://` and `memory://`; everything else comes from
   `iceberg-storage-opendal`, which this shim always installs as the storage
   factory. That covers `s3://`, `s3a://`, `gs://`/`gcs://`, `oss://`,
   `abfss://`/`abfs://`/`wasbs://` and `hf://`.

2. **Credentials must reach it.** iceberg-rust 0.10's REST catalog has **no
   SigV4 request signing** for the catalog API itself, so the supported path is
   **vended credentials**: ask the server for scoped storage credentials with

   ```text
   "header.X-Iceberg-Access-Delegation": "vended-credentials"
   ```

   and the catalog response carries per-table S3/GCS/Azure credentials that the
   storage layer picks up. If your deployment does *not* vend credentials, fall
   back to ambient credentials in the process environment (`AWS_ACCESS_KEY_ID`,
   `AWS_SECRET_ACCESS_KEY`, `AWS_REGION`, `AWS_SESSION_TOKEN`, or the GCS/Azure
   equivalents) — OpenDAL reads those the usual way.

   Signing the *catalog HTTP calls* with SigV4 (as AWS Glue's REST endpoint and
   S3 Tables require) is not supported by iceberg-rust 0.10. For those, use the
   Glue catalog (`iceberg-catalog-glue`, not wired into this shim yet) or put a
   signing proxy in front.

## Minio / self-hosted S3

For a local Minio behind a REST catalog, the storage properties usually come from
the catalog config endpoint. If you need to force them, they can also be set as
table or catalog properties on the server (`s3.endpoint`, `s3.region`,
`s3.path-style-access`) — this shim passes the catalog's storage config through
to OpenDAL unchanged.

## What still won't work

Same limits as everywhere else in this bridge, because they are iceberg-rust
0.10 limits, not REST limits:

* **Deletion vectors are not applied on read.** Positional and equality delete
  *files* are, but a v3 table whose deletes live in Puffin deletion vectors will
  read back rows that should be gone.
* **Writes are append-only.** No overwrite, no merge-on-read, no compaction.
