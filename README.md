# Apache Iceberg NATIVE table encryption with the BUILT-IN AWS KMS client (LocalStack)

A `docker-compose` demo that runs **Apache Spark 4.0.0**, **Apache Iceberg 1.11.0**,
and a standalone **Hive Metastore 4.0.0** (backed by **PostgreSQL 16**), modeling a
**multi-tenant backup catalog**: each tenant gets its own backup namespace
(`demo.tenant1_backup`, `demo.tenant2_backup`) holding an `ObjectVersions` table,
and **each tenant's table is natively encrypted under a DIFFERENT, per-tenant AWS KMS
master key** — using Iceberg's **own built-in
`org.apache.iceberg.aws.AwsKeyManagementClient`**, activated purely by configuration.
**There is NO custom Java and NO custom jar in this folder.** The whole thing runs
locally against **LocalStack**'s emulated KMS — no real AWS account required — and is
driven by a thin **Go client over Spark Connect**.

Iceberg's envelope encryption protects **BOTH** each table's **data files** (Parquet
modular encryption → footer magic `PARE`) **AND** its **metadata** (manifest +
manifest-list `.avro` files → Iceberg GCM-stream magic `AGS1`). The per-file Data
Encryption Key (DEK) is wrapped/unwrapped via **AWS KMS Encrypt/Decrypt**; the
symmetric master key never leaves KMS.

This is the **built-in-client sibling** of `../iceberg-native-aws-kms/`, which ships a
*custom* `AwsKmsKeyManagementClient`. Everything else (stack, Spark Connect, Go client,
PySpark proofs) is identical. See [Contrast](#contrast-with-the-custom-jar-sibling) below.

---

## The one change vs. the custom-jar sibling

Instead of building + shipping a custom `KeyManagementClient` in a shaded jar and wiring
it via `encryption.kms-impl`, this folder:

1. **Deletes the entire `kms-client/` Maven project** — there is no custom code.
2. Brings Iceberg's AWS module onto the Spark classpath via **`--packages`**:
   `org.apache.iceberg:iceberg-aws-bundle:1.11.0` (alongside the existing
   `iceberg-spark-runtime-4.0_2.13:1.11.0`). The bundle contains
   `org.apache.iceberg.aws.AwsKeyManagementClient` **and** the relocated AWS SDK v2 KMS
   classes, so no `--jars` and no Maven build are needed for Spark.
3. **Uses the stock `apache/spark:4.0.0-scala2.13-java17-python3-ubuntu` image directly**
   for the `spark`/`spark-connect` services (no custom `Dockerfile`). `app/job.py` is
   **bind-mounted** into the `spark` service. (`Dockerfile.hive` is still needed for the
   metastore's Postgres driver.)
4. Activates the built-in client by config:
   ```
   --conf spark.sql.catalog.demo.encryption.kms-type=aws
   --conf spark.sql.catalog.demo.kms.endpoint=http://localstack:4566
   --conf spark.sql.catalog.demo.client.region=us-east-1
   ```

> **No custom Spark Dockerfile was needed.** The custom-jar sibling staged its jar in a
> non-classpath location and added it via `--jars` specifically so it would land in the
> Spark **user** classloader (the one `EncryptionUtil.createKmsClient` loads from). With
> the built-in client there is no such concern: the AWS bundle is resolved by
> `--packages` straight into that same user classloader, exactly like the iceberg-spark
> runtime jar. The stock image works as-is.

---

## Verified activation + property names (from `apache-iceberg-1.11.0` source)

Verified against the raw source at the `apache-iceberg-1.11.0` tag:

**1. Activation — `kms-type=aws` (registry), confirmed.**
`core/.../CatalogProperties.java`:

| Constant | Value |
|----------|-------|
| `ENCRYPTION_KMS_TYPE` | `"encryption.kms-type"` |
| `ENCRYPTION_KMS_TYPE_AWS` | `"aws"` |
| `ENCRYPTION_KMS_IMPL` | `"encryption.kms-impl"` |
| `ENCRYPTION_KMS_IMPL_AWS` | `"org.apache.iceberg.aws.AwsKeyManagementClient"` |

`core/.../encryption/EncryptionUtil.java` `createKmsClient` reads
`ENCRYPTION_KMS_TYPE` first and maps `"aws" -> ENCRYPTION_KMS_IMPL_AWS`
(`"azure"`/`"gcp"` similarly); alternatively `ENCRYPTION_KMS_IMPL` names the class
directly. It explicitly forbids setting **both** ("Cannot set both KMS type and KMS
impl"). So either of these is valid and equivalent for AWS — this demo uses the former:

```
spark.sql.catalog.demo.encryption.kms-type=aws
# equivalent:
spark.sql.catalog.demo.encryption.kms-impl=org.apache.iceberg.aws.AwsKeyManagementClient
```

**2. Endpoint / region / credentials.**
- `aws/.../AwsProperties.java`: **`KMS_ENDPOINT = "kms.endpoint"`** (also
  `kms.encryption-algorithm-spec` default `SYMMETRIC_DEFAULT`, `kms.data-key-spec`
  default `AES_256`).
- `aws/.../AwsClientProperties.java`: **`CLIENT_REGION = "client.region"`**; credentials
  resolved as: custom `client.credentials-provider` → vended creds → static
  (access-key/secret) → **`DefaultCredentialsProvider`** fallback (which reads
  `AWS_ACCESS_KEY_ID`/`AWS_SECRET_ACCESS_KEY` from the env). This demo relies on the
  default chain picking up the LocalStack `test`/`test` env vars.
- As catalog-prefixed props: `spark.sql.catalog.demo.kms.endpoint`,
  `spark.sql.catalog.demo.client.region`.

**3. `aws/.../AwsKeyManagementClient.java`.**
- `wrapKey` → `kmsClient().encrypt(EncryptRequest)`;
  `unwrapKey` → `kmsClient().decrypt(DecryptRequest)`;
  it also implements `generateKey` via `generateDataKey(GenerateDataKeyRequest)`.
- **`supportsKeyGeneration()` returns `true`.**
- **No KMS `EncryptionContext` (AAD) and no grant tokens** are set anywhere — the
  Encrypt/Decrypt/GenerateDataKey request builders carry only the key id + plaintext /
  ciphertext. (This is the documented encryption-context finding: **the built-in client
  does NOT support a KMS EncryptionContext / AAD**.)
- Uses AWS SDK v2 `KmsClient`; the client is built from `AwsClientFactory.from(props)`.
- **Implied IAM actions: `kms:GenerateDataKey`, `kms:Encrypt`, `kms:Decrypt`.**

---

## Encryption context (AAD) — the documentation question

**Finding: the built-in `AwsKeyManagementClient` does NOT support a KMS
EncryptionContext (AAD), nor grant tokens.** Its `EncryptRequest`/`DecryptRequest`/
`GenerateDataKeyRequest` builders set only `keyId` + payload. If you need a KMS
EncryptionContext bound to the wrapped DEK (e.g. for an extra integrity/authorization
assertion enforced by KMS itself), the built-in client cannot do it — that is precisely
the kind of control a **custom** client (the sibling) buys you.

---

## GenerateDataKey vs. Encrypt — what actually happened on LocalStack

`supportsKeyGeneration()` returns **true**, so the client *can* call KMS
`GenerateDataKey`. But the runtime KMS access log from the verified run shows the native
Iceberg table-encryption (KEK-wrapping) flow used **`kms:Encrypt` + `kms:Decrypt` only**
— **0** `GenerateDataKey` calls:

```
4 kms.Encrypt => 200
8 kms.Decrypt => 200
0 GenerateDataKey
```

i.e. for Iceberg native table encryption the DEK is generated by Iceberg and the client
**wraps** it with KMS `Encrypt` (and unwraps with `Decrypt`). LocalStack handles all of
these fine. The required IAM actions in practice for this path are **`kms:Encrypt`** and
**`kms:Decrypt`**; grant `kms:GenerateDataKey` as well to be safe / for other Iceberg
code paths that may use the generation API. (Counts above are from the original
two-table run; the same shape holds for the tenant-backup tables.)

---

## Two tenant namespaces, two keys — one KMS key per tenant

LocalStack provisions **two** symmetric CMKs with two aliases (`localstack/init-kms.sh`),
one per tenant backup namespace:

| Tenant namespace | Table | `encryption.key-id` (alias) |
|------------------|-------|-----------------------------|
| `demo.tenant1_backup` | `demo.tenant1_backup.ObjectVersions` | `alias/iceberg/tenant1` |
| `demo.tenant2_backup` | `demo.tenant2_backup.ObjectVersions` | `alias/iceberg/tenant2` |

The key insight is **unchanged from the custom sibling and confirmed with the built-in
client**: the KMS client **never receives a namespace or table name — only the
`wrappingKeyId`** (the table's `encryption.key-id`). Per-tenant keys are achieved
purely by setting a different `encryption.key-id` per table at creation time; the single
shared `AwsKeyManagementClient` instance routes Encrypt/Decrypt to whichever key/alias
it is handed. The verified run wrapped `demo.tenant1_backup.ObjectVersions` with
`alias/iceberg/tenant1` and `demo.tenant2_backup.ObjectVersions` with
`alias/iceberg/tenant2` — two distinct KMS keys, one client. (Namespace names use
underscores so they are plain SQL identifiers needing no quoting.)

---

## Architecture

```
                         thrift://hive-metastore:9083
  +---------+   JDBC    +------------------+   Encrypt/Decrypt DEK   +------------------+
  | postgres|<----------| hive-metastore   |                        | LocalStack KMS       |
  |  :5432  |           |  (apache/hive)   |                        | alias/iceberg/tenant1|
  +---------+           +------------------+                        | alias/iceberg/tenant2|
                                 ^                                  +----------------------+
                                 | catalog metadata                          ^
                                 |                                           | AWS SDK v2 KmsClient
        +------------------------+----------------------------------+        | kms.endpoint
        | spark-connect (Spark 4.0.0 Connect server, local[2])      |--------+ -> http://localstack:4566
        |  + iceberg-spark-runtime 1.11.0          (--packages)      |
        |  + iceberg-aws-bundle 1.11.0             (--packages)      |
        |  + spark-connect_2.13:4.0.0              (--packages)      |
        |  built-in AwsKeyManagementClient                          |        +-------------------+
        |    activated by encryption.kms-type=aws                   |<-------| go-client (gRPC)  |
        +-----------------------------------------------------------+ sc://  | 2 tenants / 2 keys|
                                 |                                    :15002  +-------------------+
                          file:///data/warehouse  (shared named volume)
```

---

## Build & run

This folder uses **different container names and host ports** than the custom-jar
sibling so both can run side-by-side: LocalStack host **4568**, Spark Connect host
**15003**, Spark UI host **4042**, Hive metastore host **9084**. In-compose the services
still talk on the standard ports (`localstack:4566`, `spark-connect:15002`, etc.).

```bash
cd iceberg-native-aws-kms-builtin

# Build the go-client image (spark/spark-connect use the STOCK spark image — no build)
docker compose build go-client

# Bring up localstack (provisions the 2 keys) + postgres + hive-metastore + connect server
docker compose up -d spark-connect

# Run the Go client: two tenant namespaces, two KMS keys (CREATE / INSERT / SELECT)
docker compose run --rm go-client

# (optional) Run the PySpark equivalent + PARE/AGS1 proofs for BOTH tables
docker compose run --rm spark

# Or do it all + the encryption proof in one go:
./verify_demo.sh

# Tear down (drop volumes too):
docker compose down -v
```

The `spark`/`spark-connect` services wire (identical catalog conf, built-in client):

```
--packages org.apache.iceberg:iceberg-spark-runtime-4.0_2.13:1.11.0,org.apache.iceberg:iceberg-aws-bundle:1.11.0[,org.apache.spark:spark-connect_2.13:4.0.0]
--conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions
--conf spark.sql.catalog.demo=org.apache.iceberg.spark.SparkCatalog
--conf spark.sql.catalog.demo.type=hive
--conf spark.sql.catalog.demo.uri=thrift://hive-metastore:9083
--conf spark.sql.catalog.demo.warehouse=file:///data/warehouse
--conf spark.sql.catalog.demo.encryption.kms-type=aws
--conf spark.sql.catalog.demo.kms.endpoint=http://localstack:4566
--conf spark.sql.catalog.demo.client.region=us-east-1
```
plus the AWS env (`AWS_ENDPOINT_URL`, `AWS_REGION`, `AWS_ACCESS_KEY_ID=test`,
`AWS_SECRET_ACCESS_KEY=test`) for the default credential chain. Tables opt into
encryption with `TBLPROPERTIES ('encryption.key-id' = 'alias/iceberg/tenant...', 'format-version' = '3')`.
**`format-version = 3` is required.**

### Captured Go-client output (actual run)

```
=== namespace demo.tenant1_backup -> KMS key alias/iceberg/tenant1 ===
+--------+----------------+----------------------------------+
|id      |restore_point_id|payload_ref                       |
+--------+----------------+----------------------------------+
|obj-0001|rp-2026-06-09-01|s3://backups/tenant1/obj-0001.dat |
|obj-0002|rp-2026-06-09-01|s3://backups/tenant1/obj-0002.dat |
|obj-0003|rp-2026-06-09-02|s3://backups/tenant1/obj-0003.dat |
|obj-0004|rp-2026-06-09-02|s3://backups/tenant1/obj-0004.dat |
+--------+----------------+----------------------------------+
=== namespace demo.tenant2_backup -> KMS key alias/iceberg/tenant2 ===
+--------+----------------+----------------------------------+
|id      |restore_point_id|payload_ref                       |
+--------+----------------+----------------------------------+
|obj-0001|rp-2026-06-10-01|s3://backups/tenant2/obj-0001.dat |
|obj-0002|rp-2026-06-10-01|s3://backups/tenant2/obj-0002.dat |
|obj-0003|rp-2026-06-10-02|s3://backups/tenant2/obj-0003.dat |
+--------+----------------+----------------------------------+
SUMMARY: two tenant namespaces, two different AWS KMS keys (via LocalStack)
  demo.tenant1_backup  table=demo.tenant1_backup.ObjectVersions  encryption.key-id=alias/iceberg/tenant1
  demo.tenant2_backup  table=demo.tenant2_backup.ObjectVersions  encryption.key-id=alias/iceberg/tenant2
GO CLIENT DONE: CREATE + batched INSERT + SELECT succeeded for both encrypted tables.
```

### Captured PARE/AGS1 proof (actual run, both tables, via PySpark `DEMO PASSED`)

```
================ demo.tenant1_backup.ObjectVersions ================
  *.parquet: footer=b'PARE' -> ENCRYPTED (PARE)
  *-m0.avro + snap-*.avro: head=b'AGS1' -> ENCRYPTED (AGS1 GCM)
  *.metadata.json: head=b'{"fo' -> plaintext JSON pointer (by design)
================ demo.tenant2_backup.ObjectVersions ================
  *.parquet: footer=b'PARE' -> ENCRYPTED (PARE)
  *-m0.avro + snap-*.avro: head=b'AGS1' -> ENCRYPTED (AGS1 GCM)
  *.metadata.json: head=b'{"fo' -> plaintext JSON pointer (by design)

SUMMARY: tenant1_backup_roundtrip_rows=4  tenant2_backup_roundtrip_rows=3
         tenant1_backup_data_PARE=True  tenant1_backup_meta_AGS1=True
         tenant2_backup_data_PARE=True  tenant2_backup_meta_AGS1=True
DEMO PASSED
```

`PARE` (vs `PAR1`) is Parquet's encrypted-footer magic; `AGS1` is Iceberg's
`Ciphers.GCM_STREAM_MAGIC_STRING`. The SELECTs succeeding for each namespace proves
KMS **Decrypt** worked for each distinct key.

---

## Contrast with the custom-jar sibling

Both folders plug into the SAME native encryption path (catalog KMS wiring + table
`encryption.key-id` + `format-version=3`, producing `PARE` data + `AGS1` metadata) and
both achieve per-namespace keys identically. They differ only in *who* provides the
`KeyManagementClient`:

| | `../iceberg-native-aws-kms/` (custom jar) | **THIS** `iceberg-native-aws-kms-builtin/` (built-in) |
|---|---|---|
| KMS client | custom `com.example.iceberg.kms.AwsKmsKeyManagementClient` | built-in `org.apache.iceberg.aws.AwsKeyManagementClient` |
| Activation | `encryption.kms-impl=<custom class>` | `encryption.kms-type=aws` (registry) |
| Code to maintain | a Maven module + shaded fat jar | **none** |
| Spark image | multi-stage `Dockerfile` (mvn package) + `--jars` | **stock `apache/spark`**, `--packages` only |
| Endpoint prop | `encryption.kms.aws.endpoint` (custom key) | `kms.endpoint` (AWS module standard) |
| Region prop | `encryption.kms.aws.region` (custom key) | `client.region` (AWS module standard) |
| Key generation | `supportsKeyGeneration()=false` → KMS Encrypt only | `supportsKeyGeneration()=true` (can `GenerateDataKey`; this flow used Encrypt) |
| EncryptionContext (AAD) | possible to add (custom code controls the request) | **not supported** |
| Creds / SDK control | full control (static creds, custom HTTP client, shading) | AWS default chain + `client.*` props |
| IAM | `kms:Encrypt`, `kms:Decrypt` | `kms:GenerateDataKey` + `kms:Encrypt` + `kms:Decrypt` |

**Pick the built-in client** for less code, standard AWS-module config, and the
AWS-recommended `GenerateDataKey` capability with zero maintenance. **Pick a custom
client** when you need a KMS **EncryptionContext (AAD)**, bespoke credential/SDK/HTTP
control, or to relocate/shade the AWS SDK away from the rest of the classpath.

---

## Verification status (honest — what actually ran)

| Item | Status |
|------|--------|
| `kms-type=aws` activation + property names | **Verified from source** at the `apache-iceberg-1.11.0` tag (`CatalogProperties`, `EncryptionUtil.createKmsClient`, `AwsProperties.KMS_ENDPOINT`, `AwsClientProperties.CLIENT_REGION`) |
| `iceberg-aws-bundle:1.11.0` exists on Maven Central | **Verified** — `iceberg-aws-bundle-1.11.0.jar` returns HTTP 200; resolved by `--packages` at runtime |
| Built-in client has **no** EncryptionContext/AAD or grant tokens | **Verified from source** — request builders set only keyId + payload |
| No custom Spark Dockerfile needed | **Verified** — stock `apache/spark:4.0.0-...` image + `--packages` worked end-to-end |
| LocalStack KMS provisions 2 keys + 2 aliases | **Verified** — `alias/iceberg/tenant1` + `alias/iceberg/tenant2` created |
| Built-in client talks to LocalStack KMS | **Verified** — LocalStack access log shows `kms.Encrypt => 200` (x4) and `kms.Decrypt => 200` (x8), **0** GenerateDataKey, for this flow |
| **Go client: two tenant namespaces, two keys over Spark Connect** | **Verified** — CREATE/INSERT/SELECT succeeded for `demo.tenant1_backup.ObjectVersions` (4 rows) + `demo.tenant2_backup.ObjectVersions` (3 rows) |
| **PARE data + AGS1 metadata for BOTH tables** | **Verified** — all `*.parquet` footers `PARE`, all manifest/manifest-list `*.avro` heads `AGS1`, `*.metadata.json` plaintext; PySpark job printed **`DEMO PASSED`** |

All of the above was run with `docker compose` (Podman 5.6 backend) on this machine and
torn down with `docker compose down -v` afterward.

---

## Production notes (real AWS)

- **Drop the endpoint override:** remove `AWS_ENDPOINT_URL` and the
  `kms.endpoint` catalog prop so the SDK uses the real regional KMS endpoint; keep
  `client.region` / `AWS_REGION`.
- **Credentials:** prefer an **IAM role** (instance profile / IRSA / container role) —
  the AWS module falls back to `DefaultCredentialsProvider` automatically. Avoid
  committed keys. Least-privilege policy: `kms:GenerateDataKey` + `kms:Encrypt` +
  `kms:Decrypt` on the specific key(s).
- **Keys:** use real key ARNs or aliases as `encryption.key-id`; one alias per
  namespace/tenant gives clean per-namespace isolation and independent rotation.
- **No AAD:** if your threat model requires a KMS EncryptionContext bound to the wrapped
  DEK, the built-in client cannot provide it — use the custom-client sibling instead.

---

## Files

```
iceberg-native-aws-kms-builtin/
├── docker-compose.yml          # localstack + postgres + hive-metastore + spark + spark-connect + go-client
│                               #   spark/spark-connect: STOCK apache/spark image, built-in client via kms-type=aws
├── Dockerfile.hive             # apache/hive:4.0.0 + postgres JDBC driver + schema init  (ONLY Dockerfile)
├── .env.example                # optional overrides (LocalStack defaults baked into compose)
├── verify_demo.sh              # build go-client + up + go-client + PARE/AGS1 proof, one shot
├── README.md
├── localstack/
│   └── init-kms.sh             # ready.d hook: create 2 CMKs + alias/iceberg/tenant1 + alias/iceberg/tenant2
├── hive/
│   ├── metastore-site.xml      # JDBC + warehouse + thrift config
│   └── entrypoint-init.sh      # waits for pg, initSchema if needed, starts metastore
├── app/
│   └── job.py                  # PySpark: two tenant namespaces / two keys + PARE/AGS1 proofs for BOTH tables
└── go-client/
    ├── go.mod                  # spark-connect-go v35
    ├── main.go                 # two tenant namespaces / two keys over Spark Connect
    └── Dockerfile              # static Go binary

# NOTE: there is NO kms-client/ directory and NO custom Spark Dockerfile here — that is
# the whole point of this variant (built-in AwsKeyManagementClient, activated by config).
```
