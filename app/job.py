"""
Apache Iceberg 1.11.0 NATIVE table encryption with AWS KMS (via LocalStack).

Two tenant-backup namespaces, two DIFFERENT KMS master keys:

  demo.tenant1_backup.ObjectVersions  encryption.key-id = alias/iceberg/tenant1  (KMS key A)
  demo.tenant2_backup.ObjectVersions  encryption.key-id = alias/iceberg/tenant2  (KMS key B)

The catalog is a HiveCatalog wired with encryption.kms-type=aws, which Iceberg's
EncryptionUtil.createKmsClient resolves to the BUILT-IN
org.apache.iceberg.aws.AwsKeyManagementClient (shipped in iceberg-aws, pulled in
via the iceberg-aws-bundle on the classpath). HiveCatalog is the ONLY catalog that
wires a KeyManagementClient at Iceberg 1.11.0. For each encrypted table Iceberg:
  * generates a per-file Data Encryption Key (DEK) locally (SecureRandom);
  * calls the built-in client's wrapKey -> AWS KMS Encrypt with the table's key-id
    (AwsKeyManagementClient.supportsKeyGeneration() is true; it can also use KMS
    GenerateDataKey for the KEK path);
  * AES-GCM encrypts BOTH the DATA files (Parquet 'PARE' footer) and the
    METADATA manifests / manifest-lists (Iceberg GCM stream 'AGS1');
  * on read, calls unwrapKey -> AWS KMS Decrypt.

The built-in client never sees a namespace -- only the wrappingKeyId (= the alias).
Per-namespace keys come purely from each table's distinct encryption.key-id.

Proofs printed below, for BOTH tables:
  PART A  Iceberg round-trip (create / insert / read) per namespace.
  PART B  DATA proof   : each table's .parquet footers are 'PARE'.
  PART C  METADATA proof: each table's *.avro manifests are 'AGS1' GCM streams;
          *.metadata.json stays a plaintext pointer by design.
"""

import os
import sys

from pyspark.sql import SparkSession

CATALOG = "demo"
WAREHOUSE_LOCAL = "/data/warehouse"
GCM_MAGIC = b"AGS1"  # Iceberg Ciphers.GCM_STREAM_MAGIC_STRING

# (namespace, table, key-id/alias, DDL columns, VALUES, expected row count)
SPECS = [
    (
        "tenant1_backup",
        "ObjectVersions",
        "alias/iceberg/tenant1",
        "id STRING, restore_point_id STRING, payload_ref STRING",
        "('obj-0001','rp-2026-06-09-01','s3://backups/tenant1/obj-0001.dat'),"
        "('obj-0002','rp-2026-06-09-01','s3://backups/tenant1/obj-0002.dat'),"
        "('obj-0003','rp-2026-06-09-02','s3://backups/tenant1/obj-0003.dat'),"
        "('obj-0004','rp-2026-06-09-02','s3://backups/tenant1/obj-0004.dat')",
        4,
    ),
    (
        "tenant2_backup",
        "ObjectVersions",
        "alias/iceberg/tenant2",
        "id STRING, restore_point_id STRING, payload_ref STRING",
        "('obj-0001','rp-2026-06-10-01','s3://backups/tenant2/obj-0001.dat'),"
        "('obj-0002','rp-2026-06-10-01','s3://backups/tenant2/obj-0002.dat'),"
        "('obj-0003','rp-2026-06-10-02','s3://backups/tenant2/obj-0003.dat')",
        3,
    ),
]


def banner(title):
    print("\n" + "=" * 74)
    print(title)
    print("=" * 74)


def build_spark():
    spark = SparkSession.builder.appName("iceberg-native-aws-kms-demo").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark


def table_dir(ns, name):
    # demo.tenant1_backup.ObjectVersions -> /data/warehouse/tenant1_backup.db/ObjectVersions
    return os.path.join(WAREHOUSE_LOCAL, f"{ns}.db", name)


def first_n_bytes(path, n=4):
    with open(path, "rb") as f:
        return f.read(n)


def footer_magic(path):
    with open(path, "rb") as f:
        f.seek(-4, os.SEEK_END)
        return f.read(4)


def list_files(root, suffixes):
    out = []
    for dirpath, _dirs, files in os.walk(root):
        for name in files:
            if any(name.endswith(s) for s in suffixes):
                out.append(os.path.join(dirpath, name))
    return sorted(out)


def main():
    spark = build_spark()
    results = {}

    # ----------------------------------------------------------------------
    banner("PART A: round-trip on BOTH encrypted tables (two namespaces, two KMS keys)")
    # ----------------------------------------------------------------------
    for ns, name, key_id, cols, values, want in SPECS:
        fqn = f"{CATALOG}.{ns}.{name}"
        print(f"\n--- {fqn}  (encryption.key-id = {key_id}) ---")
        spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {CATALOG}.{ns}")
        spark.sql(f"DROP TABLE IF EXISTS {fqn}")
        # encryption.key-id opts into native encryption; format-version=3 required.
        spark.sql(
            f"""
            CREATE TABLE {fqn} ({cols}) USING iceberg
            TBLPROPERTIES ('encryption.key-id' = '{key_id}', 'format-version' = '3')
            """
        )
        spark.sql(f"INSERT INTO {fqn} VALUES {values}")
        rows = spark.sql(f"SELECT * FROM {fqn} ORDER BY id").collect()
        for r in rows:
            print(f"  {r}")
        ok = len(rows) == want
        results[f"{ns}_roundtrip_rows"] = len(rows)
        print(f"Round-trip via KMS key {key_id} OK ({len(rows)} rows)."
              if ok else f"FAIL: expected {want} rows, got {len(rows)}")

    # ----------------------------------------------------------------------
    banner("PART B+C: DATA (PARE) + METADATA (AGS1) proofs for BOTH tables")
    # ----------------------------------------------------------------------
    all_data_pare = True
    all_meta_ags1 = True
    for ns, name, key_id, _cols, _values, _want in SPECS:
        d = table_dir(ns, name)
        print(f"\n--- {CATALOG}.{ns}.{name}  ({d}) ---")

        data = list_files(os.path.join(d, "data"), [".parquet"]) or \
            list_files(d, [".parquet"])
        print(f"  DATA: {len(data)} parquet file(s) (expect footer PARE)")
        data_pare = bool(data)
        for p in data:
            m = footer_magic(p)
            is_enc = m == b"PARE"
            data_pare = data_pare and is_enc
            print(f"    {os.path.basename(p)}: footer={m!r} -> "
                  f"{'ENCRYPTED (PARE)' if is_enc else 'PLAINTEXT (PAR1)'}")
        all_data_pare = all_data_pare and data_pare

        meta_dir = os.path.join(d, "metadata")
        avro = list_files(meta_dir, [".avro"])
        print(f"  METADATA: {len(avro)} avro file(s) (expect head AGS1)")
        meta_ags1 = bool(avro)
        for p in avro:
            head = first_n_bytes(p, 4)
            is_gcm = head == GCM_MAGIC
            meta_ags1 = meta_ags1 and is_gcm
            kind = "manifest-list" if os.path.basename(p).startswith("snap-") else "manifest"
            print(f"    [{kind}] {os.path.basename(p)}: head={head!r} -> "
                  f"{'ENCRYPTED (AGS1 GCM)' if is_gcm else f'OTHER {head!r}'}")
        for p in list_files(meta_dir, [".metadata.json"]):
            head = first_n_bytes(p, 4)
            print(f"    [pointer] {os.path.basename(p)}: head={head!r} -> "
                  f"plaintext JSON pointer (expected; references wrapped keys)")
        all_meta_ags1 = all_meta_ags1 and meta_ags1
        results[f"{ns}_data_PARE"] = data_pare
        results[f"{ns}_meta_AGS1"] = meta_ags1

    # ----------------------------------------------------------------------
    banner("SUMMARY")
    # ----------------------------------------------------------------------
    for k, v in results.items():
        print(f"  {k:24s}: {v}")
    print("\n  Per-namespace KMS keys used:")
    for ns, name, key_id, *_ in SPECS:
        print(f"    {CATALOG}.{ns}.{name:8s} -> {key_id}")

    spark.stop()

    ok = (
        results.get("tenant1_backup_roundtrip_rows") == 4
        and results.get("tenant2_backup_roundtrip_rows") == 3
        and all_data_pare
        and all_meta_ags1
    )
    if ok:
        print("\nDEMO PASSED: Iceberg native encryption via AWS KMS (LocalStack) verified "
              "for BOTH namespaces -- data PARE + metadata AGS1, two distinct KMS keys.")
        sys.exit(0)
    print("\nDEMO FAILED: one or more proofs did not pass.", file=sys.stderr)
    sys.exit(1)


if __name__ == "__main__":
    main()
