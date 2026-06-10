#!/usr/bin/env bash
# Convenience driver for the Iceberg-native + BUILT-IN-AWS-KMS-via-LocalStack demo.
#
#   1. builds the go-client image (spark/spark-connect use the STOCK spark image,
#      no custom jar -- the built-in AwsKeyManagementClient ships in iceberg-aws-bundle),
#   2. brings up localstack (init script provisions alias/iceberg/tenant1 + alias/iceberg/tenant2),
#      postgres, hive-metastore, and the Spark Connect server,
#   3. waits until spark-connect is healthy (listening on :15002),
#   4. runs the external Go client: TWO tenant-backup namespaces, TWO different KMS keys
#      (CREATE / batched INSERT / SELECT for demo.tenant1_backup.ObjectVersions + demo.tenant2_backup.ObjectVersions),
#   5. proves BOTH tables are encrypted in the warehouse volume:
#        - data  .parquet footers == 'PARE'  (Parquet modular encryption)
#        - manifest/manifest-list .avro head == 'AGS1' (Iceberg GCM stream)
#      while *.metadata.json stays a plaintext pointer (by design).
#
# Run from this directory:  ./verify_demo.sh
set -euo pipefail
cd "$(dirname "$0")"

echo "==> building images"
docker compose build go-client

echo "==> starting localstack + postgres + hive-metastore + spark-connect"
docker compose up -d spark-connect

echo "==> KMS keys provisioned by LocalStack:"
docker compose exec -T localstack awslocal kms list-aliases --region us-east-1 \
  --query "Aliases[?starts_with(AliasName, 'alias/iceberg/')].[AliasName,TargetKeyId]" --output text

echo "==> waiting for spark-connect to become healthy"
for i in $(seq 1 60); do
  status=$(docker inspect --format '{{.State.Health.Status}}' iceberg-awskms-builtin-spark-connect 2>/dev/null || echo "starting")
  echo "    [$i] spark-connect health: $status"
  [ "$status" = "healthy" ] && break
  sleep 5
done
[ "$status" = "healthy" ] || { echo "spark-connect never became healthy"; docker compose logs --tail=80 spark-connect; exit 1; }

echo "==> running the Go client (two namespaces, two KMS keys)"
docker compose run --rm go-client

echo
echo "==> ENCRYPTION PROOF for BOTH tables (inspected server-side in the warehouse volume)"
docker compose exec -T spark-connect bash -c '
ok=1
for T in /data/warehouse/tenant1_backup.db/ObjectVersions /data/warehouse/tenant2_backup.db/ObjectVersions; do
  echo "================ $T ================"
  echo "--- DATA files (expect footer PARE) ---"
  for f in $(find "$T/data" -name "*.parquet" 2>/dev/null | sort); do
    m=$(tail -c 4 "$f" | tr -d "\0")
    if [ "$m" = "PARE" ]; then v="ENCRYPTED (PARE)"; else v="PLAINTEXT/OTHER ($m)"; ok=0; fi
    echo "  $(basename "$f"): footer=$m -> $v"
  done
  echo "--- METADATA .avro (expect head AGS1) ---"
  for f in $(find "$T/metadata" -name "*.avro" 2>/dev/null | sort); do
    h=$(head -c 4 "$f" | tr -d "\0")
    if [ "$h" = "AGS1" ]; then v="ENCRYPTED (AGS1 GCM)"; else v="PLAINTEXT/OTHER ($h)"; ok=0; fi
    echo "  $(basename "$f"): head=$h -> $v"
  done
done
if [ "$ok" = "1" ]; then echo "ENCRYPTION PROOF: PASS (both tables: data PARE + manifests AGS1)"; else echo "ENCRYPTION PROOF: FAIL"; exit 1; fi
'
echo
echo "Demo OK. Optionally run the PySpark equivalent: docker compose run --rm spark"
echo "Tear down with: docker compose down -v"
