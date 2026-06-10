#!/usr/bin/env bash
# Idempotent Hive Metastore startup:
#   1. Wait for Postgres.
#   2. Initialize the metastore schema if it is not already present.
#   3. Exec the stock apache/hive entrypoint to launch the metastore service.
set -euo pipefail

HIVE_HOME="${HIVE_HOME:-/opt/hive}"
SCHEMATOOL="${HIVE_HOME}/bin/schematool"

echo "[init] waiting for postgres:5432 ..."
for i in $(seq 1 30); do
  if (exec 3<>/dev/tcp/postgres/5432) 2>/dev/null; then
    echo "[init] postgres is up"
    break
  fi
  sleep 2
done

echo "[init] checking metastore schema ..."
if "${SCHEMATOOL}" -dbType postgres -info >/dev/null 2>&1; then
  echo "[init] schema already initialized"
else
  echo "[init] initializing metastore schema ..."
  "${SCHEMATOOL}" -dbType postgres -initSchema
fi

# Hand off to the official Hive entrypoint (starts the metastore Thrift server).
# SERVICE_NAME=metastore is provided via compose env.
echo "[init] starting hive metastore service ..."
exec /entrypoint.sh "$@"
