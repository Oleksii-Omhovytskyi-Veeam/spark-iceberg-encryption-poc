#!/usr/bin/env bash
# LocalStack KMS init hook.
#
# Mounted at /etc/localstack/init/ready.d/init-kms.sh. LocalStack 4 runs every
# executable script under ready.d/ ONCE, after the requested services are up
# (here SERVICES=kms). It provisions the two symmetric KMS master keys this demo
# uses -- one per tenant backup namespace -- and gives each a stable,
# human-readable ALIAS that the tables reference as their encryption.key-id:
#
#   alias/iceberg/tenant1 -> demo.tenant1_backup.ObjectVersions (encryption.key-id = alias/iceberg/tenant1)
#   alias/iceberg/tenant2 -> demo.tenant2_backup.ObjectVersions (encryption.key-id = alias/iceberg/tenant2)
#
# NOTE: AWS KMS alias names MUST start with the literal "alias/" prefix, so the
# per-tenant key for "iceberg/tenant1" is the alias "alias/iceberg/tenant1".
#
# The built-in AwsKeyManagementClient never sees a namespace -- only the
# wrappingKeyId (= the alias). Different tenants therefore wrap their DEKs with
# DIFFERENT KMS master keys purely because each table was created with a different
# encryption.key-id.
#
# `awslocal` is the AWS CLI pre-wired to the LocalStack endpoint; it ships in the
# localstack/localstack:4 image. The script is idempotent-ish: creating an alias
# that already exists is treated as "already provisioned" so re-runs do not fail.
set -uo pipefail

REGION="${AWS_DEFAULT_REGION:-us-east-1}"

log() { echo "[init-kms] $*"; }

# Create a symmetric encrypt/decrypt CMK and point an alias at it. If the alias
# already exists (e.g. a persisted LocalStack volume), reuse the existing key.
provision_key() {
  local alias_name="$1"     # e.g. alias/iceberg/tenant1
  local description="$2"

  local existing
  existing="$(awslocal kms list-aliases --region "$REGION" \
      --query "Aliases[?AliasName=='${alias_name}'].TargetKeyId | [0]" \
      --output text 2>/dev/null)"

  if [ -n "$existing" ] && [ "$existing" != "None" ]; then
    log "alias ${alias_name} already exists -> key ${existing} (reusing)"
    return 0
  fi

  local key_id
  key_id="$(awslocal kms create-key \
      --region "$REGION" \
      --description "$description" \
      --key-usage ENCRYPT_DECRYPT \
      --key-spec SYMMETRIC_DEFAULT \
      --query 'KeyMetadata.KeyId' --output text)"

  if [ -z "$key_id" ] || [ "$key_id" = "None" ]; then
    log "ERROR: failed to create key for ${alias_name}"
    return 1
  fi

  awslocal kms create-alias \
      --region "$REGION" \
      --alias-name "$alias_name" \
      --target-key-id "$key_id"

  log "created ${alias_name} -> key ${key_id}"
}

log "provisioning per-tenant KMS master keys in region ${REGION} ..."
provision_key "alias/iceberg/tenant1" "Iceberg native encryption KEK for namespace demo.tenant1_backup (tenant1 backups)"
provision_key "alias/iceberg/tenant2" "Iceberg native encryption KEK for namespace demo.tenant2_backup (tenant2 backups)"

log "current KMS aliases:"
awslocal kms list-aliases --region "$REGION" \
    --query "Aliases[?starts_with(AliasName, 'alias/iceberg/')].[AliasName,TargetKeyId]" \
    --output text | sed 's/^/[init-kms]   /'

log "KMS init complete."
