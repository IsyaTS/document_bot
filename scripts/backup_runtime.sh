#!/usr/bin/env bash
set -euo pipefail

BACKUP_DIR="${1:-backups/runtime}"
TIMESTAMP="$(date -u +%Y%m%dT%H%M%SZ)"
POSTGRES_USER="${POSTGRES_USER:-hermes}"
POSTGRES_DB="${POSTGRES_DB:-hermes_platform}"

mkdir -p "$BACKUP_DIR"
OUTPUT_FILE="$BACKUP_DIR/runtime_${TIMESTAMP}.sql.gz"

docker compose exec -T postgres pg_dump \
  -U "$POSTGRES_USER" \
  -d "$POSTGRES_DB" \
  --clean \
  --if-exists \
  --create \
  --no-owner \
  --no-privileges | gzip > "$OUTPUT_FILE"

echo "$OUTPUT_FILE"
