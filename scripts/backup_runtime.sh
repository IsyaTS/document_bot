#!/usr/bin/env bash
set -euo pipefail

BACKUP_DIR="${1:-backups/runtime}"
TIMESTAMP="$(date -u +%Y%m%dT%H%M%SZ)"
POSTGRES_USER="${POSTGRES_USER:-hermes}"
POSTGRES_DB="${POSTGRES_DB:-hermes_platform}"

mkdir -p "$BACKUP_DIR"
mkdir -p data/runtime_status
OUTPUT_FILE="$BACKUP_DIR/runtime_${TIMESTAMP}.sql.gz"

docker compose exec -T postgres pg_dump \
  -U "$POSTGRES_USER" \
  -d "$POSTGRES_DB" \
  --clean \
  --if-exists \
  --create \
  --no-owner \
  --no-privileges | gzip > "$OUTPUT_FILE"

printf '{"status":"ok","written_at":"%s","backup_path":"%s","database":"%s","postgres_user":"%s"}\n' \
  "$TIMESTAMP" \
  "$OUTPUT_FILE" \
  "$POSTGRES_DB" \
  "$POSTGRES_USER" > data/runtime_status/backup_status.json

echo "$OUTPUT_FILE"
