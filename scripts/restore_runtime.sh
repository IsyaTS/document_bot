#!/usr/bin/env bash
set -euo pipefail

if [[ $# -lt 1 ]]; then
  echo "Usage: $0 <backup.sql.gz>" >&2
  exit 1
fi

BACKUP_FILE="$1"
POSTGRES_USER="${POSTGRES_USER:-hermes}"

if [[ ! -f "$BACKUP_FILE" ]]; then
  echo "Backup file not found: $BACKUP_FILE" >&2
  exit 1
fi

gzip -dc "$BACKUP_FILE" | docker compose exec -T postgres psql -U "$POSTGRES_USER" -d postgres
