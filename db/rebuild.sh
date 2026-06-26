#!/usr/bin/env bash
# =============================================================================
# rebuild.sh — apply rosmerta's full schema to a PostgreSQL database, in order.
# =============================================================================
# Runs every DDL file (and only the DDL files) needed to reproduce the live
# `stocks` schema, in dependency order. init.sql must run first because the
# other four files reference/alter the tables it creates; the three ALTERs use
# IF NOT EXISTS, so re-running this script against an existing DB is safe.
#
# Connection settings come from the project's .env (same vars db_config.py
# reads): DB_HOST, DB_PORT, DB_NAME, DB_USER, DB_PASS. Override any of them on
# the command line, e.g.:
#
#   ./db/rebuild.sh                       # use .env as-is
#   DB_NAME=stocks_dev ./db/rebuild.sh    # target the dev database instead
#   DB_HOST=localhost ./db/rebuild.sh
#
# This script does NOT create the database or role, and does NOT drop anything
# (init.sql is not idempotent — point it at a fresh/empty database). To create
# an empty database + owner role first, run something like:
#
#   createdb -h <host> -U postgres -O stock_user stocks
#
# =============================================================================
set -euo pipefail

# Resolve paths relative to this script so it works from any cwd.
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"

# Load .env (DB_* vars) without clobbering values already set in the environment.
ENV_FILE="$ROOT_DIR/.env"
if [[ -f "$ENV_FILE" ]]; then
  while IFS='=' read -r key val; do
    [[ "$key" =~ ^[A-Za-z_][A-Za-z0-9_]*$ ]] || continue   # skip blanks/comments
    [[ -n "${!key:-}" ]] && continue                       # don't override the environment
    printf -v "$key" '%s' "$val"
    export "$key"
  done < "$ENV_FILE"
fi

DB_HOST="${DB_HOST:-10.0.0.1}"
DB_PORT="${DB_PORT:-5432}"
DB_NAME="${DB_NAME:-stocks}"
DB_USER="${DB_USER:-stock_user}"
export PGPASSWORD="${DB_PASS:-}"

# DDL files in dependency order. init.sql FIRST; the rest only need init's tables.
FILES=(
  "db/init.sql"                                    # base tables, views, trigger
  "db/add_watchlists.sql"                          # watchlist_members + v_watchlists
  "db/extend_dividents.sql"                        # ALTER dividends (idempotent)
  "db_scripts/migrations/add_rnd_sga_columns.sql"  # ALTER fundamentals (idempotent)
  "db_scripts/migrations/migrate_finanancial_columns.sql"  # ALTER fundamentals (idempotent)
)

echo "Applying rosmerta schema → ${DB_USER}@${DB_HOST}:${DB_PORT}/${DB_NAME}"
echo

for rel in "${FILES[@]}"; do
  path="$ROOT_DIR/$rel"
  if [[ ! -f "$path" ]]; then
    echo "ERROR: missing DDL file: $rel" >&2
    exit 1
  fi
  echo "→ $rel"
  psql -v ON_ERROR_STOP=1 \
       -h "$DB_HOST" -p "$DB_PORT" -U "$DB_USER" -d "$DB_NAME" \
       -q -f "$path"
done

echo
echo "Done — all 5 DDL files applied successfully."
