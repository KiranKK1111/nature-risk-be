#!/bin/bash
# ============================================================================
# NatureRisk Database Setup Script
# ============================================================================
# Prerequisites:
#   - PostgreSQL installed and running
#   - psql command available
#   - Python 3 with psycopg2 installed
#
# Usage:
#   chmod +x database/setup.sh
#   ./database/setup.sh
#
# Environment variables (optional):
#   PGHOST     - PostgreSQL host (default: localhost)
#   PGPORT     - PostgreSQL port (default: 5432)
#   PGUSER     - PostgreSQL superuser (default: postgres)
#   PGPASSWORD - PostgreSQL password
#   DATABASE_URL - Full connection string for Python scripts
# ============================================================================

set -e

DB_NAME="naturerisk"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

echo "============================================"
echo "  NatureRisk Database Setup"
echo "============================================"

# Step 1: Create database
echo ""
echo "[1/4] Creating database '$DB_NAME'..."
psql -U "${PGUSER:-postgres}" -tc "SELECT 1 FROM pg_database WHERE datname = '$DB_NAME'" | grep -q 1 || \
    psql -U "${PGUSER:-postgres}" -f "$SCRIPT_DIR/00_create_database.sql"
echo "  Done."

# Step 2: Create schema
echo ""
echo "[2/4] Creating schema..."
psql -U "${PGUSER:-postgres}" -d "$DB_NAME" -f "$SCRIPT_DIR/01_schema.sql"
echo "  Done."

# Step 3: Load seed data
echo ""
echo "[3/4] Loading seed data..."
psql -U "${PGUSER:-postgres}" -d "$DB_NAME" -f "$SCRIPT_DIR/02_seed_data.sql"
echo "  Done."

# Step 4: Load TopoJSON data
echo ""
echo "[4/4] Loading TopoJSON data into database..."
export DATABASE_URL="${DATABASE_URL:-postgresql://${PGUSER:-postgres}:${PGPASSWORD:-postgres}@${PGHOST:-localhost}:${PGPORT:-5432}/$DB_NAME}"
cd "$SCRIPT_DIR/.."
python database/03_load_topojson.py
echo "  Done."

echo ""
echo "============================================"
echo "  Setup complete!"
echo "  Database: $DB_NAME"
echo "  Connection: $DATABASE_URL"
echo "============================================"
echo ""
echo "Verify with: psql -d $DB_NAME -c '\\dt'"
