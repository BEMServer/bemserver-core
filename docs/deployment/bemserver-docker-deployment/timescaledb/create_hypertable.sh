#!/usr/bin/env sh

dbname="bemserver"

echo "=== create hypertable ==="
psql -U $POSTGRES_USER -d $dbname -c "SELECT create_hypertable('ts_data', 'timestamp')"
