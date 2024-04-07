#!/usr/bin/env sh

echo "=== create hypertable ==="
psql -U $POSTGRES_USER -d $POSTGRES_DBNAME -c "SELECT create_hypertable('ts_data', 'timestamp')"
