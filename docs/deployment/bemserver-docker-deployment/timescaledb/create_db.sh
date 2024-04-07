#!/usr/bin/env bash

echo "=== createdb '$dbname' with user '$POSTGRES_USER' ==="
createdb -U $POSTGRES_USER $POSTGRES_DBNAME

echo "=== create extension ==="
psql -U $POSTGRES_USER -d $POSTGRES_DBNAME -c "CREATE EXTENSION IF NOT EXISTS timescaledb"
