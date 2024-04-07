#!/usr/bin/env bash

echo "=== createdb '$BEMSERVER_DBNAME' with user '$POSTGRES_USER' ==="
createdb -U $POSTGRES_USER $BEMSERVER_DBNAME

echo "=== create extension ==="
psql -U $POSTGRES_USER -d $BEMSERVER_DBNAME -c "CREATE EXTENSION IF NOT EXISTS timescaledb"
