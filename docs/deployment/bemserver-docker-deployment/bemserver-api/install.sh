#!/usr/bin/env sh

echo "=== create config ==="
python /install/create_config.py

echo "=== database setup ==="
/install/database_setup.sh
