#!/usr/bin/env sh

echo "=== bemserver_db_upgrade ==="
bemserver_db_upgrade

echo "=== bemserver_create_user ==="
bemserver_create_user --name $BEMSERVER_NAME --email $BEMSERVER_EMAIL --admin
