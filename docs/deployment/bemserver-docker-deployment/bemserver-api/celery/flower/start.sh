#!/usr/bin/env sh

celery flower --loglevel INFO --basic-auth="$CELERY_FLOWER_USERNAME:$CELERY_FLOWER_PASSWORD" --keyfile=/etc/ssl/server.key --certfile=/etc/ssl/server.crt
