#!/usr/bin/env sh

celery flower --loglevel INFO --basic-auth="$CELERY_FLOWER_USERNAME:$CELERY_FLOWER_PASSWORD"
