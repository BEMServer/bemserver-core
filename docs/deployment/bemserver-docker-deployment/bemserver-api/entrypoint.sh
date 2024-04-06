#!/usr/bin/env sh

celery worker &

celery beat &

FLASK_APP=/app/app.py flask run --host=0.0.0.0 --debug
