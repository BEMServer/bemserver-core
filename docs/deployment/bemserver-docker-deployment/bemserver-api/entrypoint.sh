#!/usr/bin/env sh

if [ "$FLASK_ENV" = "production" ]; then
    if [ "$BEMSERVER_API_USE_SSL" -eq 1 ]; then
        echo "Run app using gunicorn with SSL"
        gunicorn --bind 0.0.0.0:5000 --certfile=/etc/ssl/server.crt --keyfile=/etc/ssl/server.key "app:create_app()"
    else
        echo "Run app gunicorn without SSL"
        gunicorn --bind 0.0.0.0:5000 "app:create_app()"
    fi
else
    echo "App is not running in production mode."
    flask run
fi
