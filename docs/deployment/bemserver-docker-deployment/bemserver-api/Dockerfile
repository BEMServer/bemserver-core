FROM python:alpine3.19
RUN apk add --no-cache python3-dev libpq-dev build-base
RUN pip install bemserver_api psycopg_binary psycopg2 flower
RUN bemserver_db_upgrade
COPY database_setup.sh /install/database_setup.sh
COPY create_config.py /install/create_config.py
COPY install.sh /install/install.sh
RUN mkdir /config
COPY app.py /app/app.py
COPY entrypoint.sh /app/entrypoint.sh
WORKDIR /app
EXPOSE 5000
# Expose Celery Flower
# EXPOSE 5555
#ENTRYPOINT FLASK_APP=/app/app.py flask run --host=0.0.0.0 --debug
ENTRYPOINT ["/app/entrypoint.sh"]
