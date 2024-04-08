FROM python:alpine3.19

RUN apk add --no-cache python3-dev libpq-dev build-base
RUN pip install --no-cache bemserver_api psycopg_binary psycopg2 gunicorn flower

RUN bemserver_db_upgrade

COPY database_setup.sh /install/database_setup.sh
RUN sed -i 's/\r$//g' /install/database_setup.sh

COPY create_config.py /install/create_config.py

COPY install.sh /install/install.sh
RUN sed -i 's/\r$//g' /install/install.sh

RUN mkdir /config

COPY app.py /app/app.py

COPY celery/worker/start.sh /app/start-celery-worker.sh
RUN sed -i 's/\r$//g' /app/start-celery-worker.sh

COPY celery/beat/start.sh /app/start-celery-beat.sh
RUN sed -i 's/\r$//g' /app/start-celery-beat.sh

COPY celery/flower/start.sh /app/start-celery-flower.sh
RUN sed -i 's/\r$//g' /app/start-celery-flower.sh

COPY entrypoint.sh /app/entrypoint.sh
RUN sed -i 's/\r$//g' /app/entrypoint.sh

WORKDIR /app
EXPOSE 5000

ENTRYPOINT ["/app/entrypoint.sh"]
