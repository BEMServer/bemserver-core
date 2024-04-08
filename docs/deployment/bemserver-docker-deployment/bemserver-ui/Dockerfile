FROM python:alpine3.19

RUN pip --no-cache install bemserver_ui gunicorn
COPY create_config.py /install/create_config.py

COPY install.sh /install/install.sh
RUN sed -i 's/\r$//g' /install/install.sh

RUN mkdir /config

COPY app.py /app/app.py

COPY entrypoint.sh /app/entrypoint.sh
RUN sed -i 's/\r$//g' /app/entrypoint.sh

WORKDIR /app

EXPOSE 5001

ENTRYPOINT ["/app/entrypoint.sh"]
