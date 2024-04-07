FROM python:alpine3.19

RUN pip --no-cache install bemserver_ui gunicorn
COPY create_config.py /install/create_config.py

COPY install.sh /install/install.sh
RUN sed -i 's/\r$//g' /install/install.sh

RUN mkdir /config

COPY app.py /app/app.py

WORKDIR /app

EXPOSE 5001

#ENTRYPOINT ["flask", "run"]
CMD ["gunicorn", "--bind", "0.0.0.0:5001", "app:create_app()"]
#CMD ["gunicorn", "--bind", "0.0.0.0:5001", "--certfile=/etc/ssl/server.crt", "--keyfile=/etc/ssl/server.key", "app:create_app()"]
