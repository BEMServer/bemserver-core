FROM python:alpine3.19
RUN pip install bemserver_ui
COPY create_config.py /install/create_config.py
COPY install.sh /install/install.sh
RUN mkdir /config
COPY app.py /app/app.py
WORKDIR /app
EXPOSE 5001
ENTRYPOINT FLASK_APP=/app/app.py flask run --host=0.0.0.0 --debug -p 5001
