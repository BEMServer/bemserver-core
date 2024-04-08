FROM python:alpine3.19

ARG USER=nonroot
ARG UID=1010
ARG GID=1010
ENV GROUP=${USER}

RUN apk add --no-cache python3-dev libpq-dev build-base

# Create a non-root user
RUN addgroup -g ${GID} ${GROUP} && \
    adduser -u ${UID} -G $GROUP -D -g "" ${USER}
# Add /home/${USER}/.local/bin
ENV PATH=${PATH}:/home/${USER}/.local/bin
# Change user to a non-root user
USER $USER

RUN pip install --no-cache bemserver_api psycopg_binary psycopg2 gunicorn flower

RUN bemserver_db_upgrade

COPY --chown=${USER}:${GROUP} --chmod=500 database_setup.sh /home/${USER}/install/database_setup.sh
RUN sed -i 's/\r$//g' /home/${USER}/install/database_setup.sh

COPY --chown=${USER}:${GROUP} --chmod=500 create_config.py /home/${USER}/install/create_config.py

COPY --chown=${USER}:${GROUP} --chmod=500 install.sh /home/${USER}/install/install.sh
RUN sed -i 's/\r$//g' /home/${USER}/install/install.sh

RUN mkdir /home/${USER}/config

COPY --chown=${USER}:${GROUP} --chmod=500 app.py /home/${USER}/app/app.py

COPY --chown=${USER}:${GROUP} --chmod=500 celery/worker/start.sh /home/${USER}/app/start-celery-worker.sh
RUN sed -i 's/\r$//g' /home/${USER}/app/start-celery-worker.sh

COPY --chown=${USER}:${GROUP} --chmod=500 celery/beat/start.sh /home/${USER}/app/start-celery-beat.sh
RUN sed -i 's/\r$//g' /home/${USER}/app/start-celery-beat.sh

COPY --chown=${USER}:${GROUP} --chmod=500 celery/flower/start.sh /home/${USER}/app/start-celery-flower.sh
RUN sed -i 's/\r$//g' /home/${USER}/app/start-celery-flower.sh

COPY --chown=${USER}:${GROUP} --chmod=500 entrypoint.sh /home/${USER}/app/entrypoint.sh
RUN sed -i 's/\r$//g' /home/${USER}/app/entrypoint.sh

WORKDIR /home/${USER}/app
EXPOSE 5000

ENTRYPOINT ["/home/nonroot/app/entrypoint.sh"]
