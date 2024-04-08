FROM python:alpine3.19

ARG USER=nonroot
ARG UID=1010
ARG GID=1010
ENV GROUP=${USER}

# Create a non-root user
RUN addgroup -g ${GID} ${GROUP} && \
    adduser -u ${UID} -G $GROUP -D -g "" ${USER}
# Add /home/${USER}/.local/bin
ENV PATH=${PATH}:/home/${USER}/.local/bin
# Change user to a non-root user
USER $USER

RUN pip --no-cache install bemserver_ui gunicorn

COPY --chown=${USER}:${GROUP} create_config.py /home/${USER}/install/create_config.py

COPY --chown=${USER}:${GROUP} install.sh /home/${USER}/install/install.sh
RUN sed -i 's/\r$//g' /home/${USER}/install/install.sh

RUN mkdir /home/${USER}/config

COPY --chown=${USER}:${GROUP} app.py /home/${USER}/app/app.py

COPY --chown=${USER}:${GROUP} entrypoint.sh /home/${USER}/app/entrypoint.sh
RUN sed -i 's/\r$//g' /home/${USER}/app/entrypoint.sh

WORKDIR /home/${USER}/app

EXPOSE 5001

ENTRYPOINT ["/home/nonroot/app/entrypoint.sh"]
