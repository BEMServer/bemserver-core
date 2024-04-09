# BEMServer - docker development install

WIP

This configuration is for a development server.

This is currently not suitable for production!

## Installation

### Environment variables

Copy `.env.default` to `.env` and change values to your own settings.

### Create SSL certificate

Install [mkcert](https://github.com/FiloSottile/mkcert).

mkcert is a simple tool for making locally-trusted development certificates. It requires no configuration. It's only for development.

    $ cd certs
    $ mkcert -install bemserver.localhost

You should get 2 files named `bemserver.localhost-key.pem` and `bemserver.localhost.pem`.

### TimescaleDB

Run TimescaleDB (a PostgreSQL extension for timeseries) and pgAdmin (web interface for PostgreSQL database)

    $ docker compose up -d timescaledb pgadmin

Create database `bemserver`

    $ docker compose exec timescaledb /install/create_db.sh

With pgAdmin, you can have a look at created database browsing https://127.0.0.1:8888

### Redis

Run Redis

    $ docker compose up -d redis-stack

Enter into running container to verify auth

    $ docker compose exec redis-stack /bin/sh
    # redis-cli

or directly

    $ docker compose exec redis-stack redis-cli
    127.0.0.1:6379> AUTH redisuser password
    OK
    127.0.0.1:6379> ACL LIST
    1) "user default off nopass sanitize-payload resetchannels -@all"
    2) "user redisuser on sanitize-payload #5e884898da28047151d0e56f8dc6292773603d0d6aabbdd62a11ef721d1542d8 ~* &* +@all"

You can also browse Redis Insight at http://127.0.0.1:8001

### BEMServer API

Build image

    $ docker compose build bemserver-api

Create config file `bemserver-core-settings.py`

    $ docker compose run --rm --entrypoint="" --env USER=nonroot bemserver-api /home/nonroot/install/install.sh

this should display

    === create config ===
    === database setup ===
    === bemserver_db_upgrade ===
    INFO  [alembic.runtime.migration] Context impl PostgresqlImpl.
    INFO  [alembic.runtime.migration] Will assume transactional DDL.
    INFO  [alembic.runtime.migration] Running upgrade  -> 0.1, v0.1
    INFO  [alembic.runtime.migration] Running upgrade 0.1 -> 0.3, v0.3
    INFO  [alembic.runtime.migration] Running upgrade 0.3 -> 0.3.1, v0.3.1
    INFO  [alembic.runtime.migration] Running upgrade 0.3.1 -> 0.4, v0.4
    INFO  [alembic.runtime.migration] Running upgrade 0.4 -> 0.6, v0.6
    INFO  [alembic.runtime.migration] Running upgrade 0.6 -> 0.8, v0.8
    INFO  [alembic.runtime.migration] Running upgrade 0.8 -> 0.10, v0.10
    INFO  [alembic.runtime.migration] Running upgrade 0.10 -> 0.11, v0.11
    INFO  [alembic.runtime.migration] Running upgrade 0.11 -> 0.12, v0.12
    INFO  [alembic.runtime.migration] Running upgrade 0.12 -> 0.13, v0.13
    INFO  [alembic.runtime.migration] Running upgrade 0.13 -> 0.14, v0.14
    INFO  [alembic.runtime.migration] Running upgrade 0.14 -> 0.15, v0.15

and ask for password and confirmation

    === bemserver_create_user ===
    Password:
    Repeat for confirmation:

Create shell in a new container from `bemserver-api` image to show config

    $ docker compose run --rm --entrypoint="" bemserver-api cat /home/nonroot/config/bemserver-core-settings.py
    SQLALCHEMY_DATABASE_URI="postgresql+psycopg2://timescaledb:password@timescaledb:5432/bemserver"  # this is just an example
    WEATHER_DATA_CLIENT_API_KEY="apikey"

Create hypertable

    $ docker compose exec timescaledb /install/create_hypertable.sh

Run BEMServer API

    $ docker compose up -d bemserver-api

Browse to BEMServer API web user interface at http://127.0.0.1:5000 (or https://)

### BEMServer UI

Build image

    $ docker compose build bemserver-ui

Create config file `bemserver-ui.cfg`

    $ docker compose run --rm --entrypoint="" --env USER=nonroot bemserver-ui /home/nonroot/install/install.sh

Create shell in a new container from `bemserver-ui` image to see config

    $ docker compose run --rm --entrypoint="" --env USER=nonroot bemserver-ui cat /home/nonroot/config/bemserver-ui.cfg
    BEMSERVER_API_HOST="bemserver-api:5000"
    BEMSERVER_API_USE_SSL=False  # This is just for testing...
    SECRET_KEY="c55...9c5"

Run BEMServer UI

    $ docker compose up -d bemserver-ui

Browse to BEMServer web user interface at http://127.0.0.1:5001/ (or https://) and log with `BEMSERVER_EMAIL` (default was admin@domain.com) and password defined previously in `bemserver_create_user` step.

### Scheduled Tasks

#### Configure tasks

Modify `bemserver_api/config/bemserver-celery-settings.py`

#### Launch celery worker

    $ docker compose up -d celery-worker

Display worker logs

    $ docker compose logs celery-worker
    -------------- celery@53ea74e281d4 v5.3.6 (emerald-rush)
    --- ***** -----
    -- ******* ---- Linux-5.15.146.1-microsoft-standard-WSL2-x86_64-with 2024-04-05 19:06:39
    - *** --- * ---
    - ** ---------- [config]
    - ** ---------- .> app:         BEMServer Core:0x7fb9d1830dd0
    - ** ---------- .> transport:   redis://default:**@redis-stack:6379/0
    - ** ---------- .> results:     redis://default:**@redis-stack:6379/0
    - *** --- * --- .> concurrency: 8 (prefork)
    -- ******* ---- .> task events: OFF (enable -E to monitor tasks in this worker)
    --- ***** -----
    -------------- [queues]
                    .> bemserver_core   exchange=bemserver_core(direct) key=bemserver_core

#### Launch celery beat

    $ docker compose up -d celery-beat

Display celery beat logs

    $ docker compose logs celery-beat

#### Celery Flower (optional)

Celery Flower https://flower.readthedocs.io/ is a web UI to show tasks

    $ docker compose up -d celery-flower

Browse Flower web user interface at https://127.0.0.1:5555/ log with `CELERY_FLOWER_USERNAME` / `CELERY_FLOWER_PASSWORD` (default is `admin` / `password`).
