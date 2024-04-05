# BEMServer - docker development install

WIP

This configuration is for a development server.

This is currently not suitable for production!

## Installation

### Environment variables

Copy `.env.default` to `.env` and change values to your own settings.

### TimescaleDB

Run TimescaleDB and pgAdmin

    $ docker compose up -d timescaledb pgadmin

Create database `bemserver`

    $ docker compose exec timescaledb /install/create_db.sh
    
### Redis

Run Redis

    $ docker compose up -d redis-stack

### BEMServer API

Build image

    $ docker compose build bemserver-api

Create config file `bemserver-core-settings.py`

    $ docker compose run --rm --entrypoint="" bemserver-api /install/install.sh

this should display

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

Enter container?

    $ docker compose run --rm --entrypoint="" bemserver-api /bin/sh
    # cat /config/bemserver-core-settings.py
    SQLALCHEMY_DATABASE_URI="postgresql+psycopg2://timescaledb:password@timescaledb:5432/bemserver"  # this is just an example
    WEATHER_DATA_CLIENT_API_KEY="apikey"

Run BEMServer API

    $ docker compose up -d bemserver-api

### BEMServer UI

Build image

    $ docker compose build bemserver-ui

Create config file `bemserver-ui.cfg`

    $ docker compose run --rm --entrypoint="" bemserver-ui /install/install.sh

Enter container?

    $ docker compose run --rm --entrypoint="" bemserver-ui /bin/sh
    # cat /config/bemserver-ui.cfg
    BEMSERVER_API_HOST="bemserver-api:5000"
    BEMSERVER_API_USE_SSL=False  # This is just for testing...
    SECRET_KEY="c55...9c5"

Run BEMServer UI

    $ docker compose up -d bemserver-ui
