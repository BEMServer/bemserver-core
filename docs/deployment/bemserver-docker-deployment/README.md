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

    $ docker compose run --rm --entrypoint="" bemserver-api /install/install.sh

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

    $ docker compose run --rm --entrypoint="" bemserver-api /bin/sh
    # cat /config/bemserver-core-settings.py
    SQLALCHEMY_DATABASE_URI="postgresql+psycopg2://timescaledb:password@timescaledb:5432/bemserver"  # this is just an example
    WEATHER_DATA_CLIENT_API_KEY="apikey"

Create hypertable

    $ docker compose exec timescaledb /install/create_hypertable.sh

Run BEMServer API

    $ docker compose up -d bemserver-api

Browse to BEMServer API web user interface at http://127.0.0.1:5000

### BEMServer UI

Build image

    $ docker compose build bemserver-ui

Create config file `bemserver-ui.cfg`

    $ docker compose run --rm --entrypoint="" bemserver-ui /install/install.sh

Create shell in a new container from `bemserver-ui` image to see config

    $ docker compose run --rm --entrypoint="" bemserver-ui /bin/sh
    # cat /config/bemserver-ui.cfg
    BEMSERVER_API_HOST="bemserver-api:5000"
    BEMSERVER_API_USE_SSL=False  # This is just for testing...
    SECRET_KEY="c55...9c5"

Run BEMServer UI

    $ docker compose up -d bemserver-ui

Browse to BEMServer web user interface at http://127.0.0.1:5001/ and log with `BEMSERVER_EMAIL` (default was admin@domain.com) and password defined previously in `bemserver_create_user` step.

### Scheduled Tasks

Modify `bemserver_api/config/bemserver-celery-settings.py`


Enter into running container `bemserver-api` and launch worker

    $ docker compose exec bemserver-api /bin/sh
    # celery worker
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

You should also see

    SecurityWarning: You're running the worker with superuser privileges: this is absolutely not recommended! 

This is just for testing. Not production!

In an another shell, start Celery beat to trigger tasks at regular intervals

    $ docker compose exec bemserver-api /bin/sh
    # celery beat
    celery beat v5.3.6 (emerald-rush) is starting.
    __    -    ... __   -        _
    LocalTime -> 2024-04-05 19:10:54
    Configuration ->
        . broker -> redis://default:**@redis-stack:6379/0
        . loader -> celery.loaders.app.AppLoader
        . scheduler -> celery.beat.PersistentScheduler
        . db -> celerybeat-schedule
        . logfile -> [stderr]@%WARNING
        . maxinterval -> 5.00 minutes (300s)


(optional)
Start Celery Flower https://flower.readthedocs.io/

    $ docker compose up -d celery-flower

Browse Flower web user interface at http://127.0.0.1:5555/
