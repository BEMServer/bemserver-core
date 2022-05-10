==============
BEMServer core
==============


Installation
============

Install PostgreSQL and TimescaleDB.

https://docs.timescale.com/latest/getting-started/setup

Create a database and install TimescaleDB extension (should be done after each
TimescaleDB update).

Assuming the user has database creation permission:::

$ createdb bemserver
$ psql -U $USER -d bemserver -c "CREATE EXTENSION IF NOT EXISTS timescaledb"

Install bemserver_core (typically in a virtual environment)::

$ pip install bemserver_core


Database setup
==============

Set DB URI in an evironment variable::

$ EXPORT SQLALCHEMY_DATABASE_URI="postgresql+psycopg2://user:password@localhost:5432/bemserver"

Alternatively, one may write this env var in a .env file and install python-dotenv to load it.


Development mode
----------------

Use `setup_db` command to initialize a clean database::

$ bemserver_setup_db


Production mode
---------------

Use alembic to manage initial setup and migrations::

$ alembic upgrade head

Please refer to alembic documentation for more information about the migration commands.
