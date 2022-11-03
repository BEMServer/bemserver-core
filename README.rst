==============
BEMServer core
==============


Installation
============

Install PostgreSQL::

$ aptitude install postgresql

Install prerequisites for psycopg2 compilation (assuming Debian system)::

$ aptitude install python3-dev libpq-dev

Install bemserver_core (typically in a virtual environment)::

$ pip install bemserver_core


Database setup
==============

Create a database.

Assuming the user has database creation permission::

$ createdb bemserver

Set DB URI in an evironment variable::

$ export SQLALCHEMY_DATABASE_URI="postgresql+psycopg2://user:password@localhost:5432/bemserver"


Development mode
----------------

Use `setup_db` command to initialize a clean database::

$ bemserver_setup_db


Production mode
---------------

Use alembic to manage initial setup and migrations::

$ alembic upgrade head

Please refer to alembic documentation for more information about the migration commands.


User creation
-------------

Create an admin user::

$ bemserver_create_user --name chuck --email chuck@norris.com --admin

Other users may be created later from the web GUI using this one.


Using TimescaleDB
=================

TimescaleDB is a PostgreSQL extension specialized in timeseries data. It may be
used to improve the performance of read-write operations in the table storing
timeseries data.

Installation instructions are detailed in the documentation:

https://docs.timescale.com/latest/getting-started/setup

Install TimescaleDB as described there, then setup the extension in the database
(this should be done after each TimescaleDB update)::

$ psql -U $USER -d bemserver -c "CREATE EXTENSION IF NOT EXISTS timescaledb"

After the tables are created by the first migration or the `setup_db` command,
create hypertables in timeseries data table::

$ psql -U $USER -d bemserver -c "SELECT create_hypertable('ts_data', 'timestamp')"
