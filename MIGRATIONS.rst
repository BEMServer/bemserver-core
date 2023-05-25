Database Migrations
===================

BEMServer uses `Alembic`_ to manage migrations. It keeps track of the changes
in the database and generates migration scripts versionned in this code
repository.

While this process is mostly automatic, there may be manual work involved.

Environment Setup
-----------------

Alembic commands require a connection to a live database. The connection URI
must be passed as environment variable::

    $ export SQLALCHEMY_DATABASE_URI="postgresql+psycopg://user:password@localhost:5432/bemserver"

Generate Migration Scripts
--------------------------

A good time to generate migration scripts is before a bemserver-core release.

To generate the script, Alembic compares the SQLAlchemy model in the code with
a live database. The database must be in a state corresponding to the latest
migration script (typically, a database in production with former
bemserver-core version).

Create an automatic revision file::

    $ alembic --config bemserver_core/alembic.ini revision \
        --autogenerate -m "Message" \
        --rev-id "Revision ID"

Automatic generation may not be perfect, so revision files should be checked
manually.

Also, Alembic is meant to detect changes in database tables, not rows, so rows
that are created by default in the code must be set and updated manually. This
is the case, for instance, for default timeseries properties and data states.

To check the migration file, use ``pg_dump`` to dump the database from both
migration files and ``bemserver_setup_db`` command. They should only differ by
alembic related tables::

    $ dropdb bemserver
    $ createdb bemserver
    $ bemserver_db_upgrade
    $ pg_dump bemserver > dump_migration.sql

    $ dropdb bemserver
    $ createdb bemserver
    $ bemserver_setup_db
    $ pg_dump bemserver > dump_setup_db.sql

Once a revision file is ready, it can be committed to the repository.

Manage Migrations
-----------------

bemserver-core provides the following commands to manage the database.

Display current database revision::

    $ bemserver_db_current

Upgrade database (to head revision by default) ::

    $ bemserver_db_upgrade -r revision

Downgrade database::

    $ bemserver_db_downgrade -r revision

.. _Alembic: https://alembic.sqlalchemy.org/
