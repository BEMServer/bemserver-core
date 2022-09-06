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

    $ export SQLALCHEMY_DATABASE_URI="postgresql+psycopg2://user:password@localhost:5432/bemserver"

Also, those commands work when Alembic files are accessible, which means they
must be launched from the root of this repository.

Generate Migration Scripts
--------------------------

A good time to generate migration scripts is before a bemserver-core release.

To generate the script, Alembic compares the SQLAlchemy model in the code with
a live database. The database must be in a state corresponding to the latest
migration script (typically, a database in production with former
bemserver-core version).

Create an automatic revision file::

    $ alembic revision --autogenerate -m "0.1"

The last part of the command is a message appended to the revision file name.
It seems sensible to use the bemserver-core version here.

Automatic generation may not be perfect, so revision files should be checked
manually.

Also, Alembic is meant to detect changes in database tables, not rows, so rows
that are created by default in the code must be set and updated manually. This
is the case, for instance, for default timeseries properties and data states.

Once a revision file is ready, it can be committed to the repository.

Use Migration Scripts
---------------------

In an environment where bemserver-core is installed, the database can be
updated using an Alembic command::

    $ alembic upgrade head

For other actions, see Alembic documentation.


.. _Alembic: https://alembic.sqlalchemy.org/
