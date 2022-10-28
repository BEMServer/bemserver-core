import sys
import os
import contextlib
import logging
from logging.config import fileConfig

from sqlalchemy import engine_from_config
from sqlalchemy import pool

from alembic import context

from bemserver_core.database import db, Base


# this is the Alembic Config object, which provides
# access to the values within the .ini file in use.
config = context.config

# Interpret the config file for Python logging.
# This line sets up loggers basically.
if config.config_file_name is not None:
    fileConfig(config.config_file_name)
logger = logging.getLogger("alembic")

# If running in custom BEMServer command, connection is provided by caller
# If running as alembic command, set connection from DB URL as env var
connectable = config.attributes.get("connection")
if connectable is None:
    DB_URL = os.getenv("SQLALCHEMY_DATABASE_URI")
    if DB_URL is None:
        logger.error("SQLALCHEMY_DATABASE_URI environment variable not set.")
        sys.exit()
    db.set_db_url(DB_URL)
    config.set_main_option("sqlalchemy.url", str(db.url).replace("%", "%%"))

target_metadata = Base.metadata

# other values from the config, defined by the needs of env.py,
# can be acquired:
# my_important_option = config.get_main_option("my_important_option")
# ... etc.


def run_migrations_offline():
    """Run migrations in 'offline' mode.

    This configures the context with just a URL
    and not an Engine, though an Engine is acceptable
    here as well.  By skipping the Engine creation
    we don't even need a DBAPI to be available.

    Calls to context.execute() here emit the given string to the
    script output.

    """
    url = config.get_main_option("sqlalchemy.url")
    context.configure(
        url=url,
        target_metadata=target_metadata,
        literal_binds=True,
        dialect_opts={"paramstyle": "named"},
    )

    with context.begin_transaction():
        context.run_migrations()


def run_migrations_online():
    """Run migrations in 'online' mode.

    In this scenario we need to create an Engine
    and associate a connection with the context.

    """

    # https://alembic.sqlalchemy.org/en/latest/cookbook.html
    # don-t-generate-empty-migrations-with-autogenerate
    def process_revision_directives(context, revision, directives):
        if config.cmd_opts.autogenerate:
            script = directives[0]
            if script.upgrade_ops.is_empty():
                directives[:] = []
                logger.info("No database schema change.")

    # https://alembic.sqlalchemy.org/en/latest/cookbook.html#connection-sharing
    # Running as alembic command. Create connection.
    if connectable is None:
        connection_ctx = engine_from_config(
            config.get_section(config.config_ini_section),
            prefix="sqlalchemy.",
            poolclass=pool.NullPool,
        ).connect()
    # Running as custom command.
    else:
        connection_ctx = contextlib.nullcontext(connectable)

    with connection_ctx as connection:
        context.configure(
            connection=connection,
            target_metadata=target_metadata,
            process_revision_directives=process_revision_directives,
        )
        with context.begin_transaction():
            context.run_migrations()


if context.is_offline_mode():
    run_migrations_offline()
else:
    run_migrations_online()
