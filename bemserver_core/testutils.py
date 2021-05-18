"""Test utilities worth sharing with client libraries for their own tests"""
from bemserver_core.database import db


def setup_db(postgresql):
    """Configure a test database

    This functions is meant to be used in a pytest fixture. It configures the
    test database by creating all tables, yieds the ``db`` accessor set to
    operate on this database, then does the cleanup when the test is done.

    :param psycopg2.extensions.connection postgresql: Database connection
    """
    db_url = (
        "postgresql+psycopg2://"
        f"{postgresql.info.user}:{postgresql.info.password}"
        f"@{postgresql.info.host}:{postgresql.info.port}/"
        f"{postgresql.info.dbname}"
    )
    db.set_db_url(db_url)

    with db.session() as session:
        session.execute("CREATE EXTENSION IF NOT EXISTS timescaledb;")
        session.commit()

    db.create_all()
    yield db
    db.session.remove()
    # Destroy DB engine, mainly for threaded code (as MQTT service).
    db.dispose()
