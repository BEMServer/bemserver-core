import os

from bemserver_core import model, database


def _set_db_url():
    # Allow the use of a .env file to store SQLALCHEMY_DATABASE_URI environment variable
    try:
        from dotenv import load_dotenv
    except ImportError:
        pass
    else:
        load_dotenv()

    db_url = os.getenv("SQLALCHEMY_DATABASE_URI")
    database.db.set_db_url(db_url)


def setup_db():
    """Create initial DB data

    Create tables and initial data in a clean database.

    This function assumes DB URI is set.
    """
    database.db.create_all()
    model.events.init_db_events()
    model.timeseries.init_db_timeseries()
    model.timeseries_data.init_db_timeseries_data()


def setup_db_cmd():
    """Create tables and initial data in a clean database.

    This command is meant to be used for dev setups.
    Production setups should rely on migration scripts.
    """
    _set_db_url()
    setup_db()
