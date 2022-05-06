"""BEMServer Core"""
from bemserver_core.authorization import auth, AUTH_POLAR_FILES

from . import model  # noqa
from . import database  # noqa
from . import csv_io  # noqa


class BEMServerCore:
    def __init__(self):
        self.auth_model_classes = list(model.AUTH_MODEL_CLASSES)
        self.auth_polar_files = list(AUTH_POLAR_FILES)

    def init_auth(self):
        auth.init_authorization(
            self.auth_model_classes,
            self.auth_polar_files,
        )


def setup_db():
    """Create and add initial data to DB

    This method is meant to be used for tests or dev setups.
    Production setups should rely on migration scripts.
    """
    database.db.create_all()
    model.events.init_db_events()
    model.timeseries.init_db_timeseries()
    model.timeseries_data.init_db_timeseries_data()
