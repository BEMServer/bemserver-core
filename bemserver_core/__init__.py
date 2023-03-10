"""BEMServer Core"""
import os

from bemserver_core import authorization
from bemserver_core import model
from bemserver_core import common
from bemserver_core import database
from bemserver_core import input_output  # noqa
from bemserver_core import scheduled_tasks
from bemserver_core import settings
from bemserver_core import utils
from bemserver_core.exceptions import BEMServerCoreSettingsError


__version__ = "0.11.1"


class BEMServerCore:
    def __init__(self):
        self.auth_model_classes = (
            model.AUTH_MODEL_CLASSES + scheduled_tasks.AUTH_MODEL_CLASSES
        )
        self.auth_polar_files = [
            authorization.AUTH_POLAR_FILE,
            model.AUTH_POLAR_FILE,
            scheduled_tasks.AUTH_POLAR_FILE,
        ]

        # Load config
        self.config = settings.DEFAULT_CONFIG.copy()
        file_path = os.environ.get("BEMSERVER_CORE_SETTINGS_FILE")
        if file_path is None:
            raise BEMServerCoreSettingsError(
                "Missing BEMSERVER_CORE_SETTINGS_FILE environment variable"
            )
        self.config.update(utils.get_dict_from_pyfile(file_path))

        # Set db URL
        database.db.set_db_url(self.config["SQLALCHEMY_DATABASE_URI"])

    def init_auth(self):
        authorization.auth.init_authorization(
            self.auth_model_classes,
            self.auth_polar_files,
        )

    def load_units_definitions_file(self, file_path):
        common.ureg.load_definitions(file_path)
