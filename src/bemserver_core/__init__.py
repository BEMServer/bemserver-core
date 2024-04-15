"""BEMServer Core"""

import os

import pandas as pd

from bemserver_core import (
    authorization,
    common,
    database,
    input_output,  # noqa
    model,
    scheduled_tasks,
    settings,
    utils,
)
from bemserver_core.email import ems
from bemserver_core.exceptions import BEMServerCoreSettingsError
from bemserver_core.process.weather import wdp

# Set pandas future flags to silence deprecation warnings
pd.set_option("future.no_silent_downcasting", True)


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
        try:
            cfg = utils.get_dict_from_pyfile(file_path)
        except (OSError, SyntaxError) as exc:
            raise BEMServerCoreSettingsError(str(exc)) from exc
        self.config.update(cfg)

        # Set db URL
        database.db.set_db_url(self.config["SQLALCHEMY_DATABASE_URI"])

        # Init auth
        authorization.auth.init_authorization(
            self.auth_model_classes,
            self.auth_polar_files,
        )

        # Load unit definition files
        for file_path in self.config["UNIT_DEFINITION_FILES"]:
            common.ureg.load_definitions(file_path)

        # Init weather data processor
        wdp.init_core(self)

        # Init SMTP
        ems.init_core(self)

    def load_units_definitions_file(self, file_path):
        common.ureg.load_definitions(file_path)
