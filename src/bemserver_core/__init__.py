"""BEMServer Core"""

import os

from bemserver_core import (
    common,
    database,
    input_output,  # noqa
    plugins,
    settings,
    tasks,  # noqa
    utils,
)
from bemserver_core.celery import celery as celery_app
from bemserver_core.email import ems
from bemserver_core.exceptions import BEMServerCoreSettingsError
from bemserver_core.processing.weather import wdp


class BEMServerCore:
    def __init__(self):
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

        # Load unit definition files
        for file_path in self.config["UNIT_DEFINITION_FILES"]:
            common.ureg.load_definitions(file_path)

        # Init weather data processor
        wdp.init_core(self)

        # Init SMTP
        ems.init_core(self)

        # Configure Celery
        celery_app.set_default()
        celery_app.init_app(self)

        # Load plugins
        plugins.init_core(self)

    def load_units_definitions_file(self, file_path):
        common.ureg.load_definitions(file_path)
