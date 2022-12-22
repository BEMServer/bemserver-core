"""BEMServer Core"""
from bemserver_core import authorization

from . import model
from . import database  # noqa
from . import input_output  # noqa
from . import scheduled_tasks


__version__ = "0.5.0"


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

    def init_auth(self):
        authorization.auth.init_authorization(
            self.auth_model_classes,
            self.auth_polar_files,
        )
