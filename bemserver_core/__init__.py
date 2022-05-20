"""BEMServer Core"""
from bemserver_core.authorization import auth, AUTH_POLAR_FILES

from . import model  # noqa
from . import database  # noqa
from . import input_output  # noqa


__version__ = "0.0.1"


class BEMServerCore:
    def __init__(self):
        self.auth_model_classes = list(model.AUTH_MODEL_CLASSES)
        self.auth_polar_files = list(AUTH_POLAR_FILES)

    def init_auth(self):
        auth.init_authorization(
            self.auth_model_classes,
            self.auth_polar_files,
        )
