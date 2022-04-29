"""BEMServer core plugin"""


class BEMServerCorePlugin:
    """Base class for BEMServer core plugins"""

    #: Model classes to register for authorizations
    AUTH_MODEL_CLASSES = []
    #: Polar files to laod for authorizations
    AUTH_POLAR_FILES = []
