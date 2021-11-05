"""Authorization"""
from contextvars import ContextVar
from contextlib import AbstractContextManager

from .exceptions import BEMServerAuthorizationError


CURRENT_USER = ContextVar("current_user", default=None)
OPEN_BAR = ContextVar("open_bar", default=False)


def get_current_user():
    current_user = CURRENT_USER.get()
    if current_user is None or not current_user.is_active:
        if not OPEN_BAR.get():
            raise BEMServerAuthorizationError("No user")
    return current_user


class CurrentUser(AbstractContextManager):
    """Set current user for context"""
    def __init__(self, user):
        self._token = None
        self._user = user

    def __enter__(self):
        self._token = CURRENT_USER.set(self._user)

    def __exit__(self, *args, **kwargs):
        CURRENT_USER.reset(self._token)


class OpenBar(AbstractContextManager):
    """Open bar mode

    Used for tests or admin tasks.
    "With great power comes great responsibility."
    """
    def __init__(self):
        self._token = None

    def __enter__(self):
        self._token = OPEN_BAR.set(True)

    def __exit__(self, *args, **kwargs):
        OPEN_BAR.reset(self._token)
