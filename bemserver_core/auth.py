"""Authorization"""
from contextvars import ContextVar
from contextlib import AbstractContextManager


CURRENT_USER = ContextVar("current_user", default=None)


class BEMServerAuthorizationError(Exception):
    """Operation not autorized to current user"""


class CurrentUser(AbstractContextManager):
    """Set current user for context"""
    def __init__(self, user):
        self._token = None
        self._user = user

    def __enter__(self):
        self._token = CURRENT_USER.set(self._user)

    def __exit__(self, *args, **kwargs):
        CURRENT_USER.reset(self._token)


class AuthMixin:

    @classmethod
    def new(cls, **kwargs):
        current_user = CURRENT_USER.get()
        if current_user and not current_user.is_admin:
            raise BEMServerAuthorizationError("User can't create item")
        return super().new(**kwargs)

    def check_read_permissions(self, current_user, **kwargs):
        """Check read persmissions"""

    @classmethod
    def get_by_id(cls, item_id, **kwargs):
        item = super().get_by_id(item_id)
        if item is None:
            return None
        current_user = CURRENT_USER.get()
        if current_user:
            item.check_read_permissions(current_user, **kwargs)
        return item

    def update(self, **kwargs):
        current_user = CURRENT_USER.get()
        if current_user and not current_user.is_admin:
            raise BEMServerAuthorizationError("User can't update item")
        super().update(**kwargs)

    def delete(self):
        # Authorization
        current_user = CURRENT_USER.get()
        if current_user and not current_user.is_admin:
            raise BEMServerAuthorizationError("User can't delete item")
        super().delete()
