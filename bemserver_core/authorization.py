"""Authorization"""
from contextvars import ContextVar
from contextlib import AbstractContextManager
from pathlib import Path

from oso import Oso, OsoError, Relation  # noqa, republishing
from polar.data.adapter.sqlalchemy_adapter import SqlAlchemyAdapter

from bemserver_core.database import db
from bemserver_core.exceptions import BEMServerAuthorizationError


CURRENT_USER = ContextVar("current_user", default=None)
OPEN_BAR = ContextVar("open_bar", default=False)


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


def get_current_user():
    current_user = CURRENT_USER.get()
    if current_user is None or not current_user.is_active:
        if not OPEN_BAR.get():
            raise BEMServerAuthorizationError("No user")
    return current_user


class OpenBarPolarClass:
    @staticmethod
    def get():
        return OPEN_BAR.get()


auth = Oso(
    forbidden_error=BEMServerAuthorizationError,
    not_found_error=BEMServerAuthorizationError,
)

auth.set_data_filtering_adapter(SqlAlchemyAdapter(db.session))


def init_authorization(model_classes):
    """Register model classes and load rules

    Must be done after model classes are imported
    """
    AuthMixin.register_class(name="Base")

    for cls in model_classes:
        cls.register_class()

    auth.register_class(OpenBarPolarClass)

    # Load authorization policy
    auth.load_files([Path(__file__).parent / "authorization.polar"])


class AuthMixin:
    @classmethod
    def register_class(cls, *args, **kwargs):
        auth.register_class(cls, *args, **kwargs)

    @classmethod
    def get(cls, **kwargs):
        user = get_current_user()
        # TODO: Workaround for https://github.com/osohq/oso/issues/1536
        if user.is_admin:
            query = super().get()
        else:
            query = auth.authorized_query(get_current_user(), "read", cls)
        for key, val in kwargs.items():
            query = query.filter(getattr(cls, key) == val)
        return query

    @classmethod
    def new(cls, **kwargs):
        item = super().new(**kwargs)
        auth.authorize(get_current_user(), "create", item)
        return item

    @classmethod
    def get_by_id(cls, item_id, **kwargs):
        item = super().get_by_id(item_id)
        if item is None:
            return None
        auth.authorize(get_current_user(), "read", item)
        return item

    def update(self, **kwargs):
        auth.authorize(get_current_user(), "update", self)
        super().update(**kwargs)

    def delete(self):
        auth.authorize(get_current_user(), "delete", self)
        super().delete()
