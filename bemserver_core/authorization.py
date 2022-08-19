"""Authorization"""
from pathlib import Path
import functools
from contextvars import ContextVar

from oso import Oso, OsoError, Relation  # noqa, republishing
from polar.data.adapter.sqlalchemy_adapter import SqlAlchemyAdapter

from bemserver_core.database import db
from bemserver_core.exceptions import BEMServerAuthorizationError
from bemserver_core.utils import make_context_var_manager


CURRENT_USER = ContextVar("current_user", default=None)
OPEN_BAR = ContextVar("open_bar", default=False)

CurrentUser = make_context_var_manager(CURRENT_USER)
OpenBar = functools.partial(make_context_var_manager(OPEN_BAR), True)


AUTH_POLAR_FILE = Path(__file__).parent / "authorization.polar"


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


class OsoProxy:
    """Oso proxy class

    Provides lazy loading of classes and authorization rules
    """

    def __init__(self, *args, **kwargs):
        self.oso = None
        self.oso_args = args
        self.oso_kwargs = kwargs

    def __getattr__(self, attr):
        return getattr(self.oso, attr)

    def init_authorization(self, model_classes, polar_files):
        """Register model classes and load rules

        Must be done after model classes are imported
        """
        self.oso = Oso(*self.oso_args, **self.oso_kwargs)
        self.set_data_filtering_adapter(SqlAlchemyAdapter(db.session))

        # Register classes
        self.register_class(OpenBarPolarClass)
        AuthMixin.register_class(name="Base")
        for cls in model_classes:
            cls.register_class()

        # Load authorization policy
        self.load_files(polar_files)


auth = OsoProxy(
    forbidden_error=BEMServerAuthorizationError,
    not_found_error=BEMServerAuthorizationError,
)


class AuthMixin:
    @classmethod
    def register_class(cls, *args, **kwargs):
        auth.register_class(cls, *args, **kwargs)

    @classmethod
    def _query(cls, **kwargs):
        user = get_current_user()
        # TODO: Workaround for https://github.com/osohq/oso/issues/1536
        if OPEN_BAR.get() or user.is_admin:
            query = db.session.query(cls)
        else:
            query = auth.authorized_query(user, "read", cls)
        for key, val in kwargs.items():
            query = query.filter(getattr(cls, key) == val)
        return query

    @classmethod
    def new(cls, **kwargs):
        # Override Base.new to avoid adding to the session if auth failed
        item = cls(**kwargs)
        auth.authorize(get_current_user(), "create", item)
        db.session.add(item)
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
