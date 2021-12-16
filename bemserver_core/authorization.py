"""Authorization"""
from contextvars import ContextVar
from contextlib import AbstractContextManager
from pathlib import Path

import sqlalchemy as sqla
from oso import Oso, OsoError, Relation  # noqa, republishing
from polar import polar_class

from bemserver_core.database import db
from bemserver_core.exceptions import BEMServerAuthorizationError


CURRENT_USER = ContextVar("current_user", default=None)
CURRENT_CAMPAIGN = ContextVar("current_campaign", default=None)
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


class CurrentCampaign(AbstractContextManager):
    """Set current campaign for context"""
    def __init__(self, campaign):
        self._token = None
        self._campaign = campaign

    def __enter__(self):
        self._token = CURRENT_CAMPAIGN.set(self._campaign)

    def __exit__(self, *args, **kwargs):
        CURRENT_CAMPAIGN.reset(self._token)


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


def get_current_campaign():
    return CURRENT_CAMPAIGN.get()


@polar_class
class OpenBarPolarClass:
    @staticmethod
    def get():
        return OPEN_BAR.get()


def query_builder(model):
    # A "filter" is an object returned from Oso that describes
    # a condition that must hold on an object. This turns an
    # Oso filter into one that can be applied to an SQLAlchemy
    # query.
    def to_sqlalchemy_filter(filter):
        if filter.field is not None:
            field = getattr(model, filter.field)
            value = filter.value
        else:
            field = model.id
            value = filter.value.id

        if filter.kind == "Eq":
            return field == value
        elif filter.kind == "In":
            return field.in_(value)
        else:
            raise OsoError(f"Unsupported filter kind: {filter.kind}")

    # Turn a collection of Oso filters into one SQLAlchemy filter.
    def combine_filters(filters):
        filter = sqla.and_(True, *[to_sqlalchemy_filter(f) for f in filters])
        return db.session().query(model).filter(filter)

    return combine_filters


class SQLAlchemyOso(Oso):
    def register_class(self, cls, *args, **kwargs):
        super().register_class(
            cls, *args, build_query=query_builder(cls), **kwargs
        )


auth = SQLAlchemyOso(
    forbidden_error=BEMServerAuthorizationError,
    not_found_error=BEMServerAuthorizationError,
)


auth.set_data_filtering_query_defaults(
    combine_query=lambda q,
    r: q.union(r),
    exec_query=lambda q: q.distinct().all()
)


def init_authorization(model_classes):
    """Register model classes and load rules

    Must be done after model classes are imported
    """
    for cls in model_classes:
        cls.register_class()

    # Load authorization policy
    auth.load_files([Path(__file__).parent / "authorization.polar"])


class AuthMixin:

    @classmethod
    def register_class(cls):
        auth.register_class(cls)

    @classmethod
    def get(cls, **kwargs):
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
