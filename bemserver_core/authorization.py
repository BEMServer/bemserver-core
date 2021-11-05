from pathlib import Path

import sqlalchemy as sqla
from oso import Oso, OsoError, Relation  # noqa, republishing
from polar import polar_class

from .database import db
from .authentication import OPEN_BAR, get_current_user
from .exceptions import BEMServerAuthorizationError


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


# class SQLAlchemyOso(Oso):
#     def register_class(cls):
#         super().register_class(cls, build_query=query_builder(cls))


auth = Oso(
    forbidden_error=BEMServerAuthorizationError,
    not_found_error=BEMServerAuthorizationError,
)


auth.set_data_filtering_query_defaults(
    combine_query=lambda q,
    r: q.union(r),
    exec_query=lambda q: q.distinct().all()
)


def register_class(model_cls):
    auth.register_class(
        model_cls,
        build_query=query_builder(model_cls),
    )


def init_authorization(model_classes):
    """Register model classes and load rules

    Must be done after model classes are importer
    """
    for cls in model_classes:
        cls.register_class()

    # Load authorization policy.
    auth.load_files([Path(__file__).parent / "authorization.polar"])


class AuthMixin:

    @classmethod
    def register_class(cls):
        auth.register_class(
            cls,
            build_query=query_builder(cls),
        )

    @classmethod
    def current_user(cls):
        return get_current_user()

    @classmethod
    def get(cls, **kwargs):
        query = auth.authorized_query(cls.current_user(), "read", cls)
        for key, val in kwargs.items():
            query = query.filter(getattr(cls, key) == val)
        return query

    @classmethod
    def new(cls, **kwargs):
        item = super().new(**kwargs)
        auth.authorize(cls.current_user(), "create", item)
        return item

    @classmethod
    def get_by_id(cls, item_id, **kwargs):
        item = super().get_by_id(item_id)
        if item is None:
            return None
        auth.authorize(cls.current_user(), "read", item)
        return item

    def update(self, **kwargs):
        auth.authorize(self.current_user(), "update", self)
        super().update(**kwargs)

    def delete(self):
        auth.authorize(self.current_user(), "delete", self)
        super().delete()
