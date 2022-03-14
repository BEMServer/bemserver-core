"""Users"""
from passlib.hash import argon2
import sqlalchemy as sqla
from sqlalchemy.ext.hybrid import hybrid_property

from bemserver_core.database import Base
from bemserver_core.authorization import AuthMixin, auth, Relation, get_current_user


class User(AuthMixin, Base):
    __tablename__ = "users"

    id = sqla.Column(sqla.Integer, primary_key=True)
    name = sqla.Column(sqla.String(80), unique=True, nullable=False)
    email = sqla.Column(sqla.String(80), unique=True, nullable=False)
    password = sqla.Column(sqla.String(200), nullable=False)
    _is_admin = sqla.Column(sqla.Boolean(), nullable=False)
    _is_active = sqla.Column(sqla.Boolean(), nullable=False)

    @classmethod
    def register_class(cls):
        auth.register_class(cls)
        auth.register_class(cls, name="UserActor")

    def __repr__(self):
        return (
            f"<User {self.name} <{self.email}>, "
            f"admin: {self.is_admin}, active: {self.is_active}>"
        )

    @hybrid_property
    def is_admin(self):
        return self._is_admin

    @is_admin.setter
    def is_admin(self, is_admin):
        auth.authorize(get_current_user(), "set_admin", self)
        self._is_admin = is_admin

    @hybrid_property
    def is_active(self):
        return self._is_active

    @is_active.setter
    def is_active(self, is_active):
        auth.authorize(get_current_user(), "set_active", self)
        self._is_active = is_active

    def set_password(self, password: str) -> None:
        auth.authorize(get_current_user(), "update", self)
        self.password = argon2.hash(password)

    def check_password(self, password: str) -> bool:
        return argon2.verify(password, self.password)


class UserGroup(AuthMixin, Base):
    __tablename__ = "user_groups"

    id = sqla.Column(sqla.Integer, primary_key=True)
    name = sqla.Column(sqla.String(80), unique=True, nullable=False)

    @classmethod
    def register_class(cls):
        auth.register_class(
            cls,
            fields={
                "user_groups_by_campaigns": Relation(
                    kind="many",
                    other_type="UserGroupByCampaign",
                    my_field="id",
                    other_field="user_group_id",
                ),
                "users_by_user_groups": Relation(
                    kind="many",
                    other_type="UserByUserGroup",
                    my_field="id",
                    other_field="user_group_id",
                ),
            },
        )


class UserByUserGroup(AuthMixin, Base):
    """UserGroup x User associations"""

    __tablename__ = "users_groups_by_users"
    __table_args__ = (sqla.UniqueConstraint("user_id", "user_group_id"),)

    id = sqla.Column(sqla.Integer, primary_key=True)
    user_id = sqla.Column(sqla.ForeignKey("users.id"), nullable=False)
    user_group_id = sqla.Column(sqla.ForeignKey("user_groups.id"), nullable=False)

    @classmethod
    def register_class(cls):
        auth.register_class(
            cls,
            fields={
                "user": Relation(
                    kind="one",
                    other_type="User",
                    my_field="user_id",
                    other_field="id",
                ),
            },
        )
