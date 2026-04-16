"""Users"""

import sqlalchemy as sqla
from sqlalchemy.ext.hybrid import hybrid_property

import argon2

from bemserver_core.authorization import AuthMgrMixin, auth_mgr
from bemserver_core.database import Base, db

ph = argon2.PasswordHasher()


class User(AuthMgrMixin, Base):
    __tablename__ = "users"

    id = sqla.Column(sqla.Integer, primary_key=True)
    name = sqla.Column(sqla.String(80), nullable=False)
    email = sqla.Column(sqla.String(80), unique=True, nullable=False)
    password = sqla.Column(sqla.String(200), nullable=False)
    _is_admin = sqla.Column(sqla.Boolean(), nullable=False)
    _is_active = sqla.Column(sqla.Boolean(), nullable=False)

    def __repr__(self):
        return (
            f"<User {self.name} <{self.email}>, "
            f"admin: {self._is_admin}, active: {self._is_active}>"
        )

    @hybrid_property
    def is_admin(self):
        return self._is_admin

    @is_admin.setter
    def is_admin(self, is_admin):
        auth_mgr.authorize("set_admin", self)
        self._is_admin = is_admin

    @hybrid_property
    def is_active(self):
        return self._is_active

    @is_active.setter
    def is_active(self, is_active):
        auth_mgr.authorize("set_active", self)
        self._is_active = is_active

    def set_password(self, password: str) -> None:
        auth_mgr.authorize("update", self)
        self.password = ph.hash(password)

    def check_password(self, password: str) -> bool:
        if self.password is None:
            return False
        try:
            ph.verify(self.password, password)
        except argon2.exceptions.VerifyMismatchError:
            return False
        if ph.check_needs_rehash(self.password):
            self.password = ph.hash(password)
        return True

    @classmethod
    def authorize_query(cls, actor, query):
        return query.filter(cls.id == actor.id)

    def authorize_read(self, actor):
        return actor.id == self.id

    def authorize_update(self, actor):
        return actor.id == self.id


@auth_mgr.add_rule("set_admin")
def authorize_set_admin(actor: User, user: User) -> bool:
    return False


@auth_mgr.add_rule("set_active")
def authorize_set_active(actor: User, user: User) -> bool:
    return False


class UserGroup(AuthMgrMixin, Base):
    __tablename__ = "u_groups"

    id = sqla.Column(sqla.Integer, primary_key=True)
    name = sqla.Column(sqla.String(80), unique=True, nullable=False)

    @classmethod
    def authorize_query(cls, actor, query):
        return UserByUserGroup.authorize_query(actor, query.join(UserByUserGroup))

    def authorize_create(self, actor):
        return False

    def authorize_read(self, actor):
        return db.session.query(
            db.session.query(UserByUserGroup)
            .filter_by(user_id=actor.id, user_group_id=self.id)
            .exists()
        ).scalar()


class UserByUserGroup(AuthMgrMixin, Base):
    """UserGroup x User associations"""

    __tablename__ = "users_by_u_groups"
    __table_args__ = (sqla.UniqueConstraint("user_id", "user_group_id"),)

    id = sqla.Column(sqla.Integer, primary_key=True)
    user_id = sqla.Column(sqla.ForeignKey("users.id"), nullable=False)
    user_group_id = sqla.Column(sqla.ForeignKey("u_groups.id"), nullable=False)

    user = sqla.orm.relationship(
        User,
        backref=sqla.orm.backref("users_by_user_groups", cascade="all, delete-orphan"),
    )
    user_group = sqla.orm.relationship(
        UserGroup,
        backref=sqla.orm.backref("users_by_user_groups", cascade="all, delete-orphan"),
    )

    @classmethod
    def authorize_query(cls, actor, query):
        return query.filter(UserByUserGroup.user_id == actor.id)

    def authorize_read(self, actor):
        return actor.id == self.user_id
