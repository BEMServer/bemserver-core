"""Users"""
from passlib.hash import argon2
import sqlalchemy as sqla

from bemserver_core.database import Base
from bemserver_core.auth import AuthMixin, BEMServerAuthorizationError


class User(AuthMixin, Base):
    __tablename__ = "users"

    id = sqla.Column(
        sqla.Integer,
        primary_key=True
    )
    name = sqla.Column(
        sqla.String(80),
        unique=True,
        nullable=False
    )
    email = sqla.Column(
        sqla.String(80),
        unique=True,
        nullable=False
    )
    password = sqla.Column(
        sqla.String(200),
        nullable=False
    )
    is_admin = sqla.Column(
        sqla.Boolean(),
        nullable=False
    )
    is_active = sqla.Column(
        sqla.Boolean(),
        nullable=False
    )

    def __repr__(self):
        return (
            f"<User {self.name} <{self.email}>, "
            f"admin: {self.is_admin}, active: {self.is_active}>"
        )

    def set_password(self, password: str) -> None:
        self.check_update_permissions()
        self.password = argon2.hash(password)

    def check_password(self, password: str) -> bool:
        return argon2.verify(password, self.password)

    @classmethod
    def get(cls, **kwargs):
        """Get objects"""
        current_user = cls.current_user()
        if not current_user.is_admin:
            raise BEMServerAuthorizationError("User can't read users")
        return super().get(**kwargs)

    def check_read_permissions(self, current_user, **kwargs):
        """Check read persmissions"""
        if not current_user.is_admin and current_user.id != self.id:
            raise BEMServerAuthorizationError("User can't read other user")

    def check_update_permissions(self, **kwargs):
        current_user = self.current_user()
        if not current_user.is_admin:
            if current_user.id != self.id:
                raise BEMServerAuthorizationError(
                    "User can't modify other user"
                )
            if set(kwargs.keys()) & {"is_admin", "is_active"}:
                raise BEMServerAuthorizationError(
                    "User can't modify read-only fields"
                )

    def update(self, **kwargs):
        """Update object with kwargs"""
        # Circumvent AuthMixin.update: users can update themselves
        self.check_update_permissions(**kwargs)
        Base.update(self, **kwargs)
