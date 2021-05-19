"""Users"""
from passlib.hash import argon2
import sqlalchemy as sqla

from bemserver_core.database import Base


class User(Base):
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
        sqla.String(80),
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
        self.password = argon2.hash(password)

    def check_password(self, password: str) -> bool:
        return argon2.verify(password, self.password)
