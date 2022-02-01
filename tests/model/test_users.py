"""User tests"""
import pytest

from bemserver_core.model import User
from bemserver_core.database import db
from bemserver_core.authorization import CurrentUser
from bemserver_core.exceptions import BEMServerAuthorizationError


class TestUserModel:
    @pytest.mark.usefixtures("as_admin")
    def test_user_repr(self):
        user = User(
            name="Chuck",
            email="chuck@norris.com",
            is_admin=True,
            is_active=True,
        )
        assert (
            repr(user) == "<User Chuck <chuck@norris.com>, admin: True, active: True>"
        )

    @pytest.mark.usefixtures("as_admin")
    def test_user_password_hash_check(self):
        user = User(name="Chuck", email="chuck@norris.com")
        user.set_password("correct horse battery staple")
        assert user.check_password("correct horse battery staple")
        assert not user.check_password("Tr0ub4dor&3")
        assert not user.check_password("rosebud")

    def test_user_authorizations_as_admin(self, users):
        admin_user = users[0]
        assert admin_user.is_admin
        user_1 = users[1]
        assert not user_1.is_admin

        with CurrentUser(admin_user):
            user = User.new(
                name="Jane",
                email="jane@test.com",
                is_admin=True,
                is_active=True,
            )
            user.set_password("pwd")
            user.update(email="chuck@norris.com")
            user.is_admin = False
            user.is_active = False
            db.session.commit()
            users = list(User.get())
            assert len(users) == 3
            user.delete()
            db.session.commit()
            user = User.get_by_id(admin_user.id)
            assert user.id == admin_user.id
            assert user.name == admin_user.name
            users = list(User.get())
            assert len(users) == 2
            users = list(User.get(is_admin=True))
            assert len(users) == 1
            assert users[0].id == admin_user.id
            users = list(User.get(is_admin=False))
            assert len(users) == 1
            assert users[0].id == user_1.id
            user.update(is_admin=True)
            assert user.is_admin is True

    def test_user_authorizations_as_user(self, users):
        admin_user = users[0]
        assert admin_user.is_admin
        user_1 = users[1]
        assert not user_1.is_admin

        with CurrentUser(user_1):
            user_1 = User.get_by_id(user_1.id)
            user_1.set_password("new_pwd_1")
            user_1.update(email="john@doe.com")
            db.session.commit()
            with pytest.raises(BEMServerAuthorizationError):
                User.new()
            with pytest.raises(BEMServerAuthorizationError):
                user_1.is_admin = True
            with pytest.raises(BEMServerAuthorizationError):
                user_1.is_active = False
            with pytest.raises(BEMServerAuthorizationError):
                user_1.delete()
            with pytest.raises(BEMServerAuthorizationError):
                User.get_by_id(admin_user.id)
            with pytest.raises(BEMServerAuthorizationError):
                admin_user.set_password("new_pwd_2")
            with pytest.raises(BEMServerAuthorizationError):
                admin_user.update(email="jane@doe.com")
            with pytest.raises(BEMServerAuthorizationError):
                admin_user.is_admin = True
            with pytest.raises(BEMServerAuthorizationError):
                admin_user.is_active = False
            with pytest.raises(BEMServerAuthorizationError):
                admin_user.delete()
            users = list(User.get())
            assert len(users) == 1

            with pytest.raises(BEMServerAuthorizationError):
                user_1.update(is_admin=True)
