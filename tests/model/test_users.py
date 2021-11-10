"""User tests"""
import pytest

from bemserver_core.model import User


class TestUserModel:

    def test_user_repr(self):
        user = User(
            name="Chuck",
            email="chuck@norris.com",
            is_admin=True,
            is_active=True,
        )
        assert (
            repr(user) ==
            "<User Chuck <chuck@norris.com>, admin: True, active: True>"
        )

    @pytest.mark.usefixtures("as_admin")
    def test_user_password_hash_check(self):
        user = User(name="Chuck", email="chuck@norris.com")
        user.set_password("correct horse battery staple")
        assert user.check_password("correct horse battery staple")
        assert not user.check_password("Tr0ub4dor&3")
        assert not user.check_password("rosebud")
