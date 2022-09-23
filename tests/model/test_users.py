"""User tests"""
import pytest

from bemserver_core.model import (
    User,
    UserGroup,
    UserByUserGroup,
    UserGroupByCampaign,
    UserGroupByCampaignScope,
)
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

    @pytest.mark.usefixtures("users_by_user_groups")
    def test_user_delete_cascade(self, users):
        admin_user = users[0]
        user_1 = users[1]

        with CurrentUser(admin_user):
            assert len(list(UserByUserGroup.get())) == 2

            user_1.delete()
            db.session.commit()
            assert len(list(UserByUserGroup.get())) == 1

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


class TestUserGroupModel:
    @pytest.mark.usefixtures("users_by_user_groups")
    @pytest.mark.usefixtures("user_groups_by_campaigns")
    @pytest.mark.usefixtures("user_groups_by_campaign_scopes")
    def test_user_group_delete_cascade(self, users, user_groups):
        admin_user = users[0]
        user_group_1 = user_groups[0]

        with CurrentUser(admin_user):
            assert len(list(UserByUserGroup.get())) == 2
            assert len(list(UserGroupByCampaign.get())) == 3
            assert len(list(UserGroupByCampaignScope.get())) == 3

            user_group_1.delete()
            db.session.commit()
            assert len(list(UserByUserGroup.get())) == 1
            assert len(list(UserGroupByCampaign.get())) == 2
            assert len(list(UserGroupByCampaignScope.get())) == 2

    def test_user_group_authorizations_as_admin(self, users):
        admin_user = users[0]
        assert admin_user.is_admin
        user_1 = users[1]
        assert not user_1.is_admin

        with CurrentUser(admin_user):
            user_group = UserGroup.new(name="User group 1")
            user_group.update(name="Super user group 2")
            db.session.commit()
            user_groups = list(UserGroup.get())
            assert len(user_groups) == 1
            ug_1 = UserGroup.get_by_id(user_group.id)
            assert ug_1.id == user_group.id
            assert ug_1.name == user_group.name
            user_group.delete()
            db.session.commit()

    @pytest.mark.usefixtures("users_by_user_groups")
    def test_user_group_authorizations_as_user(self, users, user_groups):
        admin_user = users[0]
        assert admin_user.is_admin
        user_1 = users[1]
        assert not user_1.is_admin
        user_group_1 = user_groups[0]
        user_group_2 = user_groups[1]

        with CurrentUser(user_1):
            UserGroup.get_by_id(user_group_2.id)
            user_groups = list(UserGroup.get())
            assert len(user_groups) == 1
            with pytest.raises(BEMServerAuthorizationError):
                UserGroup.new()
            with pytest.raises(BEMServerAuthorizationError):
                UserGroup.get_by_id(user_group_1.id)
            with pytest.raises(BEMServerAuthorizationError):
                user_group_2.update(name="Super user group 2")
            with pytest.raises(BEMServerAuthorizationError):
                user_group_2.delete()


class TestUserByUserGroupModel:
    def test_user_by_user_group_authorizations_as_admin(self, users, user_groups):
        admin_user = users[0]
        assert admin_user.is_admin
        user_1 = users[1]
        assert not user_1.is_admin
        user_group_1 = user_groups[0]
        user_group_2 = user_groups[0]

        with CurrentUser(admin_user):
            ubug_1 = UserByUserGroup.new(
                user_id=user_1.id, user_group_id=user_group_1.id
            )
            ubug_1.update(user_group_id=user_group_2.id)
            db.session.commit()
            ubug_l = list(UserByUserGroup.get())
            assert len(ubug_l) == 1
            ubug_1.delete()
            db.session.commit()

    def test_user_by_user_group_authorizations_as_user(
        self, users, user_groups, users_by_user_groups
    ):
        admin_user = users[0]
        assert admin_user.is_admin
        user_1 = users[1]
        assert not user_1.is_admin
        user_group_2 = user_groups[1]
        ubug_1 = users_by_user_groups[0]
        ubug_2 = users_by_user_groups[1]

        with CurrentUser(user_1):
            ubug = UserByUserGroup.get_by_id(ubug_2.id)
            ubug_l = list(UserByUserGroup.get())
            assert len(ubug_l) == 1
            with pytest.raises(BEMServerAuthorizationError):
                UserByUserGroup.get_by_id(ubug_1.id)
            with pytest.raises(BEMServerAuthorizationError):
                UserByUserGroup.new(user_id=user_1.id, user_group_id=user_group_2.id)
            with pytest.raises(BEMServerAuthorizationError):
                ubug.update(user_group_id=user_group_2.id)
            with pytest.raises(BEMServerAuthorizationError):
                ubug.delete()
