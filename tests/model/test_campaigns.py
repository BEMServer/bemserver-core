"""Campaign tests"""
import sqlalchemy as sqla

import pytest

from bemserver_core.model import (
    Campaign,
    CampaignScope,
    UserGroupByCampaign,
    UserGroupByCampaignScope,
    Site,
    Building,
    Storey,
    Space,
    Zone,
    Timeseries,
    Event,
)
from bemserver_core.database import db
from bemserver_core.authorization import CurrentUser
from bemserver_core.exceptions import BEMServerAuthorizationError


class TestCampaignModel:
    @pytest.mark.usefixtures("campaign_scopes")
    @pytest.mark.usefixtures("user_groups_by_campaigns")
    @pytest.mark.usefixtures("spaces")
    @pytest.mark.usefixtures("zones")
    @pytest.mark.usefixtures("timeseries")
    def test_campaign_delete_cascade(self, users, campaigns):
        admin_user = users[0]
        campaign_1 = campaigns[0]

        with CurrentUser(admin_user):
            assert len(list(CampaignScope.get())) == 3
            assert len(list(UserGroupByCampaign.get())) == 3
            assert len(list(Site.get())) == 2
            assert len(list(Building.get())) == 2
            assert len(list(Storey.get())) == 2
            assert len(list(Space.get())) == 2
            assert len(list(Zone.get())) == 2
            assert len(list(Timeseries.get())) == 2

            campaign_1.delete()
            db.session.commit()
            assert len(list(CampaignScope.get())) == 2
            assert len(list(UserGroupByCampaign.get())) == 2
            assert len(list(Site.get())) == 1
            assert len(list(Building.get())) == 1
            assert len(list(Storey.get())) == 1
            assert len(list(Space.get())) == 1
            assert len(list(Zone.get())) == 1
            assert len(list(Timeseries.get())) == 1

    def test_campaign_get_in_name(self, users, campaigns):
        admin_user = users[0]
        campaign_1 = campaigns[0]

        with CurrentUser(admin_user):
            ret = Campaign.get(in_name=campaign_1.name[4:])
            assert len(list(ret)) == 1
            assert ret[0] == campaign_1
            assert len(list(Campaign.get(in_name="toto"))) == 0

    def test_campaign_authorizations_as_admin(self, users):
        admin_user = users[0]
        assert admin_user.is_admin

        with CurrentUser(admin_user):
            campaign_1 = Campaign.new(
                name="Campaign 1",
            )
            assert campaign_1.id is None
            assert campaign_1.timezone is None
            db.session.commit()
            assert campaign_1.id is not None
            assert campaign_1.timezone == "UTC"

            campaign = Campaign.get_by_id(campaign_1.id)
            assert campaign.id == campaign_1.id
            assert campaign.name == campaign_1.name
            campaigns = list(Campaign.get())
            assert len(campaigns) == 1
            assert campaigns[0].id == campaign_1.id
            campaign.update(name="Super campaign 1")
            campaign.delete()
            db.session.commit()

    @pytest.mark.usefixtures("users_by_user_groups")
    @pytest.mark.usefixtures("user_groups_by_campaigns")
    def test_campaign_authorizations_as_user(self, users, campaigns):
        user_1 = users[1]
        assert not user_1.is_admin
        campaign_1 = campaigns[0]
        campaign_2 = campaigns[1]

        with CurrentUser(user_1):
            with pytest.raises(BEMServerAuthorizationError):
                Campaign.new(
                    name="Campaign 1",
                )

            campaign = Campaign.get_by_id(campaign_2.id)
            campaigns = list(Campaign.get())
            assert len(campaigns) == 2
            assert campaigns[0].id == campaign_2.id
            with pytest.raises(BEMServerAuthorizationError):
                Campaign.get_by_id(campaign_1.id)
            with pytest.raises(BEMServerAuthorizationError):
                campaign.update(name="Super campaign 1")
            with pytest.raises(BEMServerAuthorizationError):
                campaign.delete()


class TestUserGroupByCampaignModel:
    @pytest.mark.usefixtures("users_by_user_groups")
    def test_user_group_by_campaign_authorizations_as_admin(
        self, users, user_groups, campaigns
    ):
        admin_user = users[0]
        assert admin_user.is_admin
        user_group_1 = user_groups[1]
        campaign_1 = campaigns[0]
        campaign_2 = campaigns[1]

        with CurrentUser(admin_user):
            ugbc_1 = UserGroupByCampaign.new(
                user_group_id=user_group_1.id,
                campaign_id=campaign_1.id,
            )
            db.session.commit()

            ugbc = UserGroupByCampaign.get_by_id(ugbc_1.id)
            assert ugbc.id == ugbc_1.id
            ugbcs = list(UserGroupByCampaign.get())
            assert len(ugbcs) == 1
            assert ugbcs[0].id == ugbc_1.id
            ugbc.update(campaign_id=campaign_2.id)
            ugbc.delete()

    @pytest.mark.usefixtures("users_by_user_groups")
    def test_user_group_by_campaign_authorizations_as_user(
        self, users, campaigns, user_groups, user_groups_by_campaigns
    ):
        user_1 = users[1]
        assert not user_1.is_admin
        user_group_2 = user_groups[1]
        campaign_1 = campaigns[0]
        campaign_2 = campaigns[1]
        ugbc_1 = user_groups_by_campaigns[0]
        ugbc_2 = user_groups_by_campaigns[1]

        with CurrentUser(user_1):
            with pytest.raises(BEMServerAuthorizationError):
                UserGroupByCampaign.new(
                    user_group_id=user_group_2.id,
                    campaign_id=campaign_2.id,
                )
            ugbc = UserGroupByCampaign.get_by_id(ugbc_2.id)
            ugbcs = list(UserGroupByCampaign.get())
            assert len(ugbcs) == 2
            assert ugbcs[0].id == ugbc_2.id
            with pytest.raises(BEMServerAuthorizationError):
                UserGroupByCampaign.get_by_id(ugbc_1.id)
            with pytest.raises(BEMServerAuthorizationError):
                ugbc.update(campaign_id=campaign_1.id)
            with pytest.raises(BEMServerAuthorizationError):
                ugbc.delete()


class TestCampaignScopeModel:
    @pytest.mark.usefixtures("user_groups_by_campaign_scopes")
    @pytest.mark.usefixtures("timeseries")
    @pytest.mark.usefixtures("events")
    def test_campaign_scope_delete_cascade(self, users, campaign_scopes):
        admin_user = users[0]
        cs_1 = campaign_scopes[0]

        with CurrentUser(admin_user):
            assert len(list(UserGroupByCampaignScope.get())) == 3
            assert len(list(Timeseries.get())) == 2
            assert len(list(Event.get())) == 2

            cs_1.delete()
            db.session.commit()
            assert len(list(UserGroupByCampaignScope.get())) == 2
            assert len(list(Timeseries.get())) == 1
            assert len(list(Event.get())) == 1

    @pytest.mark.usefixtures("as_admin")
    def test_campaign_scope_read_only_fields(self, campaigns):
        """Check campaign can't be modified

        This is kind of a "framework test".
        """
        campaign_1 = campaigns[0]
        campaign_2 = campaigns[1]

        cs_1 = CampaignScope.new(
            name="Campaign scope 1",
            campaign_id=campaign_1.id,
        )
        db.session.commit()

        cs_1.update(campaign_id=campaign_2.id)
        with pytest.raises(
            sqla.exc.IntegrityError,
            match="campaign_id cannot be modified",
        ):
            db.session.commit()
        db.session.rollback()

        cs_list = list(CampaignScope.get(campaign_id=1))
        assert cs_list == [cs_1]
        cs_list = list(CampaignScope.get(campaign_id=2))
        assert cs_list == []

    def test_campaign_scope_authorizations_as_admin(self, users, campaigns):
        admin_user = users[0]
        assert admin_user.is_admin

        campaign_1 = campaigns[0]

        with CurrentUser(admin_user):
            campaign_scope_1 = CampaignScope.new(
                name="Campaign scope 1",
                campaign_id=campaign_1.id,
            )
            db.session.commit()

            campaign_scope = CampaignScope.get_by_id(campaign_scope_1.id)
            assert campaign_scope.id == campaign_scope_1.id
            assert campaign_scope.name == campaign_scope_1.name
            campaign_scopes = list(CampaignScope.get())
            assert len(campaign_scopes) == 1
            assert campaign_scopes[0].id == campaign_scope_1.id
            campaign_scope.update(name="Super campaign_scope 1")
            campaign_scope.delete()
            db.session.commit()

    @pytest.mark.usefixtures("users_by_user_groups")
    @pytest.mark.usefixtures("user_groups_by_campaign_scopes")
    def test_campaign_scope_authorizations_as_user(self, users, campaign_scopes):
        user_1 = users[1]
        assert not user_1.is_admin
        campaign_scope_1 = campaign_scopes[0]
        campaign_scope_2 = campaign_scopes[1]

        with CurrentUser(user_1):
            with pytest.raises(BEMServerAuthorizationError):
                CampaignScope.new(
                    name="CampaignScope 1",
                )

            campaign_scope = CampaignScope.get_by_id(campaign_scope_2.id)
            campaign_scopes = list(CampaignScope.get())
            assert len(campaign_scopes) == 2
            assert campaign_scopes[0].id == campaign_scope_2.id
            with pytest.raises(BEMServerAuthorizationError):
                CampaignScope.get_by_id(campaign_scope_1.id)
            with pytest.raises(BEMServerAuthorizationError):
                campaign_scope.update(name="Super campaign_scope 1")
            with pytest.raises(BEMServerAuthorizationError):
                campaign_scope.delete()


class TestUserGroupByCampaignScopeModel:
    @pytest.mark.usefixtures("users_by_user_groups")
    def test_user_group_by_campaign_scope_authorizations_as_admin(
        self, users, user_groups, campaign_scopes
    ):
        admin_user = users[0]
        assert admin_user.is_admin
        user_group_1 = user_groups[1]
        campaign_scope_1 = campaign_scopes[0]
        campaign_scope_2 = campaign_scopes[1]

        with CurrentUser(admin_user):
            ugbcs_1 = UserGroupByCampaignScope.new(
                user_group_id=user_group_1.id,
                campaign_scope_id=campaign_scope_1.id,
            )
            db.session.commit()

            ugbcs = UserGroupByCampaignScope.get_by_id(ugbcs_1.id)
            assert ugbcs.id == ugbcs_1.id
            ugbcss = list(UserGroupByCampaignScope.get())
            assert len(ugbcss) == 1
            assert ugbcss[0].id == ugbcs_1.id
            ugbcs.update(campaign_scope_id=campaign_scope_2.id)
            ugbcs.delete()

    @pytest.mark.usefixtures("users_by_user_groups")
    def test_user_group_by_campaign_scope_authorizations_as_user(
        self, users, campaign_scopes, user_groups, user_groups_by_campaign_scopes
    ):
        user_1 = users[1]
        assert not user_1.is_admin
        user_group_2 = user_groups[1]
        campaign_scope_1 = campaign_scopes[0]
        campaign_scope_2 = campaign_scopes[1]
        ugbcs_1 = user_groups_by_campaign_scopes[0]
        ugbcs_2 = user_groups_by_campaign_scopes[1]

        with CurrentUser(user_1):
            with pytest.raises(BEMServerAuthorizationError):
                UserGroupByCampaignScope.new(
                    user_group_id=user_group_2.id,
                    campaign_scope_id=campaign_scope_2.id,
                )
            ugbcs = UserGroupByCampaignScope.get_by_id(ugbcs_2.id)
            ugbcss = list(UserGroupByCampaignScope.get())
            assert len(ugbcss) == 2
            assert ugbcss[0].id == ugbcs_2.id
            with pytest.raises(BEMServerAuthorizationError):
                UserGroupByCampaignScope.get_by_id(ugbcs_1.id)
            with pytest.raises(BEMServerAuthorizationError):
                ugbcs.update(campaign_scope_id=campaign_scope_1.id)
            with pytest.raises(BEMServerAuthorizationError):
                ugbcs.delete()
