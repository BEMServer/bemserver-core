"""Campaign tests"""
import pytest

from bemserver_core.model import Campaign, UserGroupByCampaign

from bemserver_core.database import db
from bemserver_core.authorization import CurrentUser
from bemserver_core.exceptions import BEMServerAuthorizationError


class TestCampaignModel:
    def test_campaign_authorizations_as_admin(self, users):
        admin_user = users[0]
        assert admin_user.is_admin

        with CurrentUser(admin_user):
            campaign_1 = Campaign.new(
                name="Campaign 1",
            )
            db.session.add(campaign_1)
            db.session.commit()

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
            assert len(campaigns) == 1
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
            db.session.add(ugbc_1)
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
            assert len(ugbcs) == 1
            assert ugbcs[0].id == ugbc_2.id
            with pytest.raises(BEMServerAuthorizationError):
                UserGroupByCampaign.get_by_id(ugbc_1.id)
            with pytest.raises(BEMServerAuthorizationError):
                ugbc.update(campaign_id=campaign_1.id)
            with pytest.raises(BEMServerAuthorizationError):
                ugbc.delete()
