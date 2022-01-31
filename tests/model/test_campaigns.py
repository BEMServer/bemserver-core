"""Campaign tests"""
import pytest

from bemserver_core.model import (
    Campaign,
    UserByCampaign,
    TimeseriesClusterGroupByCampaign,
)
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

    @pytest.mark.usefixtures("users_by_campaigns")
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


class TestUserByCampaignModel:
    def test_user_by_campaign_authorizations_as_admin(self, users, campaigns):
        admin_user = users[0]
        assert admin_user.is_admin
        user_1 = users[1]
        campaign_1 = campaigns[0]
        campaign_2 = campaigns[1]

        with CurrentUser(admin_user):
            ubc_1 = UserByCampaign.new(
                user_id=user_1.id,
                campaign_id=campaign_1.id,
            )
            db.session.add(ubc_1)
            db.session.commit()

            ubc = UserByCampaign.get_by_id(ubc_1.id)
            assert ubc.id == ubc_1.id
            ubcs = list(UserByCampaign.get())
            assert len(ubcs) == 1
            assert ubcs[0].id == ubc_1.id
            ubc.update(campaign_id=campaign_2.id)
            ubc.delete()

    def test_user_by_campaign_authorizations_as_user(
        self, users, campaigns, users_by_campaigns
    ):
        user_1 = users[1]
        assert not user_1.is_admin
        campaign_1 = campaigns[0]
        campaign_2 = campaigns[1]
        ubc_1 = users_by_campaigns[0]
        ubc_2 = users_by_campaigns[1]

        with CurrentUser(user_1):
            with pytest.raises(BEMServerAuthorizationError):
                UserByCampaign.new(
                    user_id=user_1.id,
                    campaign_id=campaign_2.id,
                )

            ubc = UserByCampaign.get_by_id(ubc_2.id)
            ubcs = list(UserByCampaign.get())
            assert len(ubcs) == 1
            assert ubcs[0].id == ubc_2.id
            with pytest.raises(BEMServerAuthorizationError):
                UserByCampaign.get_by_id(ubc_1.id)
            with pytest.raises(BEMServerAuthorizationError):
                ubc.update(campaign_id=campaign_1.id)
            with pytest.raises(BEMServerAuthorizationError):
                ubc.delete()


class TestTimeseriesClusterGroupByCampaignModel:
    def test_timeseries_cluster_group_by_campaign_authorizations_as_admin(
        self, users, campaigns, timeseries_cluster_groups
    ):
        admin_user = users[0]
        assert admin_user.is_admin
        campaign_1 = campaigns[0]
        campaign_2 = campaigns[1]
        tg_1 = timeseries_cluster_groups[0]

        with CurrentUser(admin_user):
            tgbc_1 = TimeseriesClusterGroupByCampaign.new(
                timeseries_cluster_group_id=tg_1.id,
                campaign_id=campaign_1.id,
            )
            db.session.add(tgbc_1)
            db.session.commit()

            tgbc = TimeseriesClusterGroupByCampaign.get_by_id(tgbc_1.id)
            assert tgbc.id == tgbc_1.id
            tgbcs = list(TimeseriesClusterGroupByCampaign.get())
            assert len(tgbcs) == 1
            assert tgbcs[0].id == tgbc_1.id
            tgbc.update(campaign_id=campaign_2.id)
            tgbc.delete()

    @pytest.mark.usefixtures("timeseries_cluster_groups_by_users")
    @pytest.mark.usefixtures("users_by_campaigns")
    def test_timeseries_cluster_group_by_campaign_authorizations_as_user(
        self, users, campaigns, timeseries_cluster_groups_by_campaigns
    ):
        user_1 = users[1]
        assert not user_1.is_admin
        campaign_1 = campaigns[0]
        campaign_2 = campaigns[1]
        tgbc_1 = timeseries_cluster_groups_by_campaigns[0]
        tgbc_2 = timeseries_cluster_groups_by_campaigns[1]

        with CurrentUser(user_1):
            with pytest.raises(BEMServerAuthorizationError):
                TimeseriesClusterGroupByCampaign.new(
                    timeseries_cluster_group_id=user_1.id,
                    campaign_id=campaign_2.id,
                )

            tgbc = TimeseriesClusterGroupByCampaign.get_by_id(tgbc_2.id)
            tgbcs = list(TimeseriesClusterGroupByCampaign.get())
            assert len(tgbcs) == 1
            assert tgbcs[0].id == tgbc_2.id
            with pytest.raises(BEMServerAuthorizationError):
                TimeseriesClusterGroupByCampaign.get_by_id(tgbc_1.id)
            with pytest.raises(BEMServerAuthorizationError):
                tgbc.update(campaign_id=campaign_1.id)
            with pytest.raises(BEMServerAuthorizationError):
                tgbc.delete()
