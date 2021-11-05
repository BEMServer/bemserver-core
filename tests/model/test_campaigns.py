"""Campaign tests"""
import pytest

from bemserver_core.model import (
    Campaign, UserByCampaign, TimeseriesByCampaign, TimeseriesByCampaignByUser
)
from bemserver_core.database import db
from bemserver_core.authentication import CurrentUser
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


class TestTimeseriesByCampaignModel:

    def test_timeseries_by_campaign_authorizations_as_admin(
            self, users, campaigns, timeseries
    ):
        admin_user = users[0]
        assert admin_user.is_admin
        campaign_1 = campaigns[0]
        campaign_2 = campaigns[1]
        ts_1 = timeseries[0]

        with CurrentUser(admin_user):
            tbc_1 = TimeseriesByCampaign.new(
                timeseries_id=ts_1.id,
                campaign_id=campaign_1.id,
            )
            db.session.add(tbc_1)
            db.session.commit()

            tbc = TimeseriesByCampaign.get_by_id(tbc_1.id)
            assert tbc.id == tbc_1.id
            tbcs = list(TimeseriesByCampaign.get())
            assert len(tbcs) == 1
            assert tbcs[0].id == tbc_1.id
            tbc.update(campaign_id=campaign_2.id)
            tbc.delete()

    @pytest.mark.usefixtures("users_by_campaigns")
    def test_timeseries_by_campaign_authorizations_as_user(
        self, users, campaigns, timeseries_by_campaigns
    ):
        user_1 = users[1]
        assert not user_1.is_admin
        campaign_1 = campaigns[0]
        campaign_2 = campaigns[1]
        tbc_1 = timeseries_by_campaigns[0]
        tbc_2 = timeseries_by_campaigns[1]
        tbc_4 = timeseries_by_campaigns[3]

        with CurrentUser(user_1):
            with pytest.raises(BEMServerAuthorizationError):
                TimeseriesByCampaign.new(
                    timeseries_id=user_1.id,
                    campaign_id=campaign_2.id,
                )

            tbc = TimeseriesByCampaign.get_by_id(tbc_2.id)
            tbcs = list(TimeseriesByCampaign.get())
            assert len(tbcs) == 2
            assert tbcs[0].id == tbc_2.id
            assert tbcs[1].id == tbc_4.id
            with pytest.raises(BEMServerAuthorizationError):
                TimeseriesByCampaign.get_by_id(tbc_1.id)
            with pytest.raises(BEMServerAuthorizationError):
                tbc.update(campaign_id=campaign_1.id)
            with pytest.raises(BEMServerAuthorizationError):
                tbc.delete()


class TestTimeseriesByCampaignByUserModel:

    @pytest.mark.usefixtures("users_by_campaigns")
    def test_timeseries_by_campaign_by_user_authorizations_as_admin(
        self, users, timeseries_by_campaigns
    ):
        admin_user = users[0]
        assert admin_user.is_admin
        user_1 = users[1]
        tbc_1 = timeseries_by_campaigns[0]
        tbc_2 = timeseries_by_campaigns[1]

        with CurrentUser(admin_user):
            tbcbu_1 = TimeseriesByCampaignByUser.new(
                user_id=user_1.id,
                timeseries_by_campaign_id=tbc_1.id,
            )
            db.session.add(tbc_1)
            db.session.commit()

            tbcbu = TimeseriesByCampaignByUser.get_by_id(tbcbu_1.id)
            assert tbcbu.id == tbcbu_1.id
            tbcbus = list(TimeseriesByCampaignByUser.get())
            assert len(tbcbus) == 1
            assert tbcbus[0].id == tbcbu_1.id
            tbcbu.update(timeseries_by_campaign_id=tbc_2.id)
            tbcbu.delete()

    @pytest.mark.usefixtures("users_by_campaigns")
    def test_timeseries_by_campaign_by_user_authorizations_as_user(
        self, users, timeseries_by_campaigns, timeseries_by_campaigns_by_users
    ):
        user_1 = users[1]
        assert not user_1.is_admin
        tbc_1 = timeseries_by_campaigns[0]
        tbcbu_1 = timeseries_by_campaigns_by_users[0]
        tbcbu_2 = timeseries_by_campaigns_by_users[1]

        with CurrentUser(user_1):
            with pytest.raises(BEMServerAuthorizationError):
                TimeseriesByCampaignByUser.new(
                    user_id=user_1.id,
                    timeseries_by_campaign_id=tbc_1.id,
                )

            tbcbu = TimeseriesByCampaignByUser.get_by_id(tbcbu_2.id)
            tbcbus = list(TimeseriesByCampaignByUser.get())
            assert len(tbcbus) == 1
            assert tbcbus[0].id == tbcbu_2.id
            with pytest.raises(BEMServerAuthorizationError):
                TimeseriesByCampaignByUser.get_by_id(tbcbu_1.id)
            with pytest.raises(BEMServerAuthorizationError):
                tbcbu.update(timeseries_by_campaign_id=tbc_1.id)
            with pytest.raises(BEMServerAuthorizationError):
                tbcbu.delete()
