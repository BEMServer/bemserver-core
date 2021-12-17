"""Campaign tests"""
import datetime as dt

import pytest

from bemserver_core.model import Campaign, UserByCampaign, TimeseriesByCampaign
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

    @pytest.mark.usefixtures("database")
    @pytest.mark.usefixtures("as_admin")
    def test_campaign_auth_dates(self):
        dt_1 = dt.datetime(2020, 1, 1, tzinfo=dt.timezone.utc)
        dt_2 = dt.datetime(2020, 2, 1, tzinfo=dt.timezone.utc)
        dt_3 = dt.datetime(2020, 3, 1, tzinfo=dt.timezone.utc)
        dt_4 = dt.datetime(2020, 4, 1, tzinfo=dt.timezone.utc)

        campaign_1 = Campaign.new(
            name="Campaign 1",
        )
        campaign_1.auth_dates((dt_1, dt_2, dt_3, dt_4))
        campaign_2 = Campaign.new(name="Campaign 2", start_time=dt_2)
        campaign_2.auth_dates((dt_2, dt_3, dt_4))
        with pytest.raises(BEMServerAuthorizationError):
            campaign_2.auth_dates((dt_1,))
        campaign_3 = Campaign.new(name="Campaign 3", end_time=dt_3)
        campaign_3.auth_dates((dt_1, dt_2, dt_3))
        with pytest.raises(BEMServerAuthorizationError):
            campaign_3.auth_dates((dt_4,))
        campaign_4 = Campaign.new(
            name="Campaign 4",
            start_time=dt_2,
            end_time=dt_3,
        )
        campaign_4.auth_dates((dt_2, dt_3))
        with pytest.raises(BEMServerAuthorizationError):
            campaign_4.auth_dates((dt_1,))
        with pytest.raises(BEMServerAuthorizationError):
            campaign_4.auth_dates((dt_4,))
        with pytest.raises(BEMServerAuthorizationError):
            campaign_4.auth_dates(
                (
                    dt_1,
                    dt_4,
                )
            )


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

        with CurrentUser(user_1):
            with pytest.raises(BEMServerAuthorizationError):
                TimeseriesByCampaign.new(
                    timeseries_id=user_1.id,
                    campaign_id=campaign_2.id,
                )

            tbc = TimeseriesByCampaign.get_by_id(tbc_2.id)
            tbcs = list(TimeseriesByCampaign.get())
            assert len(tbcs) == 1
            assert tbcs[0].id == tbc_2.id
            with pytest.raises(BEMServerAuthorizationError):
                TimeseriesByCampaign.get_by_id(tbc_1.id)
            with pytest.raises(BEMServerAuthorizationError):
                tbc.update(campaign_id=campaign_1.id)
            with pytest.raises(BEMServerAuthorizationError):
                tbc.delete()
