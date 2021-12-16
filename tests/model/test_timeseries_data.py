"""Timeseries data tests"""
import datetime as dt

import pytest

from bemserver_core.model import TimeseriesData
from bemserver_core.authorization import CurrentUser, CurrentCampaign
from bemserver_core.exceptions import (
    BEMServerAuthorizationError, BEMServerCoreMissingCampaignError,
)


class TestTimeseriesDataModel:

    @pytest.mark.usefixtures("timeseries_by_campaigns")
    def test_timeseries_data_authorizations_as_admin(
        self, users, campaigns, timeseries
    ):
        admin_user = users[0]
        assert admin_user.is_admin
        campaign_1 = campaigns[0]
        campaign_2 = campaigns[1]
        ts_1 = timeseries[0]

        start_dt = dt.datetime(2020, 1, 1, tzinfo=dt.timezone.utc)
        end_dt = dt.datetime(2020, 2, 1, tzinfo=dt.timezone.utc)
        end_dt_out = dt.datetime(2020, 5, 1, tzinfo=dt.timezone.utc)

        with CurrentUser(admin_user):

            # Export
            with pytest.raises(BEMServerCoreMissingCampaignError):
                TimeseriesData.check_can_export(start_dt, end_dt, [ts_1.id])
            with CurrentCampaign(campaign_1):
                TimeseriesData.check_can_export(start_dt, end_dt, [ts_1.id])
            # TS not in Campaign
            with CurrentCampaign(campaign_2):
                with pytest.raises(BEMServerAuthorizationError):
                    TimeseriesData.check_can_export(
                        start_dt, end_dt, [ts_1.id]
                    )
            # Dates out of Campaign
            with CurrentCampaign(campaign_1):
                with pytest.raises(BEMServerAuthorizationError):
                    TimeseriesData.check_can_export(
                        start_dt, end_dt_out, [ts_1.id]
                    )

            # Import
            with pytest.raises(BEMServerCoreMissingCampaignError):
                TimeseriesData.check_can_import(start_dt, end_dt, [ts_1.id])
            with CurrentCampaign(campaign_1):
                TimeseriesData.check_can_import(start_dt, end_dt, [ts_1.id])
            # TS not in Campaign
            with CurrentCampaign(campaign_2):
                with pytest.raises(BEMServerAuthorizationError):
                    TimeseriesData.check_can_import(
                        start_dt, end_dt, [ts_1.id]
                    )
            # Dates out of Campaign
            with CurrentCampaign(campaign_1):
                with pytest.raises(BEMServerAuthorizationError):
                    TimeseriesData.check_can_import(
                        start_dt, end_dt_out, [ts_1.id]
                    )

    @pytest.mark.usefixtures("users_by_campaigns")
    @pytest.mark.usefixtures("timeseries_by_campaigns")
    def test_timeseries_data_authorizations_as_user(
        self, users, campaigns, timeseries
    ):
        user_1 = users[1]
        assert not user_1.is_admin
        campaign_1 = campaigns[0]
        campaign_2 = campaigns[1]
        ts_2 = timeseries[1]

        start_dt = dt.datetime(2020, 1, 1, tzinfo=dt.timezone.utc)
        end_dt = dt.datetime(2020, 2, 1, tzinfo=dt.timezone.utc)
        end_dt_out = dt.datetime(2020, 5, 1, tzinfo=dt.timezone.utc)

        with CurrentUser(user_1):

            # Export
            with pytest.raises(BEMServerCoreMissingCampaignError):
                TimeseriesData.check_can_export(start_dt, end_dt, [ts_2.id])
            with CurrentCampaign(campaign_2):
                TimeseriesData.check_can_export(start_dt, end_dt, [ts_2.id])
            # TS not in Campaign
            with CurrentCampaign(campaign_1):
                with pytest.raises(BEMServerAuthorizationError):
                    TimeseriesData.check_can_export(
                        start_dt, end_dt, [ts_2.id]
                    )
            # Dates out of Campaign
            with CurrentCampaign(campaign_2):
                with pytest.raises(BEMServerAuthorizationError):
                    TimeseriesData.check_can_export(
                        start_dt, end_dt_out, [ts_2.id]
                    )

            # Import
            with pytest.raises(BEMServerCoreMissingCampaignError):
                TimeseriesData.check_can_import(
                    start_dt, end_dt, [ts_2.id]
                )
            with CurrentCampaign(campaign_2):
                TimeseriesData.check_can_import(
                    start_dt, end_dt, [ts_2.id]
                )
            # TS not in Campaign
            with CurrentCampaign(campaign_1):
                with pytest.raises(BEMServerAuthorizationError):
                    TimeseriesData.check_can_import(
                        start_dt, end_dt, [ts_2.id]
                    )
            # Dates out of Campaign
            with CurrentCampaign(campaign_2):
                with pytest.raises(BEMServerAuthorizationError):
                    TimeseriesData.check_can_import(
                        start_dt, end_dt_out, [ts_2.id]
                    )
