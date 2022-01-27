"""Timeseries data tests"""
import datetime as dt

import pytest

from bemserver_core.model import TimeseriesData
from bemserver_core.authorization import CurrentUser
from bemserver_core.exceptions import BEMServerAuthorizationError


class TestTimeseriesDataModel:
    def test_timeseries_data_authorizations_as_admin(self, users, timeseries):
        admin_user = users[0]
        assert admin_user.is_admin
        ts_1 = timeseries[0]

        start_dt = dt.datetime(2020, 1, 1, tzinfo=dt.timezone.utc)
        end_dt = dt.datetime(2020, 2, 1, tzinfo=dt.timezone.utc)

        with CurrentUser(admin_user):
            # Export
            TimeseriesData.check_can_export(start_dt, end_dt, [ts_1.id])
            # Import
            TimeseriesData.check_can_import(start_dt, end_dt, [ts_1.id])

    @pytest.mark.usefixtures("timeseries_groups_by_users")
    def test_timeseries_data_authorizations_as_user(self, users, timeseries):
        user_1 = users[1]
        assert not user_1.is_admin
        ts_1 = timeseries[0]
        ts_2 = timeseries[1]

        start_dt = dt.datetime(2020, 1, 1, tzinfo=dt.timezone.utc)
        end_dt = dt.datetime(2020, 2, 1, tzinfo=dt.timezone.utc)

        with CurrentUser(user_1):

            # Export
            with pytest.raises(BEMServerAuthorizationError):
                TimeseriesData.check_can_export(start_dt, end_dt, [ts_1.id])
            TimeseriesData.check_can_export(start_dt, end_dt, [ts_2.id])

            # Import
            with pytest.raises(BEMServerAuthorizationError):
                TimeseriesData.check_can_import(start_dt, end_dt, [ts_1.id])
            TimeseriesData.check_can_import(start_dt, end_dt, [ts_2.id])
