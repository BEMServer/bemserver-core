"""Cleanup task tests"""

import datetime as dt

import pytest

import pandas as pd

from bemserver_core.authorization import OpenBar
from bemserver_core.database import db
from bemserver_core.input_output import tsdio
from bemserver_core.model import (
    TimeseriesDataState,
    TimeseriesProperty,
    TimeseriesPropertyData,
)
from bemserver_core.tasks.cleanup import cleanup_data
from tests.utils import create_timeseries_data


class TestCleanupScheduledTask:
    @pytest.mark.parametrize("timeseries", (4,), indirect=True)
    def test_cleanup_data(self, users, timeseries, campaigns):
        admin_user = users[0]
        assert admin_user.is_admin
        ts_0 = timeseries[0]
        ts_1 = timeseries[1]
        campaign_1 = campaigns[0]

        with OpenBar():
            ds_1 = TimeseriesDataState.get(name="Raw").first()
            ds_2 = TimeseriesDataState.get(name="Clean").first()
            ts_p_min = TimeseriesProperty.get(name="Min").first()
            TimeseriesPropertyData.new(
                timeseries_id=ts_0.id,
                property_id=ts_p_min.id,
                value="12",
            )
            db.session.flush()

        start_dt = dt.datetime(2020, 1, 1, tzinfo=dt.timezone.utc)
        end_dt = dt.datetime(2020, 1, 2, tzinfo=dt.timezone.utc)
        timestamps = pd.date_range(start_dt, end_dt, inclusive="left", freq="12h")
        values = [0, 13]
        create_timeseries_data(ts_0, ds_1, timestamps, values)
        create_timeseries_data(ts_1, ds_1, timestamps, values)

        with OpenBar():
            cleanup_data(campaign_1, start_dt, end_dt)

            # Campaign 1, TS 0, min 12, max None, [0, 13] -> [-, 13]
            data_df = tsdio.get_timeseries_data(start_dt, end_dt, (ts_0,), ds_2)
            index = pd.DatetimeIndex(
                [
                    "2020-01-01T12:00:00+00:00",
                ],
                name="timestamp",
                tz="UTC",
            )
            val_0 = [13.0]
            expected_data_df = pd.DataFrame({ts_0.id: val_0}, index=index)
            assert data_df.equals(expected_data_df)

            # Campaign 2 (not cleaned), TS 1 -> no clean data
            data_df = tsdio.get_timeseries_data(start_dt, end_dt, (ts_1,), ds_2)
            index = pd.DatetimeIndex([], name="timestamp", tz="UTC")
            no_data_df = pd.DataFrame({ts_1.id: []}, index=index)
            assert data_df.equals(no_data_df)
