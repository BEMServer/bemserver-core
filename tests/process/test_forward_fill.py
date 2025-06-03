"""Forward fill tests"""

import datetime as dt
from zoneinfo import ZoneInfo

import pytest

import numpy as np
import pandas as pd
from pandas.testing import assert_frame_equal

from bemserver_core.authorization import CurrentUser, OpenBar
from bemserver_core.model import (
    TimeseriesDataState,
)
from bemserver_core.process.forward_fill import ffill
from tests.utils import create_timeseries_data


class TestForewardFillProcess:
    @pytest.mark.parametrize("timeseries", (4,), indirect=True)
    def test_ffill_process(self, users, timeseries):
        admin_user = users[0]
        assert admin_user.is_admin
        # Data with higher frequency
        ts_0 = timeseries[0]
        # No data at beginning but data before
        ts_1 = timeseries[1]
        # Single data, not at beginning
        ts_2 = timeseries[2]
        # No data at all
        ts_3 = timeseries[3]

        with OpenBar():
            ds_1 = TimeseriesDataState.get(name="Clean").first()

        start_dt = dt.datetime(2020, 1, 1, 0, 0, tzinfo=dt.timezone.utc)
        h4_dt = dt.datetime(2020, 1, 1, 4, 0, tzinfo=dt.timezone.utc)
        h6_dt = dt.datetime(2020, 1, 1, 6, 0, tzinfo=dt.timezone.utc)
        end_dt = dt.datetime(2020, 1, 1, 12, 0, tzinfo=dt.timezone.utc)

        timestamps_1 = pd.date_range(start_dt, h6_dt, inclusive="left", freq="h")
        values_1 = [ts.hour % 2 for ts in timestamps_1]
        create_timeseries_data(ts_0, ds_1, timestamps_1, values_1)

        timestamps_2 = [start_dt]
        values_2 = [ts.hour for ts in timestamps_2]
        create_timeseries_data(ts_1, ds_1, timestamps_2, values_2)
        timestamps_2 = pd.date_range(h6_dt, end_dt, inclusive="left", freq="3h")
        values_2 = [ts.hour for ts in timestamps_2]
        create_timeseries_data(ts_1, ds_1, timestamps_2, values_2)

        timestamps_3 = [h6_dt]
        values_3 = [42]
        create_timeseries_data(ts_2, ds_1, timestamps_3, values_3)

        with CurrentUser(admin_user):
            ts_l = (ts_0, ts_1, ts_2, ts_3)

            data_df = ffill(h4_dt, end_dt, ts_l, ds_1, 2, "hour")

            timestamps = [
                dt.datetime(2020, 1, 1, hour, 0, tzinfo=dt.timezone.utc)
                for hour in (4, 5, 6, 8, 9, 10)
            ]
            expected_data_df = pd.DataFrame(
                {
                    ts_0.id: [0.0, 1.0, 1.0, 1.0, np.nan, 1.0],
                    ts_1.id: [0.0, np.nan, 6.0, 6.0, 9.0, 9.0],
                    ts_2.id: [np.nan, np.nan, 42.0, 42.0, np.nan, 42.0],
                    ts_3.id: [np.nan, np.nan, np.nan, np.nan, np.nan, np.nan],
                },
                index=pd.DatetimeIndex(timestamps, name="timestamp"),
            )
            assert_frame_equal(data_df, expected_data_df)

            # Check start datetime is ceiled
            data_df = ffill(
                h4_dt + dt.timedelta(seconds=250), end_dt, ts_l, ds_1, 2, "hour"
            )

            timestamps = [
                dt.datetime(2020, 1, 1, hour, 0, tzinfo=dt.timezone.utc)
                for hour in (6, 8, 9, 10)
            ]
            expected_data_df = pd.DataFrame(
                {
                    ts_0.id: [1.0, 1.0, np.nan, 1.0],
                    ts_1.id: [6.0, 6.0, 9.0, 9.0],
                    ts_2.id: [42.0, 42.0, np.nan, 42.0],
                    ts_3.id: [np.nan, np.nan, np.nan, np.nan],
                },
                index=pd.DatetimeIndex(timestamps, name="timestamp"),
            )
            assert_frame_equal(data_df, expected_data_df)

            # Test with TS duplicate to ensure it doesn't crash
            ts_l = (ts_3, ts_2, ts_2, ts_0)

            data_df = ffill(h4_dt, end_dt, ts_l, ds_1, 2, "hour")

            timestamps = [
                dt.datetime(2020, 1, 1, hour, 0, tzinfo=dt.timezone.utc)
                for hour in (4, 5, 6, 8, 10)
            ]
            expected_data_df = pd.DataFrame(
                {
                    1: [np.nan, np.nan, np.nan, np.nan, np.nan],
                    2: [np.nan, np.nan, 42.0, 42.0, 42.0],
                    3: [np.nan, np.nan, 42.0, 42.0, 42.0],
                    4: [0.0, 1.0, 1.0, 1.0, 1.0],
                },
                index=pd.DatetimeIndex(timestamps, name="timestamp"),
            )
            expected_data_df.columns = [ts_3.id, ts_2.id, ts_2.id, ts_0.id]
            assert_frame_equal(data_df, expected_data_df)

            # Check result is in start_dt timezone
            ts_l = (ts_0, ts_1, ts_2, ts_3)

            data_df = ffill(
                h4_dt.astimezone(ZoneInfo("Europe/Paris")),
                end_dt,
                ts_l,
                ds_1,
                2,
                "hour",
            )

            timestamps = [
                dt.datetime(2020, 1, 1, hour, 0, tzinfo=ZoneInfo("Europe/Paris"))
                for hour in (6, 7, 8, 10, 12)
            ]
            expected_data_df = pd.DataFrame(
                {
                    ts_0.id: [1.0, np.nan, 1.0, 1.0, 1.0],
                    ts_1.id: [0.0, 6.0, 6.0, 9.0, 9.0],
                    ts_2.id: [np.nan, 42.0, 42.0, 42.0, 42.0],
                    ts_3.id: [np.nan, np.nan, np.nan, np.nan, np.nan],
                },
                index=pd.DatetimeIndex(timestamps, name="timestamp"),
            )
            assert_frame_equal(data_df, expected_data_df)
