"""Cleanup tests"""
import datetime as dt

import numpy as np
import pandas as pd

import pytest
from tests.utils import create_timeseries_data

from bemserver_core.model import (
    TimeseriesDataState,
    TimeseriesProperty,
    TimeseriesPropertyData,
)
from bemserver_core.authorization import CurrentUser, OpenBar
from bemserver_core.process.cleanup import cleanup


class TestCleanupProcess:
    @pytest.mark.parametrize("timeseries", (4,), indirect=True)
    def test_cleanup_process(self, users, timeseries):
        admin_user = users[0]
        assert admin_user.is_admin
        # Min/Max
        ts_0 = timeseries[0]
        # Min only
        ts_1 = timeseries[1]
        # None
        ts_2 = timeseries[2]
        # Min/Max, no data
        ts_3 = timeseries[3]

        with OpenBar():
            ds_1 = TimeseriesDataState.get(name="Raw").first()
            ts_p_min = TimeseriesProperty.get(name="Min").first()
            ts_p_max = TimeseriesProperty.get(name="Max").first()
            TimeseriesPropertyData.new(
                timeseries_id=ts_0.id,
                property_id=ts_p_min.id,
                value="12",
            )
            TimeseriesPropertyData.new(
                timeseries_id=ts_0.id,
                property_id=ts_p_max.id,
                value="42",
            )
            TimeseriesPropertyData.new(
                timeseries_id=ts_1.id,
                property_id=ts_p_min.id,
                value="12",
            )
            TimeseriesPropertyData.new(
                timeseries_id=ts_3.id,
                property_id=ts_p_min.id,
                value="12",
            )
            TimeseriesPropertyData.new(
                timeseries_id=ts_3.id,
                property_id=ts_p_max.id,
                value="42",
            )

        start_dt = dt.datetime(2020, 1, 1, tzinfo=dt.timezone.utc)
        end_dt = dt.datetime(2020, 1, 2, tzinfo=dt.timezone.utc)
        timestamps = pd.date_range(start_dt, end_dt, inclusive="both", freq="6H")
        values = [0, 13, 33, 42, 69]
        create_timeseries_data(ts_0, ds_1, timestamps, values)
        create_timeseries_data(ts_1, ds_1, timestamps, values)
        create_timeseries_data(ts_2, ds_1, timestamps, values)

        with CurrentUser(admin_user):
            ts_l = (ts_0, ts_1, ts_2, ts_3)
            ret = cleanup(start_dt, end_dt, ts_l, ds_1, inclusive="both")
            expected = pd.DataFrame(
                {
                    ts_0.id: [np.nan, 13, 33, 42, np.nan],
                    ts_1.id: [np.nan, 13, 33, 42, 69],
                    ts_2.id: [0, 13, 33, 42, 69],
                    ts_3.id: [np.nan, np.nan, np.nan, np.nan, np.nan],
                },
                index=pd.DatetimeIndex(timestamps, name="timestamp"),
                dtype=float,
            )
            assert ret.equals(expected)
