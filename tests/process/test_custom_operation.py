"""Custom operation tests"""
import datetime as dt

import numpy as np
import pandas as pd

import pytest
from tests.utils import create_timeseries_data

from bemserver_core.database import db
from bemserver_core.model import (
    TimeseriesDataState,
    TimeseriesProperty,
    TimeseriesPropertyData,
)
from bemserver_core.authorization import CurrentUser, OpenBar
from bemserver_core.process.custom_operation import evaluate


class TestCleanup:
    @pytest.mark.parametrize("timeseries", (4,), indirect=True)
    def test_custom_operation(self, users, timeseries):
        admin_user = users[0]
        assert admin_user.is_admin

        ts_0 = timeseries[0]
        ts_1 = timeseries[1]

        with OpenBar():
            ds_1 = TimeseriesDataState.get(name="Raw").first()

        start_dt = dt.datetime(2020, 1, 1, 0, 0, tzinfo=dt.timezone.utc)
        end_dt = dt.datetime(2020, 1, 1, 4, 0, tzinfo=dt.timezone.utc)
        timestamps_1 = pd.date_range(start_dt, end_dt, inclusive="left", freq="1H")
        timestamps_2 = pd.date_range(start_dt, end_dt, inclusive="left", freq="2H")
        values_1 = [12, 13, 33, 69]
        values_2 = [12, 0]
        create_timeseries_data(ts_0, ds_1, timestamps_1, values_1)
        create_timeseries_data(ts_1, ds_1, timestamps_2, values_2)

        with CurrentUser(admin_user):
            ts_l = (ts_0, ts_1)

            operation = f"ts_{ts_0.id} + ts_{ts_1.id}"
            print(operation)
            ret = evaluate(start_dt, end_dt, ds_1, operation)
            print(ret)


#             expected = pd.Series(
#                 [np.nan, 13, 33, np.nan],
#                 index=pd.DatetimeIndex(timestamps_1, name="timestamp"),
#                 dtype=float,
#             )
#             assert ret.equals(expected)
