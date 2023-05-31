"""Energy <=> Power conversions tests"""
import datetime as dt

import numpy as np
import pandas as pd
from pandas.testing import assert_series_equal

import pytest
from tests.utils import create_timeseries_data

from bemserver_core.model import (
    TimeseriesDataState,
)
from bemserver_core.authorization import CurrentUser, OpenBar
from bemserver_core.process.energy_power import power2energy
from bemserver_core.exceptions import (
    BEMServerCoreUndefinedUnitError,
    BEMServerCoreDimensionalityError,
)


class TestEnergyPowerProcess:
    @pytest.mark.parametrize("timeseries", (4,), indirect=True)
    def test_power2energy_process(self, users, timeseries):
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
            ts_0.unit_symbol = "W"
            ts_1.unit_symbol = "W"
            ts_2.unit_symbol = "kW"
            ts_3.unit_symbol = "W"

        start_dt = dt.datetime(2020, 1, 1, 0, 0, tzinfo=dt.timezone.utc)
        h4_dt = dt.datetime(2020, 1, 1, 4, 0, tzinfo=dt.timezone.utc)
        h6_dt = dt.datetime(2020, 1, 1, 6, 0, tzinfo=dt.timezone.utc)
        end_dt = dt.datetime(2020, 1, 1, 12, 0, tzinfo=dt.timezone.utc)

        timestamps_1 = pd.date_range(start_dt, h6_dt, inclusive="left", freq="H")
        values_1 = [ts.hour % 2 for ts in timestamps_1]
        create_timeseries_data(ts_0, ds_1, timestamps_1, values_1)

        timestamps_2 = [start_dt]
        values_2 = [ts.hour for ts in timestamps_2]
        create_timeseries_data(ts_1, ds_1, timestamps_2, values_2)
        timestamps_2 = pd.date_range(h6_dt, end_dt, inclusive="left", freq="3H")
        values_2 = [ts.hour for ts in timestamps_2]
        create_timeseries_data(ts_1, ds_1, timestamps_2, values_2)

        timestamps_3 = [h6_dt]
        values_3 = [42]
        create_timeseries_data(ts_2, ds_1, timestamps_3, values_3)

        with CurrentUser(admin_user):
            data_s = power2energy(start_dt, h4_dt, ts_0, ds_1, 1800, "Wh")
            timestamps = sum(
                [
                    [
                        dt.datetime(2020, 1, 1, hour, 0, tzinfo=dt.timezone.utc),
                        dt.datetime(2020, 1, 1, hour, 30, tzinfo=dt.timezone.utc),
                    ]
                    for hour in (0, 1, 2, 3)
                ],
                [],
            )
            expected_data_s = pd.Series(
                [0.0, 0.0, 0.5, 0.5, 0.0, 0.0, 0.5, 0.5],
                index=pd.DatetimeIndex(timestamps, name="timestamp", freq="1800S"),
            )
            assert_series_equal(data_s, expected_data_s)

            data_s = power2energy(start_dt, h4_dt, ts_0, ds_1, 3600, "Wh")
            timestamps = [
                dt.datetime(2020, 1, 1, hour, 0, tzinfo=dt.timezone.utc)
                for hour in (0, 1, 2, 3)
            ]
            expected_data_s = pd.Series(
                [0.0, 1.0, 0.0, 1.0],
                index=pd.DatetimeIndex(timestamps, name="timestamp", freq="3600S"),
            )
            assert_series_equal(data_s, expected_data_s)

            data_s = power2energy(start_dt, h4_dt, ts_0, ds_1, 7200, "Wh")
            timestamps = [
                dt.datetime(2020, 1, 1, hour, 0, tzinfo=dt.timezone.utc)
                for hour in (0, 2)
            ]
            expected_data_s = pd.Series(
                [1.0, 1.0],
                index=pd.DatetimeIndex(timestamps, name="timestamp", freq="7200S"),
            )
            assert_series_equal(data_s, expected_data_s)

            data_s = power2energy(start_dt, h4_dt, ts_0, ds_1, 3600, "mWh")
            timestamps = [
                dt.datetime(2020, 1, 1, hour, 0, tzinfo=dt.timezone.utc)
                for hour in (0, 1, 2, 3)
            ]
            expected_data_s = pd.Series(
                [0.0, 1000.0, 0.0, 1000.0],
                index=pd.DatetimeIndex(timestamps, name="timestamp", freq="3600S"),
            )
            assert_series_equal(data_s, expected_data_s)

            data_s = power2energy(h4_dt, end_dt, ts_1, ds_1, 7200, "Wh")
            timestamps = [
                dt.datetime(2020, 1, 1, hour, 0, tzinfo=dt.timezone.utc)
                for hour in (4, 6, 8, 10)
            ]
            expected_data_s = pd.Series(
                [0.0, 12.0, 15.0, 18.0],
                index=pd.DatetimeIndex(timestamps, name="timestamp", freq="7200S"),
            )
            assert_series_equal(data_s, expected_data_s)

            data_s = power2energy(start_dt, end_dt, ts_2, ds_1, 14400, "Wh")
            timestamps = [
                dt.datetime(2020, 1, 1, hour, 0, tzinfo=dt.timezone.utc)
                for hour in (0, 4, 8)
            ]
            expected_data_s = pd.Series(
                [np.nan, 168000.0, 168000.0],
                index=pd.DatetimeIndex(timestamps, name="timestamp", freq="14400S"),
            )
            assert_series_equal(data_s, expected_data_s)

            data_s = power2energy(start_dt, end_dt, ts_3, ds_1, 14400, "Wh")
            timestamps = [
                dt.datetime(2020, 1, 1, hour, 0, tzinfo=dt.timezone.utc)
                for hour in (0, 4, 8)
            ]
            expected_data_s = pd.Series(
                [np.nan, np.nan, np.nan],
                index=pd.DatetimeIndex(timestamps, name="timestamp", freq="14400S"),
            )
            assert_series_equal(data_s, expected_data_s)

            with pytest.raises(BEMServerCoreUndefinedUnitError):
                data_s = power2energy(start_dt, h4_dt, ts_0, ds_1, 3600, "dummy")

            with pytest.raises(BEMServerCoreDimensionalityError):
                data_s = power2energy(start_dt, h4_dt, ts_0, ds_1, 3600, "°C")
