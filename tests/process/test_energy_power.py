"""Energy <=> Power conversions tests"""

import datetime as dt
from zoneinfo import ZoneInfo

import pytest

import numpy as np
import pandas as pd
from pandas.testing import assert_series_equal

from bemserver_core.authorization import CurrentUser, OpenBar
from bemserver_core.exceptions import (
    BEMServerCoreDimensionalityError,
    BEMServerCoreEnergyPowerProcessMissingIntervalError,
    BEMServerCoreUndefinedUnitError,
)
from bemserver_core.model import (
    TimeseriesDataState,
    TimeseriesProperty,
    TimeseriesPropertyData,
)
from bemserver_core.process.energy_power import (
    energy2power,
    energyindex2energy,
    energyindex2power,
    power2energy,
)
from tests.utils import create_timeseries_data


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
                index=pd.DatetimeIndex(timestamps, name="timestamp", freq="1800s"),
            )
            assert_series_equal(data_s, expected_data_s)

            data_s = power2energy(start_dt, h4_dt, ts_0, ds_1, 3600, "Wh")
            timestamps = [
                dt.datetime(2020, 1, 1, hour, 0, tzinfo=dt.timezone.utc)
                for hour in (0, 1, 2, 3)
            ]
            expected_data_s = pd.Series(
                [0.0, 1.0, 0.0, 1.0],
                index=pd.DatetimeIndex(timestamps, name="timestamp", freq="3600s"),
            )
            assert_series_equal(data_s, expected_data_s)

            data_s = power2energy(start_dt, h4_dt, ts_0, ds_1, 7200, "Wh")
            timestamps = [
                dt.datetime(2020, 1, 1, hour, 0, tzinfo=dt.timezone.utc)
                for hour in (0, 2)
            ]
            expected_data_s = pd.Series(
                [1.0, 1.0],
                index=pd.DatetimeIndex(timestamps, name="timestamp", freq="7200s"),
            )
            assert_series_equal(data_s, expected_data_s)

            data_s = power2energy(start_dt, h4_dt, ts_0, ds_1, 3600, "mWh")
            timestamps = [
                dt.datetime(2020, 1, 1, hour, 0, tzinfo=dt.timezone.utc)
                for hour in (0, 1, 2, 3)
            ]
            expected_data_s = pd.Series(
                [0.0, 1000.0, 0.0, 1000.0],
                index=pd.DatetimeIndex(timestamps, name="timestamp", freq="3600s"),
            )
            assert_series_equal(data_s, expected_data_s)

            data_s = power2energy(h4_dt, end_dt, ts_1, ds_1, 7200, "Wh")
            timestamps = [
                dt.datetime(2020, 1, 1, hour, 0, tzinfo=dt.timezone.utc)
                for hour in (4, 6, 8, 10)
            ]
            expected_data_s = pd.Series(
                [0.0, 12.0, 15.0, 18.0],
                index=pd.DatetimeIndex(timestamps, name="timestamp", freq="7200s"),
            )
            assert_series_equal(data_s, expected_data_s)

            data_s = power2energy(start_dt, end_dt, ts_2, ds_1, 14400, "Wh")
            timestamps = [
                dt.datetime(2020, 1, 1, hour, 0, tzinfo=dt.timezone.utc)
                for hour in (0, 4, 8)
            ]
            expected_data_s = pd.Series(
                [np.nan, 168000.0, 168000.0],
                index=pd.DatetimeIndex(timestamps, name="timestamp", freq="14400s"),
            )
            assert_series_equal(data_s, expected_data_s)

            data_s = power2energy(start_dt, end_dt, ts_3, ds_1, 14400, "Wh")
            timestamps = [
                dt.datetime(2020, 1, 1, hour, 0, tzinfo=dt.timezone.utc)
                for hour in (0, 4, 8)
            ]
            expected_data_s = pd.Series(
                [np.nan, np.nan, np.nan],
                index=pd.DatetimeIndex(timestamps, name="timestamp", freq="14400s"),
            )
            assert_series_equal(data_s, expected_data_s)

            # Check result is in start_dt timezone
            data_s = power2energy(
                start_dt.astimezone(ZoneInfo("Europe/Paris")),
                h4_dt,
                ts_0,
                ds_1,
                1800,
                "Wh",
            )
            timestamps = sum(
                [
                    [
                        dt.datetime(
                            2020, 1, 1, hour, 0, tzinfo=dt.timezone.utc
                        ).astimezone(ZoneInfo("Europe/Paris")),
                        dt.datetime(
                            2020, 1, 1, hour, 30, tzinfo=dt.timezone.utc
                        ).astimezone(ZoneInfo("Europe/Paris")),
                    ]
                    for hour in (0, 1, 2, 3)
                ],
                [],
            )
            expected_data_s = pd.Series(
                [0.0, 0.0, 0.5, 0.5, 0.0, 0.0, 0.5, 0.5],
                index=pd.DatetimeIndex(timestamps, name="timestamp", freq="1800s"),
            )
            assert_series_equal(data_s, expected_data_s)

            with pytest.raises(BEMServerCoreUndefinedUnitError):
                data_s = power2energy(start_dt, h4_dt, ts_0, ds_1, 3600, "dummy")

            with pytest.raises(BEMServerCoreDimensionalityError):
                data_s = power2energy(start_dt, h4_dt, ts_0, ds_1, 3600, "°C")

    @pytest.mark.parametrize("timeseries", (5,), indirect=True)
    def test_energy2power_process(self, users, timeseries):
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
        # No data and no interval
        ts_4 = timeseries[4]

        with OpenBar():
            ds_1 = TimeseriesDataState.get(name="Clean").first()
            ts_p_int = TimeseriesProperty.get(name="Interval").first()
            ts_0.unit_symbol = "Wh"
            ts_1.unit_symbol = "Wh"
            ts_2.unit_symbol = "kWh"
            ts_3.unit_symbol = "Wh"
            TimeseriesPropertyData.new(
                timeseries_id=ts_0.id,
                property_id=ts_p_int.id,
                value="3600",
            )
            TimeseriesPropertyData.new(
                timeseries_id=ts_1.id,
                property_id=ts_p_int.id,
                value="3600",
            )
            TimeseriesPropertyData.new(
                timeseries_id=ts_2.id,
                property_id=ts_p_int.id,
                value="7200",
            )
            TimeseriesPropertyData.new(
                timeseries_id=ts_3.id,
                property_id=ts_p_int.id,
                value="3600",
            )

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
            data_s = energy2power(start_dt, h4_dt, ts_0, ds_1, "W")
            timestamps = [
                dt.datetime(2020, 1, 1, hour, 0, tzinfo=dt.timezone.utc)
                for hour in (0, 1, 2, 3)
            ]
            expected_data_s = pd.Series(
                [0.0, 1.0, 0.0, 1.0],
                index=pd.DatetimeIndex(timestamps, name="timestamp"),
            )
            assert_series_equal(data_s, expected_data_s)

            data_s = energy2power(start_dt, h4_dt, ts_0, ds_1, "mW")
            timestamps = [
                dt.datetime(2020, 1, 1, hour, 0, tzinfo=dt.timezone.utc)
                for hour in (0, 1, 2, 3)
            ]
            expected_data_s = pd.Series(
                [0.0, 1000.0, 0.0, 1000.0],
                index=pd.DatetimeIndex(timestamps, name="timestamp"),
            )
            assert_series_equal(data_s, expected_data_s)

            data_s = energy2power(h4_dt, end_dt, ts_1, ds_1, "W")
            timestamps = [
                dt.datetime(2020, 1, 1, hour, 0, tzinfo=dt.timezone.utc)
                for hour in (6, 9)
            ]
            expected_data_s = pd.Series(
                [6.0, 9.0],
                index=pd.DatetimeIndex(timestamps, name="timestamp"),
            )
            assert_series_equal(data_s, expected_data_s)

            data_s = energy2power(start_dt, end_dt, ts_2, ds_1, "W")
            timestamps = [dt.datetime(2020, 1, 1, 6, 0, tzinfo=dt.timezone.utc)]
            expected_data_s = pd.Series(
                [
                    21000.0,
                ],
                index=pd.DatetimeIndex(timestamps, name="timestamp"),
            )
            assert_series_equal(data_s, expected_data_s)

            data_s = energy2power(start_dt, end_dt, ts_3, ds_1, "W")
            timestamps = []
            expected_data_s = pd.Series(
                [],
                index=pd.DatetimeIndex(
                    timestamps, name="timestamp", tz=dt.timezone.utc
                ),
                dtype=float,
            )
            assert_series_equal(data_s, expected_data_s)

            with pytest.raises(
                BEMServerCoreEnergyPowerProcessMissingIntervalError,
                match=f"Missing interval for timeseries {ts_4.name}",
            ):
                data_s = energy2power(start_dt, end_dt, ts_4, ds_1, "W")

            # Check result is in start_dt timezone
            data_s = energy2power(
                start_dt.astimezone(ZoneInfo("Europe/Paris")), h4_dt, ts_0, ds_1, "W"
            )
            timestamps = [
                dt.datetime(2020, 1, 1, hour, 0, tzinfo=dt.timezone.utc).astimezone(
                    ZoneInfo("Europe/Paris")
                )
                for hour in (0, 1, 2, 3)
            ]
            expected_data_s = pd.Series(
                [0.0, 1.0, 0.0, 1.0],
                index=pd.DatetimeIndex(timestamps, name="timestamp"),
            )
            assert_series_equal(data_s, expected_data_s)

            with pytest.raises(BEMServerCoreUndefinedUnitError):
                data_s = energy2power(start_dt, end_dt, ts_0, ds_1, "dummy")

            with pytest.raises(BEMServerCoreDimensionalityError):
                data_s = energy2power(start_dt, end_dt, ts_0, ds_1, "°C")

    @pytest.mark.parametrize("timeseries", (4,), indirect=True)
    def test_energyindex2power_process(self, users, timeseries):
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
            ts_0.unit_symbol = "Wh"
            ts_1.unit_symbol = "Wh"
            ts_2.unit_symbol = "kWh"
            ts_3.unit_symbol = "Wh"

        start_dt = dt.datetime(2020, 1, 1, 0, 0, tzinfo=dt.timezone.utc)
        h4_dt = dt.datetime(2020, 1, 1, 4, 0, tzinfo=dt.timezone.utc)
        h6_dt = dt.datetime(2020, 1, 1, 6, 0, tzinfo=dt.timezone.utc)
        end_dt = dt.datetime(2020, 1, 1, 12, 0, tzinfo=dt.timezone.utc)

        timestamps_1 = pd.date_range(start_dt, h6_dt, inclusive="left", freq="h")
        values_1 = [ts.hour**2 for ts in timestamps_1]
        create_timeseries_data(ts_0, ds_1, timestamps_1, values_1)

        timestamps_2 = [start_dt]
        values_2 = [ts.hour**2 for ts in timestamps_2]
        create_timeseries_data(ts_1, ds_1, timestamps_2, values_2)
        timestamps_2 = pd.date_range(h6_dt, end_dt, inclusive="left", freq="2h")
        values_2 = [ts.hour**2 for ts in timestamps_2]
        create_timeseries_data(ts_1, ds_1, timestamps_2, values_2)

        timestamps_3 = [h6_dt]
        values_3 = [42]
        create_timeseries_data(ts_2, ds_1, timestamps_3, values_3)

        with CurrentUser(admin_user):
            data_s = energyindex2power(start_dt, h6_dt, ts_0, ds_1, 3600, "W")
            timestamps = [
                dt.datetime(2020, 1, 1, hour, 0, tzinfo=dt.timezone.utc)
                for hour in (0, 1, 2, 3, 4, 5)
            ]
            expected_data_s = pd.Series(
                [1.0, 3.0, 5.0, 7.0, 9.0, np.nan],
                index=pd.DatetimeIndex(timestamps, name="timestamp", freq="h"),
            )
            assert_series_equal(data_s, expected_data_s)

            data_s = energyindex2power(start_dt, h6_dt, ts_0, ds_1, 7200, "W")
            timestamps = [
                dt.datetime(2020, 1, 1, hour, 0, tzinfo=dt.timezone.utc)
                for hour in (0, 2, 4)
            ]
            expected_data_s = pd.Series(
                [2.0, 6.0, 9.0],
                index=pd.DatetimeIndex(timestamps, name="timestamp", freq="2h"),
            )
            assert_series_equal(data_s, expected_data_s)

            data_s = energyindex2power(start_dt, h6_dt, ts_0, ds_1, 3600, "mW")
            timestamps = [
                dt.datetime(2020, 1, 1, hour, 0, tzinfo=dt.timezone.utc)
                for hour in (0, 1, 2, 3, 4, 5)
            ]
            expected_data_s = pd.Series(
                [1000.0, 3000.0, 5000.0, 7000.0, 9000.0, np.nan],
                index=pd.DatetimeIndex(timestamps, name="timestamp", freq="h"),
            )
            assert_series_equal(data_s, expected_data_s)

            data_s = energyindex2power(h4_dt, end_dt, ts_1, ds_1, 3600, "W")
            timestamps = [
                dt.datetime(2020, 1, 1, hour, 0, tzinfo=dt.timezone.utc)
                for hour in (4, 5, 6, 7, 8, 9, 10, 11)
            ]
            expected_data_s = pd.Series(
                [np.nan, np.nan, 14.0, 14.0, 18.0, 18.0, np.nan, np.nan],
                index=pd.DatetimeIndex(timestamps, name="timestamp", freq="h"),
            )
            assert_series_equal(data_s, expected_data_s)

            data_s = energyindex2power(start_dt, end_dt, ts_1, ds_1, 3600, "W")
            timestamps = [
                dt.datetime(2020, 1, 1, hour, 0, tzinfo=dt.timezone.utc)
                for hour in (0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11)
            ]
            expected_data_s = pd.Series(
                [6.0, 6.0, 6.0, 6.0, 6.0, 6.0, 14.0, 14.0, 18.0, 18.0, np.nan, np.nan],
                index=pd.DatetimeIndex(timestamps, name="timestamp", freq="h"),
            )
            assert_series_equal(data_s, expected_data_s)

            data_s = energyindex2power(start_dt, end_dt, ts_2, ds_1, 3600, "W")
            timestamps = [
                dt.datetime(2020, 1, 1, hour, 0, tzinfo=dt.timezone.utc)
                for hour in range(12)
            ]
            expected_data_s = pd.Series(
                12 * [np.nan],
                index=pd.DatetimeIndex(timestamps, name="timestamp", freq="h"),
            )
            assert_series_equal(data_s, expected_data_s)

            data_s = energyindex2power(start_dt, end_dt, ts_3, ds_1, 14400, "W")
            timestamps = [
                dt.datetime(2020, 1, 1, hour, 0, tzinfo=dt.timezone.utc)
                for hour in range(0, 12, 4)
            ]
            expected_data_s = pd.Series(
                3 * [np.nan],
                index=pd.DatetimeIndex(
                    timestamps, name="timestamp", tz=dt.timezone.utc, freq="4h"
                ),
                dtype=float,
            )
            assert_series_equal(data_s, expected_data_s)

            # Check result is in start_dt timezone
            data_s = energyindex2power(
                start_dt.astimezone(ZoneInfo("Europe/Paris")),
                h6_dt,
                ts_0,
                ds_1,
                3600,
                "W",
            )
            timestamps = [
                dt.datetime(2020, 1, 1, hour, 0, tzinfo=dt.timezone.utc).astimezone(
                    ZoneInfo("Europe/Paris")
                )
                for hour in (0, 1, 2, 3, 4, 5)
            ]
            expected_data_s = pd.Series(
                [1.0, 3.0, 5.0, 7.0, 9.0, np.nan],
                index=pd.DatetimeIndex(timestamps, name="timestamp", freq="h"),
            )
            assert_series_equal(data_s, expected_data_s)

            with pytest.raises(BEMServerCoreUndefinedUnitError):
                data_s = energyindex2power(start_dt, end_dt, ts_0, ds_1, 3600, "dummy")

            with pytest.raises(BEMServerCoreDimensionalityError):
                data_s = energyindex2power(start_dt, end_dt, ts_0, ds_1, 3600, "°C")

    @pytest.mark.parametrize("timeseries", (4,), indirect=True)
    def test_energyindex2energy_process(self, users, timeseries):
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
            ts_0.unit_symbol = "Wh"
            ts_1.unit_symbol = "Wh"
            ts_2.unit_symbol = "kWh"
            ts_3.unit_symbol = "Wh"

        start_dt = dt.datetime(2020, 1, 1, 0, 0, tzinfo=dt.timezone.utc)
        h4_dt = dt.datetime(2020, 1, 1, 4, 0, tzinfo=dt.timezone.utc)
        h6_dt = dt.datetime(2020, 1, 1, 6, 0, tzinfo=dt.timezone.utc)
        end_dt = dt.datetime(2020, 1, 1, 12, 0, tzinfo=dt.timezone.utc)

        timestamps_1 = pd.date_range(start_dt, h6_dt, inclusive="left", freq="h")
        values_1 = [ts.hour**2 for ts in timestamps_1]
        create_timeseries_data(ts_0, ds_1, timestamps_1, values_1)

        timestamps_2 = [start_dt]
        values_2 = [ts.hour**2 for ts in timestamps_2]
        create_timeseries_data(ts_1, ds_1, timestamps_2, values_2)
        timestamps_2 = pd.date_range(h6_dt, end_dt, inclusive="left", freq="2h")
        values_2 = [ts.hour**2 for ts in timestamps_2]
        create_timeseries_data(ts_1, ds_1, timestamps_2, values_2)

        timestamps_3 = [h6_dt]
        values_3 = [42]
        create_timeseries_data(ts_2, ds_1, timestamps_3, values_3)

        with CurrentUser(admin_user):
            data_s = energyindex2energy(start_dt, h6_dt, ts_0, ds_1, 3600, "Wh")
            timestamps = [
                dt.datetime(2020, 1, 1, hour, 0, tzinfo=dt.timezone.utc)
                for hour in (0, 1, 2, 3, 4, 5)
            ]
            expected_data_s = pd.Series(
                [1.0, 3.0, 5.0, 7.0, 9.0, np.nan],
                index=pd.DatetimeIndex(timestamps, name="timestamp", freq="h"),
            )
            assert_series_equal(data_s, expected_data_s)

            data_s = energyindex2energy(start_dt, h6_dt, ts_0, ds_1, 7200, "Wh")
            timestamps = [
                dt.datetime(2020, 1, 1, hour, 0, tzinfo=dt.timezone.utc)
                for hour in (0, 2, 4)
            ]
            expected_data_s = pd.Series(
                [4.0, 12.0, 18.0],
                index=pd.DatetimeIndex(timestamps, name="timestamp", freq="2h"),
            )
            assert_series_equal(data_s, expected_data_s)

            data_s = energyindex2energy(start_dt, h6_dt, ts_0, ds_1, 3600, "mWh")
            timestamps = [
                dt.datetime(2020, 1, 1, hour, 0, tzinfo=dt.timezone.utc)
                for hour in (0, 1, 2, 3, 4, 5)
            ]
            expected_data_s = pd.Series(
                [1000.0, 3000.0, 5000.0, 7000.0, 9000.0, np.nan],
                index=pd.DatetimeIndex(timestamps, name="timestamp", freq="h"),
            )
            assert_series_equal(data_s, expected_data_s)

            data_s = energyindex2energy(h4_dt, end_dt, ts_1, ds_1, 3600, "Wh")
            timestamps = [
                dt.datetime(2020, 1, 1, hour, 0, tzinfo=dt.timezone.utc)
                for hour in (4, 5, 6, 7, 8, 9, 10, 11)
            ]
            expected_data_s = pd.Series(
                [np.nan, np.nan, 14.0, 14.0, 18.0, 18.0, np.nan, np.nan],
                index=pd.DatetimeIndex(timestamps, name="timestamp", freq="h"),
            )
            assert_series_equal(data_s, expected_data_s)

            data_s = energyindex2energy(start_dt, end_dt, ts_1, ds_1, 3600, "Wh")
            timestamps = [
                dt.datetime(2020, 1, 1, hour, 0, tzinfo=dt.timezone.utc)
                for hour in (0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11)
            ]
            expected_data_s = pd.Series(
                [6.0, 6.0, 6.0, 6.0, 6.0, 6.0, 14.0, 14.0, 18.0, 18.0, np.nan, np.nan],
                index=pd.DatetimeIndex(timestamps, name="timestamp", freq="h"),
            )
            assert_series_equal(data_s, expected_data_s)

            data_s = energyindex2energy(start_dt, end_dt, ts_2, ds_1, 3600, "Wh")
            timestamps = [
                dt.datetime(2020, 1, 1, hour, 0, tzinfo=dt.timezone.utc)
                for hour in range(12)
            ]
            expected_data_s = pd.Series(
                12 * [np.nan],
                index=pd.DatetimeIndex(timestamps, name="timestamp", freq="h"),
            )
            assert_series_equal(data_s, expected_data_s)

            data_s = energyindex2energy(start_dt, end_dt, ts_3, ds_1, 14400, "Wh")
            timestamps = [
                dt.datetime(2020, 1, 1, hour, 0, tzinfo=dt.timezone.utc)
                for hour in range(0, 12, 4)
            ]
            expected_data_s = pd.Series(
                3 * [np.nan],
                index=pd.DatetimeIndex(
                    timestamps, name="timestamp", tz=dt.timezone.utc, freq="4h"
                ),
                dtype=float,
            )
            assert_series_equal(data_s, expected_data_s)

            # Check result is in start_dt timezone
            data_s = energyindex2energy(
                start_dt.astimezone(ZoneInfo("Europe/Paris")),
                h6_dt,
                ts_0,
                ds_1,
                3600,
                "Wh",
            )
            timestamps = [
                dt.datetime(2020, 1, 1, hour, 0, tzinfo=dt.timezone.utc).astimezone(
                    ZoneInfo("Europe/Paris")
                )
                for hour in (0, 1, 2, 3, 4, 5)
            ]
            expected_data_s = pd.Series(
                [1.0, 3.0, 5.0, 7.0, 9.0, np.nan],
                index=pd.DatetimeIndex(timestamps, name="timestamp", freq="h"),
            )
            assert_series_equal(data_s, expected_data_s)

            with pytest.raises(BEMServerCoreUndefinedUnitError):
                data_s = energyindex2energy(start_dt, end_dt, ts_0, ds_1, 3600, "dummy")

            with pytest.raises(BEMServerCoreDimensionalityError):
                data_s = energyindex2energy(start_dt, end_dt, ts_0, ds_1, 3600, "°C")
