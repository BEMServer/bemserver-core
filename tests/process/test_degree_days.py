"""Degree days tests"""

import datetime as dt
from zoneinfo import ZoneInfo

import pytest

import numpy as np
import pandas as pd
from pandas.testing import assert_series_equal

from bemserver_core.exceptions import (
    BEMServerCoreDegreeDayProcessMissingTemperatureError,
)
from bemserver_core.input_output import tsdio
from bemserver_core.model import (
    TimeseriesDataState,
    WeatherParameterEnum,
)
from bemserver_core.process.degree_days import compute_dd, compute_dd_for_site


@pytest.mark.parametrize("type_", ("heating", "cooling"))
@pytest.mark.parametrize("base", (18, 19.5))
@pytest.mark.parametrize("timezone", (dt.timezone.utc, ZoneInfo("Europe/Paris")))
def test_compute_dd(type_, base, timezone):
    index = pd.date_range(
        "2020-01-01", "2021-01-01", freq="h", tz=timezone, inclusive="left"
    )
    weather_df = pd.DataFrame(index=index)
    weather_df["temperature"] = index.month
    if type_ == "cooling":
        weather_df["temperature"] += 20

    # Introduce a bias to check that computation method uses min/max
    weather_df["temperature"] += 5
    weather_df.loc[index.hour == 1, "temperature"] -= 10

    # Daily HDD
    dd_s = compute_dd(weather_df["temperature"], "day", type_=type_, base=base)
    expected_d_index = pd.date_range(
        "2020-01-01", "2021-01-01", freq="D", tz=timezone, inclusive="left"
    )
    month_avg_temp = expected_d_index.month
    if type_ == "cooling":
        month_avg_temp += 20
    expected_d = pd.Series(
        (month_avg_temp - base) if type_ == "cooling" else (base - month_avg_temp),
        index=expected_d_index,
        dtype="float",
        name="dd",
    )
    assert_series_equal(dd_s, expected_d)

    # Monthly HDD
    dd_s = compute_dd(weather_df["temperature"], "month", type_=type_, base=base)
    expected_m = expected_d.resample("MS").sum()
    assert_series_equal(dd_s, expected_m)

    # Yearly HDD
    dd_s = compute_dd(weather_df["temperature"], "year", type_=type_, base=base)
    expected_y = expected_d.resample("YS").sum()
    assert_series_equal(dd_s, expected_y)

    # Return NaN if no value
    weather_df.loc[index.month > 6, "temperature"] = np.nan
    dd_s = compute_dd(weather_df["temperature"], "day", type_=type_, base=base)
    expected_d[expected_d.index.month > 6] = np.nan
    assert_series_equal(dd_s, expected_d)


@pytest.mark.parametrize("type_", ("heating", "cooling"))
@pytest.mark.parametrize("base", (18, 19.5))
@pytest.mark.parametrize("unit", ("°C", "°F"))
@pytest.mark.usefixtures("as_admin")
def test_compute_dd_for_site(sites, weather_timeseries_by_sites, type_, base, unit):
    site_1 = sites[0]
    wtbs_1 = weather_timeseries_by_sites[0]

    assert wtbs_1.site == site_1
    assert wtbs_1.parameter == WeatherParameterEnum.AIR_TEMPERATURE
    assert site_1.campaign.timezone == "UTC"

    if unit == "°F":
        base = base * 9 / 5 + 32

    start_d = dt.date(2020, 1, 1)
    end_d = dt.date(2021, 1, 1)
    post_d = dt.date(2022, 1, 1)

    ds_clean = TimeseriesDataState.get(name="Clean").first()

    index = pd.date_range(
        start_d, end_d, freq="h", tz="UTC", inclusive="left", name="timestamp"
    )
    weather_df = pd.DataFrame(index=index)
    weather_df[wtbs_1.timeseries_id] = index.month
    if type_ == "cooling":
        weather_df[wtbs_1.timeseries_id] += 20

    # Introduce a bias to check that computation method uses min/max
    weather_df[wtbs_1.timeseries_id] += 5
    weather_df.loc[index.hour == 1, wtbs_1.timeseries_id] -= 10

    tsdio.set_timeseries_data(weather_df, data_state=ds_clean)

    # Daily DD
    dd_s = compute_dd_for_site(
        site_1, start_d, end_d, "day", type_=type_, base=base, unit=unit
    )

    expected_d_index = pd.date_range(
        "2020-01-01",
        "2021-01-01",
        freq="D",
        tz="UTC",
        inclusive="left",
        name="timestamp",
    )
    month_avg_temp = expected_d_index.month
    if type_ == "cooling":
        month_avg_temp += 20
    if unit == "°F":
        month_avg_temp = month_avg_temp * 9 / 5 + 32
    expected_d = pd.Series(
        (month_avg_temp - base) if type_ == "cooling" else (base - month_avg_temp),
        index=expected_d_index,
        dtype="float",
        name="dd",
    )
    assert_series_equal(dd_s, expected_d)

    # Monthly DD
    dd_s = compute_dd_for_site(
        site_1, start_d, end_d, "month", type_=type_, base=base, unit=unit
    )
    expected_m = expected_d.resample("MS").sum()
    assert_series_equal(dd_s, expected_m)

    # Yearly DD
    dd_s = compute_dd_for_site(
        site_1, start_d, end_d, "year", type_=type_, base=base, unit=unit
    )
    expected_y = expected_d.resample("YS").sum()
    assert_series_equal(dd_s, expected_y)

    # Missing data
    dd_s = compute_dd_for_site(
        site_1, start_d, post_d, "day", type_=type_, base=base, unit=unit
    )
    assert_series_equal(dd_s[dd_s.index.year == 2020], expected_d)
    assert (dd_s[dd_s.index.year == 2022] == np.nan).all()

    # Missing temperature
    wtbs_1.forecast = True
    with pytest.raises(
        BEMServerCoreDegreeDayProcessMissingTemperatureError,
        match="Air temperature for site undefined or access denied.",
    ):
        compute_dd_for_site(
            site_1, start_d, end_d, "day", type_=type_, base=base, unit=unit
        )
    wtbs_1.delete()
    with pytest.raises(
        BEMServerCoreDegreeDayProcessMissingTemperatureError,
        match="Air temperature for site undefined or access denied.",
    ):
        compute_dd_for_site(
            site_1, start_d, end_d, "day", type_=type_, base=base, unit=unit
        )
