"""Degree days tests"""
import datetime as dt
from zoneinfo import ZoneInfo

import pandas as pd
from pandas.testing import assert_series_equal

import pytest

from bemserver_core.model import (
    TimeseriesDataState,
    WeatherParameterEnum,
)
from bemserver_core.input_output import tsdio
from bemserver_core.process.degree_days import (
    compute_hdd,
    compute_cdd,
    compute_dd_for_site,
)
from bemserver_core.exceptions import (
    BEMServerCoreDegreeDayProcessMissingTemperatureError,
)


@pytest.mark.parametrize("base", (18, 19.5))
@pytest.mark.parametrize("timezone", (dt.timezone.utc, ZoneInfo("Europe/Paris")))
def test_compute_hdd(base, timezone):
    index = pd.date_range(
        "2020-01-01", "2021-01-01", freq="H", tz=timezone, inclusive="left"
    )
    weather_df = pd.DataFrame(index=index)
    weather_df["temperature"] = index.month

    # Introduce a bias to check that computation method uses min/max
    weather_df["temperature"] += 5
    weather_df["temperature"][index.hour == 1] -= 10

    # Daily HDD
    hdd_s = compute_hdd(weather_df["temperature"], "day", base=base)
    expected_d_index = pd.date_range(
        "2020-01-01", "2021-01-01", freq="D", tz=timezone, inclusive="left"
    )
    expected_d = pd.Series(
        base - expected_d_index.month,
        index=expected_d_index,
        dtype="float",
        name="hdd",
    )
    assert_series_equal(hdd_s, expected_d)

    # Monthly HDD
    hdd_s = compute_hdd(weather_df["temperature"], "month", base=base)
    expected_m = expected_d.resample("MS").sum()
    assert_series_equal(hdd_s, expected_m)

    # Yearly HDD
    hdd_s = compute_hdd(weather_df["temperature"], "year", base=base)
    expected_y = expected_d.resample("AS").sum()
    assert_series_equal(hdd_s, expected_y)


@pytest.mark.parametrize("base", (18, 19.5))
@pytest.mark.parametrize("timezone", (dt.timezone.utc, ZoneInfo("Europe/Paris")))
def test_compute_cdd(base, timezone):
    index = pd.date_range(
        "2020-01-01", "2021-01-01", freq="H", tz=timezone, inclusive="left"
    )
    weather_df = pd.DataFrame(index=index)
    weather_df["temperature"] = index.month + 20

    # Introduce a bias to check that computation method uses min/max
    weather_df["temperature"] += 5
    weather_df["temperature"][index.hour == 1] -= 10

    # Daily HDD
    cdd_s = compute_cdd(weather_df["temperature"], "day", base=base)
    expected_d_index = pd.date_range(
        "2020-01-01", "2021-01-01", freq="D", tz=timezone, inclusive="left"
    )
    expected_d = pd.Series(
        expected_d_index.month + 20 - base,
        index=expected_d_index,
        dtype="float",
        name="cdd",
    )
    assert_series_equal(cdd_s, expected_d)

    # Monthly HDD
    cdd_s = compute_cdd(weather_df["temperature"], "month", base=base)
    expected_m = expected_d.resample("MS").sum()
    assert_series_equal(cdd_s, expected_m)

    # Yearly HDD
    cdd_s = compute_cdd(weather_df["temperature"], "year", base=base)
    expected_y = expected_d.resample("AS").sum()
    assert_series_equal(cdd_s, expected_y)


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

    ds_clean = TimeseriesDataState.get(name="Clean").first()

    index = pd.date_range(
        start_d, end_d, freq="H", tz="UTC", inclusive="left", name="timestamp"
    )
    weather_df = pd.DataFrame(index=index)
    weather_df[wtbs_1.timeseries_id] = index.month
    if type_ == "cooling":
        weather_df[wtbs_1.timeseries_id] += 20

    # Introduce a bias to check that computation method uses min/max
    weather_df[wtbs_1.timeseries_id] += 5
    weather_df[wtbs_1.timeseries_id][index.hour == 1] -= 10

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
        name={"heating": "hdd", "cooling": "cdd"}[type_],
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
    expected_y = expected_d.resample("AS").sum()
    assert_series_equal(dd_s, expected_y)

    # Missing temperature
    wtbs_1.delete()
    with pytest.raises(
        BEMServerCoreDegreeDayProcessMissingTemperatureError,
        match="Air temperature for site undefined or access denied.",
    ):
        compute_dd_for_site(
            site_1, start_d, end_d, "day", type_=type_, base=base, unit=unit
        )
