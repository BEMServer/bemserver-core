"""Degree days tests"""
import datetime as dt
from zoneinfo import ZoneInfo

import pandas as pd
from pandas.testing import assert_series_equal

import pytest

from bemserver_core.process.degree_days import compute_hdd, compute_cdd


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
