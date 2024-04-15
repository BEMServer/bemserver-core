"""Heating / Cooling Degree Days computations"""

import datetime as dt
from zoneinfo import ZoneInfo

from bemserver_core.exceptions import (
    BEMServerCoreDegreeDayProcessMissingTemperatureError,
)
from bemserver_core.input_output import tsdio
from bemserver_core.model import (
    TimeseriesDataState,
    WeatherParameterEnum,
    WeatherTimeseriesBySite,
)
from bemserver_core.time_utils import PANDAS_PERIOD_ALIASES


def compute_dd(air_temp, period="year", type_="heating", base=18):
    """Compute heating/cooling degree days

    :param Series air_temp: Outside air temperature
    :param string period: One of "day", "month", "year"
    :param string type_: Type of degree days to compute ("heating" or "cooling")
    :param int|float base: Base temperature

    :returns Series: Heating degree days

    Note: base unit must match air_temp unit
    """
    min_s = air_temp.resample("D").min()
    max_s = air_temp.resample("D").max()
    avg_s = (min_s + max_s) / 2
    if type_ == "cooling":
        diff_s = avg_s - base
    else:
        diff_s = base - avg_s
    dd_s = diff_s.clip(0).rename("dd")
    return dd_s.resample(PANDAS_PERIOD_ALIASES[period]).sum(min_count=1)


def compute_dd_for_site(
    site, start_d, end_d, period="year", *, type_="heating", base=18, unit="°C"
):
    """Compute heating/cooling degree days for a given site

    :param datetime start_dt: Time interval lower bound (tz-aware)
    :param datetime end_dt: Time interval exclusive upper bound (tz-aware)
    :param string period: One of "day", "month", "year"
    :param string type_: Type of degree days to compute ("heating" or "cooling")
    :param int|float base: Base temperature
    :param string unit: Unit to express the result

    :returns Series: Heating degree days

    Note: base unit must match unit
    """
    ds_clean = TimeseriesDataState.get(name="Clean").first()

    wtbs = WeatherTimeseriesBySite.get(
        site_id=site.id,
        parameter=WeatherParameterEnum.AIR_TEMPERATURE,
        forecast=False,
    ).first()

    if wtbs is None:
        raise BEMServerCoreDegreeDayProcessMissingTemperatureError(
            "Air temperature for site undefined or access denied."
        )

    air_temp_ts = wtbs.timeseries

    timezone = site.campaign.timezone
    tzinfo = ZoneInfo(timezone)

    start_dt = dt.datetime(start_d.year, start_d.month, start_d.day, tzinfo=tzinfo)
    end_dt = dt.datetime(end_d.year, end_d.month, end_d.day, tzinfo=tzinfo)

    data_df = tsdio.get_timeseries_buckets_data(
        start_dt,
        end_dt,
        (air_temp_ts,),
        ds_clean,
        1,
        "hour",
        aggregation="avg",
        convert_to={air_temp_ts.id: unit},
        timezone=timezone,
    )

    return compute_dd(data_df[air_temp_ts.id], period=period, type_=type_, base=base)
