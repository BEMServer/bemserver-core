"""Heating / Cooling Degree Days computations"""
import datetime as dt
from zoneinfo import ZoneInfo

from bemserver_core.model import (
    TimeseriesDataState,
    WeatherParameterEnum,
    WeatherTimeseriesBySite,
)
from bemserver_core.input_output import tsdio
from bemserver_core.time_utils import PANDAS_PERIOD_ALIASES
from bemserver_core.exceptions import (
    BEMServerCoreDegreeDayProcessMissingTemperatureError,
)


def compute_hdd(air_temp, period="year", base=18):
    """Compute heating degree days

    :param Series air_temp: Outside air temperature
    :param string period: One of "day", "month", "year"
    :param int|float base: Base temperature

    :returns Series: Heating degree days

    Note: base unit must match air_temp unit
    """
    min_s = air_temp.resample("D").min()
    max_s = air_temp.resample("D").max()
    avg_s = (min_s + max_s) / 2
    hdd = (base - avg_s).clip(0).rename("hdd")
    return hdd.resample(PANDAS_PERIOD_ALIASES[period]).sum()


def compute_cdd(air_temp, period="year", base=18):
    """Compute cooling degree days

    :param Series air_temp: Outside air temperature
    :param string period: One of "day", "month", "year"
    :param int|float base: Base temperature

    :returns Series: Cooling degree days

    Note: base unit must match air_temp unit
    """
    min_s = air_temp.resample("D").min()
    max_s = air_temp.resample("D").max()
    avg_s = (min_s + max_s) / 2
    hdd = (avg_s - base).clip(0).rename("cdd")
    return hdd.resample(PANDAS_PERIOD_ALIASES[period]).sum()


def compute_dd_for_site(
    site, start_d, end_d, period="year", *, type_="heating", base=18, unit="°C"
):
    """Compute degree days for a given site

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

    process = {
        "heating": compute_hdd,
        "cooling": compute_cdd,
    }[type_]

    return process(data_df[air_temp_ts.id], period=period, base=base)
