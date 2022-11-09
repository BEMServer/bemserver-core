"""Time utils"""
import datetime as dt
from zoneinfo import ZoneInfo

import pandas as pd
from pandas.tseries.offsets import DateOffset

from bemserver_core.exceptions import BEMServerCorePeriodError


PANDAS_PERIOD_ALIASES = {
    "second": "S",
    "minute": "T",
    "hour": "H",
    "day": "D",
    "week": "W-MON",
    "month": "MS",
    "year": "AS",
}


# Day / week may not be fixed size periods because of DST
PERIODS = ("second", "minute", "hour", "day", "week", "month", "year")
FIXED_SIZE_PERIODS = {"second", "minute", "hour"}


def floor(datetime, period, period_multiplier=1):
    """Floor datetime to a given time period

    :param str period: Period in
        ["second", "minute", "hour", "day", "week", "month", "year"]
    :param int period_multiplier: Period multiplier.

    Period multipliers are only allowed for fixed size periods (second, minute, hour).
    If using multipliers, note that the origin of the period is on Jan 1st 1970.
    This can lead to surprising results if the multiplied period is not a divider of
    above period. E.g. "4 hours" starts at midnight, while "5 hours" may not.
    """
    if period in FIXED_SIZE_PERIODS:
        pd_freq = f"{period_multiplier}{PANDAS_PERIOD_ALIASES[period]}"
        return pd.Timestamp(datetime).floor(pd_freq, ambiguous=datetime.fold)

    if period_multiplier != 1:
        raise BEMServerCorePeriodError(
            "Period multipliers only allowed for fixed size periods"
        )

    tz = datetime.tzinfo

    if period == "year":
        return pd.Timestamp(datetime.year, 1, 1, tzinfo=tz)
    if period == "month":
        return pd.Timestamp(datetime.year, datetime.month, 1, tzinfo=tz)
    if period == "day":
        return pd.Timestamp(datetime.year, datetime.month, datetime.day, tzinfo=tz)
    if period == "week":
        # Week: align on monday
        # Note that timedelta arithmetics respect wall clock so subtracting days
        # works even across DST
        return pd.Timestamp(
            datetime.year, datetime.month, datetime.day, tzinfo=tz
        ) - dt.timedelta(days=datetime.weekday())

    raise BEMServerCorePeriodError(f'Invalid period: "{period}"')


def ceil(datetime, period, period_multiplier=1):
    """Ceil datetime to a given time period

    :param str period: Period in
        ["second", "minute", "hour", "day", "week", "month", "year"]
    :param int period_multiplier: Period multiplier.

    Period multipliers are only allowed for fixed size periods (second, minute, hour).
    If using multipliers, note that the origin of the period is on Jan 1st 1970.
    This can lead to surprising results if the multiplied period is not a divider of
    above period. E.g. "4 hours" starts at midnight, while "5 hours" may not.
    """
    if period in FIXED_SIZE_PERIODS:
        pd_freq = f"{period_multiplier}{PANDAS_PERIOD_ALIASES[period]}"
        return pd.Timestamp(datetime).ceil(pd_freq, ambiguous=datetime.fold)

    if period_multiplier != 1:
        raise BEMServerCorePeriodError(
            "Period multipliers only allowed for fixed size periods"
        )

    tz = datetime.tzinfo

    if period == "year":
        ret = pd.Timestamp(datetime.year, 1, 1, tzinfo=tz)
        if datetime != floor(datetime, period):
            ret += DateOffset(years=1)
        return ret
    if period == "month":
        ret = pd.Timestamp(datetime.year, datetime.month, 1, tzinfo=tz)
        if datetime != floor(datetime, period):
            ret += DateOffset(months=1)
        return ret
    if period == "day":
        ret = pd.Timestamp(datetime.year, datetime.month, datetime.day, tzinfo=tz)
        if datetime != floor(datetime, period):
            ret += DateOffset(days=1)
        return ret
    if period == "week":
        # Week: align on monday
        # Note that timedelta arithmetics respect wall clock so subtracting days
        # works even across DST
        ret = pd.Timestamp(
            datetime.year, datetime.month, datetime.day, tzinfo=tz
        ) - dt.timedelta(days=datetime.weekday())
        if datetime != floor(datetime, period):
            ret += DateOffset(days=7)
        return ret

    raise BEMServerCorePeriodError(f'Invalid period: "{period}"')


def gen_date_range(
    start_dt, end_dt, bucket_width_value, bucket_width_unit, timezone="UTC"
):
    """Generate a complete index for a given time period and bucket width"""

    pd_freq = f"{bucket_width_value}{PANDAS_PERIOD_ALIASES[bucket_width_unit]}"

    tz = ZoneInfo(timezone)
    start_dt = start_dt.astimezone(tz)
    end_dt = end_dt.astimezone(tz)

    start_dt = floor(start_dt, bucket_width_unit, bucket_width_value)

    return pd.date_range(
        start_dt,
        end_dt,
        freq=pd_freq,
        tz=tz,
        name="timestamp",
        inclusive="left",
    )