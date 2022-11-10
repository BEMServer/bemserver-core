"""Time utils"""
import datetime as dt

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


def make_pandas_freq(period, period_multiplier):
    return f"{period_multiplier}{PANDAS_PERIOD_ALIASES[period]}"


def make_date_offset(period, period_multiplier):
    if period == "week":
        period = "day"
        period_multiplier *= 7
    period = f"{period}s"
    return DateOffset(**{period: period_multiplier})


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
        pd_freq = make_pandas_freq(period, period_multiplier)
        return pd.Timestamp(datetime).floor(pd_freq, ambiguous=datetime.fold)

    if period_multiplier != 1:
        raise BEMServerCorePeriodError(
            "Period multipliers only allowed for fixed size periods"
        )

    tz = datetime.tzinfo

    if period == "year":
        return pd.Timestamp(datetime.year, 1, 1, tzinfo=tz, fold=datetime.fold)
    if period == "month":
        return pd.Timestamp(
            datetime.year, datetime.month, 1, tzinfo=tz, fold=datetime.fold
        )
    if period == "day":
        return pd.Timestamp(
            datetime.year, datetime.month, datetime.day, tzinfo=tz, fold=datetime.fold
        )
    if period == "week":
        # Week: align on monday
        # Note that timedelta arithmetics respect wall clock so subtracting days
        # works even across DST
        return pd.Timestamp(
            datetime.year, datetime.month, datetime.day, tzinfo=tz, fold=datetime.fold
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

    ret = floor(datetime, period)
    if ret != datetime:
        ret += make_date_offset(period, 1)
    return ret
