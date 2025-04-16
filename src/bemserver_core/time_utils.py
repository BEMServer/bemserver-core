"""Time utils"""

import datetime as dt
import enum

import pandas as pd
from pandas.tseries.offsets import DateOffset

from bemserver_core.exceptions import BEMServerCorePeriodError

PANDAS_PERIOD_ALIASES = {
    "second": "s",
    "minute": "min",
    "hour": "h",
    "day": "D",
    "week": "W-MON",
    "month": "MS",
    "year": "YS",
}


class PeriodEnum(enum.Enum):
    """Period names enum"""

    second = "second"
    minute = "minute"
    hour = "hour"
    day = "day"
    week = "week"
    month = "month"
    year = "year"


# Day / week may not be fixed size periods because of DST
PERIODS = ("second", "minute", "hour", "day", "week", "month", "year")
FIXED_SIZE_PERIODS = ("second", "minute", "hour")


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
        return pd.Timestamp(datetime).floor(pd_freq, ambiguous=not bool(datetime.fold))

    if period_multiplier != 1:
        raise BEMServerCorePeriodError(
            "Period multipliers only allowed for fixed size periods"
        )

    tz = datetime.tzinfo

    if period == "year":
        return pd.Timestamp(
            year=datetime.year, month=1, day=1, tzinfo=tz, fold=datetime.fold
        )
    if period == "month":
        return pd.Timestamp(
            year=datetime.year,
            month=datetime.month,
            day=1,
            tzinfo=tz,
            fold=datetime.fold,
        )
    if period == "day":
        return pd.Timestamp(
            year=datetime.year,
            month=datetime.month,
            day=datetime.day,
            tzinfo=tz,
            fold=datetime.fold,
        )
    if period == "week":
        # Week: align on monday
        # Note that timedelta arithmetics respect wall clock so subtracting days
        # works even across DST
        return pd.Timestamp(
            year=datetime.year,
            month=datetime.month,
            day=datetime.day,
            tzinfo=tz,
            fold=datetime.fold,
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
        return pd.Timestamp(datetime).ceil(pd_freq, ambiguous=not bool(datetime.fold))

    if period_multiplier != 1:
        raise BEMServerCorePeriodError(
            "Period multipliers only allowed for fixed size periods"
        )

    ret = floor(datetime, period)
    if ret != datetime:
        ret += make_date_offset(period, 1)
    return ret


def make_date_range_around_datetime(
    datetime, period, period_multiplier, periods_before, periods_after
):
    """Make date range around datetime

    :param datetime datetime: Timezone aware datetime
    :param str period: Period in
        ["second", "minute", "hour", "day", "week", "month", "year"]
    :param int period_multiplier: Period multiplier.
    :param int periods_before: Number of periods before datetime
    :param int periods_after: Number of periods after datetime

    Returns a tuple of timezone aware datetimes.
    """
    period_offset = make_date_offset(period, period_multiplier)
    start_dt = datetime - periods_before * period_offset
    end_dt = datetime + periods_after * period_offset
    return start_dt, end_dt
