"""Test time utils"""
import datetime as dt
from zoneinfo import ZoneInfo

from pandas.tseries.offsets import DateOffset

import pytest

from bemserver_core.time_utils import make_date_offset, floor, ceil
from bemserver_core.exceptions import BEMServerCorePeriodError


PERIODS = ("second", "minute", "hour", "day", "week", "month", "year")


def test_time_utils_make_date_offset():
    assert make_date_offset("year", 1) == DateOffset(years=1)
    assert make_date_offset("month", 2) == DateOffset(months=2)
    assert make_date_offset("week", 3) == DateOffset(days=21)
    assert make_date_offset("day", 4) == DateOffset(days=4)
    assert make_date_offset("hour", 5) == DateOffset(hours=5)
    assert make_date_offset("minute", 6) == DateOffset(minutes=6)
    assert make_date_offset("second", 7) == DateOffset(seconds=7)


class TestTimeUtilsFloor:
    @pytest.mark.parametrize("timezone", (dt.timezone.utc, ZoneInfo("Europe/Paris")))
    def test_floor(self, timezone):
        datetime = dt.datetime(2020, 3, 17, 5, 23, 54, 123, tzinfo=timezone)
        assert floor(datetime, "year") == dt.datetime(2020, 1, 1, tzinfo=timezone)
        assert floor(datetime, "month") == dt.datetime(2020, 3, 1, tzinfo=timezone)
        assert floor(datetime, "week") == dt.datetime(2020, 3, 16, tzinfo=timezone)
        assert floor(datetime, "week").weekday() == 0
        assert floor(datetime, "day") == dt.datetime(2020, 3, 17, tzinfo=timezone)
        assert floor(datetime, "hour") == dt.datetime(2020, 3, 17, 5, tzinfo=timezone)
        assert floor(datetime, "hour", 2) == dt.datetime(
            2020, 3, 17, 4, tzinfo=timezone
        )
        assert floor(datetime, "hour", 12) == dt.datetime(
            2020, 3, 17, 0, tzinfo=timezone
        )
        assert floor(datetime, "minute") == dt.datetime(
            2020, 3, 17, 5, 23, tzinfo=timezone
        )
        assert floor(datetime, "minute", 10) == dt.datetime(
            2020, 3, 17, 5, 20, tzinfo=timezone
        )
        assert floor(datetime, "minute", 15) == dt.datetime(
            2020, 3, 17, 5, 15, tzinfo=timezone
        )
        assert floor(datetime, "second") == dt.datetime(
            2020, 3, 17, 5, 23, 54, tzinfo=timezone
        )
        assert floor(datetime, "second", 4) == dt.datetime(
            2020, 3, 17, 5, 23, 52, tzinfo=timezone
        )
        assert floor(datetime, "second", 20) == dt.datetime(
            2020, 3, 17, 5, 23, 40, tzinfo=timezone
        )

    def test_floor_invalid_period(self):
        with pytest.raises(
            BEMServerCorePeriodError, match='Invalid period: "dummy period"'
        ):
            floor(dt.datetime(2020, 1, 1), "dummy period")

    @pytest.mark.parametrize("period", ("day", "week", "month", "year"))
    def test_floor_variable_size_with_multiplier(self, period):
        with pytest.raises(
            BEMServerCorePeriodError,
            match="Period multipliers only allowed for fixed size periods",
        ):
            floor(dt.datetime(2020, 1, 1), period, 2)


class TestTimeUtilsCeil:
    @pytest.mark.parametrize("timezone", (dt.timezone.utc, ZoneInfo("Europe/Paris")))
    def test_ceil(self, timezone):
        datetime = dt.datetime(2020, 3, 17, 5, 23, 54, 123, tzinfo=timezone)
        assert ceil(datetime, "year") == dt.datetime(2021, 1, 1, tzinfo=timezone)
        assert ceil(datetime, "month") == dt.datetime(2020, 4, 1, tzinfo=timezone)
        assert ceil(datetime, "week") == dt.datetime(2020, 3, 23, tzinfo=timezone)
        assert ceil(datetime, "week").weekday() == 0
        assert ceil(datetime, "day") == dt.datetime(2020, 3, 18, tzinfo=timezone)
        assert ceil(datetime, "hour") == dt.datetime(2020, 3, 17, 6, tzinfo=timezone)
        assert ceil(datetime, "hour", 2) == dt.datetime(2020, 3, 17, 6, tzinfo=timezone)
        assert ceil(datetime, "hour", 12) == dt.datetime(
            2020, 3, 17, 12, tzinfo=timezone
        )
        assert ceil(datetime, "minute") == dt.datetime(
            2020, 3, 17, 5, 24, tzinfo=timezone
        )
        assert ceil(datetime, "minute", 10) == dt.datetime(
            2020, 3, 17, 5, 30, tzinfo=timezone
        )
        assert ceil(datetime, "minute", 15) == dt.datetime(
            2020, 3, 17, 5, 30, tzinfo=timezone
        )
        assert ceil(datetime, "second") == dt.datetime(
            2020, 3, 17, 5, 23, 55, tzinfo=timezone
        )
        assert ceil(datetime, "second", 4) == dt.datetime(
            2020, 3, 17, 5, 23, 56, tzinfo=timezone
        )
        assert ceil(datetime, "second", 20) == dt.datetime(
            2020, 3, 17, 5, 24, 0, tzinfo=timezone
        )

    @pytest.mark.parametrize("timezone", (dt.timezone.utc, ZoneInfo("Europe/Paris")))
    def test_ceil_round_value(self, timezone):
        datetime = dt.datetime(2018, 1, 1, 0, 0, 0, 0, tzinfo=timezone)
        assert ceil(datetime, "year") == datetime
        assert ceil(datetime, "month") == datetime
        assert ceil(datetime, "week") == datetime
        assert ceil(datetime, "week").weekday() == 0
        assert ceil(datetime, "day") == datetime
        assert ceil(datetime, "hour") == datetime
        assert ceil(datetime, "hour", 2) == datetime
        assert ceil(datetime, "hour", 12) == datetime
        assert ceil(datetime, "minute") == datetime
        assert ceil(datetime, "minute", 10) == datetime
        assert ceil(datetime, "minute", 15) == datetime
        assert ceil(datetime, "second") == datetime
        assert ceil(datetime, "second", 4) == datetime
        assert ceil(datetime, "second", 20) == datetime

    def test_ceil_invalid_period(self):
        with pytest.raises(
            BEMServerCorePeriodError, match='Invalid period: "dummy period"'
        ):
            ceil(dt.datetime(2020, 1, 1), "dummy period")

    @pytest.mark.parametrize("period", ("day", "week", "month", "year"))
    def test_ceil_variable_size_with_multiplier(self, period):
        with pytest.raises(
            BEMServerCorePeriodError,
            match="Period multipliers only allowed for fixed size periods",
        ):
            ceil(dt.datetime(2020, 1, 1), period, 2)
