import datetime as dt
from zoneinfo import ZoneInfo

import pytest

from bemserver_core.time_utils import floor
from bemserver_core.exceptions import BEMServerCorePeriodError


PERIODS = ("second", "minute", "hour", "day", "week", "month", "year")


class TestTimeUtils:
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
