"""Check missing data task tests"""

import datetime as dt

import pytest

import sqlalchemy as sqla

import pandas as pd

from bemserver_core.authorization import OpenBar
from bemserver_core.database import db
from bemserver_core.model import (
    Event,
    EventCategory,
    EventLevelEnum,
    TimeseriesByEvent,
    TimeseriesDataState,
    TimeseriesProperty,
    TimeseriesPropertyData,
)
from bemserver_core.tasks.check_missing import check_missing_ts_data
from tests.utils import create_timeseries_data


class TestCheckMissingScheduledTask:
    @pytest.mark.parametrize("campaigns", (2,), indirect=True)
    @pytest.mark.parametrize("timeseries", (4,), indirect=True)
    def test_check_missing_ts_data(self, users, timeseries, campaigns):
        admin_user = users[0]
        assert admin_user.is_admin
        campaign_1 = campaigns[0]
        campaign_2 = campaigns[1]

        # 10 min, full
        ts_0 = timeseries[0]
        # None, 50% missing
        ts_1 = timeseries[1]
        # 10 min, 50% missing
        ts_2 = timeseries[2]
        # None, no data
        ts_3 = timeseries[3]

        assert ts_0.campaign_scope_id == ts_2.campaign_scope_id
        assert ts_1.campaign_scope_id == ts_3.campaign_scope_id

        with OpenBar():
            ds_1 = TimeseriesDataState.get(name="Raw").first()
            ec_data_missing = EventCategory.get(name="Data missing").first()
            ec_data_present = EventCategory.get(name="Data present").first()
            interval_prop = TimeseriesProperty.get(name="Interval").first()
            TimeseriesPropertyData.new(
                timeseries_id=ts_0.id,
                property_id=interval_prop.id,
                value="600",
            )
            TimeseriesPropertyData.new(
                timeseries_id=ts_2.id,
                property_id=interval_prop.id,
                value="600",
            )
            db.session.flush()

        start_dt = dt.datetime(2020, 1, 1, tzinfo=dt.timezone.utc)
        intermediate_dt = dt.datetime(2020, 1, 1, 12, 0, tzinfo=dt.timezone.utc)
        end_dt = dt.datetime(2020, 1, 2, tzinfo=dt.timezone.utc)

        timestamps_1 = pd.date_range(start_dt, end_dt, inclusive="left", freq="600s")
        values_1 = range(len(timestamps_1))
        create_timeseries_data(ts_0, ds_1, timestamps_1, values_1)

        timestamps_2 = pd.date_range(
            start_dt, intermediate_dt, inclusive="left", freq="600s"
        )
        values_2 = range(len(timestamps_2))
        create_timeseries_data(ts_1, ds_1, timestamps_2, values_2)

        timestamps_3 = pd.date_range(
            start_dt, intermediate_dt, inclusive="left", freq="600s"
        )
        values_3 = range(len(timestamps_3))
        create_timeseries_data(ts_2, ds_1, timestamps_3, values_3)

        with OpenBar():
            assert not list(Event.get())
            assert not list(TimeseriesByEvent.get())

            # Min ratio = 90 % -> 2 TS with missing data (different campaign scope)

            check_missing_ts_data(
                campaign_1, start_dt, end_dt, min_completeness_ratio=0.9
            )
            check_missing_ts_data(
                campaign_2, start_dt, end_dt, min_completeness_ratio=0.9
            )

            events = list(Event.get(category=ec_data_missing))
            assert len(events) == 2
            event_1 = events[0]
            assert event_1.campaign_scope_id == ts_0.campaign_scope.id
            assert event_1.category == ec_data_missing
            assert event_1.level == EventLevelEnum.WARNING
            assert event_1.timestamp == start_dt
            assert event_1.source == "BEMServer - Check missing data"
            assert (
                event_1.description
                == "The following timeseries are missing: Timeseries 3"
            )
            tbes = list(TimeseriesByEvent.get(event=event_1))
            assert len(tbes) == 1
            assert tbes[0].timeseries_id == ts_2.id
            event_2 = events[1]
            assert event_2.campaign_scope_id == ts_1.campaign_scope.id
            assert event_2.category == ec_data_missing
            assert event_2.level == EventLevelEnum.WARNING
            assert event_2.timestamp == start_dt
            assert event_2.source == "BEMServer - Check missing data"
            assert (
                event_2.description
                == "The following timeseries are missing: Timeseries 4"
            )
            tbes = list(TimeseriesByEvent.get(event=event_2))
            assert len(tbes) == 1
            assert tbes[0].timeseries_id == ts_3.id

            # Min ratio = 90 % -> 3 TS with missing data (2 in same campaign scope)

            # Set interval for ts_1 to detect (50 %) missing data
            TimeseriesPropertyData.new(
                timeseries_id=ts_1.id,
                property_id=interval_prop.id,
                value="600",
            )

            check_missing_ts_data(
                campaign_1, start_dt, end_dt, min_completeness_ratio=0.9
            )
            check_missing_ts_data(
                campaign_2, start_dt, end_dt, min_completeness_ratio=0.9
            )

            # TS 0 Campaign scope
            # 1 newly missing event from last iteration (TS 2)
            # 1 already missing event (TS 2)
            # 0 present event (TS 0 was never missing)
            events = list(
                Event.get(
                    category=ec_data_missing,
                    campaign_scope_id=ts_0.campaign_scope_id,
                ).order_by(sqla.asc(Event.timestamp))
            )
            assert len(events) == 2
            # Already existing event from last check (TS 2 missing)
            assert events[0] == event_1
            # New event (TS 2 still missing)
            event_3 = events[1]
            assert event_3.timestamp == start_dt
            assert event_3.level == EventLevelEnum.INFO
            assert event_3.source == "BEMServer - Check missing data"
            assert (
                event_3.description
                == "The following timeseries are still missing: Timeseries 3"
            )
            events = list(
                Event.get(
                    category=ec_data_present,
                    campaign_scope_id=ts_0.campaign_scope_id,
                )
            )
            # No event because TS 0 was never missing
            assert not events

            # TS 1 Campaign scope
            # 1 newly missing event from last iteration (TS 3)
            # 1 newly missing event (TS 1)
            # 1 already missing event (TS 3)
            events = list(
                Event.get(
                    category=ec_data_missing,
                    campaign_scope_id=ts_1.campaign_scope_id,
                    level=EventLevelEnum.WARNING,
                ).order_by(sqla.asc(Event.timestamp))
            )
            assert len(events) == 2
            # Already existing event from last check (TS 3 missing)
            assert events[0] == event_2
            # New event (TS 1 missing)
            event_4 = events[1]
            assert event_4.timestamp == start_dt
            assert event_4.source == "BEMServer - Check missing data"
            assert (
                event_4.description
                == "The following timeseries are missing: Timeseries 2"
            )
            tbes = list(TimeseriesByEvent.get(event=event_4))
            assert len(tbes) == 1
            assert tbes[0].timeseries_id == ts_1.id
            events = list(
                Event.get(
                    category=ec_data_missing,
                    campaign_scope_id=ts_1.campaign_scope_id,
                    level=EventLevelEnum.INFO,
                ).order_by(sqla.asc(Event.timestamp))
            )
            assert len(events) == 1
            # New event (TS 3 still missing)
            event_5 = events[0]
            assert event_5.timestamp == start_dt
            assert event_5.level == EventLevelEnum.INFO
            assert event_5.source == "BEMServer - Check missing data"
            assert (
                event_5.description
                == "The following timeseries are still missing: Timeseries 4"
            )
            tbes = list(TimeseriesByEvent.get(event=event_5))
            assert len(tbes) == 1
            assert tbes[0].timeseries_id == ts_3.id
            events = list(
                Event.get(
                    category=ec_data_present,
                    campaign_scope_id=ts_1.campaign_scope_id,
                )
            )
            assert not events

            # Min ratio = 40 % -> 1 TS with missing data (TS 3)

            check_missing_ts_data(
                campaign_1, start_dt, end_dt, min_completeness_ratio=0.4
            )
            check_missing_ts_data(
                campaign_2, start_dt, end_dt, min_completeness_ratio=0.4
            )

            # TS 0 Campaign scope
            # 1 present event (TS 2)
            events = list(
                Event.get(
                    category=ec_data_missing,
                    campaign_scope_id=ts_0.campaign_scope_id,
                )
            )
            assert set(events) == {event_1, event_3}
            events = list(
                Event.get(
                    category=ec_data_present,
                    campaign_scope_id=ts_0.campaign_scope_id,
                )
            )
            assert len(events) == 1
            # TS 0 never gets a present event because it was never missing
            event_6 = events[0]
            assert event_6.timestamp == start_dt
            assert event_6.level == EventLevelEnum.INFO
            assert event_6.source == "BEMServer - Check missing data"
            assert (
                event_6.description
                == "The following timeseries are not missing anymore: Timeseries 3"
            )
            tbes = list(TimeseriesByEvent.get(event=event_6))
            assert len(tbes) == 1
            assert tbes[0].timeseries_id == ts_2.id

            # TS 1 Campaign scope
            # 1 already missing event (TS 3)
            # 1 present event (TS 1)
            events = list(
                Event.get(
                    category=ec_data_missing,
                    campaign_scope_id=ts_1.campaign_scope_id,
                )
            )
            assert len(events) == 4
            assert not {event_2, event_4, event_5} - set(events)
            event_7 = (set(events) - {event_2, event_4, event_5}).pop()
            assert event_7.timestamp == start_dt
            assert event_7.level == EventLevelEnum.INFO
            assert event_7.source == "BEMServer - Check missing data"
            assert (
                event_7.description
                == "The following timeseries are still missing: Timeseries 4"
            )
            tbes = list(TimeseriesByEvent.get(event=event_7))
            assert len(tbes) == 1
            assert tbes[0].timeseries_id == ts_3.id
            events = list(
                Event.get(
                    category=ec_data_present,
                    campaign_scope_id=ts_1.campaign_scope_id,
                    timestamp=start_dt,
                )
            )
            assert len(events) == 1
            event_8 = events[0]
            assert event_8.level == EventLevelEnum.INFO
            assert event_8.source == "BEMServer - Check missing data"
            assert (
                event_8.description
                == "The following timeseries are not missing anymore: Timeseries 2"
            )
            tbes = list(TimeseriesByEvent.get(event=event_8))
            assert len(tbes) == 1
            assert tbes[0].timeseries_id == ts_1.id
