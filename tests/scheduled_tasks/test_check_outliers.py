"""Check outliers task tests"""

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
from bemserver_core.tasks.check_outliers import check_outliers_ts_data
from tests.utils import create_timeseries_data


class TestCheckOutliersScheduledTask:
    @pytest.mark.parametrize("campaigns", (2,), indirect=True)
    @pytest.mark.parametrize("timeseries", (4,), indirect=True)
    def test_check_outliers_ts_data(self, users, timeseries, campaigns):
        admin_user = users[0]
        assert admin_user.is_admin
        campaign_1 = campaigns[0]
        campaign_2 = campaigns[1]

        # Min/Max, no data
        ts_0 = timeseries[0]
        # None
        ts_1 = timeseries[1]
        # Max only
        ts_2 = timeseries[2]
        # Min/Max
        ts_3 = timeseries[3]

        assert ts_0.campaign_scope_id == ts_2.campaign_scope_id
        assert ts_1.campaign_scope_id == ts_3.campaign_scope_id

        with OpenBar():
            ds_1 = TimeseriesDataState.get(name="Raw").first()
            ts_p_min = TimeseriesProperty.get(name="Min").first()
            ts_p_max = TimeseriesProperty.get(name="Max").first()
            TimeseriesPropertyData.new(
                timeseries_id=ts_0.id,
                property_id=ts_p_min.id,
                value="12",
            )
            TimeseriesPropertyData.new(
                timeseries_id=ts_0.id,
                property_id=ts_p_max.id,
                value="42",
            )
            TimeseriesPropertyData.new(
                timeseries_id=ts_2.id,
                property_id=ts_p_max.id,
                value="42",
            )
            TimeseriesPropertyData.new(
                timeseries_id=ts_3.id,
                property_id=ts_p_min.id,
                value="12",
            )
            TimeseriesPropertyData.new(
                timeseries_id=ts_3.id,
                property_id=ts_p_max.id,
                value="42",
            )
            ec_data_outliers = EventCategory.get(name="Data outliers").first()
            ec_data_no_outliers = EventCategory.get(name="No data outliers").first()
            db.session.flush()

        start_dt = dt.datetime(2020, 1, 1, tzinfo=dt.timezone.utc)
        end_dt = dt.datetime(2020, 1, 2, tzinfo=dt.timezone.utc)
        timestamps = pd.date_range(start_dt, end_dt, inclusive="left", freq="5h")
        values = [0, 13, 33, 42, 69]
        create_timeseries_data(ts_1, ds_1, timestamps, values)
        create_timeseries_data(ts_2, ds_1, timestamps, values)
        create_timeseries_data(ts_3, ds_1, timestamps, values)

        with OpenBar():
            # Min ratio = 90 % -> 2 TS with outliers data (different campaign scopes)

            assert not list(Event.get())
            assert not list(TimeseriesByEvent.get())

            check_outliers_ts_data(
                campaign_1, start_dt, end_dt, min_correctness_ratio=0.9
            )
            check_outliers_ts_data(
                campaign_2, start_dt, end_dt, min_correctness_ratio=0.9
            )

            events = list(Event.get(category=ec_data_outliers))
            assert len(events) == 2
            event_1 = events[0]
            assert event_1.campaign_scope_id == ts_0.campaign_scope.id
            assert event_1.category == ec_data_outliers
            assert event_1.level == EventLevelEnum.WARNING
            assert event_1.timestamp == start_dt
            assert event_1.source == "BEMServer - Check outliers"
            assert (
                event_1.description
                == "The following timeseries have outliers: Timeseries 3"
            )
            tbes = list(TimeseriesByEvent.get(event=event_1))
            assert len(tbes) == 1
            assert tbes[0].timeseries_id == ts_2.id
            event_2 = events[1]
            assert event_2.campaign_scope_id == ts_1.campaign_scope.id
            assert event_2.category == ec_data_outliers
            assert event_2.level == EventLevelEnum.WARNING
            assert event_2.timestamp == start_dt
            assert event_2.source == "BEMServer - Check outliers"
            assert (
                event_2.description
                == "The following timeseries have outliers: Timeseries 4"
            )
            tbes = list(TimeseriesByEvent.get(event=event_2))
            assert len(tbes) == 1
            assert tbes[0].timeseries_id == ts_3.id

            # Min ratio = 90 % -> 3 TS with outliers data (2 in same campaign scope)

            # Set min for ts_1 to detect outliers data
            TimeseriesPropertyData.new(
                timeseries_id=ts_1.id,
                property_id=ts_p_min.id,
                value="12",
            )

            check_outliers_ts_data(
                campaign_1, start_dt, end_dt, min_correctness_ratio=0.9
            )
            check_outliers_ts_data(
                campaign_2, start_dt, end_dt, min_correctness_ratio=0.9
            )

            # TS 0 Campaign scope
            # 1 new outliers event from last iteration (TS 2)
            # 1 already outliers event (TS 2)
            # 0 no outlier event (TS 0 never had outliers)
            events = list(
                Event.get(
                    category=ec_data_outliers,
                    campaign_scope_id=ts_0.campaign_scope_id,
                ).order_by(sqla.asc(Event.timestamp))
            )
            assert len(events) == 2
            # Already existing event from last check (TS 2 outliers)
            assert events[0] == event_1
            # New event (TS 2 still has outliers)
            event_3 = events[1]
            assert event_3.timestamp == start_dt
            assert event_3.level == EventLevelEnum.INFO
            assert event_3.source == "BEMServer - Check outliers"
            assert (
                event_3.description
                == "The following timeseries still have outliers: Timeseries 3"
            )
            events = list(
                Event.get(
                    category=ec_data_no_outliers,
                    campaign_scope_id=ts_0.campaign_scope_id,
                )
            )
            # No event because TS 0 never had outliers
            assert not events

            # TS 1 Campaign scope
            # 1 new outliers event from last iteration (TS 3)
            # 1 new outliers event (TS 1)
            # 1 already outliers event (TS 3)
            events = list(
                Event.get(
                    category=ec_data_outliers,
                    campaign_scope_id=ts_1.campaign_scope_id,
                    level=EventLevelEnum.WARNING,
                ).order_by(sqla.asc(Event.timestamp))
            )
            assert len(events) == 2
            # Already existing event from last check (TS 3 outliers)
            assert events[0] == event_2
            # New event (TS 1 outliers)
            event_4 = events[1]
            assert event_4.timestamp == start_dt
            assert event_4.source == "BEMServer - Check outliers"
            assert (
                event_4.description
                == "The following timeseries have outliers: Timeseries 2"
            )
            tbes = list(TimeseriesByEvent.get(event=event_4))
            assert len(tbes) == 1
            assert tbes[0].timeseries_id == ts_1.id
            events = list(
                Event.get(
                    category=ec_data_outliers,
                    campaign_scope_id=ts_1.campaign_scope_id,
                    level=EventLevelEnum.INFO,
                ).order_by(sqla.asc(Event.timestamp))
            )
            assert len(events) == 1
            # New event (TS 3 still having outliers)
            event_5 = events[0]
            assert event_5.timestamp == start_dt
            assert event_5.level == EventLevelEnum.INFO
            assert event_5.source == "BEMServer - Check outliers"
            assert (
                event_5.description
                == "The following timeseries still have outliers: Timeseries 4"
            )
            tbes = list(TimeseriesByEvent.get(event=event_5))
            assert len(tbes) == 1
            assert tbes[0].timeseries_id == ts_3.id
            events = list(
                Event.get(
                    category=ec_data_no_outliers,
                    campaign_scope_id=ts_1.campaign_scope_id,
                )
            )
            assert not events

            # Min ratio = 70 % -> 1 TS with outliers data (TS 3)

            check_outliers_ts_data(
                campaign_1, start_dt, end_dt, min_correctness_ratio=0.7
            )
            check_outliers_ts_data(
                campaign_2, start_dt, end_dt, min_correctness_ratio=0.7
            )

            # TS 0 Campaign scope
            # 2 outliers events from last iterations (TS 2, TS 0)
            # 1 no outlier event (TS 2)
            events = list(
                Event.get(
                    category=ec_data_outliers,
                    campaign_scope_id=ts_0.campaign_scope_id,
                )
            )
            assert set(events) == {event_1, event_3}
            events = list(
                Event.get(
                    category=ec_data_no_outliers,
                    campaign_scope_id=ts_0.campaign_scope_id,
                )
            )
            assert len(events) == 1
            # TS 0 never gets a no outlier event because it never had outliers
            event_6 = events[0]
            assert event_6.timestamp == start_dt
            assert event_6.level == EventLevelEnum.INFO
            assert event_6.source == "BEMServer - Check outliers"
            assert (
                event_6.description
                == "The following timeseries don't have outliers anymore: Timeseries 3"
            )
            tbes = list(TimeseriesByEvent.get(event=event_6))
            assert len(tbes) == 1
            assert tbes[0].timeseries_id == ts_2.id

            # TS 1 Campaign scope
            # 2 new outliers events from last iterations (TS 1, TS 3)
            # 1 already outliers event from last iteration (TS 3)
            # 1 already outliers event (TS 3)
            # 1 no outliers event (TS 1)
            events = list(
                Event.get(
                    category=ec_data_outliers,
                    campaign_scope_id=ts_1.campaign_scope_id,
                )
            )
            assert not {event_2, event_4, event_5} - set(events)
            event_7 = (set(events) - {event_2, event_4, event_5}).pop()
            assert event_7.timestamp == start_dt
            assert event_7.level == EventLevelEnum.INFO
            assert event_7.source == "BEMServer - Check outliers"
            assert (
                event_7.description
                == "The following timeseries still have outliers: Timeseries 4"
            )
            tbes = list(TimeseriesByEvent.get(event=event_7))
            assert len(tbes) == 1
            assert tbes[0].timeseries_id == ts_3.id
            events = list(
                Event.get(
                    category=ec_data_no_outliers,
                    campaign_scope_id=ts_1.campaign_scope_id,
                )
            )
            assert len(events) == 1
            event_8 = events[0]
            assert event_8.timestamp == start_dt
            assert event_8.level == EventLevelEnum.INFO
            assert event_8.source == "BEMServer - Check outliers"
            assert (
                event_8.description
                == "The following timeseries don't have outliers anymore: Timeseries 2"
            )
            tbes = list(TimeseriesByEvent.get(event=event_8))
            assert len(tbes) == 1
            assert tbes[0].timeseries_id == ts_1.id
