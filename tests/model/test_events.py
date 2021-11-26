"""Event tests"""
import datetime as dt

import pytest

from bemserver_core.model import Event
from bemserver_core.database import db


class TestEventModel:

    @pytest.mark.usefixtures("database")
    def test_event_open_extend_close(self):

        # Open a new event.
        ts_now = dt.datetime.now(dt.timezone.utc)
        evt_1 = Event.open("observation_missing", "src", "TIMESERIES", 42)
        assert evt_1.state == "NEW"
        assert evt_1.level == "ERROR"
        assert evt_1.timestamp_start is not None
        assert evt_1.timestamp_start > ts_now
        assert evt_1.timestamp_last_update == evt_1.timestamp_start
        assert evt_1.timestamp_end is None
        assert evt_1.description is None
        assert evt_1.duration == dt.timedelta(0)

        # Open with timestamp start.
        ts_start = dt.datetime.now(dt.timezone.utc)
        evt_2 = Event.open(
            "observation_missing", "src", "TIMESERIES", 42,
            timestamp_start=ts_start)
        assert evt_2.timestamp_start == ts_start
        assert evt_2.timestamp_last_update > evt_2.timestamp_start
        assert evt_2.timestamp_end is None
        assert evt_2.duration == (
            evt_2.timestamp_last_update - evt_2.timestamp_start)

        # Extend events.
        evt_1.extend()
        assert evt_1.state == "ONGOING"
        assert evt_1.timestamp_last_update > evt_1.timestamp_start
        assert evt_1.duration == (
            evt_1.timestamp_last_update - evt_1.timestamp_start)

        evt_2.extend()
        assert evt_2.state == "ONGOING"
        assert evt_2.timestamp_last_update > evt_2.timestamp_start
        assert evt_2.duration == (
            evt_2.timestamp_last_update - evt_2.timestamp_start)

        # Close events.
        evt_1.close()
        assert evt_1.state == "CLOSED"
        assert evt_1.timestamp_end is not None
        assert evt_1.timestamp_last_update == evt_1.timestamp_end
        assert evt_1.timestamp_last_update > evt_1.timestamp_start
        assert evt_1.duration == evt_1.timestamp_end - evt_1.timestamp_start

        ts_end = dt.datetime.now(dt.timezone.utc)
        evt_2.close(timestamp_end=ts_end)
        assert evt_2.state == "CLOSED"
        assert evt_2.timestamp_end == ts_end
        assert evt_2.timestamp_last_update > evt_2.timestamp_end
        assert evt_2.duration == ts_end - ts_start

    def test_event_list_by_state(self, database):

        # no events at all
        evts = Event.list_by_state()
        assert evts == []
        evts = Event.list_by_state(states=("NEW",))
        assert evts == []
        evts = Event.list_by_state(states=("ONGOING",))
        assert evts == []
        evts = Event.list_by_state(states=("CLOSED",))
        assert evts == []

        # create 2 events
        evt_1 = Event.open("observation_missing", "src", "TIMESERIES", 42)
        evt_2 = Event.open("observation_missing", "src", "TIMESERIES", 69)
        db.session.commit()

        # all events' state is NEW, so we have 2 events listing NEW or ONGOING
        evts = Event.list_by_state()
        assert evts == [(evt_1,), (evt_2,)]

        # close one event
        evt_2.close()
        # open and extend an event to ONGOING state
        evt_3 = Event.open("observation_missing", "src", "TIMESERIES", 666)
        evt_3.extend()
        db.session.commit()

        # 2 of 3 events are in NEW or ONGOING state
        evts = Event.list_by_state()
        assert evts == [(evt_1,), (evt_3,)]
        # one is NEW
        evts = Event.list_by_state(states=("NEW",))
        assert evts == [(evt_1,)]
        # one is ONGOING
        evts = Event.list_by_state(states=("ONGOING",))
        assert evts == [(evt_3,)]
        # one is closed
        evts = Event.list_by_state(states=("CLOSED",))
        assert evts == [(evt_2,)]
