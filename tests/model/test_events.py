"""Event tests"""
import datetime as dt

import pytest

from bemserver_core.model import TimeseriesEvent
from bemserver_core.database import db


class TestTimeseriesEventModel:

    @pytest.mark.usefixtures("database")
    def test_event_open(self, channels):
        channel_1 = channels[0]

        # Open a new event.
        ts_now = dt.datetime.now(dt.timezone.utc)
        evt_1 = TimeseriesEvent.open(
            channel_1.id, "observation_missing", "src")
        assert evt_1.state == "NEW"
        assert evt_1.level == "ERROR"
        assert evt_1.timestamp_start is not None
        assert evt_1.timestamp_start > ts_now
        assert evt_1.timestamp_end is None
        assert evt_1.description is None

        # Open with timestamp start.
        ts_start = dt.datetime.now(dt.timezone.utc)
        evt_2 = TimeseriesEvent.open(
            channel_1.id, "observation_missing", "src",
            timestamp_start=ts_start)
        assert evt_2.timestamp_start == ts_start
        assert evt_2.timestamp_end is None

    @pytest.mark.usefixtures("database")
    def test_event_list_by_state(self, channels):
        channel_1 = channels[0]

        # no events at all
        evts = TimeseriesEvent.list_by_state()
        assert evts == []
        evts = TimeseriesEvent.list_by_state(states=("NEW",))
        assert evts == []
        evts = TimeseriesEvent.list_by_state(states=("ONGOING",))
        assert evts == []
        evts = TimeseriesEvent.list_by_state(states=("CLOSED",))
        assert evts == []

        # create 2 events
        evt_1 = TimeseriesEvent.open(
            channel_1.id, "observation_missing", "src")
        evt_2 = TimeseriesEvent.open(
            channel_1.id, "observation_missing", "src")
        db.session.commit()

        # all events' state is NEW, so we have 2 events listing NEW or ONGOING
        evts = TimeseriesEvent.list_by_state()
        assert evts == [(evt_1,), (evt_2,)]

        # close one event
        evt_2.state = "CLOSED"
        # open and set an event to ONGOING state
        evt_3 = TimeseriesEvent.open(
            channel_1.id, "observation_missing", "src")
        evt_3.state = "ONGOING"
        db.session.commit()

        # 2 of 3 events are in NEW or ONGOING state
        evts = TimeseriesEvent.list_by_state()
        assert evts == [(evt_1,), (evt_3,)]
        # one is NEW
        evts = TimeseriesEvent.list_by_state(states=("NEW",))
        assert evts == [(evt_1,)]
        # one is ONGOING
        evts = TimeseriesEvent.list_by_state(states=("ONGOING",))
        assert evts == [(evt_3,)]
        # one is closed
        evts = TimeseriesEvent.list_by_state(states=("CLOSED",))
        assert evts == [(evt_2,)]
