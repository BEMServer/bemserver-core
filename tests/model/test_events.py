"""Event tests"""
import datetime as dt

import pytest

from bemserver_core.model import TimeseriesEvent
from bemserver_core.database import db


class TestTimeseriesEventModel:

    @pytest.mark.usefixtures("database")
    def test_event_list_by_state(self, channels):
        channel_1 = channels[0]

        evts = TimeseriesEvent.list_by_state()
        assert evts == []
        evts = TimeseriesEvent.list_by_state(states=("NEW",))
        assert evts == []
        evts = TimeseriesEvent.list_by_state(states=("ONGOING",))
        assert evts == []
        evts = TimeseriesEvent.list_by_state(states=("CLOSED",))
        assert evts == []

        evt_1 = TimeseriesEvent.new(
            channel_id=channel_1.id,
            timestamp=dt.datetime.now(dt.timezone.utc),
            category="observation_missing",
            source="src",
            level="ERROR",
            state="NEW",
        )
        evt_2 = TimeseriesEvent.new(
            channel_id=channel_1.id,
            timestamp=dt.datetime.now(dt.timezone.utc),
            category="observation_missing",
            source="src",
            level="ERROR",
            state="NEW",
        )
        db.session.commit()

        evts = TimeseriesEvent.list_by_state()
        assert evts == [(evt_1,), (evt_2,)]

        evt_2.state = "CLOSED"
        evt_3 = TimeseriesEvent.new(
            channel_id=channel_1.id,
            timestamp=dt.datetime.now(dt.timezone.utc),
            category="observation_missing",
            source="src",
            level="ERROR",
            state="ONGOING",
        )
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
