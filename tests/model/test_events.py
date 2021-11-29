"""Event tests"""
import datetime as dt

import pytest

from bemserver_core.model import (
    EventCategory, EventState, EventLevel,
    EventChannel, EventChannelByCampaign,
    TimeseriesEvent,
)
from bemserver_core.model.events import TimeseriesEventByTimeseries
from bemserver_core.authorization import CurrentUser, CurrentCampaign
from bemserver_core.database import db
from bemserver_core.exceptions import (
    BEMServerAuthorizationError, BEMServerCoreMissingCampaignError)


class TestEventStateModel:

    def test_event_state_authorizations_as_admin(self, users):
        admin_user = users[0]
        assert admin_user.is_admin

        with CurrentUser(admin_user):
            nb_event_states = len(list(EventState.get()))
            event_state_1 = EventState.new(
                id="TEST",
                description="Event state 1",
            )
            db.session.add(event_state_1)
            db.session.commit()
            EventState.get_by_id(event_state_1.id)
            event_states = list(EventState.get())
            assert len(event_states) == nb_event_states + 1
            event_state_1.update(name="Super event_state 1")
            event_state_1.delete()
            db.session.commit()

    def test_event_state_authorizations_as_user(self, users):
        user_1 = users[1]
        assert not user_1.is_admin

        with CurrentUser(user_1):
            event_states = list(EventState.get())
            event_state_1 = EventState.get_by_id(event_states[0].id)
            with pytest.raises(BEMServerAuthorizationError):
                EventState.new(
                    id="TEST",
                    description="Event state 1",
                )
            with pytest.raises(BEMServerAuthorizationError):
                event_state_1.update(name="Super event_state 1")
            with pytest.raises(BEMServerAuthorizationError):
                event_state_1.delete()


class TestEventLevelModel:

    def test_event_level_authorizations_as_admin(self, users):
        admin_user = users[0]
        assert admin_user.is_admin

        with CurrentUser(admin_user):
            nb_event_levels = len(list(EventLevel.get()))
            event_level_1 = EventLevel.new(
                id="TEST",
                description="Event level 1",
            )
            db.session.add(event_level_1)
            db.session.commit()
            EventLevel.get_by_id(event_level_1.id)
            event_levels = list(EventLevel.get())
            assert len(event_levels) == nb_event_levels + 1
            event_level_1.update(name="Super event_level 1")
            event_level_1.delete()
            db.session.commit()

    def test_event_level_authorizations_as_user(self, users):
        user_1 = users[1]
        assert not user_1.is_admin

        with CurrentUser(user_1):
            event_levels = list(EventLevel.get())
            event_level_1 = EventLevel.get_by_id(event_levels[0].id)
            with pytest.raises(BEMServerAuthorizationError):
                EventLevel.new(
                    id="TEST",
                    description="Event level 1",
                )
            with pytest.raises(BEMServerAuthorizationError):
                event_level_1.update(name="Super event_level 1")
            with pytest.raises(BEMServerAuthorizationError):
                event_level_1.delete()


class TestEventCategoryModel:

    def test_event_category_authorizations_as_admin(self, users):
        admin_user = users[0]
        assert admin_user.is_admin

        with CurrentUser(admin_user):
            nb_event_categories = len(list(EventCategory.get()))
            event_category_1 = EventCategory.new(
                id="TEST",
                description="Event category 1",
            )
            db.session.add(event_category_1)
            db.session.commit()
            EventCategory.get_by_id(event_category_1.id)
            event_categories = list(EventCategory.get())
            assert len(event_categories) == nb_event_categories + 1
            event_category_1.update(name="Super event_category 1")
            event_category_1.delete()
            db.session.commit()

    def test_event_category_authorizations_as_user(self, users):
        user_1 = users[1]
        assert not user_1.is_admin

        with CurrentUser(user_1):
            event_categories = list(EventCategory.get())
            event_category_1 = EventCategory.get_by_id(event_categories[0].id)
            with pytest.raises(BEMServerAuthorizationError):
                EventCategory.new(
                    id="TEST",
                    description="Event category 1",
                )
            with pytest.raises(BEMServerAuthorizationError):
                event_category_1.update(name="Super event_category 1")
            with pytest.raises(BEMServerAuthorizationError):
                event_category_1.delete()


class TestEventChannelModel:

    def test_event_channel_authorizations_as_admin(self, users):
        admin_user = users[0]
        assert admin_user.is_admin

        with CurrentUser(admin_user):
            channel_1 = EventChannel.new(
                name="Event channel 1",
            )
            db.session.add(channel_1)
            db.session.commit()

            event_channel = EventChannel.get_by_id(channel_1.id)
            assert event_channel.id == channel_1.id
            assert event_channel.name == channel_1.name
            event_channels = list(EventChannel.get())
            assert len(event_channels) == 1
            assert event_channels[0].id == channel_1.id
            event_channel.update(name="Super event channel 1")
            event_channel.delete()
            db.session.commit()

    @pytest.mark.usefixtures("users_by_campaigns")
    @pytest.mark.usefixtures("event_channels_by_campaigns")
    def test_event_channel_authorizations_as_user(self, users, channels):
        user_1 = users[1]
        assert not user_1.is_admin
        channel_1 = channels[0]
        channel_2 = channels[1]

        with CurrentUser(user_1):
            with pytest.raises(BEMServerAuthorizationError):
                EventChannel.new(
                    name="Event channel 1",
                )

            event_channel = EventChannel.get_by_id(channel_2.id)
            event_channel_list = list(EventChannel.get())
            assert len(event_channel_list) == 1
            assert event_channel_list[0].id == channel_2.id
            with pytest.raises(BEMServerAuthorizationError):
                EventChannel.get_by_id(channel_1.id)
            with pytest.raises(BEMServerAuthorizationError):
                event_channel.update(name="Super event_channel 1")
            with pytest.raises(BEMServerAuthorizationError):
                event_channel.delete()


class TestEventModel:

    @pytest.mark.usefixtures("database")
    @pytest.mark.usefixtures("as_admin")
    def test_event_list_by_state(self, channels, campaigns):
        channel_1 = channels[0]
        campaign_1 = campaigns[0]

        with pytest.raises(BEMServerCoreMissingCampaignError):
            TimeseriesEvent.list_by_state()

        with CurrentCampaign(campaign_1):

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
                timestamp=campaign_1.start_time,
                category="observation_missing",
                source="src",
                level="ERROR",
                state="NEW",
            )
            evt_2 = TimeseriesEvent.new(
                channel_id=channel_1.id,
                timestamp=campaign_1.end_time,
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
                timestamp=campaign_1.start_time,
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

    @pytest.mark.usefixtures("database")
    @pytest.mark.usefixtures("as_admin")
    def test_event_read_only_fields(self, channels, campaigns):
        """Check channel and timestamp can't be modified after commit

        Also check the getter/setter don't get in the way when querying.
        This is kind of a "framework test".
        """
        channel_1 = channels[0]
        channel_2 = channels[1]
        campaign_1 = campaigns[0]

        with CurrentCampaign(campaign_1):
            evt_1 = TimeseriesEvent.new(
                channel_id=channel_1.id,
                timestamp=campaign_1.start_time,
                category="observation_missing",
                source="src",
                level="ERROR",
                state="NEW",
            )
            evt_1.update(timestamp=campaign_1.end_time)
            evt_1.update(channel_id=channel_2.id)
            db.session.commit()

            with pytest.raises(AttributeError):
                evt_1.update(timestamp=campaign_1.end_time)
            with pytest.raises(AttributeError):
                evt_1.update(channel_id=channel_2.id)

            tse_list = list(TimeseriesEvent.get(channel_id=2))
            assert tse_list == [evt_1]
            tse_list = list(TimeseriesEvent.get(channel_id=1))
            assert tse_list == []
            tse_list = list(TimeseriesEvent.get(timestamp=campaign_1.end_time))
            assert tse_list == [evt_1]
            tse_list = list(
                TimeseriesEvent.get(timestamp=campaign_1.start_time))
            assert tse_list == []


class TestEventChannelByCampaignModel:

    def test_event_channels_by_campaign_authorizations_as_admin(
            self, users, campaigns, channels
    ):
        admin_user = users[0]
        assert admin_user.is_admin
        campaign_1 = campaigns[0]
        campaign_2 = campaigns[1]
        channel_1 = channels[0]

        with CurrentUser(admin_user):
            ecbc_1 = EventChannelByCampaign.new(
                event_channel_id=channel_1.id,
                campaign_id=campaign_1.id,
            )
            db.session.add(ecbc_1)
            db.session.commit()

            ecbc = EventChannelByCampaign.get_by_id(ecbc_1.id)
            assert ecbc.id == ecbc_1.id
            ecbcs = list(EventChannelByCampaign.get())
            assert len(ecbcs) == 1
            assert ecbcs[0].id == ecbc_1.id
            ecbc.update(campaign_id=campaign_2.id)
            ecbc.delete()

    @pytest.mark.usefixtures("users_by_campaigns")
    def test_event_channels_by_campaign_authorizations_as_user(
        self, users, campaigns, event_channels_by_campaigns
    ):
        user_1 = users[1]
        assert not user_1.is_admin
        campaign_1 = campaigns[0]
        campaign_2 = campaigns[1]
        ecbc_1 = event_channels_by_campaigns[0]
        ecbc_2 = event_channels_by_campaigns[1]

        with CurrentUser(user_1):
            with pytest.raises(BEMServerAuthorizationError):
                EventChannelByCampaign.new(
                    event_channel_id=user_1.id,
                    campaign_id=campaign_2.id,
                )

            ecbc = EventChannelByCampaign.get_by_id(ecbc_2.id)
            ecbcs = list(EventChannelByCampaign.get())
            assert len(ecbcs) == 1
            assert ecbcs[0].id == ecbc_2.id
            with pytest.raises(BEMServerAuthorizationError):
                EventChannelByCampaign.get_by_id(ecbc_1.id)
            with pytest.raises(BEMServerAuthorizationError):
                ecbc.update(campaign_id=campaign_1.id)
            with pytest.raises(BEMServerAuthorizationError):
                ecbc.delete()


class TestTimeseriesEventModel:

    @pytest.mark.usefixtures("event_channels_by_campaigns")
    def test_timeseries_event_authorizations_as_admin(
        self, users, campaigns, channels, timeseries_events, timeseries,
    ):
        admin_user = users[0]
        assert admin_user.is_admin
        campaign_1 = campaigns[0]
        channel_1 = channels[0]
        timeseries_1 = timeseries[0]
        timeseries_2 = timeseries[1]
        ts_event_1 = timeseries_events[0]
        ts_event_2 = timeseries_events[1]

        ooc_dt = dt.datetime(2020, 5, 1, tzinfo=dt.timezone.utc)

        with CurrentUser(admin_user):
            with pytest.raises(BEMServerCoreMissingCampaignError):
                TimeseriesEvent.get()
            with pytest.raises(BEMServerCoreMissingCampaignError):
                TimeseriesEvent.get_by_id(ts_event_1.id)
            with pytest.raises(BEMServerCoreMissingCampaignError):
                TimeseriesEvent.new(
                    channel_id=channel_1.id,
                    timestamp=campaign_1.start_time,
                    category="observation_missing",
                    source="src",
                    level="ERROR",
                    state="NEW",
                )
            with pytest.raises(BEMServerCoreMissingCampaignError):
                ts_event_2.update(level="WARNING", state="ONGOING")
            with pytest.raises(BEMServerCoreMissingCampaignError):
                ts_event_2.delete()
            with CurrentCampaign(campaign_1):
                ts_events = list(TimeseriesEvent.get())
                assert set(ts_events) == {ts_event_1, ts_event_2}
                ts_events = list(TimeseriesEvent.get(channel_id=channel_1.id))
                assert ts_events == [ts_event_1]
                assert TimeseriesEvent.get_by_id(ts_event_1.id) == ts_event_1
                with pytest.raises(BEMServerAuthorizationError):
                    TimeseriesEvent.new(
                        channel_id=channel_1.id,
                        timestamp=ooc_dt,
                        category="observation_missing",
                        source="src",
                        level="ERROR",
                        state="NEW",
                    )
                TimeseriesEvent.new(
                    channel_id=channel_1.id,
                    timestamp=campaign_1.start_time,
                    category="observation_missing",
                    source="src",
                    level="ERROR",
                    state="NEW",
                    timeseries_ids=[timeseries_1.id, timeseries_2.id]
                )
                db.session.commit()
                ts_event_2.update(level="WARNING", state="ONGOING")
                db.session.commit()
                ts_event_2.delete()
                db.session.commit()

    @pytest.mark.usefixtures("users_by_campaigns")
    @pytest.mark.usefixtures("event_channels_by_campaigns")
    def test_timeseries_event_authorizations_as_user(
        self, users, campaigns, channels, timeseries_events
    ):
        user_1 = users[1]
        assert not user_1.is_admin
        campaign_1 = campaigns[0]
        campaign_2 = campaigns[1]
        channel_1 = channels[0]
        channel_2 = channels[1]
        ts_event_2 = timeseries_events[1]

        ooc_dt = dt.datetime(2020, 5, 1, tzinfo=dt.timezone.utc)

        with CurrentUser(user_1):
            # Without Campaing
            with pytest.raises(BEMServerCoreMissingCampaignError):
                assert not TimeseriesEvent.get()
            with pytest.raises(BEMServerCoreMissingCampaignError):
                TimeseriesEvent.get_by_id(ts_event_2.id)
            with pytest.raises(BEMServerCoreMissingCampaignError):
                TimeseriesEvent.new(
                    channel_id=channel_2.id,
                    timestamp=campaign_2.start_time,
                    category="observation_missing",
                    source="src",
                    level="ERROR",
                    state="NEW",
                )
            with pytest.raises(BEMServerCoreMissingCampaignError):
                ts_event_2.update(level="WARNING", state="ONGOING")
            with pytest.raises(BEMServerCoreMissingCampaignError):
                ts_event_2.delete()

            # Get while not member of Campaign
            with CurrentCampaign(campaign_1):
                with pytest.raises(BEMServerAuthorizationError):
                    TimeseriesEvent.get()
            with CurrentCampaign(campaign_2):
                events = list(TimeseriesEvent.get())
                assert events == [ts_event_2]

            with CurrentCampaign(campaign_2):
                assert TimeseriesEvent.get_by_id(ts_event_2.id) == ts_event_2
            # Create with Channel not in Campaign
            with CurrentCampaign(campaign_2):
                with pytest.raises(BEMServerAuthorizationError):
                    TimeseriesEvent.new(
                        channel_id=channel_1.id,
                        timestamp=campaign_2.start_time,
                        category="observation_missing",
                        source="src",
                        level="ERROR",
                        state="NEW",
                    )
            # Create with timestamps out of Campaign
            with CurrentCampaign(campaign_2):
                with pytest.raises(BEMServerAuthorizationError):
                    TimeseriesEvent.new(
                        channel_id=channel_2.id,
                        timestamp=ooc_dt,
                        category="observation_missing",
                        source="src",
                        level="ERROR",
                        state="NEW",
                    )
            with CurrentCampaign(campaign_2):
                TimeseriesEvent.new(
                    channel_id=channel_2.id,
                    timestamp=campaign_2.start_time,
                    category="observation_missing",
                    source="src",
                    level="ERROR",
                    state="NEW",
                )
            with CurrentCampaign(campaign_2):
                ts_event_2.update(level="WARNING", state="ONGOING")
                db.session.commit()
                ts_event_2.delete()
                db.session.commit()


class TestTimeseriesEventByTimeseriesModel:

    @pytest.mark.usefixtures("event_channels_by_campaigns")
    def test_timeseries_event_by_timeseries(
        self, users, campaigns, channels, timeseries,
    ):
        """Check TS_event x TS associations are created/deleted.

        This test merely checks the SQLAlchemy framework, more specifically
        the behaviour of the association table.
        """
        admin_user = users[0]
        assert admin_user.is_admin
        campaign_1 = campaigns[0]
        channel_1 = channels[0]
        timeseries_1 = timeseries[0]
        timeseries_2 = timeseries[1]

        with CurrentUser(admin_user), CurrentCampaign(campaign_1):
            assert not list(db.session.query(TimeseriesEventByTimeseries))
            tse_1 = TimeseriesEvent.new(
                channel_id=channel_1.id,
                timestamp=campaign_1.start_time,
                category="observation_missing",
                source="src",
                level="ERROR",
                state="NEW",
                timeseries_ids=[timeseries_1.id, timeseries_2.id],
            )
            db.session.commit()
            assert len(
                list(db.session.query(TimeseriesEventByTimeseries))) == 2
            tse_1.update(timeseries_ids=[timeseries_2.id])
            db.session.commit()
            assert len(
                list(db.session.query(TimeseriesEventByTimeseries))) == 1
            tse_1.delete()
            db.session.commit()
            assert not list(db.session.query(TimeseriesEventByTimeseries))
