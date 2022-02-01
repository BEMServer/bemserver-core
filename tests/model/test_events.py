"""Event tests"""
import datetime as dt

import pytest

from bemserver_core.model import (
    EventCategory,
    EventState,
    EventLevel,
    EventChannel,
    EventChannelByCampaign,
    EventChannelByUser,
    Event,
)
from bemserver_core.authorization import CurrentUser
from bemserver_core.database import db
from bemserver_core.exceptions import BEMServerAuthorizationError


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
    @pytest.mark.usefixtures("event_channels_by_users")
    @pytest.mark.usefixtures("event_channels_by_campaigns")
    def test_event_channels_filter_by_campaign_or_user(
        self, users, event_channels, campaigns
    ):
        admin_user = users[0]
        assert admin_user.is_admin
        campaign_1 = campaigns[0]
        campaign_2 = campaigns[1]
        user_1 = users[1]
        ec_1 = event_channels[0]
        ec_2 = event_channels[1]

        with CurrentUser(admin_user):
            ec_l = list(EventChannel.get(campaign_id=campaign_1.id))
            assert len(ec_l) == 1
            assert ec_l[0] == ec_1

        with CurrentUser(admin_user):
            ec_l = list(EventChannel.get(user_id=user_1.id))
            assert len(ec_l) == 1
            assert ec_l[0] == ec_2

        with CurrentUser(admin_user):
            ec_l = list(EventChannel.get(user_id=user_1.id, campaign_id=campaign_2.id))
            assert len(ec_l) == 1
            assert ec_l[0] == ec_2

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

    @pytest.mark.usefixtures("event_channels_by_users")
    def test_event_channel_authorizations_as_user(self, users, event_channels):
        user_1 = users[1]
        assert not user_1.is_admin
        channel_1 = event_channels[0]
        channel_2 = event_channels[1]

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


class TestEventChannelByCampaignModel:
    def test_event_channels_by_campaign_authorizations_as_admin(
        self, users, campaigns, event_channels
    ):
        admin_user = users[0]
        assert admin_user.is_admin
        campaign_1 = campaigns[0]
        campaign_2 = campaigns[1]
        channel_1 = event_channels[0]

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


class TestEventChannelByUserModel:
    def test_event_channel_by_user_authorizations_as_admin(self, users, event_channels):
        admin_user = users[0]
        assert admin_user.is_admin
        user_1 = users[1]
        event_channel_1 = event_channels[0]
        event_channel_2 = event_channels[1]

        with CurrentUser(admin_user):
            tgbu_1 = EventChannelByUser.new(
                user_id=user_1.id,
                event_channel_id=event_channel_1.id,
            )
            db.session.add(tgbu_1)
            db.session.commit()

            tgbu = EventChannelByUser.get_by_id(tgbu_1.id)
            assert tgbu.id == tgbu_1.id
            tgbus = list(EventChannelByUser.get())
            assert len(tgbus) == 1
            assert tgbus[0].id == tgbu_1.id
            tgbu.update(event_channel_id=event_channel_2.id)
            tgbu.delete()

    def test_event_channel_by_user_authorizations_as_user(
        self, users, event_channels, event_channels_by_users
    ):
        user_1 = users[1]
        assert not user_1.is_admin
        event_channel_1 = event_channels[0]
        event_channel_2 = event_channels[1]
        tgbu_1 = event_channels_by_users[0]
        tgbu_2 = event_channels_by_users[1]

        with CurrentUser(user_1):
            with pytest.raises(BEMServerAuthorizationError):
                EventChannelByUser.new(
                    user_id=user_1.id,
                    event_channel_id=event_channel_2.id,
                )

            tgbu = EventChannelByUser.get_by_id(tgbu_2.id)
            tgbus = list(EventChannelByUser.get())
            assert len(tgbus) == 1
            assert tgbus[0].id == tgbu_2.id
            with pytest.raises(BEMServerAuthorizationError):
                EventChannelByUser.get_by_id(tgbu_1.id)
            with pytest.raises(BEMServerAuthorizationError):
                tgbu.update(event_channel_id=event_channel_1.id)
            with pytest.raises(BEMServerAuthorizationError):
                tgbu.delete()


class TestEventModel:
    @pytest.mark.usefixtures("as_admin")
    def test_event_list_by_state(self, event_channels):
        channel_1 = event_channels[0]

        evts = Event.list_by_state()
        assert evts == []
        evts = Event.list_by_state(states=("NEW",))
        assert evts == []
        evts = Event.list_by_state(states=("ONGOING",))
        assert evts == []
        evts = Event.list_by_state(states=("CLOSED",))
        assert evts == []

        timestamp = dt.datetime(2020, 5, 1, tzinfo=dt.timezone.utc)

        evt_1 = Event.new(
            channel_id=channel_1.id,
            timestamp=timestamp,
            category="observation_missing",
            source="src",
            level="ERROR",
            state="NEW",
        )
        evt_2 = Event.new(
            channel_id=channel_1.id,
            timestamp=timestamp,
            category="observation_missing",
            source="src",
            level="ERROR",
            state="NEW",
        )
        db.session.commit()

        evts = Event.list_by_state()
        assert evts == [(evt_1,), (evt_2,)]

        evt_2.state = "CLOSED"
        evt_3 = Event.new(
            channel_id=channel_1.id,
            timestamp=timestamp,
            category="observation_missing",
            source="src",
            level="ERROR",
            state="ONGOING",
        )
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

    @pytest.mark.usefixtures("as_admin")
    def test_event_read_only_fields(self, event_channels):
        """Check channel and timestamp can't be modified after commit

        Also check the getter/setter don't get in the way when querying.
        This is kind of a "framework test".
        """
        channel_1 = event_channels[0]
        channel_2 = event_channels[1]

        timestamp_1 = dt.datetime(2020, 5, 1, tzinfo=dt.timezone.utc)
        timestamp_2 = dt.datetime(2020, 6, 1, tzinfo=dt.timezone.utc)

        evt_1 = Event.new(
            channel_id=channel_1.id,
            timestamp=timestamp_1,
            category="observation_missing",
            source="src",
            level="ERROR",
            state="NEW",
        )
        evt_1.update(timestamp=timestamp_2)
        evt_1.update(channel_id=channel_2.id)
        db.session.commit()

        with pytest.raises(AttributeError):
            evt_1.update(timestamp=timestamp_1)
        with pytest.raises(AttributeError):
            evt_1.update(channel_id=channel_2.id)

        tse_list = list(Event.get(channel_id=2))
        assert tse_list == [evt_1]
        tse_list = list(Event.get(channel_id=1))
        assert tse_list == []
        tse_list = list(Event.get(timestamp=timestamp_2))
        assert tse_list == [evt_1]
        tse_list = list(Event.get(timestamp=timestamp_1))
        assert tse_list == []

    @pytest.mark.usefixtures("event_channels_by_users")
    def test_event_authorizations_as_admin(
        self,
        users,
        event_channels,
        events,
    ):
        admin_user = users[0]
        assert admin_user.is_admin
        channel_1 = event_channels[0]
        event_1 = events[0]
        event_2 = events[1]

        with CurrentUser(admin_user):
            events = list(Event.get())
            assert set(events) == {event_1, event_2}
            events = list(Event.get(channel_id=channel_1.id))
            assert events == [event_1]
            assert Event.get_by_id(event_1.id) == event_1
            Event.new(
                channel_id=channel_1.id,
                timestamp=dt.datetime(2020, 5, 1, tzinfo=dt.timezone.utc),
                category="observation_missing",
                source="src",
                level="ERROR",
                state="NEW",
            )
            db.session.commit()
            event_2.update(level="WARNING", state="ONGOING")
            db.session.commit()
            event_2.delete()
            db.session.commit()

    @pytest.mark.usefixtures("event_channels_by_users")
    def test_event_authorizations_as_user(self, users, event_channels, events):
        user_1 = users[1]
        assert not user_1.is_admin
        channel_1 = event_channels[0]
        channel_2 = event_channels[1]
        event_1 = events[0]
        event_2 = events[1]

        with CurrentUser(user_1):

            events = list(Event.get())
            assert set(events) == {event_2}
            events = list(Event.get(channel_id=channel_1.id))
            assert not events
            assert Event.get_by_id(event_2.id) == event_2

            # Not member of channel
            with pytest.raises(BEMServerAuthorizationError):
                Event.new(
                    channel_id=channel_1.id,
                    timestamp=dt.datetime(2020, 5, 1, tzinfo=dt.timezone.utc),
                    category="observation_missing",
                    source="src",
                    level="ERROR",
                    state="NEW",
                )
            with pytest.raises(BEMServerAuthorizationError):
                event_1.update(level="WARNING", state="ONGOING")
            with pytest.raises(BEMServerAuthorizationError):
                event_1.delete()

            # Member of channel
            Event.new(
                channel_id=channel_2.id,
                timestamp=dt.datetime(2020, 5, 1, tzinfo=dt.timezone.utc),
                category="observation_missing",
                source="src",
                level="ERROR",
                state="NEW",
            )
            db.session.commit()
            event_2.update(level="WARNING", state="ONGOING")
            db.session.commit()
            event_2.delete()
            db.session.commit()
