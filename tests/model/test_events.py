"""Event tests"""
import datetime as dt
import sqlalchemy as sqla

import pytest

from bemserver_core.model import EventCategory, EventLevel, Event
from bemserver_core.authorization import CurrentUser
from bemserver_core.database import db
from bemserver_core.exceptions import BEMServerAuthorizationError


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
            event_categories = list(EventCategory.get())
            nb_categs = len(event_categories)
            event_category_1 = EventCategory.new(
                id="TEST",
                description="Event category 1",
            )
            db.session.add(event_category_1)
            db.session.commit()
            EventCategory.get_by_id(event_category_1.id)
            event_categories = list(EventCategory.get())
            assert len(event_categories) == nb_categs + 1
            event_category_1.update(name="Super event_category 1")
            event_category_1.delete()
            db.session.commit()

    def test_event_category_authorizations_as_user(self, users, event_categories):
        user_1 = users[1]
        assert not user_1.is_admin

        with CurrentUser(user_1):
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


class TestEventModel:
    @pytest.mark.usefixtures("as_admin")
    def test_event_read_only_fields(self, campaign_scopes, event_categories):
        """Check campaign_scope and timestamp can't be modified

        This is kind of a "framework test".
        """
        campaign_scope_1 = campaign_scopes[0]
        campaign_scope_2 = campaign_scopes[1]
        ec_1 = event_categories[0]

        timestamp_1 = dt.datetime(2020, 5, 1, tzinfo=dt.timezone.utc)
        timestamp_2 = dt.datetime(2020, 6, 1, tzinfo=dt.timezone.utc)

        evt_1 = Event.new(
            campaign_scope_id=campaign_scope_1.id,
            timestamp=timestamp_1,
            category=ec_1.id,
            source="src",
            level="ERROR",
        )
        db.session.commit()

        evt_1.update(timestamp=timestamp_2)
        db.session.add(evt_1)
        with pytest.raises(
            sqla.exc.IntegrityError,
            match="timestamp cannot be modified",
        ):
            db.session.commit()
        db.session.rollback()
        evt_1.update(campaign_scope_id=campaign_scope_2.id)
        db.session.add(evt_1)
        with pytest.raises(
            sqla.exc.IntegrityError,
            match="campaign_scope_id cannot be modified",
        ):
            db.session.commit()
        db.session.rollback()

        tse_list = list(Event.get(campaign_scope_id=1))
        assert tse_list == [evt_1]
        tse_list = list(Event.get(campaign_scope_id=2))
        assert tse_list == []
        tse_list = list(Event.get(timestamp=timestamp_1))
        assert tse_list == [evt_1]
        tse_list = list(Event.get(timestamp=timestamp_2))
        assert tse_list == []

    def test_event_authorizations_as_admin(
        self,
        users,
        campaign_scopes,
        events,
        event_categories,
    ):
        admin_user = users[0]
        assert admin_user.is_admin
        campaign_scope_1 = campaign_scopes[0]
        event_1 = events[0]
        event_2 = events[1]
        ec_1 = event_categories[0]

        with CurrentUser(admin_user):
            events = list(Event.get())
            assert set(events) == {event_1, event_2}
            events = list(Event.get(campaign_scope_id=campaign_scope_1.id))
            assert events == [event_1]
            assert Event.get_by_id(event_1.id) == event_1
            Event.new(
                campaign_scope_id=campaign_scope_1.id,
                timestamp=dt.datetime(2020, 5, 1, tzinfo=dt.timezone.utc),
                category=ec_1.id,
                source="src",
                level="ERROR",
            )
            db.session.commit()
            event_2.update(level="WARNING")
            db.session.commit()
            event_2.delete()
            db.session.commit()

    @pytest.mark.usefixtures("users_by_user_groups")
    @pytest.mark.usefixtures("user_groups_by_campaign_scopes")
    def test_event_authorizations_as_user(
        self, users, campaign_scopes, events, event_categories
    ):
        user_1 = users[1]
        assert not user_1.is_admin
        campaign_scope_1 = campaign_scopes[0]
        campaign_scope_2 = campaign_scopes[1]
        event_1 = events[0]
        event_2 = events[1]
        ec_1 = event_categories[0]

        with CurrentUser(user_1):

            events = list(Event.get())
            assert events == [event_2]
            events = list(Event.get(campaign_scope_id=campaign_scope_1.id))
            assert not events
            assert Event.get_by_id(event_2.id) == event_2

            # Not member of campaign_scope
            with pytest.raises(BEMServerAuthorizationError):
                Event.new(
                    campaign_scope_id=campaign_scope_1.id,
                    timestamp=dt.datetime(2020, 5, 1, tzinfo=dt.timezone.utc),
                    category=ec_1.id,
                    source="src",
                    level="ERROR",
                )
            with pytest.raises(BEMServerAuthorizationError):
                event_1.update(level="WARNING")
            with pytest.raises(BEMServerAuthorizationError):
                event_1.delete()

            # Member of campaign_scope
            Event.new(
                campaign_scope_id=campaign_scope_2.id,
                timestamp=dt.datetime(2020, 5, 1, tzinfo=dt.timezone.utc),
                category=ec_1.id,
                source="src",
                level="ERROR",
            )
            db.session.commit()
            event_2.update(level="WARNING")
            db.session.commit()
            event_2.delete()
            db.session.commit()
