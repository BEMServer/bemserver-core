"""Event tests"""
import datetime as dt
import sqlalchemy as sqla

import pytest

from bemserver_core.model import (
    EventCategory,
    EventLevel,
    Event,
    TimeseriesByEvent,
    EventBySite,
    EventByBuilding,
    EventByStorey,
    EventBySpace,
    EventByZone,
)
from bemserver_core.authorization import CurrentUser
from bemserver_core.database import db
from bemserver_core.exceptions import (
    BEMServerAuthorizationError,
    BEMServerCoreCampaignError,
    BEMServerCoreCampaignScopeError,
)


class TestEventLevelModel:
    def test_event_level_authorizations_as_admin(self, users):
        admin_user = users[0]
        assert admin_user.is_admin

        with CurrentUser(admin_user):
            nb_event_levels = len(list(EventLevel.get()))
            event_level_1 = EventLevel.new(
                name="Level 1",
                description="Event level 1",
            )
            db.session.add(event_level_1)
            db.session.commit()
            EventLevel.get_by_id(event_level_1.id)
            event_levels = list(EventLevel.get())
            assert len(event_levels) == nb_event_levels + 1
            event_level_1.update(
                name="Event level 1",
                description="Super event_level 1",
            )
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
                    name="Level 1",
                    description="Event level 1",
                )
            with pytest.raises(BEMServerAuthorizationError):
                event_level_1.update(description="Super event_level 1")
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
                name="Category 1",
                description="Event category 1",
            )
            db.session.add(event_category_1)
            db.session.commit()
            EventCategory.get_by_id(event_category_1.id)
            event_categories = list(EventCategory.get())
            assert len(event_categories) == nb_categs + 1
            event_category_1.update(
                name="Event_category 1",
                description="Super event_category 1",
            )
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
                    name="Category 1",
                    description="Event category 1",
                )
            with pytest.raises(BEMServerAuthorizationError):
                event_category_1.update(description="Super event_category 1")
            with pytest.raises(BEMServerAuthorizationError):
                event_category_1.delete()


class TestEventModel:
    @pytest.mark.usefixtures("timeseries_by_events")
    def test_events_delete_cascade(self, users, events):
        admin_user = users[0]
        event_1 = events[0]

        with CurrentUser(admin_user):
            assert len(list(TimeseriesByEvent.get())) == 2

            event_1.delete()
            db.session.commit()
            assert len(list(TimeseriesByEvent.get())) == 1

    @pytest.mark.usefixtures("as_admin")
    def test_event_read_only_fields(
        self, campaign_scopes, event_categories, event_levels
    ):
        """Check campaign_scope and timestamp can't be modified

        This is kind of a "framework test".
        """
        campaign_scope_1 = campaign_scopes[0]
        campaign_scope_2 = campaign_scopes[1]
        ec_1 = event_categories[0]
        el_1 = event_levels[0]

        timestamp_1 = dt.datetime(2020, 5, 1, tzinfo=dt.timezone.utc)
        timestamp_2 = dt.datetime(2020, 6, 1, tzinfo=dt.timezone.utc)

        evt_1 = Event.new(
            campaign_scope_id=campaign_scope_1.id,
            timestamp=timestamp_1,
            category_id=ec_1.id,
            level_id=el_1.id,
            source="src",
        )
        db.session.commit()

        evt_1.update(timestamp=timestamp_2)
        db.session.add(evt_1)
        with pytest.raises(
            sqla.exc.IntegrityError,
            match="timestamp cannot be modified",
        ):
            db.session.flush()
        db.session.rollback()
        evt_1.update(campaign_scope_id=campaign_scope_2.id)
        db.session.add(evt_1)
        with pytest.raises(
            sqla.exc.IntegrityError,
            match="campaign_scope_id cannot be modified",
        ):
            db.session.flush()
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
        self, users, campaign_scopes, events, event_categories, event_levels
    ):
        admin_user = users[0]
        assert admin_user.is_admin
        campaign_scope_1 = campaign_scopes[0]
        event_1 = events[0]
        event_2 = events[1]
        ec_1 = event_categories[0]
        el_1 = event_levels[0]
        el_2 = event_levels[1]

        with CurrentUser(admin_user):
            events = list(Event.get())
            assert set(events) == {event_1, event_2}
            events = list(Event.get(campaign_scope_id=campaign_scope_1.id))
            assert events == [event_1]
            assert Event.get_by_id(event_1.id) == event_1
            Event.new(
                campaign_scope_id=campaign_scope_1.id,
                timestamp=dt.datetime(2020, 5, 1, tzinfo=dt.timezone.utc),
                category_id=ec_1.id,
                level_id=el_1.id,
                source="src",
            )
            db.session.commit()
            event_2.update(level_id=el_2.id)
            db.session.commit()
            event_2.delete()
            db.session.commit()

    @pytest.mark.usefixtures("users_by_user_groups")
    @pytest.mark.usefixtures("user_groups_by_campaign_scopes")
    def test_event_authorizations_as_user(
        self, users, campaign_scopes, events, event_categories, event_levels
    ):
        user_1 = users[1]
        assert not user_1.is_admin
        campaign_scope_1 = campaign_scopes[0]
        campaign_scope_2 = campaign_scopes[1]
        event_1 = events[0]
        event_2 = events[1]
        ec_1 = event_categories[0]
        el_1 = event_levels[0]
        el_2 = event_levels[1]

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
                    category_id=ec_1.id,
                    level_id=el_1.id,
                    source="src",
                )
            with pytest.raises(BEMServerAuthorizationError):
                event_1.update(level="WARNING")
            with pytest.raises(BEMServerAuthorizationError):
                event_1.delete()

            # Member of campaign_scope
            Event.new(
                campaign_scope_id=campaign_scope_2.id,
                timestamp=dt.datetime(2020, 5, 1, tzinfo=dt.timezone.utc),
                category_id=ec_1.id,
                level_id=el_1.id,
                source="src",
            )
            db.session.commit()
            event_2.update(level_id=el_2.id)
            db.session.commit()
            event_2.delete()
            db.session.commit()


class TestTimeseriesByEventModel:
    @pytest.mark.parametrize("timeseries", (4,), indirect=True)
    def test_timeseries_by_event_authorizations_as_admin(
        self, users, timeseries, events
    ):
        admin_user = users[0]
        assert admin_user.is_admin

        ts_1 = timeseries[0]
        ts_2 = timeseries[1]
        ts_4 = timeseries[3]
        event_1 = events[0]

        with CurrentUser(admin_user):
            with pytest.raises(BEMServerCoreCampaignScopeError):
                TimeseriesByEvent.new(timeseries_id=ts_2.id, event_id=event_1.id)
                db.session.flush()
            db.session.rollback()
            tbe_1 = TimeseriesByEvent.new(timeseries_id=ts_1.id, event_id=event_1.id)
            db.session.add(tbe_1)
            db.session.flush()
            TimeseriesByEvent.get_by_id(tbe_1.id)
            tbes = list(TimeseriesByEvent.get())
            assert len(tbes) == 1
            tbe_1.update(timeseries_id=ts_4.id)
            db.session.flush()
            with pytest.raises(BEMServerCoreCampaignScopeError):
                tbe_1.update(timeseries_id=ts_2.id)
                db.session.flush()
            db.session.rollback()
            tbe_1 = TimeseriesByEvent.new(timeseries_id=ts_1.id, event_id=event_1.id)
            db.session.flush()
            tbe_1.delete()
            db.session.flush()

    @pytest.mark.usefixtures("users_by_user_groups")
    @pytest.mark.usefixtures("user_groups_by_campaigns")
    @pytest.mark.usefixtures("user_groups_by_campaign_scopes")
    @pytest.mark.parametrize("timeseries", (5,), indirect=True)
    def test_timeseries_by_event_authorizations_as_user(
        self, users, timeseries, events, timeseries_by_events
    ):
        user_1 = users[1]
        assert not user_1.is_admin

        ts_1 = timeseries[0]
        ts_2 = timeseries[1]
        ts_5 = timeseries[4]
        event_1 = events[0]
        event_2 = events[1]
        tbe_1 = timeseries_by_events[0]
        tbe_2 = timeseries_by_events[1]

        assert (
            ts_2.campaign_scope_id
            == ts_5.campaign_scope_id
            == event_2.campaign_scope_id
        )

        with CurrentUser(user_1):
            tbe_l = list(TimeseriesByEvent.get())
            assert len(tbe_l) == 1
            assert tbe_l[0] == tbe_2
            TimeseriesByEvent.get_by_id(tbe_2.id)
            tbe = TimeseriesByEvent.new(timeseries_id=ts_5.id, event_id=event_2.id)
            tbe.delete()
            tbe_2.update(timeseries_id=ts_5.id)
            tbe_2.delete()
            with pytest.raises(BEMServerAuthorizationError):
                TimeseriesByEvent.get_by_id(tbe_1.id)
            with pytest.raises(BEMServerAuthorizationError):
                TimeseriesByEvent.new(timeseries_id=ts_1.id, event_id=event_1.id)
            with pytest.raises(BEMServerAuthorizationError):
                tbe_1.update(timeseries_id=ts_5.id)
            with pytest.raises(BEMServerAuthorizationError):
                tbe_1.delete()


class TestEventBySiteModel:
    def test_event_by_site_authorizations_as_admin(self, users, sites, events):
        admin_user = users[0]
        assert admin_user.is_admin

        site_1 = sites[0]
        site_2 = sites[1]
        event_1 = events[0]

        with CurrentUser(admin_user):
            with pytest.raises(BEMServerCoreCampaignError):
                EventBySite.new(site_id=site_2.id, event_id=event_1.id)
                db.session.flush()
            db.session.rollback()
            ebs_1 = EventBySite.new(site_id=site_1.id, event_id=event_1.id)
            db.session.add(ebs_1)
            db.session.flush()
            EventBySite.get_by_id(ebs_1.id)
            ebss = list(EventBySite.get())
            assert len(ebss) == 1
            ebs_1.update(site_id=site_1.id)
            db.session.flush()
            with pytest.raises(BEMServerCoreCampaignError):
                ebs_1.update(site_id=site_2.id)
                db.session.flush()
            db.session.rollback()
            ebs_1 = EventBySite.new(site_id=site_1.id, event_id=event_1.id)
            db.session.flush()
            ebs_1.delete()
            db.session.flush()

    @pytest.mark.usefixtures("users_by_user_groups")
    @pytest.mark.usefixtures("user_groups_by_campaigns")
    @pytest.mark.usefixtures("user_groups_by_campaign_scopes")
    def test_event_by_site_authorizations_as_user(
        self, users, sites, events, events_by_sites
    ):
        user_1 = users[1]
        assert not user_1.is_admin

        site_1 = sites[0]
        site_2 = sites[1]
        event_1 = events[0]
        event_2 = events[1]
        ebs_1 = events_by_sites[0]
        ebs_2 = events_by_sites[1]

        with CurrentUser(user_1):
            ebs_l = list(EventBySite.get())
            assert len(ebs_l) == 1
            assert ebs_l[0] == ebs_2
            EventBySite.get_by_id(ebs_2.id)
            ebs_2.update(site_id=site_2.id)
            ebs_2.delete()
            ebs = EventBySite.new(site_id=site_2.id, event_id=event_2.id)
            ebs.delete()
            with pytest.raises(BEMServerAuthorizationError):
                EventBySite.get_by_id(ebs_1.id)
            with pytest.raises(BEMServerAuthorizationError):
                EventBySite.new(site_id=site_1.id, event_id=event_1.id)
            with pytest.raises(BEMServerAuthorizationError):
                ebs_1.update(site_id=site_1.id)
            with pytest.raises(BEMServerAuthorizationError):
                ebs_1.delete()


class TestEventByBuildingModel:
    def test_event_by_building_authorizations_as_admin(self, users, buildings, events):
        admin_user = users[0]
        assert admin_user.is_admin

        building_1 = buildings[0]
        building_2 = buildings[1]
        event_1 = events[0]

        with CurrentUser(admin_user):
            with pytest.raises(BEMServerCoreCampaignError):
                EventByBuilding.new(building_id=building_2.id, event_id=event_1.id)
                db.session.flush()
            db.session.rollback()
            ebb_1 = EventByBuilding.new(building_id=building_1.id, event_id=event_1.id)
            db.session.add(ebb_1)
            db.session.flush()
            EventByBuilding.get_by_id(ebb_1.id)
            ebbs = list(EventByBuilding.get())
            assert len(ebbs) == 1
            ebb_1.update(building_id=building_1.id)
            db.session.flush()
            with pytest.raises(BEMServerCoreCampaignError):
                ebb_1.update(building_id=building_2.id)
                db.session.flush()
            db.session.rollback()
            ebb_1 = EventByBuilding.new(building_id=building_1.id, event_id=event_1.id)
            db.session.flush()
            ebb_1.delete()
            db.session.flush()

    @pytest.mark.usefixtures("users_by_user_groups")
    @pytest.mark.usefixtures("user_groups_by_campaigns")
    @pytest.mark.usefixtures("user_groups_by_campaign_scopes")
    def test_event_by_building_authorizations_as_user(
        self, users, buildings, events, events_by_buildings
    ):
        user_1 = users[1]
        assert not user_1.is_admin

        building_1 = buildings[0]
        building_2 = buildings[1]
        event_1 = events[0]
        event_2 = events[1]
        ebb_1 = events_by_buildings[0]
        ebb_2 = events_by_buildings[1]

        with CurrentUser(user_1):
            ebb_l = list(EventByBuilding.get())
            assert len(ebb_l) == 1
            assert ebb_l[0] == ebb_2
            EventByBuilding.get_by_id(ebb_2.id)
            ebb_2.update(building_id=building_2.id)
            ebb_2.delete()
            ebb = EventByBuilding.new(building_id=building_2.id, event_id=event_2.id)
            ebb.delete()
            with pytest.raises(BEMServerAuthorizationError):
                EventByBuilding.get_by_id(ebb_1.id)
            with pytest.raises(BEMServerAuthorizationError):
                EventByBuilding.new(building_id=building_1.id, event_id=event_1.id)
            with pytest.raises(BEMServerAuthorizationError):
                ebb_1.update(building_id=building_1.id)
            with pytest.raises(BEMServerAuthorizationError):
                ebb_1.delete()


class TestEventByStoreyModel:
    def test_event_by_storey_authorizations_as_admin(self, users, storeys, events):
        admin_user = users[0]
        assert admin_user.is_admin

        storey_1 = storeys[0]
        storey_2 = storeys[1]
        event_1 = events[0]

        with CurrentUser(admin_user):
            with pytest.raises(BEMServerCoreCampaignError):
                EventByStorey.new(storey_id=storey_2.id, event_id=event_1.id)
                db.session.flush()
            db.session.rollback()
            ebs_1 = EventByStorey.new(storey_id=storey_1.id, event_id=event_1.id)
            db.session.add(ebs_1)
            db.session.flush()
            EventByStorey.get_by_id(ebs_1.id)
            ebss = list(EventByStorey.get())
            assert len(ebss) == 1
            ebs_1.update(storey_id=storey_1.id)
            db.session.flush()
            with pytest.raises(BEMServerCoreCampaignError):
                ebs_1.update(storey_id=storey_2.id)
                db.session.flush()
            db.session.rollback()
            ebs_1 = EventByStorey.new(storey_id=storey_1.id, event_id=event_1.id)
            db.session.flush()
            ebs_1.delete()
            db.session.flush()

    @pytest.mark.usefixtures("users_by_user_groups")
    @pytest.mark.usefixtures("user_groups_by_campaigns")
    @pytest.mark.usefixtures("user_groups_by_campaign_scopes")
    def test_event_by_storey_authorizations_as_user(
        self, users, storeys, events, events_by_storeys
    ):
        user_1 = users[1]
        assert not user_1.is_admin

        storey_1 = storeys[0]
        storey_2 = storeys[1]
        event_1 = events[0]
        event_2 = events[1]
        ebs_1 = events_by_storeys[0]
        ebs_2 = events_by_storeys[1]

        with CurrentUser(user_1):
            ebs_l = list(EventByStorey.get())
            assert len(ebs_l) == 1
            assert ebs_l[0] == ebs_2
            EventByStorey.get_by_id(ebs_2.id)
            ebs_2.update(storey_id=storey_2.id)
            ebs_2.delete()
            ebs = EventByStorey.new(storey_id=storey_2.id, event_id=event_2.id)
            ebs.delete()
            with pytest.raises(BEMServerAuthorizationError):
                EventByStorey.get_by_id(ebs_1.id)
            with pytest.raises(BEMServerAuthorizationError):
                EventByStorey.new(storey_id=storey_1.id, event_id=event_1.id)
            with pytest.raises(BEMServerAuthorizationError):
                ebs_1.update(storey_id=storey_1.id)
            with pytest.raises(BEMServerAuthorizationError):
                ebs_1.delete()


class TestEventBySpaceModel:
    def test_event_by_space_authorizations_as_admin(self, users, spaces, events):
        admin_user = users[0]
        assert admin_user.is_admin

        space_1 = spaces[0]
        space_2 = spaces[1]
        event_1 = events[0]

        with CurrentUser(admin_user):
            with pytest.raises(BEMServerCoreCampaignError):
                EventBySpace.new(space_id=space_2.id, event_id=event_1.id)
                db.session.flush()
            db.session.rollback()
            ebs_1 = EventBySpace.new(space_id=space_1.id, event_id=event_1.id)
            db.session.add(ebs_1)
            db.session.flush()
            EventBySpace.get_by_id(ebs_1.id)
            ebss = list(EventBySpace.get())
            assert len(ebss) == 1
            ebs_1.update(space_id=space_1.id)
            db.session.flush()
            with pytest.raises(BEMServerCoreCampaignError):
                ebs_1.update(space_id=space_2.id)
                db.session.flush()
            db.session.rollback()
            ebs_1 = EventBySpace.new(space_id=space_1.id, event_id=event_1.id)
            db.session.flush()
            ebs_1.delete()
            db.session.flush()

    @pytest.mark.usefixtures("users_by_user_groups")
    @pytest.mark.usefixtures("user_groups_by_campaigns")
    @pytest.mark.usefixtures("user_groups_by_campaign_scopes")
    def test_event_by_space_authorizations_as_user(
        self, users, spaces, events, events_by_spaces
    ):
        user_1 = users[1]
        assert not user_1.is_admin

        space_1 = spaces[0]
        space_2 = spaces[1]
        event_1 = events[0]
        event_2 = events[1]
        ebs_1 = events_by_spaces[0]
        ebs_2 = events_by_spaces[1]

        with CurrentUser(user_1):
            ebs_l = list(EventBySpace.get())
            assert len(ebs_l) == 1
            assert ebs_l[0] == ebs_2
            EventBySpace.get_by_id(ebs_2.id)
            ebs_2.update(space_id=space_2.id)
            ebs_2.delete()
            ebs = EventBySpace.new(space_id=space_2.id, event_id=event_2.id)
            ebs.delete()
            with pytest.raises(BEMServerAuthorizationError):
                EventBySpace.get_by_id(ebs_1.id)
            with pytest.raises(BEMServerAuthorizationError):
                EventBySpace.new(space_id=space_1.id, event_id=event_1.id)
            with pytest.raises(BEMServerAuthorizationError):
                ebs_1.update(space_id=space_1.id)
            with pytest.raises(BEMServerAuthorizationError):
                ebs_1.delete()


class TestEventByZoneModel:
    def test_event_by_zone_authorizations_as_admin(self, users, zones, events):
        admin_user = users[0]
        assert admin_user.is_admin

        zone_1 = zones[0]
        zone_2 = zones[1]
        event_1 = events[0]

        with CurrentUser(admin_user):
            with pytest.raises(BEMServerCoreCampaignError):
                EventByZone.new(zone_id=zone_2.id, event_id=event_1.id)
                db.session.flush()
            db.session.rollback()
            ebz_1 = EventByZone.new(zone_id=zone_1.id, event_id=event_1.id)
            db.session.add(ebz_1)
            db.session.flush()
            EventByZone.get_by_id(ebz_1.id)
            ebzs = list(EventByZone.get())
            assert len(ebzs) == 1
            ebz_1.update(zone_id=zone_1.id)
            db.session.flush()
            with pytest.raises(BEMServerCoreCampaignError):
                ebz_1.update(zone_id=zone_2.id)
                db.session.flush()
            db.session.rollback()
            ebz_1 = EventByZone.new(zone_id=zone_1.id, event_id=event_1.id)
            db.session.flush()
            ebz_1.delete()
            db.session.flush()

    @pytest.mark.usefixtures("users_by_user_groups")
    @pytest.mark.usefixtures("user_groups_by_campaigns")
    @pytest.mark.usefixtures("user_groups_by_campaign_scopes")
    def test_event_by_zone_authorizations_as_user(
        self, users, zones, events, events_by_zones
    ):
        user_1 = users[1]
        assert not user_1.is_admin

        zone_1 = zones[0]
        zone_2 = zones[1]
        event_1 = events[0]
        event_2 = events[1]
        ebz_1 = events_by_zones[0]
        ebz_2 = events_by_zones[1]

        with CurrentUser(user_1):
            ebz_l = list(EventByZone.get())
            assert len(ebz_l) == 1
            assert ebz_l[0] == ebz_2
            EventByZone.get_by_id(ebz_2.id)
            ebz_2.update(zone_id=zone_2.id)
            ebz_2.delete()
            ebz = EventByZone.new(zone_id=zone_2.id, event_id=event_2.id)
            ebz.delete()
            with pytest.raises(BEMServerAuthorizationError):
                EventByZone.get_by_id(ebz_1.id)
            with pytest.raises(BEMServerAuthorizationError):
                EventByZone.new(zone_id=zone_1.id, event_id=event_1.id)
            with pytest.raises(BEMServerAuthorizationError):
                ebz_1.update(zone_id=zone_1.id)
            with pytest.raises(BEMServerAuthorizationError):
                ebz_1.delete()
