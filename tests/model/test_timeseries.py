"""Timeseries tests"""

import datetime as dt

import pytest

import sqlalchemy as sqla

from bemserver_core.authorization import CurrentUser, OpenBar
from bemserver_core.common import PropertyType
from bemserver_core.database import db
from bemserver_core.exceptions import (
    BEMServerAuthorizationError,
    BEMServerCoreUndefinedUnitError,
    PropertyTypeInvalidError,
    TimeseriesNotFoundError,
)
from bemserver_core.model import (
    EnergyConsumptionTimeseriesByBuilding,
    EnergyConsumptionTimeseriesBySite,
    Timeseries,
    TimeseriesByBuilding,
    TimeseriesByDataState,
    TimeseriesByEvent,
    TimeseriesBySite,
    TimeseriesBySpace,
    TimeseriesByStorey,
    TimeseriesByZone,
    TimeseriesData,
    TimeseriesDataState,
    TimeseriesProperty,
    TimeseriesPropertyData,
)

DUMMY_ID = 69
DUMMY_NAME = "Dummy name"


class TestTimeseriesPropertyModel:
    def test_timeseries_property_authorizations_as_admin(self, users):
        admin_user = users[0]
        assert admin_user.is_admin

        with CurrentUser(admin_user):
            nb_ts_properties = len(list(TimeseriesProperty.get()))
            ts_property_1 = TimeseriesProperty.new(name="Custom")
            db.session.commit()
            assert TimeseriesProperty.get_by_id(ts_property_1.id) == ts_property_1
            assert len(list(TimeseriesProperty.get())) == nb_ts_properties + 1
            ts_property_1.update(name="Super custom")
            ts_property_1.delete()
            db.session.commit()

    def test_timeseries_property_authorizations_as_user(self, users):
        user_1 = users[1]
        assert not user_1.is_admin

        with CurrentUser(user_1):
            ts_properties = list(TimeseriesProperty.get())
            ts_property_1 = TimeseriesProperty.get_by_id(ts_properties[0].id)
            with pytest.raises(BEMServerAuthorizationError):
                TimeseriesProperty.new(
                    name="Custom",
                )
            with pytest.raises(BEMServerAuthorizationError):
                ts_property_1.update(name="Super custom")
            with pytest.raises(BEMServerAuthorizationError):
                ts_property_1.delete()

    def test_timeseries_property_cannot_change_type(self, users):
        admin_user = users[0]
        assert admin_user.is_admin

        with CurrentUser(admin_user):
            ts_prop = TimeseriesProperty.new(
                name="New property",
                value_type=PropertyType.integer,
            )
            db.session.flush()
            ts_prop.value_type = PropertyType.boolean
            with pytest.raises(
                sqla.exc.IntegrityError,
                match="value_type cannot be modified",
            ):
                db.session.commit()
            db.session.rollback()


class TestTimeseriesDataStateModel:
    def test_timeseries_data_state_delete_cascade(
        self, users, timeseries_by_data_states
    ):
        admin_user = users[0]
        tsbds_1 = timeseries_by_data_states[0]

        with CurrentUser(admin_user):
            ts_data_state_1 = TimeseriesByDataState.get()[0]

            TimeseriesData.new(
                timestamp=dt.datetime(2020, 1, 1, tzinfo=dt.timezone.utc),
                timeseries_by_data_state_id=tsbds_1.id,
                value="12",
            )
            db.session.commit()

            assert len(list(TimeseriesByDataState.get())) == 2
            assert len(list(db.session.query(TimeseriesData))) == 1

            ts_data_state_1.delete()
            db.session.commit()
            assert len(list(TimeseriesByDataState.get())) == 1
            assert len(list(db.session.query(TimeseriesData))) == 0

    def test_timeseries_data_state_authorizations_as_admin(self, users):
        admin_user = users[0]
        assert admin_user.is_admin

        with CurrentUser(admin_user):
            nb_ts_data_states = len(list(TimeseriesDataState.get()))
            ts_data_state_1 = TimeseriesDataState.new(
                name="Quality",
            )
            db.session.commit()
            TimeseriesDataState.get_by_id(ts_data_state_1.id)
            ts_data_states = list(TimeseriesDataState.get())
            assert len(ts_data_states) == nb_ts_data_states + 1
            ts_data_state_1.update(name="Qualität")
            ts_data_state_1.delete()
            db.session.commit()

    def test_timeseries_data_state_authorizations_as_user(self, users):
        user_1 = users[1]
        assert not user_1.is_admin

        with CurrentUser(user_1):
            ts_data_states = list(TimeseriesDataState.get())
            ts_data_state_1 = TimeseriesDataState.get_by_id(ts_data_states[0].id)
            with pytest.raises(BEMServerAuthorizationError):
                TimeseriesDataState.new(
                    name="Quality",
                )
            with pytest.raises(BEMServerAuthorizationError):
                ts_data_state_1.update(name="Qualität")
            with pytest.raises(BEMServerAuthorizationError):
                ts_data_state_1.delete()


class TestTimeseriesModel:
    @pytest.mark.usefixtures("timeseries_by_sites")
    @pytest.mark.usefixtures("timeseries_by_buildings")
    @pytest.mark.usefixtures("timeseries_by_storeys")
    @pytest.mark.usefixtures("timeseries_by_spaces")
    @pytest.mark.usefixtures("timeseries_by_zones")
    @pytest.mark.usefixtures("timeseries_property_data")
    @pytest.mark.usefixtures("energy_consumption_timeseries_by_sites")
    @pytest.mark.usefixtures("energy_consumption_timeseries_by_buildings")
    @pytest.mark.usefixtures("timeseries_by_events")
    def test_timeseries_delete_cascade(
        self, users, timeseries, timeseries_by_data_states
    ):
        admin_user = users[0]
        ts_1 = timeseries[0]
        tsbds_1 = timeseries_by_data_states[0]

        with CurrentUser(admin_user):
            TimeseriesData.new(
                timestamp=dt.datetime(2020, 1, 1, tzinfo=dt.timezone.utc),
                timeseries_by_data_state_id=tsbds_1.id,
                value="12",
            )
            db.session.commit()

            assert len(list(TimeseriesBySite.get())) == 2
            assert len(list(TimeseriesByBuilding.get())) == 2
            assert len(list(TimeseriesByStorey.get())) == 2
            assert len(list(TimeseriesBySpace.get())) == 2
            assert len(list(TimeseriesByZone.get())) == 2
            assert len(list(TimeseriesPropertyData.get())) == 2
            assert len(list(TimeseriesByDataState.get())) == 2
            assert len(list(db.session.query(TimeseriesData))) == 1
            assert len(list(EnergyConsumptionTimeseriesBySite.get())) == 2
            assert len(list(EnergyConsumptionTimeseriesByBuilding.get())) == 2
            assert len(list(TimeseriesByEvent.get())) == 2

            ts_1.delete()
            db.session.commit()
            assert len(list(TimeseriesBySite.get())) == 1
            assert len(list(TimeseriesByBuilding.get())) == 1
            assert len(list(TimeseriesByStorey.get())) == 1
            assert len(list(TimeseriesBySpace.get())) == 1
            assert len(list(TimeseriesByZone.get())) == 1
            assert len(list(TimeseriesPropertyData.get())) == 1
            assert len(list(TimeseriesByDataState.get())) == 1
            assert len(list(db.session.query(TimeseriesData))) == 0
            assert len(list(EnergyConsumptionTimeseriesBySite.get())) == 1
            assert len(list(EnergyConsumptionTimeseriesByBuilding.get())) == 1
            assert len(list(TimeseriesByEvent.get())) == 1

    @pytest.mark.usefixtures("as_admin")
    def test_timeseries_get_timeseries_by_data_states(self, timeseries):
        """Check timeseries x data_states associations are created if needed"""
        ts_1 = timeseries[0]

        ts_data_state_1 = TimeseriesDataState.get()[0]

        tsbds_l = list(TimeseriesByDataState.get())
        assert not tsbds_l

        tsbds_1 = ts_1.get_timeseries_by_data_state(ts_data_state_1)
        tsbds_l = list(TimeseriesByDataState.get())
        assert len(tsbds_l) == 1

        assert ts_1.get_timeseries_by_data_state(ts_data_state_1) == tsbds_1
        tsbds_l = list(TimeseriesByDataState.get())
        assert len(tsbds_l) == 1

    @pytest.mark.usefixtures("as_admin")
    def test_timeseries_get_by_name(self, campaigns, timeseries):
        campaign_1 = campaigns[0]
        ts_1 = timeseries[0]
        ts_2 = timeseries[1]

        assert Timeseries.get_by_name(campaign_1, ts_1.name) == ts_1
        assert Timeseries.get_by_name(campaign_1, ts_2.name) is None

    @pytest.mark.usefixtures("as_admin")
    def test_timeseries_get_many_by_id(self, timeseries):
        ts_1 = timeseries[0]
        ts_2 = timeseries[1]

        assert set(Timeseries.get_many_by_id([ts_1.id, ts_2.id])) == {ts_1, ts_2}
        assert set(Timeseries.get_many_by_id([ts_1.id])) == {ts_1}

        with pytest.raises(TimeseriesNotFoundError):
            Timeseries.get_many_by_id([ts_1.id, ts_2.id, DUMMY_ID])

    @pytest.mark.usefixtures("as_admin")
    def test_timeseries_get_many_by_name(self, campaigns, timeseries):
        campaign_1 = campaigns[0]
        ts_1 = timeseries[0]
        ts_2 = timeseries[1]

        assert set(
            Timeseries.get_many_by_name(
                campaign_1,
                [
                    ts_1.name,
                ],
            )
        ) == {ts_1}

        with pytest.raises(TimeseriesNotFoundError):
            Timeseries.get_many_by_name(campaign_1, [ts_1.name, ts_2.name, DUMMY_NAME])

    def test_timeseries_get_property_value(
        self,
        users,
        timeseries,
        timeseries_properties,
    ):
        admin_user = users[0]
        assert admin_user.is_admin

        ts_1 = timeseries[0]
        ts_p_3 = timeseries_properties[2]
        ts_p_4 = timeseries_properties[3]
        ts_p_5 = timeseries_properties[4]
        ts_p_6 = timeseries_properties[5]

        with OpenBar():
            ts_p_d_3 = TimeseriesPropertyData.new(
                timeseries_id=ts_1.id,
                property_id=ts_p_3.id,
                value="12.0",
            )
            ts_p_d_4 = TimeseriesPropertyData.new(
                timeseries_id=ts_1.id,
                property_id=ts_p_4.id,
                value="42",
            )
            ts_p_d_5 = TimeseriesPropertyData.new(
                timeseries_id=ts_1.id,
                property_id=ts_p_5.id,
                value="true",
            )
            ts_p_d_6 = TimeseriesPropertyData.new(
                timeseries_id=ts_1.id,
                property_id=ts_p_6.id,
                value="test",
            )
            db.session.flush()

        with CurrentUser(admin_user):
            ret = ts_1.get_property_value(ts_p_3.name)
            assert type(ret) is ts_p_3.value_type.value
            assert ret == ts_p_3.value_type.value(ts_p_d_3.value)
            ret = ts_1.get_property_value(ts_p_4.name)
            assert type(ret) is ts_p_4.value_type.value
            assert ret == ts_p_4.value_type.value(ts_p_d_4.value)
            ret = ts_1.get_property_value(ts_p_5.name)
            assert type(ret) is ts_p_5.value_type.value
            assert ret == ts_p_5.value_type.value(ts_p_d_5.value)
            ret = ts_1.get_property_value(ts_p_6.name)
            assert type(ret) is ts_p_6.value_type.value
            assert ret == ts_p_6.value_type.value(ts_p_d_6.value)
            ret = ts_1.get_property_value(DUMMY_NAME)
            assert ret is None

    @pytest.mark.usefixtures("as_admin")
    def test_timeseries_get_property_for_many_timeseries(self, timeseries):
        ts_1 = timeseries[0]
        ts_2 = timeseries[1]
        timeseries_ids = [ts.id for ts in timeseries]

        with OpenBar():
            ts_p_1 = TimeseriesProperty.get(name="Min").first()
            ts_p_2 = TimeseriesProperty.get(name="Max").first()
            TimeseriesPropertyData.new(
                timeseries_id=ts_1.id,
                property_id=ts_p_1.id,
                value="12",
            )
            TimeseriesPropertyData.new(
                timeseries_id=ts_1.id,
                property_id=ts_p_2.id,
                value="42",
            )
            TimeseriesPropertyData.new(
                timeseries_id=ts_2.id,
                property_id=ts_p_2.id,
                value="69",
            )

        assert Timeseries.get_property_for_many_timeseries(timeseries_ids, "Min") == {
            1: "12",
            2: None,
        }
        assert Timeseries.get_property_for_many_timeseries(timeseries_ids, "Max") == {
            1: "42",
            2: "69",
        }

    @pytest.mark.usefixtures("as_admin")
    def test_timeseries_read_only_fields(self, campaigns, campaign_scopes):
        """Check campaign and campaign_scope can't be modified

        This is kind of a "framework test".
        """
        campaign_1 = campaigns[0]
        campaign_2 = campaigns[1]
        campaign_scope_1 = campaign_scopes[0]
        campaign_scope_2 = campaign_scopes[1]

        ts_1 = Timeseries.new(
            name="Timeseries 1",
            campaign_id=campaign_1.id,
            campaign_scope_id=campaign_scope_1.id,
        )
        db.session.commit()

        ts_1.update(campaign_id=campaign_2.id)
        with pytest.raises(
            sqla.exc.IntegrityError,
            match="campaign_id cannot be modified",
        ):
            db.session.commit()
        db.session.rollback()
        ts_1.update(campaign_scope_id=campaign_scope_2.id)
        with pytest.raises(
            sqla.exc.IntegrityError,
            match="campaign_scope_id cannot be modified",
        ):
            db.session.commit()
        db.session.rollback()

        ts_list = list(Timeseries.get(campaign_id=1))
        assert ts_list == [ts_1]
        ts_list = list(Timeseries.get(campaign_id=2))
        assert ts_list == []
        ts_list = list(Timeseries.get(campaign_scope_id=1))
        assert ts_list == [ts_1]
        ts_list = list(Timeseries.get(campaign_scope_id=2))
        assert ts_list == []

    @pytest.mark.usefixtures("as_admin")
    def test_timeseries_validate_unit(self, campaigns, campaign_scopes):
        """Check timeseries unit symbol is validated on flush"""
        campaign_1 = campaigns[0]
        campaign_scope_1 = campaign_scopes[0]

        # No unit: OK, defaults to ""
        ts_1 = Timeseries.new(
            name="Timeseries 1",
            campaign_id=campaign_1.id,
            campaign_scope_id=campaign_scope_1.id,
        )
        db.session.flush()
        assert ts_1.unit_symbol == ""

        # Undefined unit
        Timeseries.new(
            name="Timeseries 1",
            campaign_id=campaign_1.id,
            campaign_scope_id=campaign_scope_1.id,
            unit_symbol=DUMMY_NAME,
        )
        with pytest.raises(BEMServerCoreUndefinedUnitError):
            db.session.flush()

    @pytest.mark.usefixtures("users_by_user_groups")
    @pytest.mark.usefixtures("user_groups_by_campaigns")
    @pytest.mark.usefixtures("user_groups_by_campaign_scopes")
    @pytest.mark.usefixtures("timeseries_by_zones")
    @pytest.mark.usefixtures("timeseries_by_events")
    def test_timeseries_filters_as_admin(
        self,
        users,
        timeseries,
        campaigns,
        sites,
        buildings,
        storeys,
        spaces,
        zones,
        events,
        timeseries_by_sites,
        timeseries_by_buildings,
        timeseries_by_storeys,
        timeseries_by_spaces,
    ):
        admin_user = users[0]
        assert admin_user.is_admin
        user_1 = users[1]
        assert not user_1.is_admin
        campaign_1 = campaigns[0]
        campaign_2 = campaigns[1]
        ts_1 = timeseries[0]
        ts_2 = timeseries[1]
        site_1 = sites[0]
        building_1 = buildings[0]
        building_2 = buildings[1]
        storey_1 = storeys[0]
        space_1 = spaces[0]
        zone_1 = zones[0]
        event_1 = events[0]
        tbsi_1 = timeseries_by_sites[0]
        tbb_1 = timeseries_by_buildings[0]
        tbst_1 = timeseries_by_storeys[0]
        tbsp_1 = timeseries_by_spaces[0]

        with CurrentUser(admin_user):
            ts_l = list(Timeseries.get())
            assert len(ts_l) == 2

            ts_l = list(Timeseries.get(campaign_id=campaign_1.id))
            assert len(ts_l) == 1
            assert ts_l[0] == ts_1

            ts_l = list(Timeseries.get(user_id=user_1.id))
            assert len(ts_l) == 1
            assert ts_l[0] == ts_2

            ts_l = list(Timeseries.get(user_id=user_1.id, campaign_id=campaign_2.id))
            assert len(ts_l) == 1
            assert ts_l[0] == ts_2

            ts_l = list(Timeseries.get(site_id=site_1.id))
            assert len(ts_l) == 1
            assert ts_l[0] == ts_1

            ts_l = list(Timeseries.get(building_id=building_1.id))
            assert len(ts_l) == 1
            assert ts_l[0] == ts_1

            ts_l = list(Timeseries.get(storey_id=storey_1.id))
            assert len(ts_l) == 1
            assert ts_l[0] == ts_1

            ts_l = list(Timeseries.get(space_id=space_1.id))
            assert len(ts_l) == 1
            assert ts_l[0] == ts_1

            ts_l = list(Timeseries.get(zone_id=zone_1.id))
            assert len(ts_l) == 1
            assert ts_l[0] == ts_1

            ts_l = list(Timeseries.get(event_id=event_1.id))
            assert len(ts_l) == 1
            assert ts_l[0] == ts_1

            # Can't filter by both site and recurse site
            with pytest.raises(ValueError):
                Timeseries.get(site_id=site_1.id, recurse_site_id=site_1.id)
            with pytest.raises(ValueError):
                Timeseries.get(
                    building_id=building_1.id, recurse_building_id=building_1.id
                )
            with pytest.raises(ValueError):
                Timeseries.get(storey_id=storey_1.id, recurse_storey_id=storey_1.id)

            # Can filter by site and building (even though it doesn't make much sense)
            assert not list(
                Timeseries.get(site_id=site_1.id, building_id=building_2.id)
            )

            ts_l = list(Timeseries.get(site_id=site_1.id, event_id=event_1.id))
            assert len(ts_l) == 1
            assert ts_l[0] == ts_1

            db.session.delete(tbst_1)
            db.session.commit()

            ts_l = list(Timeseries.get(storey_id=storey_1.id))
            assert not len(ts_l)
            ts_l = list(Timeseries.get(recurse_storey_id=storey_1.id))
            assert len(ts_l) == 1
            assert ts_l[0] == ts_1

            db.session.delete(tbb_1)
            db.session.commit()

            ts_l = list(Timeseries.get(building_id=building_1.id))
            assert not len(ts_l)
            ts_l = list(Timeseries.get(recurse_building_id=building_1.id))
            assert len(ts_l) == 1
            assert ts_l[0] == ts_1

            db.session.delete(tbsi_1)
            db.session.commit()

            ts_l = list(Timeseries.get(site_id=site_1.id))
            assert not list(ts_l)
            ts_l = list(Timeseries.get(recurse_site_id=site_1.id))
            assert len(ts_l) == 1
            assert ts_l[0] == ts_1

            db.session.delete(tbsp_1)
            db.session.commit()

            assert not list(Timeseries.get(space_id=space_1.id))
            assert not list(Timeseries.get(storey_id=storey_1.id))
            assert not list(Timeseries.get(building_id=building_1.id))
            assert not list(Timeseries.get(site_id=site_1.id))

    @pytest.mark.parametrize("timeseries", (3,), indirect=True)
    def test_timeseries_filter_by_property_as_admin(
        self,
        users,
        timeseries,
        timeseries_properties,
        timeseries_property_data,
    ):
        admin_user = users[0]
        assert admin_user.is_admin
        user_1 = users[1]
        assert not user_1.is_admin
        ts_1 = timeseries[0]
        ts_3 = timeseries[2]
        ts_prop_1 = timeseries_properties[0]
        ts_prop_2 = timeseries_properties[1]
        ts_prop_val_1 = timeseries_property_data[0]
        ts_prop_val_2 = timeseries_property_data[1]

        with CurrentUser(admin_user):
            TimeseriesPropertyData.new(
                timeseries_id=ts_3.id,
                property_id=ts_prop_2.id,
                value=ts_prop_val_2.value,
            )

            ts_l = list(Timeseries.get())
            assert len(ts_l) == 3

            ts_l = list(
                Timeseries.get(properties={ts_prop_1.name: ts_prop_val_1.value})
            )
            assert len(ts_l) == 2
            assert set(ts_l) == {ts_1, ts_3}

            ts_l = list(
                Timeseries.get(
                    properties={
                        ts_prop_1.name: ts_prop_val_1.value,
                        ts_prop_2.name: ts_prop_val_2.value,
                    },
                )
            )
            assert ts_l == [ts_3]

            ts_l = list(Timeseries.get(properties={DUMMY_NAME: ts_prop_val_1.value}))
            assert not ts_l

            ts_l = list(Timeseries.get(properties={ts_prop_1.name: DUMMY_NAME}))
            assert not ts_l

    @pytest.mark.usefixtures("users_by_user_groups")
    @pytest.mark.usefixtures("user_groups_by_campaigns")
    @pytest.mark.usefixtures("user_groups_by_campaign_scopes")
    @pytest.mark.usefixtures("timeseries_by_zones")
    @pytest.mark.usefixtures("timeseries_by_events")
    def test_timeseries_filters_as_user(
        self,
        users,
        timeseries,
        campaigns,
        campaign_scopes,
        sites,
        buildings,
        storeys,
        spaces,
        zones,
        events,
        timeseries_by_sites,
        timeseries_by_buildings,
        timeseries_by_storeys,
        timeseries_by_spaces,
    ):
        admin_user = users[0]
        assert admin_user.is_admin
        user_1 = users[1]
        assert not user_1.is_admin
        campaign_1 = campaigns[0]
        campaign_2 = campaigns[1]
        cs_1 = campaign_scopes[0]
        cs_2 = campaign_scopes[1]
        ts_2 = timeseries[1]
        site_1 = sites[0]
        site_2 = sites[1]
        building_1 = buildings[0]
        building_2 = buildings[1]
        storey_1 = storeys[0]
        storey_2 = storeys[1]
        space_1 = spaces[0]
        space_2 = spaces[1]
        zone_1 = zones[0]
        zone_2 = zones[1]
        zone_1 = zones[0]
        zone_2 = zones[1]
        event_1 = events[0]
        event_2 = events[1]
        tbsi_2 = timeseries_by_sites[1]
        tbb_2 = timeseries_by_buildings[1]
        tbst_2 = timeseries_by_storeys[1]
        tbsp_2 = timeseries_by_spaces[1]

        with CurrentUser(user_1):
            with pytest.raises(BEMServerAuthorizationError):
                list(Timeseries.get(campaign_id=campaign_1.id))
            ts_l = list(Timeseries.get(campaign_id=campaign_2.id))
            assert len(ts_l) == 1
            assert ts_l[0] == ts_2

            with pytest.raises(BEMServerAuthorizationError):
                list(Timeseries.get(campaign_scope_id=cs_1.id))
            ts_l = list(Timeseries.get(campaign_id=cs_2.id))
            assert len(ts_l) == 1
            assert ts_l[0] == ts_2

            with pytest.raises(BEMServerAuthorizationError):
                list(Timeseries.get(user_id=admin_user.id))
            ts_l = list(Timeseries.get(user_id=user_1.id))
            assert len(ts_l) == 1
            assert ts_l[0] == ts_2

            with pytest.raises(BEMServerAuthorizationError):
                ts_l = list(Timeseries.get(site_id=site_1.id))
            ts_l = list(Timeseries.get(site_id=site_2.id))
            assert len(ts_l) == 1
            assert ts_l[0] == ts_2

            with pytest.raises(BEMServerAuthorizationError):
                ts_l = list(Timeseries.get(building_id=building_1.id))
            ts_l = list(Timeseries.get(building_id=building_2.id))
            assert len(ts_l) == 1
            assert ts_l[0] == ts_2

            with pytest.raises(BEMServerAuthorizationError):
                ts_l = list(Timeseries.get(storey_id=storey_1.id))
            ts_l = list(Timeseries.get(storey_id=storey_2.id))
            assert len(ts_l) == 1
            assert ts_l[0] == ts_2

            with pytest.raises(BEMServerAuthorizationError):
                ts_l = list(Timeseries.get(space_id=space_1.id))
            ts_l = list(Timeseries.get(space_id=space_2.id))
            assert len(ts_l) == 1
            assert ts_l[0] == ts_2

            with pytest.raises(BEMServerAuthorizationError):
                ts_l = list(Timeseries.get(zone_id=zone_1.id))
            ts_l = list(Timeseries.get(zone_id=zone_2.id))
            assert len(ts_l) == 1
            assert ts_l[0] == ts_2

            with pytest.raises(BEMServerAuthorizationError):
                ts_l = list(Timeseries.get(event_id=event_1.id))
            ts_l = list(Timeseries.get(event_id=event_2.id))
            assert len(ts_l) == 1
            assert ts_l[0] == ts_2

            db.session.delete(tbst_2)
            db.session.commit()

            ts_l = list(Timeseries.get(storey_id=storey_2.id))
            assert not len(ts_l)
            ts_l = list(Timeseries.get(recurse_storey_id=storey_2.id))
            assert len(ts_l) == 1
            assert ts_l[0] == ts_2

            db.session.delete(tbb_2)
            db.session.commit()

            ts_l = list(Timeseries.get(building_id=building_2.id))
            assert not len(ts_l)
            ts_l = list(Timeseries.get(recurse_building_id=building_2.id))
            assert len(ts_l) == 1
            assert ts_l[0] == ts_2

            db.session.delete(tbsi_2)
            db.session.commit()

            ts_l = list(Timeseries.get(site_id=site_2.id))
            assert not list(ts_l)
            ts_l = list(Timeseries.get(recurse_site_id=site_2.id))
            assert len(ts_l) == 1
            assert ts_l[0] == ts_2

            db.session.delete(tbsp_2)
            db.session.commit()

            assert not list(Timeseries.get(space_id=space_2.id))
            assert not list(Timeseries.get(storey_id=storey_2.id))
            assert not list(Timeseries.get(building_id=building_2.id))
            assert not list(Timeseries.get(site_id=site_2.id))

    def test_timeseries_authorizations_as_admin(
        self, users, campaigns, campaign_scopes
    ):
        admin_user = users[0]
        assert admin_user.is_admin
        campaign_1 = campaigns[0]
        cs_1 = campaign_scopes[0]

        with CurrentUser(admin_user):
            ts_1 = Timeseries.new(
                name="Timeseries 1",
                campaign_id=campaign_1.id,
                campaign_scope_id=cs_1.id,
            )
            db.session.commit()

            ts = Timeseries.get_by_id(ts_1.id)
            assert ts.id == ts_1.id
            assert ts.name == ts_1.name
            ts_l = list(Timeseries.get())
            assert len(ts_l) == 1
            assert ts_l[0].id == ts_1.id
            ts.update(name="Super timeseries 1")
            ts.delete()
            db.session.commit()

    @pytest.mark.usefixtures("users_by_user_groups")
    @pytest.mark.usefixtures("user_groups_by_campaign_scopes")
    def test_timeseries_authorizations_as_user(
        self,
        users,
        timeseries,
        campaigns,
        campaign_scopes,
    ):
        user_1 = users[1]
        assert not user_1.is_admin
        ts_1 = timeseries[0]
        ts_2 = timeseries[1]
        campaign_1 = campaigns[0]
        campaign_scope_1 = campaign_scopes[0]

        with CurrentUser(user_1):
            with pytest.raises(BEMServerAuthorizationError):
                Timeseries.new(
                    name="Timeseries 1",
                    campaign_id=campaign_1.id,
                    campaign_scope_id=campaign_scope_1.id,
                )

            timeseries = Timeseries.get_by_id(ts_2.id)
            timeseries_list = list(Timeseries.get())
            assert len(timeseries_list) == 1
            assert timeseries_list[0].id == ts_2.id
            with pytest.raises(BEMServerAuthorizationError):
                Timeseries.get_by_id(ts_1.id)
            with pytest.raises(BEMServerAuthorizationError):
                timeseries.update(name="Super timeseries 1")
            with pytest.raises(BEMServerAuthorizationError):
                timeseries.delete()


class TestTimeseriesPropertyDataModel:
    def test_timeseries_property_data_authorizations_as_admin(
        self, users, timeseries, timeseries_properties
    ):
        admin_user = users[0]
        assert admin_user.is_admin
        ts_1 = timeseries[0]
        tsp_1 = timeseries_properties[0]

        with CurrentUser(admin_user):
            assert not list(TimeseriesPropertyData.get())
            tspd_1 = TimeseriesPropertyData.new(
                timeseries_id=ts_1.id,
                property_id=tsp_1.id,
                value="12",
            )
            db.session.commit()

            tspd = TimeseriesPropertyData.get_by_id(tspd_1.id)
            assert tspd.id == tspd_1.id
            tspd_l = list(TimeseriesPropertyData.get())
            assert len(tspd_l) == 1
            assert tspd_l[0].id == tspd.id
            tspd.update(value="42")
            tspd.delete()
            db.session.commit()

    @pytest.mark.usefixtures("users_by_user_groups")
    @pytest.mark.usefixtures("user_groups_by_campaign_scopes")
    def test_timeseries_property_data_authorizations_as_user(
        self,
        users,
        timeseries_properties,
        timeseries,
        timeseries_property_data,
    ):
        user_1 = users[1]
        assert not user_1.is_admin
        tsp_1 = timeseries_properties[0]
        ts_1 = timeseries[0]
        ts_2 = timeseries[1]
        tspd_1 = timeseries_property_data[0]

        with CurrentUser(user_1):
            assert not list(TimeseriesPropertyData.get(timeseries_id=ts_1.id))
            with pytest.raises(BEMServerAuthorizationError):
                TimeseriesPropertyData.new(
                    timeseries_id=ts_2.id,
                    property_id=tsp_1.id,
                    value="12",
                )

            tspd_l = list(TimeseriesPropertyData.get(timeseries_id=ts_2.id))
            assert len(tspd_l) == 1
            tspd_2 = tspd_l[0]
            tspd = TimeseriesPropertyData.get_by_id(tspd_2.id)
            assert tspd.id == tspd_2.id
            with pytest.raises(BEMServerAuthorizationError):
                TimeseriesPropertyData.get_by_id(tspd_1.id)
            with pytest.raises(BEMServerAuthorizationError):
                tspd_2.update(data_state_id=2)
            with pytest.raises(BEMServerAuthorizationError):
                tspd_2.delete()

    def test_timeseries_property_data_type_validation_as_admin(
        self, users, timeseries, timeseries_properties
    ):
        admin_user = users[0]
        assert admin_user.is_admin

        ts_1 = timeseries[0]
        tsp_1 = timeseries_properties[0]
        tsp_4 = timeseries_properties[3]
        tsp_5 = timeseries_properties[4]
        tsp_6 = timeseries_properties[5]

        with CurrentUser(admin_user):
            # Property value is expected to be a float.
            assert tsp_1.value_type is PropertyType.float
            tspd_1 = TimeseriesPropertyData.new(
                timeseries_id=ts_1.id,
                property_id=tsp_1.id,
                value="4.2",
            )
            db.session.commit()
            assert tspd_1.value == "4.2"
            for val, exp_res in [("66.6", "66.6"), (42, "42")]:
                tspd_1.value = val
                db.session.commit()
                assert tspd_1.value == exp_res
            # Invalid property value types.
            for val in ["bad", None]:
                tspd_1.value = val
                with pytest.raises(PropertyTypeInvalidError):
                    db.session.commit()
                assert tspd_1.value == val
                db.session.rollback()

            # Property value is expected to be an integer.
            assert tsp_4.value_type is PropertyType.integer
            tspd_4 = TimeseriesPropertyData.new(
                timeseries_id=ts_1.id,
                property_id=tsp_4.id,
                value="42",
            )
            db.session.commit()
            assert tspd_4.value == "42"
            tspd_4.value = "666"
            db.session.commit()
            assert tspd_4.value == "666"
            # Invalid property value types.
            for val in ["bad", "4.2", 4.2, None]:
                tspd_4.value = val
                with pytest.raises(PropertyTypeInvalidError):
                    db.session.commit()
                assert tspd_4.value == val
                db.session.rollback()

            # Property value is expected to be a boolean.
            assert tsp_5.value_type is PropertyType.boolean
            tspd_5 = TimeseriesPropertyData.new(
                timeseries_id=ts_1.id,
                property_id=tsp_5.id,
                value="true",
            )
            db.session.commit()
            assert tspd_5.value == "true"
            tspd_5.value = "false"
            db.session.commit()
            assert tspd_5.value == "false"
            # Invalid property value types.
            for val in [True, False, 1, 0, "1", "0", "bad", 42, None]:
                tspd_5.value = val
                with pytest.raises(PropertyTypeInvalidError):
                    db.session.commit()
                assert tspd_5.value == val
                db.session.rollback()

            # Property value is expected to be a string.
            assert tsp_6.value_type is PropertyType.string
            tspd_6 = TimeseriesPropertyData.new(
                timeseries_id=ts_1.id,
                property_id=tsp_6.id,
                value="12",
            )
            db.session.commit()
            assert tspd_6.value == "12"
            for val, exp_res in [
                ("everything works", "everything works"),
                (True, "true"),
            ]:
                tspd_6.value = val
                db.session.commit()
                assert tspd_6.value == exp_res

    def test_timeseries_property_data_cannot_change_timeseries_or_property(
        self, users, timeseries, timeseries_properties
    ):
        admin_user = users[0]
        assert admin_user.is_admin

        ts_1 = timeseries[0]
        ts_2 = timeseries[1]
        ts_p_1 = timeseries_properties[0]
        ts_p_2 = timeseries_properties[1]

        with CurrentUser(admin_user):
            tspd = TimeseriesPropertyData.new(
                timeseries_id=ts_1.id,
                property_id=ts_p_1.id,
                value="12",
            )
            db.session.commit()
            tspd.timeseries_id = ts_2.id
            with pytest.raises(
                sqla.exc.IntegrityError,
                match="timeseries_id cannot be modified",
            ):
                db.session.commit()
            db.session.rollback()
            tspd.property_id = ts_p_2.id
            with pytest.raises(
                sqla.exc.IntegrityError,
                match="property_id cannot be modified",
            ):
                db.session.commit()
            db.session.rollback()


class TestTimeseriesByDataStateModel:
    def test_timeseries_by_data_state_delete_cascade(
        self, users, timeseries_by_data_states
    ):
        admin_user = users[0]
        tsbds_1 = timeseries_by_data_states[0]

        with CurrentUser(admin_user):
            TimeseriesData.new(
                timestamp=dt.datetime(2020, 1, 1, tzinfo=dt.timezone.utc),
                timeseries_by_data_state_id=tsbds_1.id,
                value="12",
            )
            db.session.commit()

            assert len(list(db.session.query(TimeseriesData))) == 1

            tsbds_1.delete()
            db.session.commit()
            assert len(list(db.session.query(TimeseriesData))) == 0

    def test_timeseries_by_data_state_authorizations_as_admin(self, users, timeseries):
        admin_user = users[0]
        assert admin_user.is_admin
        ts_1 = timeseries[0]

        with CurrentUser(admin_user):
            tsbds_1 = TimeseriesByDataState.new(
                timeseries_id=ts_1.id,
                data_state_id=1,
            )
            db.session.commit()

            tsbds = TimeseriesByDataState.get_by_id(tsbds_1.id)
            assert tsbds.id == tsbds_1.id
            assert tsbds.data_state_id == 1
            tsbds_l = list(TimeseriesByDataState.get())
            assert len(tsbds_l) == 1
            assert tsbds_l[0].id == tsbds.id
            tsbds.update(data_state_id=2)
            db.session.commit()
            tsbds.delete()
            db.session.commit()

    @pytest.mark.usefixtures("timeseries_by_data_states")
    @pytest.mark.usefixtures("users_by_user_groups")
    @pytest.mark.usefixtures("user_groups_by_campaign_scopes")
    def test_timeseries_by_data_state_authorizations_as_user(
        self,
        users,
        timeseries,
    ):
        user_1 = users[1]
        assert not user_1.is_admin
        ts_1 = timeseries[0]
        ts_2 = timeseries[1]
        ts_1 = timeseries[0]
        ts_2 = timeseries[1]

        with CurrentUser(user_1):
            timeseries_list = list(TimeseriesByDataState.get())
            assert len(timeseries_list) == 1
            assert timeseries_list[0].id == ts_2.id
            tsbds = TimeseriesByDataState.get_by_id(ts_2.id)
            tsbds.update(data_state_id=2)
            db.session.commit()
            tsbds.delete()
            db.session.commit()
            TimeseriesByDataState.new(
                timeseries_id=ts_2.id,
                data_state_id=2,
            )
            db.session.commit()

            # Not member of campaign
            with pytest.raises(BEMServerAuthorizationError):
                TimeseriesByDataState.new(
                    timeseries_id=ts_1.id,
                    data_state_id=1,
                )
            with pytest.raises(BEMServerAuthorizationError):
                TimeseriesByDataState.get_by_id(ts_1.id)


class TestTimeseriesBySiteModel:
    def test_timeseries_by_site_authorizations_as_admin(self, users, timeseries, sites):
        admin_user = users[0]
        assert admin_user.is_admin

        ts_1 = timeseries[0]
        ts_2 = timeseries[1]
        site_1 = sites[0]

        with CurrentUser(admin_user):
            tbs_1 = TimeseriesBySite.new(timeseries_id=ts_1.id, site_id=site_1.id)
            db.session.commit()
            TimeseriesBySite.get_by_id(tbs_1.id)
            tbss = list(TimeseriesBySite.get())
            assert len(tbss) == 1
            tbs_1.update(timeseries_id=ts_2.id)
            tbs_1.delete()
            db.session.commit()

    @pytest.mark.usefixtures("users_by_user_groups")
    @pytest.mark.usefixtures("user_groups_by_campaigns")
    @pytest.mark.usefixtures("user_groups_by_campaign_scopes")
    @pytest.mark.parametrize("timeseries", (4,), indirect=True)
    def test_timeseries_by_site_authorizations_as_user(
        self, users, timeseries, sites, timeseries_by_sites
    ):
        user_1 = users[1]
        assert not user_1.is_admin

        ts_2 = timeseries[1]
        ts_4 = timeseries[3]
        site_2 = sites[1]
        tbs_1 = timeseries_by_sites[0]
        tbs_2 = timeseries_by_sites[1]

        with CurrentUser(user_1):
            tbs_l = list(TimeseriesBySite.get())
            assert len(tbs_l) == 1
            assert tbs_l[0] == tbs_2
            TimeseriesBySite.get_by_id(tbs_2.id)
            with pytest.raises(BEMServerAuthorizationError):
                TimeseriesBySite.get_by_id(tbs_1.id)
            with pytest.raises(BEMServerAuthorizationError):
                TimeseriesBySite.new(timeseries_id=ts_2.id, site_id=site_2.id)
            with pytest.raises(BEMServerAuthorizationError):
                tbs_2.update(timeseries_id=ts_4.id)
            with pytest.raises(BEMServerAuthorizationError):
                tbs_2.delete()


class TestTimeseriesByBuildingModel:
    def test_timeseries_by_building_authorizations_as_admin(
        self, users, timeseries, buildings
    ):
        admin_user = users[0]
        assert admin_user.is_admin

        ts_1 = timeseries[0]
        ts_2 = timeseries[1]
        building_1 = buildings[0]

        with CurrentUser(admin_user):
            tbb_1 = TimeseriesByBuilding.new(
                timeseries_id=ts_1.id, building_id=building_1.id
            )
            db.session.commit()
            TimeseriesByBuilding.get_by_id(tbb_1.id)
            tbbs = list(TimeseriesByBuilding.get())
            assert len(tbbs) == 1
            tbb_1.update(timeseries_id=ts_2.id)
            tbb_1.delete()
            db.session.commit()

    @pytest.mark.usefixtures("users_by_user_groups")
    @pytest.mark.usefixtures("user_groups_by_campaigns")
    @pytest.mark.usefixtures("user_groups_by_campaign_scopes")
    @pytest.mark.parametrize("timeseries", (4,), indirect=True)
    def test_timeseries_by_building_authorizations_as_user(
        self, users, timeseries, buildings, timeseries_by_buildings
    ):
        user_1 = users[1]
        assert not user_1.is_admin

        ts_2 = timeseries[1]
        ts_4 = timeseries[3]
        building_2 = buildings[1]
        tbb_1 = timeseries_by_buildings[0]
        tbb_2 = timeseries_by_buildings[1]

        with CurrentUser(user_1):
            tbb_l = list(TimeseriesByBuilding.get())
            assert len(tbb_l) == 1
            assert tbb_l[0] == tbb_2
            TimeseriesByBuilding.get_by_id(tbb_2.id)
            with pytest.raises(BEMServerAuthorizationError):
                TimeseriesByBuilding.get_by_id(tbb_1.id)
            with pytest.raises(BEMServerAuthorizationError):
                TimeseriesByBuilding.new(
                    timeseries_id=ts_2.id, building_id=building_2.id
                )
            with pytest.raises(BEMServerAuthorizationError):
                tbb_2.update(timeseries_id=ts_4.id)
            with pytest.raises(BEMServerAuthorizationError):
                tbb_2.delete()


class TestTimeseriesByStoreyModel:
    def test_timeseries_by_storey_authorizations_as_admin(
        self, users, timeseries, storeys
    ):
        admin_user = users[0]
        assert admin_user.is_admin

        ts_1 = timeseries[0]
        ts_2 = timeseries[1]
        storey_1 = storeys[0]

        with CurrentUser(admin_user):
            tbs_1 = TimeseriesByStorey.new(timeseries_id=ts_1.id, storey_id=storey_1.id)
            db.session.commit()
            TimeseriesByStorey.get_by_id(tbs_1.id)
            tbss = list(TimeseriesByStorey.get())
            assert len(tbss) == 1
            tbs_1.update(timeseries_id=ts_2.id)
            tbs_1.delete()
            db.session.commit()

    @pytest.mark.usefixtures("users_by_user_groups")
    @pytest.mark.usefixtures("user_groups_by_campaigns")
    @pytest.mark.usefixtures("user_groups_by_campaign_scopes")
    @pytest.mark.parametrize("timeseries", (4,), indirect=True)
    def test_timeseries_by_storey_authorizations_as_user(
        self, users, timeseries, storeys, timeseries_by_storeys
    ):
        user_1 = users[1]
        assert not user_1.is_admin

        ts_2 = timeseries[1]
        ts_4 = timeseries[3]
        storey_2 = storeys[1]
        tbs_1 = timeseries_by_storeys[0]
        tbs_2 = timeseries_by_storeys[1]

        with CurrentUser(user_1):
            tbs_l = list(TimeseriesByStorey.get())
            assert len(tbs_l) == 1
            assert tbs_l[0] == tbs_2
            TimeseriesByStorey.get_by_id(tbs_2.id)
            with pytest.raises(BEMServerAuthorizationError):
                TimeseriesByStorey.get_by_id(tbs_1.id)
            with pytest.raises(BEMServerAuthorizationError):
                TimeseriesByStorey.new(timeseries_id=ts_2.id, storey_id=storey_2.id)
            with pytest.raises(BEMServerAuthorizationError):
                tbs_2.update(timeseries_id=ts_4.id)
            with pytest.raises(BEMServerAuthorizationError):
                tbs_2.delete()


class TestTimeseriesBySpaceModel:
    def test_timeseries_by_space_authorizations_as_admin(
        self, users, timeseries, spaces
    ):
        admin_user = users[0]
        assert admin_user.is_admin

        ts_1 = timeseries[0]
        ts_2 = timeseries[1]
        space_1 = spaces[0]

        with CurrentUser(admin_user):
            tbs_1 = TimeseriesBySpace.new(timeseries_id=ts_1.id, space_id=space_1.id)
            db.session.commit()
            TimeseriesBySpace.get_by_id(tbs_1.id)
            tbss = list(TimeseriesBySpace.get())
            assert len(tbss) == 1
            tbs_1.update(timeseries_id=ts_2.id)
            tbs_1.delete()
            db.session.commit()

    @pytest.mark.usefixtures("users_by_user_groups")
    @pytest.mark.usefixtures("user_groups_by_campaigns")
    @pytest.mark.usefixtures("user_groups_by_campaign_scopes")
    @pytest.mark.parametrize("timeseries", (4,), indirect=True)
    def test_timeseries_by_space_authorizations_as_user(
        self, users, timeseries, spaces, timeseries_by_spaces
    ):
        user_1 = users[1]
        assert not user_1.is_admin

        ts_2 = timeseries[1]
        ts_4 = timeseries[3]
        space_2 = spaces[1]
        tbs_1 = timeseries_by_spaces[0]
        tbs_2 = timeseries_by_spaces[1]

        with CurrentUser(user_1):
            tbs_l = list(TimeseriesBySpace.get())
            assert len(tbs_l) == 1
            assert tbs_l[0] == tbs_2
            TimeseriesBySpace.get_by_id(tbs_2.id)
            with pytest.raises(BEMServerAuthorizationError):
                TimeseriesBySpace.get_by_id(tbs_1.id)
            with pytest.raises(BEMServerAuthorizationError):
                TimeseriesBySpace.new(timeseries_id=ts_2.id, space_id=space_2.id)
            with pytest.raises(BEMServerAuthorizationError):
                tbs_2.update(timeseries_id=ts_4.id)
            with pytest.raises(BEMServerAuthorizationError):
                tbs_2.delete()


class TestTimeseriesByZoneModel:
    def test_timeseries_by_zone_authorizations_as_admin(self, users, timeseries, zones):
        admin_user = users[0]
        assert admin_user.is_admin

        ts_1 = timeseries[0]
        ts_2 = timeseries[1]
        zone_1 = zones[0]

        with CurrentUser(admin_user):
            tbz_1 = TimeseriesByZone.new(timeseries_id=ts_1.id, zone_id=zone_1.id)
            db.session.commit()
            TimeseriesByZone.get_by_id(tbz_1.id)
            tbzs = list(TimeseriesByZone.get())
            assert len(tbzs) == 1
            tbz_1.update(timeseries_id=ts_2.id)
            tbz_1.delete()
            db.session.commit()

    @pytest.mark.usefixtures("users_by_user_groups")
    @pytest.mark.usefixtures("user_groups_by_campaigns")
    @pytest.mark.usefixtures("user_groups_by_campaign_scopes")
    @pytest.mark.parametrize("timeseries", (4,), indirect=True)
    def test_timeseries_by_zone_authorizations_as_user(
        self, users, timeseries, zones, timeseries_by_zones
    ):
        user_1 = users[1]
        assert not user_1.is_admin

        ts_2 = timeseries[1]
        ts_4 = timeseries[3]
        zone_2 = zones[1]
        tbz_1 = timeseries_by_zones[0]
        tbz_2 = timeseries_by_zones[1]

        with CurrentUser(user_1):
            tbz_l = list(TimeseriesByZone.get())
            assert len(tbz_l) == 1
            assert tbz_l[0] == tbz_2
            TimeseriesByZone.get_by_id(tbz_2.id)
            with pytest.raises(BEMServerAuthorizationError):
                TimeseriesByZone.get_by_id(tbz_1.id)
            with pytest.raises(BEMServerAuthorizationError):
                TimeseriesByZone.new(timeseries_id=ts_2.id, zone_id=zone_2.id)
            with pytest.raises(BEMServerAuthorizationError):
                tbz_2.update(timeseries_id=ts_4.id)
            with pytest.raises(BEMServerAuthorizationError):
                tbz_2.delete()
