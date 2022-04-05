"""Timeseries tests"""
import datetime as dt

import pytest

from bemserver_core.model import (
    TimeseriesProperty,
    TimeseriesDataState,
    Timeseries,
    TimeseriesData,
    TimeseriesPropertyData,
    TimeseriesByDataState,
    TimeseriesBySite,
    TimeseriesByBuilding,
    TimeseriesByStorey,
    TimeseriesBySpace,
    TimeseriesByZone,
)
from bemserver_core.database import db
from bemserver_core.authorization import CurrentUser
from bemserver_core.exceptions import BEMServerAuthorizationError


class TestTimeseriesPropertyModel:
    def test_timeseries_property_authorizations_as_admin(self, users):
        admin_user = users[0]
        assert admin_user.is_admin

        with CurrentUser(admin_user):
            assert not list(TimeseriesProperty.get())
            ts_property_1 = TimeseriesProperty.new(name="Min")
            db.session.add(ts_property_1)
            db.session.commit()
            assert TimeseriesProperty.get_by_id(ts_property_1.id) == ts_property_1
            assert len(list(TimeseriesProperty.get())) == 1
            ts_property_1.update(name="Max")
            ts_property_1.delete()
            db.session.commit()

    @pytest.mark.usefixtures("timeseries_properties")
    def test_timeseries_property_authorizations_as_user(self, users):
        user_1 = users[1]
        assert not user_1.is_admin

        with CurrentUser(user_1):
            ts_properties = list(TimeseriesProperty.get())
            ts_property_1 = TimeseriesProperty.get_by_id(ts_properties[0].id)
            with pytest.raises(BEMServerAuthorizationError):
                TimeseriesProperty.new(
                    name="Frequency",
                )
            with pytest.raises(BEMServerAuthorizationError):
                ts_property_1.update(name="Mean")
            with pytest.raises(BEMServerAuthorizationError):
                ts_property_1.delete()


class TestTimeseriesDataStateModel:
    def test_timeseries_data_state_delete_cascade(
        self, users, timeseries_by_data_states
    ):
        admin_user = users[0]
        tsbds_1 = timeseries_by_data_states[0]

        with CurrentUser(admin_user):

            ts_data_state_1 = TimeseriesByDataState.get()[0]

            tsd = TimeseriesData(
                timestamp=dt.datetime(2020, 1, 1, tzinfo=dt.timezone.utc),
                timeseries_by_data_state_id=tsbds_1.id,
                value=12,
            )
            db.session.add(tsd)
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
            db.session.add(ts_data_state_1)
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
    def test_timeseries_delete_cascade(
        self, users, timeseries, timeseries_by_data_states
    ):
        admin_user = users[0]
        ts_1 = timeseries[0]
        tsbds_1 = timeseries_by_data_states[0]

        with CurrentUser(admin_user):
            tsd = TimeseriesData(
                timestamp=dt.datetime(2020, 1, 1, tzinfo=dt.timezone.utc),
                timeseries_by_data_state_id=tsbds_1.id,
                value=12,
            )
            db.session.add(tsd)
            db.session.commit()

            assert len(list(TimeseriesBySite.get())) == 2
            assert len(list(TimeseriesByBuilding.get())) == 2
            assert len(list(TimeseriesByStorey.get())) == 2
            assert len(list(TimeseriesBySpace.get())) == 2
            assert len(list(TimeseriesByZone.get())) == 2
            assert len(list(TimeseriesPropertyData.get())) == 4
            assert len(list(TimeseriesByDataState.get())) == 2
            assert len(list(db.session.query(TimeseriesData))) == 1

            ts_1.delete()
            db.session.commit()
            assert len(list(TimeseriesBySite.get())) == 1
            assert len(list(TimeseriesByBuilding.get())) == 1
            assert len(list(TimeseriesByStorey.get())) == 1
            assert len(list(TimeseriesBySpace.get())) == 1
            assert len(list(TimeseriesByZone.get())) == 1
            assert len(list(TimeseriesPropertyData.get())) == 2
            assert len(list(TimeseriesByDataState.get())) == 1
            assert len(list(db.session.query(TimeseriesData))) == 0

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
    def test_timeseries_read_only_fields(self, campaigns, campaign_scopes):
        """Check campaign and campaign_scope can't be modified

        Also check the getter/setter don't get in the way when querying.
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
        db.session.flush()

        with pytest.raises(AttributeError):
            ts_1.update(campaign_id=campaign_2.id)
        with pytest.raises(AttributeError):
            ts_1.update(campaign_scope_id=campaign_scope_2.id)

        ts_list = list(Timeseries.get(campaign_id=1))
        assert ts_list == [ts_1]
        ts_list = list(Timeseries.get(campaign_id=2))
        assert ts_list == []
        ts_list = list(Timeseries.get(campaign_scope_id=1))
        assert ts_list == [ts_1]
        ts_list = list(Timeseries.get(campaign_scope_id=2))
        assert ts_list == []

    @pytest.mark.usefixtures("users_by_user_groups")
    @pytest.mark.usefixtures("user_groups_by_campaigns")
    @pytest.mark.usefixtures("user_groups_by_campaign_scopes")
    @pytest.mark.usefixtures("timeseries_by_sites")
    @pytest.mark.usefixtures("timeseries_by_buildings")
    @pytest.mark.usefixtures("timeseries_by_storeys")
    @pytest.mark.usefixtures("timeseries_by_spaces")
    @pytest.mark.usefixtures("timeseries_by_zones")
    def test_timeseries_filters_as_admin(
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
        storey_1 = storeys[0]
        space_1 = spaces[0]
        zone_1 = zones[0]

        with CurrentUser(admin_user):

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

    @pytest.mark.usefixtures("users_by_user_groups")
    @pytest.mark.usefixtures("user_groups_by_campaigns")
    @pytest.mark.usefixtures("user_groups_by_campaign_scopes")
    @pytest.mark.usefixtures("timeseries_by_zones")
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

            db.session.delete(tbst_2)
            db.session.commit()

            ts_l = list(Timeseries.get(storey_id=storey_2.id))
            assert len(ts_l) == 1
            assert ts_l[0] == ts_2

            db.session.delete(tbb_2)
            db.session.commit()

            ts_l = list(Timeseries.get(building_id=building_2.id))
            assert len(ts_l) == 1
            assert ts_l[0] == ts_2

            db.session.delete(tbsi_2)
            db.session.commit()

            ts_l = list(Timeseries.get(site_id=site_2.id))
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
            db.session.add(ts_1)
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
                value=12,
            )
            db.session.add(tspd_1)
            db.session.commit()

            tspd = TimeseriesPropertyData.get_by_id(tspd_1.id)
            assert tspd.id == tspd_1.id
            tspd_l = list(TimeseriesPropertyData.get())
            assert len(tspd_l) == 1
            assert tspd_l[0].id == tspd.id
            tspd.update(value=42)
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
                    value=12,
                )

            tspd_l = list(TimeseriesPropertyData.get(timeseries_id=ts_2.id))
            assert len(tspd_l) == 2
            tspd_2 = tspd_l[0]
            tspd = TimeseriesPropertyData.get_by_id(tspd_2.id)
            assert tspd.id == tspd_2.id
            with pytest.raises(BEMServerAuthorizationError):
                TimeseriesPropertyData.get_by_id(tspd_1.id)
            with pytest.raises(BEMServerAuthorizationError):
                tspd_2.update(data_state_id=2)
            with pytest.raises(BEMServerAuthorizationError):
                tspd_2.delete()


class TestTimeseriesByDataStateModel:
    def test_timeseries_by_data_state_delete_cascade(
        self, users, timeseries_by_data_states
    ):
        admin_user = users[0]
        tsbds_1 = timeseries_by_data_states[0]

        with CurrentUser(admin_user):
            tsd = TimeseriesData(
                timestamp=dt.datetime(2020, 1, 1, tzinfo=dt.timezone.utc),
                timeseries_by_data_state_id=tsbds_1.id,
                value=12,
            )
            db.session.add(tsd)
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

    @pytest.mark.usefixtures("users_by_user_groups")
    @pytest.mark.usefixtures("user_groups_by_campaign_scopes")
    def test_timeseries_by_data_state_authorizations_as_user(
        self,
        users,
        timeseries,
        timeseries_by_data_states,
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
            db.session.add(tbs_1)
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
            db.session.add(tbb_1)
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
            db.session.add(tbs_1)
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
            db.session.add(tbs_1)
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
            db.session.add(tbz_1)
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
