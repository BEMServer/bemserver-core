""" Global conftest"""
import datetime as dt

import sqlalchemy as sqla

import pytest
from pytest_postgresql import factories as ppf

from bemserver_core import BEMServerCore
from bemserver_core.database import db
from bemserver_core.authorization import CurrentUser, OpenBar
from bemserver_core import model
from bemserver_core.commands import setup_db


postgresql_proc = ppf.postgresql_proc(
    postgres_options=(
        "-c shared_preload_libraries='timescaledb' "
        "-c timescaledb.telemetry_level=off"
    )
)
postgresql = ppf.postgresql("postgresql_proc")


def _get_db_url(postgresql):
    return (
        "postgresql+psycopg2://"
        f"{postgresql.info.user}:{postgresql.info.password}"
        f"@{postgresql.info.host}:{postgresql.info.port}/"
        f"{postgresql.info.dbname}"
    )


@pytest.fixture
def timescale_db(postgresql):
    with sqla.create_engine(_get_db_url(postgresql)).connect() as connection:
        connection.execute("CREATE EXTENSION IF NOT EXISTS timescaledb;")
    yield postgresql


@pytest.fixture
def database(timescale_db):
    db.set_db_url(_get_db_url(timescale_db))
    yield
    db.session.remove()


@pytest.fixture
def bemservercore(request, database):
    """Create and initialize BEMServerCore with a database"""
    bsc = BEMServerCore()
    setup_db()
    bsc.init_auth()


@pytest.fixture
def as_admin(bemservercore):
    """Set an admin user for the test

    Requires bemservercore to initialize autorizations.
    """
    with CurrentUser(
        model.User(
            name="Chuck",
            email="chuck@test.com",
            _is_admin=True,
            _is_active=True,
        )
    ):
        yield


@pytest.fixture
def user_groups(bemservercore):
    with OpenBar():
        user_group_1 = model.UserGroup.new(
            name="User group 1",
        )
        user_group_2 = model.UserGroup.new(
            name="User group 2",
        )
        db.session.commit()
    return (user_group_1, user_group_2)


@pytest.fixture
def users(bemservercore):
    with OpenBar():
        user_1 = model.User.new(
            name="Chuck",
            email="chuck@test.com",
            is_admin=True,
            is_active=True,
        )
        user_1.set_password("N0rr1s")
        user_2 = model.User.new(
            name="John",
            email="john@test.com",
            is_admin=False,
            is_active=True,
        )
        user_2.set_password("D0e")
        db.session.commit()
    return (user_1, user_2)


@pytest.fixture
def users_by_user_groups(bemservercore, users, user_groups):
    with OpenBar():
        ubug_1 = model.UserByUserGroup.new(
            user_id=users[0].id,
            user_group_id=user_groups[0].id,
        )
        ubug_2 = model.UserByUserGroup.new(
            user_id=users[1].id,
            user_group_id=user_groups[1].id,
        )
        db.session.commit()
    return (ubug_1, ubug_2)


@pytest.fixture
def campaigns(bemservercore):
    with OpenBar():
        campaign_1 = model.Campaign.new(
            name="Campaign 1",
            start_time=dt.datetime(2020, 1, 1, tzinfo=dt.timezone.utc),
            end_time=dt.datetime(2020, 2, 1, tzinfo=dt.timezone.utc),
        )
        campaign_2 = model.Campaign.new(
            name="Campaign 2",
            start_time=dt.datetime(2020, 1, 1, tzinfo=dt.timezone.utc),
            end_time=dt.datetime(2020, 2, 1, tzinfo=dt.timezone.utc),
        )
        db.session.commit()
    return (campaign_1, campaign_2)


@pytest.fixture
def campaign_scopes(bemservercore, campaigns):
    with OpenBar():
        cs_1 = model.CampaignScope.new(
            name="Campaign 1 - Scope 1",
            campaign_id=campaigns[0].id,
        )
        cs_2 = model.CampaignScope.new(
            name="Campaign 2 - Scope 1",
            campaign_id=campaigns[1].id,
        )
        db.session.commit()
    return (cs_1, cs_2)


@pytest.fixture
def user_groups_by_campaigns(bemservercore, user_groups, campaigns):
    with OpenBar():
        ugbc_1 = model.UserGroupByCampaign.new(
            user_group_id=user_groups[0].id,
            campaign_id=campaigns[0].id,
        )
        ugbc_2 = model.UserGroupByCampaign.new(
            user_group_id=user_groups[1].id,
            campaign_id=campaigns[1].id,
        )
        db.session.commit()
    return (ugbc_1, ugbc_2)


@pytest.fixture
def user_groups_by_campaign_scopes(bemservercore, user_groups, campaign_scopes):
    with OpenBar():
        ugbcs_1 = model.UserGroupByCampaignScope.new(
            user_group_id=user_groups[0].id,
            campaign_scope_id=campaign_scopes[0].id,
        )
        ugbcs_2 = model.UserGroupByCampaignScope.new(
            user_group_id=user_groups[1].id,
            campaign_scope_id=campaign_scopes[1].id,
        )
        db.session.commit()
    return (ugbcs_1, ugbcs_2)


@pytest.fixture
def timeseries_properties(bemservercore):
    with OpenBar():
        ts_p_1 = model.TimeseriesProperty.new(
            name="Min",
        )
        ts_p_2 = model.TimeseriesProperty.new(
            name="Max",
        )
        db.session.commit()
    return (ts_p_1, ts_p_2)


@pytest.fixture(params=[2])
def timeseries(request, bemservercore, campaigns, campaign_scopes):
    with OpenBar():
        ts_l = []
        for i in range(request.param):
            ts_i = model.Timeseries(
                name=f"Timeseries {i}",
                description=f"Test timeseries #{i}",
                campaign=campaigns[i % len(campaigns)],
                campaign_scope=campaign_scopes[i % len(campaign_scopes)],
            )
            ts_l.append(ts_i)
        db.session.add_all(ts_l)
        db.session.commit()
        return ts_l


@pytest.fixture
def timeseries_property_data(request, bemservercore, timeseries_properties, timeseries):
    with OpenBar():
        tspd_l = []
        for ts in timeseries:
            tspd_l.append(
                model.TimeseriesPropertyData(
                    timeseries_id=ts.id,
                    property_id=timeseries_properties[0].id,
                    value=12,
                )
            )
            tspd_l.append(
                model.TimeseriesPropertyData(
                    timeseries_id=ts.id,
                    property_id=timeseries_properties[1].id,
                    value=42,
                )
            )
        db.session.add_all(tspd_l)
        db.session.commit()
        return tspd_l


@pytest.fixture(params=[2])
def timeseries_by_data_states(request, bemservercore, timeseries):
    with OpenBar():
        ts_l = []
        for i in range(request.param):
            ts_i = model.TimeseriesByDataState(
                timeseries=timeseries[i % len(timeseries)],
                data_state_id=1,
            )
            ts_l.append(ts_i)
        db.session.add_all(ts_l)
        db.session.commit()
        return ts_l


@pytest.fixture
def event_categories(bemservercore):
    with OpenBar():
        ec_1 = model.EventCategory.new(id="Missing data")
        ec_2 = model.EventCategory.new(id="Outlier data")
        db.session.commit()
    return (ec_1, ec_2)


@pytest.fixture
def events(bemservercore, campaign_scopes, event_categories):
    with OpenBar():
        ts_event_1 = model.Event.new(
            campaign_scope_id=campaign_scopes[0].id,
            timestamp=dt.datetime(2020, 1, 1, tzinfo=dt.timezone.utc),
            category=event_categories[0].id,
            source="src",
            level="ERROR",
            state="NEW",
        )
        ts_event_2 = model.Event.new(
            campaign_scope_id=campaign_scopes[1].id,
            timestamp=dt.datetime(2020, 1, 15, tzinfo=dt.timezone.utc),
            category=event_categories[0].id,
            source="src",
            level="WARNING",
            state="ONGOING",
        )
        db.session.commit()
    return (ts_event_1, ts_event_2)


@pytest.fixture
def structural_element_properties(bemservercore):
    with OpenBar():
        sep_1 = model.StructuralElementProperty.new(
            name="Surface",
        )
        sep_2 = model.StructuralElementProperty.new(
            name="Volume",
        )
        db.session.commit()
    return (sep_1, sep_2)


@pytest.fixture
def site_properties(bemservercore, structural_element_properties):
    with OpenBar():
        site_p_1 = model.SiteProperty.new(
            structural_element_property_id=structural_element_properties[0].id,
        )
        site_p_2 = model.SiteProperty.new(
            structural_element_property_id=structural_element_properties[1].id,
        )
        db.session.commit()
    return (site_p_1, site_p_2)


@pytest.fixture
def building_properties(bemservercore, structural_element_properties):
    with OpenBar():
        building_p_1 = model.BuildingProperty.new(
            structural_element_property_id=structural_element_properties[0].id,
        )
        building_p_2 = model.BuildingProperty.new(
            structural_element_property_id=structural_element_properties[1].id,
        )
        db.session.commit()
    return (building_p_1, building_p_2)


@pytest.fixture
def storey_properties(bemservercore, structural_element_properties):
    with OpenBar():
        storey_p_1 = model.StoreyProperty.new(
            structural_element_property_id=structural_element_properties[0].id,
        )
        storey_p_2 = model.StoreyProperty.new(
            structural_element_property_id=structural_element_properties[1].id,
        )
        db.session.commit()
    return (storey_p_1, storey_p_2)


@pytest.fixture
def space_properties(bemservercore, structural_element_properties):
    with OpenBar():
        space_p_1 = model.SpaceProperty.new(
            structural_element_property_id=structural_element_properties[0].id,
        )
        space_p_2 = model.SpaceProperty.new(
            structural_element_property_id=structural_element_properties[1].id,
        )
        db.session.commit()
    return (space_p_1, space_p_2)


@pytest.fixture
def zone_properties(bemservercore, structural_element_properties):
    with OpenBar():
        zone_p_1 = model.ZoneProperty.new(
            structural_element_property_id=structural_element_properties[0].id,
        )
        zone_p_2 = model.ZoneProperty.new(
            structural_element_property_id=structural_element_properties[1].id,
        )
        db.session.commit()
    return (zone_p_1, zone_p_2)


@pytest.fixture
def sites(bemservercore, campaigns):
    with OpenBar():
        site_1 = model.Site.new(
            name="Site 1",
            campaign_id=campaigns[0].id,
        )
        site_2 = model.Site.new(
            name="Site 2",
            campaign_id=campaigns[1].id,
        )
        db.session.commit()
    return (site_1, site_2)


@pytest.fixture
def buildings(bemservercore, sites):
    with OpenBar():
        building_1 = model.Building.new(
            name="Building 1",
            site_id=sites[0].id,
        )
        building_2 = model.Building.new(
            name="Building 2",
            site_id=sites[1].id,
        )
        db.session.commit()
    return (building_1, building_2)


@pytest.fixture
def storeys(bemservercore, buildings):
    with OpenBar():
        storey_1 = model.Storey.new(
            name="Storey 1",
            building_id=buildings[0].id,
        )
        storey_2 = model.Storey.new(
            name="Storey 2",
            building_id=buildings[1].id,
        )
        db.session.commit()
    return (storey_1, storey_2)


@pytest.fixture
def spaces(bemservercore, storeys):
    with OpenBar():
        space_1 = model.Space.new(
            name="Space 1",
            storey_id=storeys[0].id,
        )
        space_2 = model.Space.new(
            name="Space 2",
            storey_id=storeys[1].id,
        )
        db.session.commit()
    return (space_1, space_2)


@pytest.fixture
def zones(bemservercore, campaigns):
    with OpenBar():
        zone_1 = model.Zone.new(
            name="Zone 1",
            campaign_id=campaigns[0].id,
        )
        zone_2 = model.Zone.new(
            name="Zone 2",
            campaign_id=campaigns[1].id,
        )
        db.session.commit()
    return (zone_1, zone_2)


@pytest.fixture
def timeseries_by_sites(bemservercore, timeseries, sites):
    with OpenBar():
        tbs_1 = model.TimeseriesBySite.new(
            timeseries_id=timeseries[0].id,
            site_id=sites[0].id,
        )
        tbs_2 = model.TimeseriesBySite.new(
            timeseries_id=timeseries[1].id,
            site_id=sites[1].id,
        )
        db.session.commit()
    return (tbs_1, tbs_2)


@pytest.fixture
def timeseries_by_buildings(bemservercore, timeseries, buildings):
    with OpenBar():
        tbb_1 = model.TimeseriesByBuilding.new(
            timeseries_id=timeseries[0].id,
            building_id=buildings[0].id,
        )
        tbb_2 = model.TimeseriesByBuilding.new(
            timeseries_id=timeseries[1].id,
            building_id=buildings[1].id,
        )
        db.session.commit()
    return (tbb_1, tbb_2)


@pytest.fixture
def timeseries_by_storeys(bemservercore, timeseries, storeys):
    with OpenBar():
        tbs_1 = model.TimeseriesByStorey.new(
            timeseries_id=timeseries[0].id,
            storey_id=storeys[0].id,
        )
        tbs_2 = model.TimeseriesByStorey.new(
            timeseries_id=timeseries[1].id,
            storey_id=storeys[1].id,
        )
        db.session.commit()
    return (tbs_1, tbs_2)


@pytest.fixture
def timeseries_by_spaces(bemservercore, timeseries, spaces):
    with OpenBar():
        tbs_1 = model.TimeseriesBySpace.new(
            timeseries_id=timeseries[0].id,
            space_id=spaces[0].id,
        )
        tbs_2 = model.TimeseriesBySpace.new(
            timeseries_id=timeseries[1].id,
            space_id=spaces[1].id,
        )
        db.session.commit()
    return (tbs_1, tbs_2)


@pytest.fixture
def timeseries_by_zones(bemservercore, timeseries, zones):
    with OpenBar():
        tbz_1 = model.TimeseriesByZone.new(
            timeseries_id=timeseries[0].id,
            zone_id=zones[0].id,
        )
        tbz_2 = model.TimeseriesByZone.new(
            timeseries_id=timeseries[1].id,
            zone_id=zones[1].id,
        )
        db.session.commit()
    return (tbz_1, tbz_2)


@pytest.fixture
def site_property_data(bemservercore, sites, site_properties):
    with OpenBar():
        spd_1 = model.SitePropertyData.new(
            site_id=sites[0].id,
            site_property_id=site_properties[0].id,
            value="12",
        )
        spd_2 = model.SitePropertyData.new(
            site_id=sites[1].id,
            site_property_id=site_properties[1].id,
            value="42",
        )
        db.session.commit()
    return (spd_1, spd_2)


@pytest.fixture
def building_property_data(bemservercore, buildings, building_properties):
    with OpenBar():
        bpd_1 = model.BuildingPropertyData.new(
            building_id=buildings[0].id,
            building_property_id=building_properties[0].id,
            value="12",
        )
        bpd_2 = model.BuildingPropertyData.new(
            building_id=buildings[1].id,
            building_property_id=building_properties[1].id,
            value="42",
        )
        db.session.commit()
    return (bpd_1, bpd_2)


@pytest.fixture
def storey_property_data(bemservercore, storeys, storey_properties):
    with OpenBar():
        spd_1 = model.StoreyPropertyData.new(
            storey_id=storeys[0].id,
            storey_property_id=storey_properties[0].id,
            value="12",
        )
        spd_2 = model.StoreyPropertyData.new(
            storey_id=storeys[1].id,
            storey_property_id=storey_properties[1].id,
            value="42",
        )
        db.session.commit()
    return (spd_1, spd_2)


@pytest.fixture
def space_property_data(bemservercore, spaces, space_properties):
    with OpenBar():
        spd_1 = model.SpacePropertyData.new(
            space_id=spaces[0].id,
            space_property_id=space_properties[0].id,
            value="12",
        )
        spd_2 = model.SpacePropertyData.new(
            space_id=spaces[1].id,
            space_property_id=space_properties[1].id,
            value="42",
        )
        db.session.commit()
    return (spd_1, spd_2)


@pytest.fixture
def zone_property_data(bemservercore, zones, zone_properties):
    with OpenBar():
        zpd_1 = model.ZonePropertyData.new(
            zone_id=zones[0].id,
            zone_property_id=zone_properties[0].id,
            value="12",
        )
        zpd_2 = model.ZonePropertyData.new(
            zone_id=zones[1].id,
            zone_property_id=zone_properties[1].id,
            value="42",
        )
        db.session.commit()
    return (zpd_1, zpd_2)
