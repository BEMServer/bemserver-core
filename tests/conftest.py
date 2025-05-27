"""Global conftest"""

import datetime as dt
from unittest import mock

import pytest
from pytest_postgresql import factories as ppf

from bemserver_core import BEMServerCore, model
from bemserver_core.authorization import CurrentUser, OpenBar
from bemserver_core.commands import setup_db
from bemserver_core.common import PropertyType
from bemserver_core.database import db
from bemserver_core.time_utils import PeriodEnum


@pytest.fixture(scope="session", autouse=True)
def inhibit_celery():
    """Inhibit celery system tasks by mocking the method launching tasks"""
    with mock.patch("bemserver_core.celery.BEMServerCoreSystemTask.apply_async"):
        yield


postgresql_proc = ppf.postgresql_proc()
postgresql = ppf.postgresql("postgresql_proc")


def _get_db_url(postgresql):
    return (
        "postgresql+psycopg://"
        f"{postgresql.info.user}:{postgresql.info.password}"
        f"@{postgresql.info.host}:{postgresql.info.port}/"
        f"{postgresql.info.dbname}"
    )


@pytest.fixture
def postgresql_db(postgresql):
    yield _get_db_url(postgresql)
    db.session.remove()


@pytest.fixture
def database(postgresql_db):
    db.set_db_url(postgresql_db)
    yield postgresql_db


# param dict: Additional config parameters
@pytest.fixture(params=(None,))
def config(request, postgresql_db, tmp_path, monkeypatch):
    cfg_dict = {
        "SQLALCHEMY_DATABASE_URI": postgresql_db,
        **(request.param or {}),
    }
    cfg = "".join([f"{k}={v!r}\n" for k, v in cfg_dict.items()])
    cfg_file = tmp_path / "config.py"
    cfg_file.write_text(cfg)
    monkeypatch.setenv("BEMSERVER_CORE_SETTINGS_FILE", str(cfg_file))
    yield cfg_file


# param bool: Whether or not to setup database
@pytest.fixture(params=(True,))
def bemservercore(request, config):
    """Create and initialize BEMServerCore with a database"""
    bsc = BEMServerCore()
    if request.param:
        setup_db()
    yield bsc


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


# param int: Number of campaigns
@pytest.fixture(params=[3])
def campaigns(request, bemservercore):
    with OpenBar():
        campaigns = []
        for i in range(max(2, request.param)):
            campaign_i = model.Campaign.new(
                name=f"Campaign {i + 1}",
                start_time=dt.datetime(2020, 1, 1, tzinfo=dt.timezone.utc),
                end_time=dt.datetime(2020, 2, 1, tzinfo=dt.timezone.utc),
            )
            campaigns.append(campaign_i)
        db.session.commit()
        return campaigns


@pytest.fixture
def campaign_scopes(bemservercore, campaigns):
    with OpenBar():
        cs_l = []
        cs_1 = model.CampaignScope.new(
            name="Campaign 1 - Scope 1",
            campaign_id=campaigns[0].id,
        )
        cs_l.append(cs_1)
        cs_2 = model.CampaignScope.new(
            name="Campaign 2 - Scope 1",
            campaign_id=campaigns[1].id,
        )
        cs_l.append(cs_2)
        if len(campaigns) > 2:
            cs_3 = model.CampaignScope.new(
                name="Campaign 3 - Scope 1",
                campaign_id=campaigns[2].id,
            )
            cs_l.append(cs_3)
        db.session.commit()
    return cs_l


@pytest.fixture
def user_groups_by_campaigns(bemservercore, user_groups, campaigns):
    with OpenBar():
        ugbc_l = []
        ugbc_1 = model.UserGroupByCampaign.new(
            user_group_id=user_groups[0].id,
            campaign_id=campaigns[0].id,
        )
        ugbc_l.append(ugbc_1)
        ugbc_2 = model.UserGroupByCampaign.new(
            user_group_id=user_groups[1].id,
            campaign_id=campaigns[1].id,
        )
        ugbc_l.append(ugbc_2)
        if len(campaigns) > 2:
            ugbc_3 = model.UserGroupByCampaign.new(
                user_group_id=user_groups[1].id,
                campaign_id=campaigns[2].id,
            )
            ugbc_l.append(ugbc_3)
        db.session.commit()
    return ugbc_l


@pytest.fixture
def user_groups_by_campaign_scopes(bemservercore, user_groups, campaign_scopes):
    with OpenBar():
        ugbcs_l = []
        ugbcs_1 = model.UserGroupByCampaignScope.new(
            user_group_id=user_groups[0].id,
            campaign_scope_id=campaign_scopes[0].id,
        )
        ugbcs_l.append(ugbcs_1)
        ugbcs_2 = model.UserGroupByCampaignScope.new(
            user_group_id=user_groups[1].id,
            campaign_scope_id=campaign_scopes[1].id,
        )
        ugbcs_l.append(ugbcs_2)
        if len(campaign_scopes) > 2:
            ugbcs_3 = model.UserGroupByCampaignScope.new(
                user_group_id=user_groups[1].id,
                campaign_scope_id=campaign_scopes[2].id,
            )
            ugbcs_l.append(ugbcs_3)
        db.session.commit()
    return ugbcs_l


@pytest.fixture
def timeseries_properties(bemservercore):
    with OpenBar():
        ts_p_1 = model.TimeseriesProperty.get(name="Min").first()
        ts_p_2 = model.TimeseriesProperty.get(name="Max").first()
        ts_p_3 = model.TimeseriesProperty.get(name="Interval").first()
        ts_p_4 = model.TimeseriesProperty.new(
            name="Revision",
            value_type=PropertyType.integer,
        )
        ts_p_5 = model.TimeseriesProperty.new(
            name="IsCalibrated",
            value_type=PropertyType.boolean,
        )
        ts_p_6 = model.TimeseriesProperty.new(
            name="Label",
            value_type=PropertyType.string,
        )
        db.session.commit()
    return (ts_p_1, ts_p_2, ts_p_3, ts_p_4, ts_p_5, ts_p_6)


# param int: Number of timeseries
@pytest.fixture(params=[2])
def timeseries(request, bemservercore, campaigns, campaign_scopes):
    with OpenBar():
        ts_l = []
        for i in range(request.param):
            ts_i = model.Timeseries.new(
                name=f"Timeseries {i + 1}",
                description=f"Test timeseries #{i + 1}",
                campaign=campaigns[i % len(campaigns)],
                campaign_scope=campaign_scopes[i % len(campaign_scopes)],
            )
            ts_l.append(ts_i)
        db.session.commit()
        return ts_l


@pytest.fixture
def timeseries_property_data(request, bemservercore, timeseries_properties, timeseries):
    with OpenBar():
        tspd_l = []
        for idx, ts in enumerate(timeseries):
            if idx % 2 == 0:
                tspd_l.append(
                    model.TimeseriesPropertyData.new(
                        timeseries_id=ts.id,
                        property_id=timeseries_properties[0].id,
                        value="12",
                    )
                )
            else:
                tspd_l.append(
                    model.TimeseriesPropertyData.new(
                        timeseries_id=ts.id,
                        property_id=timeseries_properties[1].id,
                        value="42",
                    )
                )
        db.session.commit()
        return tspd_l


# param int: Number of timeseries x data states associations
@pytest.fixture(params=[2])
def timeseries_by_data_states(request, bemservercore, timeseries):
    with OpenBar():
        ts_l = []
        for i in range(request.param):
            ts_i = model.TimeseriesByDataState.new(
                timeseries=timeseries[i % len(timeseries)],
                data_state_id=1,
            )
            ts_l.append(ts_i)
        db.session.commit()
        return ts_l


@pytest.fixture
def event_categories(bemservercore):
    with OpenBar():
        ec_1 = model.EventCategory.new(name="Custom event category 1")
        ec_2 = model.EventCategory.new(name="Custom event category 2")
        db.session.commit()
    return (ec_1, ec_2)


@pytest.fixture
def events(bemservercore, campaign_scopes, event_categories):
    with OpenBar():
        event_1 = model.Event.new(
            campaign_scope_id=campaign_scopes[0].id,
            timestamp=dt.datetime(2020, 1, 1, tzinfo=dt.timezone.utc),
            category_id=event_categories[0].id,
            level=model.EventLevelEnum.WARNING,
            source="src",
        )
        event_2 = model.Event.new(
            campaign_scope_id=campaign_scopes[1].id,
            timestamp=dt.datetime(2020, 1, 15, tzinfo=dt.timezone.utc),
            category_id=event_categories[1].id,
            level=model.EventLevelEnum.DEBUG,
            source="src",
        )
        db.session.commit()
    return (event_1, event_2)


@pytest.fixture
def event_categories_by_users(bemservercore, event_categories, users):
    with OpenBar():
        ecbu_1 = model.EventCategoryByUser.new(
            category_id=event_categories[0].id,
            user_id=users[0].id,
            notification_level=model.EventLevelEnum.WARNING,
        )
        ecbu_2 = model.EventCategoryByUser.new(
            category_id=event_categories[1].id,
            user_id=users[1].id,
            notification_level=model.EventLevelEnum.DEBUG,
        )
        db.session.commit()
    return (ecbu_1, ecbu_2)


@pytest.fixture
def timeseries_by_events(bemservercore, timeseries, events):
    with OpenBar():
        tbs_1 = model.TimeseriesByEvent.new(
            timeseries_id=timeseries[0].id,
            event_id=events[0].id,
        )
        tbs_2 = model.TimeseriesByEvent.new(
            timeseries_id=timeseries[1].id,
            event_id=events[1].id,
        )
        db.session.commit()
    return (tbs_1, tbs_2)


@pytest.fixture
def events_by_sites(bemservercore, events, sites):
    with OpenBar():
        ebs_1 = model.EventBySite.new(
            event_id=events[0].id,
            site_id=sites[0].id,
        )
        ebs_2 = model.EventBySite.new(
            event_id=events[1].id,
            site_id=sites[1].id,
        )
        db.session.commit()
    return (ebs_1, ebs_2)


@pytest.fixture
def events_by_buildings(bemservercore, events, buildings):
    with OpenBar():
        ebb_1 = model.EventByBuilding.new(
            event_id=events[0].id,
            building_id=buildings[0].id,
        )
        ebb_2 = model.EventByBuilding.new(
            event_id=events[1].id,
            building_id=buildings[1].id,
        )
        db.session.commit()
    return (ebb_1, ebb_2)


@pytest.fixture
def events_by_storeys(bemservercore, events, storeys):
    with OpenBar():
        ebs_1 = model.EventByStorey.new(
            event_id=events[0].id,
            storey_id=storeys[0].id,
        )
        ebs_2 = model.EventByStorey.new(
            event_id=events[1].id,
            storey_id=storeys[1].id,
        )
        db.session.commit()
    return (ebs_1, ebs_2)


@pytest.fixture
def events_by_spaces(bemservercore, events, spaces):
    with OpenBar():
        ebs_1 = model.EventBySpace.new(
            event_id=events[0].id,
            space_id=spaces[0].id,
        )
        ebs_2 = model.EventBySpace.new(
            event_id=events[1].id,
            space_id=spaces[1].id,
        )
        db.session.commit()
    return (ebs_1, ebs_2)


@pytest.fixture
def events_by_zones(bemservercore, events, zones):
    with OpenBar():
        ebz_1 = model.EventByZone.new(
            event_id=events[0].id,
            zone_id=zones[0].id,
        )
        ebz_2 = model.EventByZone.new(
            event_id=events[1].id,
            zone_id=zones[1].id,
        )
        db.session.commit()
    return (ebz_1, ebz_2)


@pytest.fixture
def notifications(bemservercore, events, users):
    with OpenBar():
        notif_1 = model.Notification.new(
            event_id=events[0].id,
            user_id=users[0].id,
            timestamp=dt.datetime(2020, 1, 1, tzinfo=dt.timezone.utc),
            read=False,
        )
        notif_2 = model.Notification.new(
            event_id=events[1].id,
            user_id=users[1].id,
            timestamp=dt.datetime(2021, 1, 1, tzinfo=dt.timezone.utc),
            read=True,
        )
        db.session.commit()
    return (notif_1, notif_2)


@pytest.fixture
def structural_element_properties(bemservercore):
    with OpenBar():
        sep_1 = model.StructuralElementProperty.new(
            name="Area",
            value_type=PropertyType.integer,
        )
        sep_2 = model.StructuralElementProperty.new(
            name="Volume",
            value_type=PropertyType.float,
        )
        sep_3 = model.StructuralElementProperty.new(
            name="Window state",
            value_type=PropertyType.boolean,
        )
        sep_4 = model.StructuralElementProperty.new(
            name="Architect",
            value_type=PropertyType.string,
        )
        db.session.commit()
    return (sep_1, sep_2, sep_3, sep_4)


@pytest.fixture
def site_properties(bemservercore, structural_element_properties):
    with OpenBar():
        site_p_1 = model.SiteProperty.new(
            structural_element_property_id=structural_element_properties[0].id,
        )
        site_p_2 = model.SiteProperty.new(
            structural_element_property_id=structural_element_properties[1].id,
        )
        site_p_3 = model.SiteProperty.new(
            structural_element_property_id=structural_element_properties[2].id,
        )
        site_p_4 = model.SiteProperty.new(
            structural_element_property_id=structural_element_properties[3].id,
        )
        db.session.commit()
    return (site_p_1, site_p_2, site_p_3, site_p_4)


@pytest.fixture
def building_properties(bemservercore, structural_element_properties):
    with OpenBar():
        building_p_1 = model.BuildingProperty.new(
            structural_element_property_id=structural_element_properties[0].id,
        )
        building_p_2 = model.BuildingProperty.new(
            structural_element_property_id=structural_element_properties[1].id,
        )
        building_p_3 = model.BuildingProperty.new(
            structural_element_property_id=structural_element_properties[2].id,
        )
        building_p_4 = model.BuildingProperty.new(
            structural_element_property_id=structural_element_properties[3].id,
        )
        db.session.commit()
    return (building_p_1, building_p_2, building_p_3, building_p_4)


@pytest.fixture
def storey_properties(bemservercore, structural_element_properties):
    with OpenBar():
        storey_p_1 = model.StoreyProperty.new(
            structural_element_property_id=structural_element_properties[0].id,
        )
        storey_p_2 = model.StoreyProperty.new(
            structural_element_property_id=structural_element_properties[1].id,
        )
        storey_p_3 = model.StoreyProperty.new(
            structural_element_property_id=structural_element_properties[2].id,
        )
        storey_p_4 = model.StoreyProperty.new(
            structural_element_property_id=structural_element_properties[3].id,
        )
        db.session.commit()
    return (storey_p_1, storey_p_2, storey_p_3, storey_p_4)


@pytest.fixture
def space_properties(bemservercore, structural_element_properties):
    with OpenBar():
        space_p_1 = model.SpaceProperty.new(
            structural_element_property_id=structural_element_properties[0].id,
        )
        space_p_2 = model.SpaceProperty.new(
            structural_element_property_id=structural_element_properties[1].id,
        )
        space_p_3 = model.SpaceProperty.new(
            structural_element_property_id=structural_element_properties[2].id,
        )
        space_p_4 = model.SpaceProperty.new(
            structural_element_property_id=structural_element_properties[3].id,
        )
        db.session.commit()
    return (space_p_1, space_p_2, space_p_3, space_p_4)


@pytest.fixture
def zone_properties(bemservercore, structural_element_properties):
    with OpenBar():
        zone_p_1 = model.ZoneProperty.new(
            structural_element_property_id=structural_element_properties[0].id,
        )
        zone_p_2 = model.ZoneProperty.new(
            structural_element_property_id=structural_element_properties[1].id,
        )
        zone_p_3 = model.ZoneProperty.new(
            structural_element_property_id=structural_element_properties[2].id,
        )
        zone_p_4 = model.ZoneProperty.new(
            structural_element_property_id=structural_element_properties[3].id,
        )
        db.session.commit()
    return (zone_p_1, zone_p_2, zone_p_3, zone_p_4)


@pytest.fixture
def sites(bemservercore, campaigns):
    with OpenBar():
        site_1 = model.Site.new(
            name="Site 1",
            campaign_id=campaigns[0].id,
            latitude=43.47394,
            longitude=-1.50940,
        )
        site_2 = model.Site.new(
            name="Site 2",
            campaign_id=campaigns[1].id,
            latitude=44.84325,
            longitude=-0.56262,
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
            value="4.2",
        )
        spd_3 = model.SitePropertyData.new(
            site_id=sites[1].id,
            site_property_id=site_properties[2].id,
            value="true",
        )
        spd_4 = model.SitePropertyData.new(
            site_id=sites[1].id,
            site_property_id=site_properties[3].id,
            value="Imhotep",
        )
        db.session.commit()
    return (spd_1, spd_2, spd_3, spd_4)


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
            value="4.2",
        )
        bpd_3 = model.BuildingPropertyData.new(
            building_id=buildings[1].id,
            building_property_id=building_properties[2].id,
            value="true",
        )
        bpd_4 = model.BuildingPropertyData.new(
            building_id=buildings[1].id,
            building_property_id=building_properties[3].id,
            value="Imhotep",
        )
        db.session.commit()
    return (bpd_1, bpd_2, bpd_3, bpd_4)


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
            value="4.2",
        )
        spd_3 = model.StoreyPropertyData.new(
            storey_id=storeys[1].id,
            storey_property_id=storey_properties[2].id,
            value="true",
        )
        spd_4 = model.StoreyPropertyData.new(
            storey_id=storeys[1].id,
            storey_property_id=storey_properties[3].id,
            value="Imhotep",
        )
        db.session.commit()
    return (spd_1, spd_2, spd_3, spd_4)


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
            value="4.2",
        )
        spd_3 = model.SpacePropertyData.new(
            space_id=spaces[1].id,
            space_property_id=space_properties[2].id,
            value="true",
        )
        spd_4 = model.SpacePropertyData.new(
            space_id=spaces[1].id,
            space_property_id=space_properties[3].id,
            value="Imhotep",
        )
        db.session.commit()
    return (spd_1, spd_2, spd_3, spd_4)


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
            value="4.2",
        )
        zpd_3 = model.ZonePropertyData.new(
            zone_id=zones[1].id,
            zone_property_id=zone_properties[2].id,
            value="true",
        )
        zpd_4 = model.ZonePropertyData.new(
            zone_id=zones[1].id,
            zone_property_id=zone_properties[3].id,
            value="Imhotep",
        )
        db.session.commit()
    return (zpd_1, zpd_2, zpd_3, zpd_4)


@pytest.fixture
def energy_consumption_timeseries_by_sites(bemservercore, timeseries, sites):
    with OpenBar():
        ectbs_1 = model.EnergyConsumptionTimeseriesBySite.new(
            site_id=sites[0].id,
            energy_id=1,
            end_use_id=1,
            timeseries_id=timeseries[0].id,
        )
        ectbs_2 = model.EnergyConsumptionTimeseriesBySite.new(
            site_id=sites[1].id,
            energy_id=2,
            end_use_id=2,
            timeseries_id=timeseries[1].id,
        )
        db.session.commit()
    return (ectbs_1, ectbs_2)


@pytest.fixture
def energy_consumption_timeseries_by_buildings(bemservercore, timeseries, buildings):
    with OpenBar():
        ectbb_1 = model.EnergyConsumptionTimeseriesByBuilding.new(
            building_id=buildings[0].id,
            energy_id=1,
            end_use_id=1,
            timeseries_id=timeseries[0].id,
        )
        ectbb_2 = model.EnergyConsumptionTimeseriesByBuilding.new(
            building_id=buildings[1].id,
            energy_id=2,
            end_use_id=2,
            timeseries_id=timeseries[1].id,
        )
        db.session.commit()
    return (ectbb_1, ectbb_2)


@pytest.fixture
def energy_production_timeseries_by_sites(bemservercore, timeseries, sites):
    with OpenBar():
        eptbs_1 = model.EnergyProductionTimeseriesBySite.new(
            site_id=sites[0].id,
            energy_id=1,
            prod_tech_id=1,
            timeseries_id=timeseries[0].id,
        )
        eptbs_2 = model.EnergyProductionTimeseriesBySite.new(
            site_id=sites[1].id,
            energy_id=2,
            prod_tech_id=2,
            timeseries_id=timeseries[1].id,
        )
        db.session.commit()
    return (eptbs_1, eptbs_2)


@pytest.fixture
def energy_production_timeseries_by_buildings(bemservercore, timeseries, buildings):
    with OpenBar():
        eptbb_1 = model.EnergyProductionTimeseriesByBuilding.new(
            building_id=buildings[0].id,
            energy_id=1,
            prod_tech_id=1,
            timeseries_id=timeseries[0].id,
        )
        eptbb_2 = model.EnergyProductionTimeseriesByBuilding.new(
            building_id=buildings[1].id,
            energy_id=2,
            prod_tech_id=2,
            timeseries_id=timeseries[1].id,
        )
        db.session.commit()
    return (eptbb_1, eptbb_2)


@pytest.fixture
def weather_timeseries_by_sites(bemservercore, timeseries, sites):
    with OpenBar():
        # Set units to timeseries
        timeseries[0].unit_symbol = "°C"
        timeseries[1].unit_symbol = "%"
        wtbs_1 = model.WeatherTimeseriesBySite.new(
            site_id=sites[0].id,
            parameter=model.WeatherParameterEnum.AIR_TEMPERATURE,
            timeseries_id=timeseries[0].id,
            forecast=False,
        )
        wtbs_2 = model.WeatherTimeseriesBySite.new(
            site_id=sites[1].id,
            parameter=model.WeatherParameterEnum.RELATIVE_HUMIDITY,
            timeseries_id=timeseries[1].id,
            forecast=True,
        )
        db.session.commit()
    return (wtbs_1, wtbs_2)


@pytest.fixture
def tasks_by_campaigns(bemservercore, campaigns):
    with OpenBar():
        tbc_1 = model.TaskByCampaign.new(
            task_name="TASK",
            campaign_id=campaigns[0].id,
            offset_unit=PeriodEnum.day,
        )
        tbc_2 = model.TaskByCampaign.new(
            task_name="TASK",
            campaign_id=campaigns[1].id,
            offset_unit=PeriodEnum.hour,
            start_offset=-6,
            is_enabled=False,
        )
        db.session.commit()
    return (tbc_1, tbc_2)
