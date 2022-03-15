""" Global conftest"""
import datetime as dt

import pytest
from pytest_postgresql import factories as ppf

from bemserver_core.database import db
from bemserver_core.authorization import CurrentUser, OpenBar
from bemserver_core import model
from bemserver_core.testutils import setup_db


postgresql_proc = ppf.postgresql_proc(
    postgres_options=(
        "-c shared_preload_libraries='timescaledb' "
        "-c timescaledb.telemetry_level=off"
    )
)
postgresql = ppf.postgresql("postgresql_proc")


@pytest.fixture
def database(postgresql):
    yield from setup_db(postgresql)


@pytest.fixture
def as_admin():
    """Set an admin user for the test"""
    with OpenBar(), CurrentUser(
        model.User(name="Chuck", email="chuck@test.com", is_admin=True, is_active=True)
    ):
        yield


@pytest.fixture
def user_groups(database):
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
def users(database):
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
def users_by_user_groups(database, users, user_groups):
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
def campaigns(database):
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
def campaign_scopes(database, campaigns):
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
def user_groups_by_campaigns(database, user_groups, campaigns):
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
def user_groups_by_campaign_scopes(database, user_groups, campaign_scopes):
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
def timeseries_properties(database):
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
def timeseries(request, database, campaigns, campaign_scopes):
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
def timeseries_property_data(request, database, timeseries_properties, timeseries):
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
def timeseries_by_data_states(request, database, timeseries):
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
def events(database, campaigns):
    with OpenBar():
        ts_event_1 = model.Event.new(
            campaign_id=campaigns[0].id,
            timestamp=dt.datetime(2020, 1, 1, tzinfo=dt.timezone.utc),
            category="observation_missing",
            source="src",
            level="ERROR",
            state="NEW",
        )
        ts_event_2 = model.Event.new(
            campaign_id=campaigns[1].id,
            timestamp=dt.datetime(2020, 1, 15, tzinfo=dt.timezone.utc),
            category="observation_missing",
            source="src",
            level="WARNING",
            state="ONGOING",
        )
        db.session.commit()
    return (ts_event_1, ts_event_2)
