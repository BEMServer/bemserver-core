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
def users(database):
    with OpenBar():
        user_1 = model.User.new(
            name="Chuck",
            email="chuck@test.com",
            is_admin=True,
            is_active=True,
        )
        user_1.set_password("N0rr1s")
        db.session.commit()
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
def users_by_campaigns(database, users, campaigns):
    with OpenBar():
        ubc_1 = model.UserByCampaign.new(
            user_id=users[0].id,
            campaign_id=campaigns[0].id,
        )
        ubc_2 = model.UserByCampaign.new(
            user_id=users[1].id,
            campaign_id=campaigns[1].id,
        )
        db.session.commit()
    return (ubc_1, ubc_2)


@pytest.fixture
def timeseries_cluster_groups(database):
    with OpenBar():
        ts_group_1 = model.TimeseriesClusterGroup.new(
            name="TS Group 1",
        )
        ts_group_2 = model.TimeseriesClusterGroup.new(
            name="TS Group 2",
        )
        db.session.commit()
    return (ts_group_1, ts_group_2)


@pytest.fixture
def timeseries_cluster_groups_by_users(database, timeseries_cluster_groups, users):
    with OpenBar():
        tscgbu_1 = model.TimeseriesClusterGroupByUser.new(
            timeseries_cluster_group_id=timeseries_cluster_groups[0].id,
            user_id=users[0].id,
        )
        tscgbu_2 = model.TimeseriesClusterGroupByUser.new(
            timeseries_cluster_group_id=timeseries_cluster_groups[1].id,
            user_id=users[1].id,
        )
        db.session.commit()
    return (tscgbu_1, tscgbu_2)


@pytest.fixture(params=[2])
def timeseries_clusters(request, database, timeseries_cluster_groups):
    with OpenBar():
        tsc_l = []
        for i in range(request.param):
            tsc_i = model.TimeseriesCluster(
                name=f"Timeseries cluster {i}",
                description=f"Test timeseries cluster #{i}",
                group=timeseries_cluster_groups[i % len(timeseries_cluster_groups)],
            )
            tsc_l.append(tsc_i)
        db.session.add_all(tsc_l)
        db.session.commit()
        return tsc_l


@pytest.fixture(params=[2])
def timeseries(request, database, timeseries_clusters):
    with OpenBar():
        ts_l = []
        for i in range(request.param):
            ts_i = model.Timeseries(
                cluster=timeseries_clusters[i % len(timeseries_clusters)],
                data_state_id=1,
            )
            ts_l.append(ts_i)
        db.session.add_all(ts_l)
        db.session.commit()
        return ts_l


@pytest.fixture
def timeseries_cluster_groups_by_campaigns(
    database, campaigns, timeseries_cluster_groups
):
    """Create timeseries groups x campaigns associations

    Example:
        campaigns = [C1, C2]
        timeseries groups = [TG1, TG2, TG3, TG4, TG5]
         timeseries x campaigns = [
            TG1 x C1,
            TG2 x C2,
            TG2 x C1,
            TG4 x C2,
            TG5 x C1,
        ]
    """
    with OpenBar():
        tbc_l = []
        for idx, tscg_i in enumerate(timeseries_cluster_groups):
            campaign = campaigns[idx % len(campaigns)]
            tbc = model.TimeseriesClusterGroupByCampaign.new(
                timeseries_cluster_group_id=tscg_i.id,
                campaign_id=campaign.id,
            )
            tbc_l.append(tbc)
        db.session.commit()
    return tbc_l


@pytest.fixture
def event_channels(database):
    with OpenBar():
        channel_1 = model.EventChannel.new(
            name="Channel 1",
        )
        channel_2 = model.EventChannel.new(
            name="Channel 2",
        )
        db.session.commit()
        return (channel_1, channel_2)


@pytest.fixture
def event_channels_by_campaigns(database, campaigns, event_channels):
    with OpenBar():
        ecc_1 = model.EventChannelByCampaign.new(
            event_channel_id=event_channels[0].id,
            campaign_id=campaigns[0].id,
        )
        ecc_2 = model.EventChannelByCampaign.new(
            event_channel_id=event_channels[1].id,
            campaign_id=campaigns[1].id,
        )
        db.session.commit()
    return (ecc_1, ecc_2)


@pytest.fixture
def event_channels_by_users(database, event_channels, users):
    with OpenBar():
        ecbu_1 = model.EventChannelByUser.new(
            event_channel_id=event_channels[0].id,
            user_id=users[0].id,
        )
        ecbu_2 = model.EventChannelByUser.new(
            event_channel_id=event_channels[1].id,
            user_id=users[1].id,
        )
        db.session.commit()
    return (ecbu_1, ecbu_2)


@pytest.fixture
def events(database, event_channels):
    with OpenBar():
        ts_event_1 = model.Event.new(
            channel_id=event_channels[0].id,
            timestamp=dt.datetime(2020, 1, 1, tzinfo=dt.timezone.utc),
            category="observation_missing",
            source="src",
            level="ERROR",
            state="NEW",
        )
        ts_event_2 = model.Event.new(
            channel_id=event_channels[1].id,
            timestamp=dt.datetime(2020, 1, 15, tzinfo=dt.timezone.utc),
            category="observation_missing",
            source="src",
            level="WARNING",
            state="ONGOING",
        )
        db.session.commit()
    return (ts_event_1, ts_event_2)
