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
        user_1 = model.User(
            name="Chuck",
            email="chuck@test.com",
            is_admin=True,
            is_active=True,
        )
        user_1.set_password("N0rr1s")
        db.session.add(user_1)
        db.session.commit()
        user_2 = model.User(
            name="John",
            email="john@test.com",
            is_admin=False,
            is_active=True,
        )
        user_2.set_password("D0e")
        db.session.add(user_2)
        db.session.commit()
    return (user_1, user_2)


@pytest.fixture
def campaigns(database):
    with OpenBar():
        campaign_1 = model.Campaign(
            name="Campaign 1",
            start_time=dt.datetime(2020, 1, 1, tzinfo=dt.timezone.utc),
            end_time=dt.datetime(2020, 2, 1, tzinfo=dt.timezone.utc),
        )
        db.session.add(campaign_1)
        campaign_2 = model.Campaign(
            name="Campaign 2",
            start_time=dt.datetime(2020, 1, 1, tzinfo=dt.timezone.utc),
            end_time=dt.datetime(2020, 2, 1, tzinfo=dt.timezone.utc),
        )
        db.session.add(campaign_2)
        db.session.commit()
    return (campaign_1, campaign_2)


@pytest.fixture
def users_by_campaigns(database, users, campaigns):
    with OpenBar():
        ubc_1 = model.UserByCampaign(
            user_id=users[0].id,
            campaign_id=campaigns[0].id,
        )
        db.session.add(ubc_1)
        ubc_2 = model.UserByCampaign(
            user_id=users[1].id,
            campaign_id=campaigns[1].id,
        )
        db.session.add(ubc_2)
        db.session.commit()
    return (ubc_1, ubc_2)


@pytest.fixture
def timeseries_groups(database):
    with OpenBar():
        ts_group_1 = model.TimeseriesGroup(
            name="TS Group 1",
        )
        db.session.add(ts_group_1)
        ts_group_2 = model.TimeseriesGroup(
            name="TS Group 2",
        )
        db.session.add(ts_group_2)
        db.session.commit()
    return (ts_group_1, ts_group_2)


@pytest.fixture(params=[2])
def timeseries(request, database, timeseries_groups):
    ts_l = []
    for i in range(request.param):
        ts_i = model.Timeseries(
            name=f"Timeseries {i}",
            description=f"Test timeseries #{i}",
            group=timeseries_groups[i % len(timeseries_groups)],
        )
        ts_l.append(ts_i)
    db.session.add_all(ts_l)
    db.session.commit()
    return ts_l


@pytest.fixture
def timeseries_groups_by_campaigns(database, campaigns, timeseries_groups):
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
        for idx, tg_i in enumerate(timeseries_groups):
            campaign = campaigns[idx % len(campaigns)]
            tbc = model.TimeseriesGroupByCampaign(
                timeseries_group_id=tg_i.id,
                campaign_id=campaign.id,
            )
            tbc_l.append(tbc)
        db.session.add_all(tbc_l)
        db.session.commit()
    return tbc_l


@pytest.fixture
@pytest.mark.usefixtures("database")
def channels():
    channel_1 = model.EventChannel(
        name="Channel 1",
    )
    db.session.add(channel_1)
    channel_2 = model.EventChannel(
        name="Channel 2",
    )
    db.session.add(channel_2)
    db.session.commit()
    return (channel_1, channel_2)


@pytest.fixture
def event_channels_by_campaigns(database, campaigns, channels):
    with OpenBar():
        ecc_1 = model.EventChannelByCampaign(
            event_channel_id=channels[0].id,
            campaign_id=campaigns[0].id,
        )
        db.session.add(ecc_1)
        ecc_2 = model.EventChannelByCampaign(
            event_channel_id=channels[1].id,
            campaign_id=campaigns[1].id,
        )
        db.session.add(ecc_2)
        db.session.commit()
    return (ecc_1, ecc_2)


@pytest.fixture
def timeseries_events(database, channels):
    with OpenBar():
        ts_event_1 = model.TimeseriesEvent(
            channel_id=channels[0].id,
            timestamp=dt.datetime(2020, 1, 1, tzinfo=dt.timezone.utc),
            category="observation_missing",
            source="src",
            level="ERROR",
            state="NEW",
        )
        db.session.add(ts_event_1)
        ts_event_2 = model.TimeseriesEvent(
            channel_id=channels[1].id,
            timestamp=dt.datetime(2020, 1, 15, tzinfo=dt.timezone.utc),
            category="observation_missing",
            source="src",
            level="WARNING",
            state="ONGOING",
        )
        db.session.add(ts_event_2)
        db.session.commit()
    return (ts_event_1, ts_event_2)
