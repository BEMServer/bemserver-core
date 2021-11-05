""" Global conftest"""
import datetime as dt

import pytest
from pytest_postgresql import factories as ppf

from bemserver_core.database import db
from bemserver_core.authentication import CurrentUser, OpenBar
from bemserver_core import model
from bemserver_core.testutils import setup_db


postgresql_proc = ppf.postgresql_proc(
    postgres_options="-c shared_preload_libraries='timescaledb'"
)
postgresql = ppf.postgresql('postgresql_proc')


@pytest.fixture
def database(postgresql):
    yield from setup_db(postgresql)


@pytest.fixture
def as_admin():
    """Set an admin user for the test"""
    with OpenBar(), CurrentUser(
        model.User(
            name="Chuck",
            email="chuck@test.com",
            is_admin=True,
            is_active=True
        )
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


def make_timeseries(nb_ts):
    ts_l = []
    for i in range(nb_ts):
        ts_i = model.Timeseries(
            name=f"Timeseries {i}",
            description=f"Test timeseries #{i}",
        )
        ts_l.append(ts_i)
        db.session.add(ts_i)
    db.session.commit()
    return ts_l


@pytest.fixture
def timeseries(database):
    return make_timeseries(4)


@pytest.fixture(params=[{}])
def timeseries_data(request, database):

    param = request.param

    nb_ts = param.get("nb_ts", 1)
    nb_tsd = param.get("nb_tsd", 24 * 100)

    ts_l = make_timeseries(nb_ts)

    for ts_i in ts_l:
        start_dt = dt.datetime(2020, 1, 1, tzinfo=dt.timezone.utc)
        for i in range(nb_tsd):
            timestamp = start_dt + dt.timedelta(hours=i)
            db.session.add(
                model.TimeseriesData(
                    timestamp=timestamp,
                    timeseries=ts_i,
                    value=i
                )
            )

    db.session.commit()

    return [
        (ts.id, nb_tsd, start_dt, start_dt + dt.timedelta(hours=nb_tsd))
        for ts in ts_l
    ]


@pytest.fixture
def timeseries_by_campaigns(database, campaigns, timeseries):
    with OpenBar():
        tbc_1 = model.TimeseriesByCampaign(
            timeseries_id=timeseries[0].id,
            campaign_id=campaigns[0].id,
        )
        db.session.add(tbc_1)
        tbc_2 = model.TimeseriesByCampaign(
            timeseries_id=timeseries[1].id,
            campaign_id=campaigns[1].id,
        )
        db.session.add(tbc_2)
        tbc_3 = model.TimeseriesByCampaign(
            timeseries_id=timeseries[2].id,
            campaign_id=campaigns[0].id,
        )
        db.session.add(tbc_3)
        tbc_4 = model.TimeseriesByCampaign(
            timeseries_id=timeseries[3].id,
            campaign_id=campaigns[1].id,
        )
        db.session.add(tbc_4)
        db.session.commit()
    return (tbc_1, tbc_2, tbc_3, tbc_4)


@pytest.fixture
def timeseries_by_campaigns_by_users(database, users, timeseries_by_campaigns):
    with OpenBar():
        tbcbu_1 = model.TimeseriesByCampaignByUser(
            user_id=users[0].id,
            timeseries_by_campaign_id=timeseries_by_campaigns[0].id,
        )
        db.session.add(tbcbu_1)
        tbcbu_2 = model.TimeseriesByCampaignByUser(
            user_id=users[1].id,
            timeseries_by_campaign_id=timeseries_by_campaigns[1].id,
        )
        db.session.add(tbcbu_2)
        db.session.commit()
    return (tbcbu_1, tbcbu_2)
