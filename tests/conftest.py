""" Global conftest"""
import datetime as dt

import pytest
from pytest_postgresql import factories as ppf

from bemserver_core.database import db
from bemserver_core import model


postgresql_proc = ppf.postgresql_proc(
    postgres_options="-c shared_preload_libraries='timescaledb'"
)
postgresql = ppf.postgresql('postgresql_proc')


@pytest.fixture
def database(postgresql):

    db_url = (
        "postgresql+psycopg2://"
        f"{postgresql.info.user}:{postgresql.info.password}"
        f"@{postgresql.info.host}:{postgresql.info.port}/"
        f"{postgresql.info.dbname}"
    )
    db.set_db_url(db_url)

    with db.session() as session:
        session.execute("CREATE EXTENSION IF NOT EXISTS timescaledb;")
        session.commit()

    db.create_all()
    yield db
    db.session.remove()
    # Destroy DB engine, mainly for threaded code (as MQTT service).
    db.dispose()


@pytest.fixture(params=[{}])
def timeseries_data(request, database):

    param = request.param

    nb_ts = param.get("nb_ts", 1)
    nb_tsd = param.get("nb_tsd", 24 * 100)

    ts_l = []

    for i in range(nb_ts):
        ts_i = model.Timeseries(
            name=f"Timeseries {i}",
            description=f"Test timeseries #{i}",
        )
        db.session.add(ts_i)

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

        ts_l.append(ts_i)

    db.session.commit()

    return [
        (ts.id, nb_tsd, start_dt, start_dt + dt.timedelta(hours=nb_tsd))
        for ts in ts_l
    ]
