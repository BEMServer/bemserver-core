"""Timeseries CSV I/O tests"""
# pylint: disable=invalid-name
import io
import datetime as dt

import pytest

from bemserver_core.model import TimeseriesData
from bemserver_core.csv_io import tscsvio
from bemserver_core.database import db
from bemserver_core.exceptions import TimeseriesCSVIOError


class TestTimeseriesCSVIO:

    @pytest.mark.parametrize(
            'timeseries_data',
            ({"nb_ts": 2, "nb_tsd": 0}, ),
            indirect=True
    )
    @pytest.mark.parametrize('mode', ('str', 'textiobase'))
    @pytest.mark.usefixtures("as_admin")
    def test_timeseries_csv_io_import_csv(self, timeseries_data, mode):

        ts_0_id, _, _, _ = timeseries_data[0]
        ts_1_id, _, _, _ = timeseries_data[1]

        assert not db.session.query(TimeseriesData).all()

        csv_file = (
            f"Datetime,{ts_0_id},{ts_1_id}\n"
            "2020-01-01T00:00:00+00:00,0,10\n"
            "2020-01-01T01:00:00+00:00,1,11\n"
            "2020-01-01T02:00:00+00:00,2,12\n"
            "2020-01-01T03:00:00+00:00,3,13\n"
        )

        if mode == "textiobase":
            csv_file = io.StringIO(csv_file)

        tscsvio.import_csv(csv_file)

        data = db.session.query(
            TimeseriesData.timestamp,
            TimeseriesData.timeseries_id,
            TimeseriesData.value,
        ).order_by(
            TimeseriesData.timeseries_id,
            TimeseriesData.timestamp,
        ).all()

        timestamps = [
            dt.datetime(2020, 1, 1, i, tzinfo=dt.timezone.utc)
            for i in range(4)
        ]

        expected = [
                (timestamp, ts_0_id, float(idx))
                for idx, timestamp in enumerate(timestamps)
            ] + [
                (timestamp, ts_1_id, float(idx) + 10)
                for idx, timestamp in enumerate(timestamps)
            ]

        assert data == expected

    @pytest.mark.parametrize(
            'timeseries_data',
            ({"nb_ts": 1, "nb_tsd": 0}, ),
            indirect=True
    )
    @pytest.mark.parametrize(
        "csv_file",
        (
            "",
            "Dummy,\n",
            "Datetime,1324564",
            "Datetime,1\n2020-01-01T00:00:00+00:00",
            "Datetime,1\n2020-01-01T00:00:00+00:00,",
            "Datetime,1\n2020-01-01T00:00:00+00:00,a",
        )
    )
    @pytest.mark.usefixtures("timeseries_data")
    @pytest.mark.usefixtures("as_admin")
    def test_timeseries_csv_io_import_csv_error(self, csv_file):
        with pytest.raises(TimeseriesCSVIOError):
            tscsvio.import_csv(io.StringIO(csv_file))

    @pytest.mark.parametrize(
            'timeseries_data',
            ({"nb_ts": 4, "nb_tsd": 0}, ),
            indirect=True
    )
    @pytest.mark.usefixtures("as_admin")
    def test_timeseries_csv_io_export_csv(self, timeseries_data):

        ts_0_id, _, _, _ = timeseries_data[0]
        ts_1_id, _, _, _ = timeseries_data[1]
        ts_2_id, _, _, _ = timeseries_data[2]
        ts_3_id, _, _, _ = timeseries_data[3]

        start_dt = dt.datetime(2020, 1, 1, tzinfo=dt.timezone.utc)
        end_dt = start_dt + dt.timedelta(hours=3)

        # Create DB data
        for i in range(3):
            timestamp = start_dt + dt.timedelta(hours=i)
            db.session.add(
                TimeseriesData(
                    timestamp=timestamp,
                    timeseries_id=ts_0_id,
                    value=i
                )
            )
        for i in range(2):
            timestamp = start_dt + dt.timedelta(hours=i)
            db.session.add(
                TimeseriesData(
                    timestamp=timestamp,
                    timeseries_id=ts_3_id,
                    value=10 + 2 * i
                )
            )
        db.session.commit()

        # Export CSV
        data = tscsvio.export_csv(
            start_dt, end_dt, (ts_0_id, ts_1_id, ts_3_id)
        )

        assert data == (
            f"Datetime,{ts_0_id},{ts_1_id},{ts_3_id}\n"
            "2020-01-01T00:00:00+0000,0.0,,10.0\n"
            "2020-01-01T01:00:00+0000,1.0,,12.0\n"
            "2020-01-01T02:00:00+0000,2.0,,\n"
        )

    @pytest.mark.parametrize(
            'timeseries_data',
            ({"nb_ts": 4, "nb_tsd": 0}, ),
            indirect=True
    )
    @pytest.mark.usefixtures("as_admin")
    def test_timeseries_csv_io_export_csv_bucket(self, timeseries_data):

        ts_0_id, _, _, _ = timeseries_data[0]
        ts_1_id, _, _, _ = timeseries_data[1]
        ts_2_id, _, _, _ = timeseries_data[2]
        ts_3_id, _, _, _ = timeseries_data[3]

        start_dt = dt.datetime(2020, 1, 1, tzinfo=dt.timezone.utc)
        end_dt = start_dt + dt.timedelta(hours=24*3)

        # Create DB data
        for i in range(24 * 3):
            timestamp = start_dt + dt.timedelta(hours=i)
            db.session.add(
                TimeseriesData(
                    timestamp=timestamp,
                    timeseries_id=ts_0_id,
                    value=i
                )
            )
        for i in range(24 * 2):
            timestamp = start_dt + dt.timedelta(hours=i)
            db.session.add(
                TimeseriesData(
                    timestamp=timestamp,
                    timeseries_id=ts_3_id,
                    value=10 + 2 * i
                )
            )
        db.session.commit()

        # Export CSV: UTC avg
        data = tscsvio.export_csv_bucket(
            start_dt, end_dt, [ts_0_id, ts_1_id, ts_3_id], "1 day"
        )
        assert data == (
            f"Datetime,{ts_0_id},{ts_1_id},{ts_3_id}\n"
            "2020-01-01T00:00:00+0000,11.5,,33.0\n"
            "2020-01-02T00:00:00+0000,35.5,,81.0\n"
            "2020-01-03T00:00:00+0000,59.5,,\n"
        )

        # Export CSV: local TZ avg
        data = tscsvio.export_csv_bucket(
            start_dt, end_dt, (ts_0_id, ts_1_id, ts_3_id), "P1D",
            timezone="Europe/Paris",
        )
        assert data == (
            f"Datetime,{ts_0_id},{ts_1_id},{ts_3_id}\n"
            "2019-12-31T23:00:00+0000,11.0,,32.0\n"
            "2020-01-01T23:00:00+0000,34.5,,79.0\n"
            "2020-01-02T23:00:00+0000,58.5,,104.0\n"
            "2020-01-03T23:00:00+0000,71.0,,\n"
        )

        # Export CSV: UTC sum
        data = tscsvio.export_csv_bucket(
            start_dt, end_dt, [ts_0_id, ts_1_id, ts_3_id], "1 day",
            aggregation="sum",
        )
        assert data == (
            f"Datetime,{ts_0_id},{ts_1_id},{ts_3_id}\n"
            "2020-01-01T00:00:00+0000,276.0,,792.0\n"
            "2020-01-02T00:00:00+0000,852.0,,1944.0\n"
            "2020-01-03T00:00:00+0000,1428.0,,\n"
        )

        # Export CSV: UTC min
        data = tscsvio.export_csv_bucket(
            start_dt, end_dt, [ts_0_id, ts_1_id, ts_3_id], "1 day",
            aggregation="min",
        )
        assert data == (
            f"Datetime,{ts_0_id},{ts_1_id},{ts_3_id}\n"
            "2020-01-01T00:00:00+0000,0.0,,10.0\n"
            "2020-01-02T00:00:00+0000,24.0,,58.0\n"
            "2020-01-03T00:00:00+0000,48.0,,\n"
        )

        # Export CSV: UTC max
        data = tscsvio.export_csv_bucket(
            start_dt, end_dt, [ts_0_id, ts_1_id, ts_3_id], "1 day",
            aggregation="max",
        )
        assert data == (
            f"Datetime,{ts_0_id},{ts_1_id},{ts_3_id}\n"
            "2020-01-01T00:00:00+0000,23.0,,56.0\n"
            "2020-01-02T00:00:00+0000,47.0,,104.0\n"
            "2020-01-03T00:00:00+0000,71.0,,\n"
        )

        # Export CSV: invalid aggregation
        with pytest.raises(ValueError):
            tscsvio.export_csv_bucket(
                start_dt, end_dt, [ts_0_id, ts_1_id, ts_3_id], "1 day",
                aggregation="lol",
            )
