"""Timeseries CSV I/O tests"""
import io
import datetime as dt

import pytest

from bemserver_core.model import TimeseriesData, TimeseriesByDataState
from bemserver_core.input_output import tsdcsvio
from bemserver_core.database import db
from bemserver_core.authorization import CurrentUser
from bemserver_core.exceptions import (
    TimeseriesDataCSVIOError,
    BEMServerAuthorizationError,
)


class TestTimeseriesDataCSVIO:
    @pytest.mark.parametrize("timeseries", (3,), indirect=True)
    @pytest.mark.parametrize("mode", ("str", "textiobase"))
    def test_timeseries_data_io_import_csv_as_admin(self, users, timeseries, mode):
        admin_user = users[0]
        assert admin_user.is_admin
        ts_0 = timeseries[0]
        ts_2 = timeseries[2]
        ds_id = 1

        assert not db.session.query(TimeseriesByDataState).all()
        assert not db.session.query(TimeseriesData).all()

        csv_file = (
            f"Datetime,{ts_0.id},{ts_2.id}\n"
            "2020-01-01T00:00:00+00:00,0,10\n"
            "2020-01-01T01:00:00+00:00,1,11\n"
            "2020-01-01T02:00:00+00:00,2,12\n"
            "2020-01-01T03:00:00+00:00,3,13\n"
        )

        if mode == "textiobase":
            csv_file = io.StringIO(csv_file)

        with CurrentUser(admin_user):
            tsdcsvio.import_csv(csv_file, ds_id)

        # Check TSBDS are correctly auto-created
        tsbds_l = (
            db.session.query(TimeseriesByDataState)
            .order_by(TimeseriesByDataState.id)
            .all()
        )
        assert all(tsbds.data_state_id == ds_id for tsbds in tsbds_l)
        tsbds_0 = tsbds_l[0]
        tsbds_2 = tsbds_l[1]
        assert tsbds_0.timeseries == ts_0
        assert tsbds_2.timeseries == ts_2

        # Check timeseries data is written
        data = (
            db.session.query(
                TimeseriesData.timestamp,
                TimeseriesData.timeseries_by_data_state_id,
                TimeseriesData.value,
            )
            .order_by(
                TimeseriesData.timeseries_by_data_state_id,
                TimeseriesData.timestamp,
            )
            .all()
        )

        timestamps = [
            dt.datetime(2020, 1, 1, i, tzinfo=dt.timezone.utc) for i in range(4)
        ]

        expected = [
            (timestamp, tsbds_0.id, float(idx))
            for idx, timestamp in enumerate(timestamps)
        ] + [
            (timestamp, tsbds_2.id, float(idx) + 10)
            for idx, timestamp in enumerate(timestamps)
        ]

        assert data == expected

    @pytest.mark.parametrize("timeseries", (3,), indirect=True)
    @pytest.mark.usefixtures("users_by_user_groups")
    @pytest.mark.usefixtures("user_groups_by_campaign_scopes")
    def test_timeseries_data_io_import_csv_as_user(self, users, timeseries):
        user_1 = users[1]
        assert not user_1.is_admin
        ts_0 = timeseries[0]
        ts_1 = timeseries[1]
        ts_2 = timeseries[2]
        ds_id = 1

        assert not db.session.query(TimeseriesData).all()

        csv_file = (
            f"Datetime,{ts_0.id},{ts_2.id}\n"
            "2020-01-01T00:00:00+00:00,0,10\n"
            "2020-01-01T01:00:00+00:00,1,11\n"
            "2020-01-01T02:00:00+00:00,2,12\n"
            "2020-01-01T03:00:00+00:00,3,13\n"
        )

        with CurrentUser(user_1):
            with pytest.raises(BEMServerAuthorizationError):
                tsdcsvio.import_csv(csv_file, ds_id)

        csv_file = (
            f"Datetime,{ts_1.id}\n"
            "2020-01-01T00:00:00+00:00,0\n"
            "2020-01-01T01:00:00+00:00,1\n"
            "2020-01-01T02:00:00+00:00,2\n"
            "2020-01-01T03:00:00+00:00,3\n"
        )

        with CurrentUser(user_1):
            tsdcsvio.import_csv(csv_file, ds_id)

    @pytest.mark.parametrize(
        "csv_file",
        (
            "",
            "Dummy,\n",
            "Datetime,1324564",
            "Datetime,1\n2020-01-01T00:00:00+00:00",
            "Datetime,1\n2020-01-01T00:00:00+00:00,",
            "Datetime,1\n2020-01-01T00:00:00+00:00,a",
        ),
    )
    @pytest.mark.usefixtures("timeseries")
    def test_timeseries_data_io_import_csv_error(self, users, csv_file):
        admin_user = users[0]
        assert admin_user.is_admin
        ds_id = 1

        with CurrentUser(admin_user):
            with pytest.raises(TimeseriesDataCSVIOError):
                tsdcsvio.import_csv(io.StringIO(csv_file), ds_id)

    @pytest.mark.usefixtures("timeseries")
    def test_timeseries_data_io_import_csv_data_state_error(self, users, timeseries):
        admin_user = users[0]
        assert admin_user.is_admin
        ts_1 = timeseries[1]
        dummy_ds_id = 42

        csv_file = (
            f"Datetime,{ts_1.id}\n"
            "2020-01-01T00:00:00+00:00,0\n"
            "2020-01-01T01:00:00+00:00,1\n"
            "2020-01-01T02:00:00+00:00,2\n"
            "2020-01-01T03:00:00+00:00,3\n"
        )

        with CurrentUser(admin_user):
            with pytest.raises(TimeseriesDataCSVIOError):
                tsdcsvio.import_csv(io.StringIO(csv_file), dummy_ds_id)

    @pytest.mark.parametrize("timeseries", (5,), indirect=True)
    @pytest.mark.parametrize("timeseries_by_data_states", (5,), indirect=True)
    def test_timeseries_data_io_export_csv_as_admin(
        self, users, timeseries, timeseries_by_data_states
    ):
        admin_user = users[0]
        assert admin_user.is_admin
        ts_0 = timeseries[0]
        ts_2 = timeseries[2]
        ts_4 = timeseries[4]
        tsbds_0 = timeseries_by_data_states[0]
        tsbds_4 = timeseries_by_data_states[4]
        dummy_ts_id = 42

        ds_id = 1
        assert all(tsbds.data_state_id == ds_id for tsbds in timeseries_by_data_states)

        start_dt = dt.datetime(2020, 1, 1, tzinfo=dt.timezone.utc)
        end_dt = start_dt + dt.timedelta(hours=3)

        # Create DB data
        for i in range(3):
            timestamp = start_dt + dt.timedelta(hours=i)
            db.session.add(
                TimeseriesData(
                    timestamp=timestamp, timeseries_by_data_state_id=tsbds_0.id, value=i
                )
            )
        for i in range(2):
            timestamp = start_dt + dt.timedelta(hours=i)
            db.session.add(
                TimeseriesData(
                    timestamp=timestamp,
                    timeseries_by_data_state_id=tsbds_4.id,
                    value=10 + 2 * i,
                )
            )
        db.session.commit()

        with CurrentUser(admin_user):
            data = tsdcsvio.export_csv(
                start_dt, end_dt, (ts_0.id, ts_2.id, ts_4.id), ds_id
            )

            assert data == (
                f"Datetime,{ts_0.id},{ts_2.id},{ts_4.id}\n"
                "2020-01-01T00:00:00+0000,0.0,,10.0\n"
                "2020-01-01T01:00:00+0000,1.0,,12.0\n"
                "2020-01-01T02:00:00+0000,2.0,,\n"
            )

            # Unknown TS ID
            with pytest.raises(TimeseriesDataCSVIOError):
                tsdcsvio.export_csv(
                    start_dt, end_dt, (ts_0.id, ts_2.id, ts_4.id, dummy_ts_id), ds_id
                )

    @pytest.mark.parametrize("timeseries", (5,), indirect=True)
    @pytest.mark.parametrize("timeseries_by_data_states", (5,), indirect=True)
    @pytest.mark.usefixtures("users_by_user_groups")
    @pytest.mark.usefixtures("user_groups_by_campaign_scopes")
    def test_timeseries_data_io_export_csv_as_user(
        self, users, timeseries, timeseries_by_data_states
    ):
        user_1 = users[1]
        assert not user_1.is_admin
        ts_0 = timeseries[0]
        ts_1 = timeseries[1]
        ts_2 = timeseries[2]
        ts_3 = timeseries[3]
        ts_4 = timeseries[4]
        tsbds_1 = timeseries_by_data_states[1]
        tsbds_3 = timeseries_by_data_states[3]

        ds_id = 1
        assert all(tsbds.data_state_id == ds_id for tsbds in timeseries_by_data_states)

        start_dt = dt.datetime(2020, 1, 1, tzinfo=dt.timezone.utc)
        end_dt = start_dt + dt.timedelta(hours=3)

        # Create DB data
        for i in range(3):
            timestamp = start_dt + dt.timedelta(hours=i)
            db.session.add(
                TimeseriesData(
                    timestamp=timestamp, timeseries_by_data_state_id=tsbds_1.id, value=i
                )
            )
        for i in range(2):
            timestamp = start_dt + dt.timedelta(hours=i)
            db.session.add(
                TimeseriesData(
                    timestamp=timestamp,
                    timeseries_by_data_state_id=tsbds_3.id,
                    value=10 + 2 * i,
                )
            )
        db.session.commit()

        with CurrentUser(user_1):
            with pytest.raises(BEMServerAuthorizationError):
                data = tsdcsvio.export_csv(
                    start_dt, end_dt, (ts_0.id, ts_2.id, ts_4.id), ds_id
                )

        with CurrentUser(user_1):
            data = tsdcsvio.export_csv(start_dt, end_dt, (ts_1.id, ts_3.id), ds_id)

            assert data == (
                f"Datetime,{ts_1.id},{ts_3.id}\n"
                "2020-01-01T00:00:00+0000,0.0,10.0\n"
                "2020-01-01T01:00:00+0000,1.0,12.0\n"
                "2020-01-01T02:00:00+0000,2.0,\n"
            )

    @pytest.mark.usefixtures("timeseries_by_data_states")
    def test_timeseries_data_io_export_csv_data_state_error(self, users, timeseries):
        admin_user = users[0]
        assert admin_user.is_admin
        ts_0 = timeseries[0]
        dummy_ds_id = 42

        start_dt = dt.datetime(2020, 1, 1, tzinfo=dt.timezone.utc)
        end_dt = start_dt + dt.timedelta(hours=3)

        with CurrentUser(admin_user):
            with pytest.raises(TimeseriesDataCSVIOError):
                tsdcsvio.export_csv(start_dt, end_dt, (ts_0.id,), dummy_ds_id)

    @pytest.mark.parametrize("timeseries", (5,), indirect=True)
    @pytest.mark.parametrize("timeseries_by_data_states", (5,), indirect=True)
    def test_timeseries_data_io_export_csv_bucket_as_admin(
        self, users, timeseries, timeseries_by_data_states
    ):
        admin_user = users[0]
        assert admin_user.is_admin
        ts_0 = timeseries[0]
        ts_2 = timeseries[2]
        ts_4 = timeseries[4]
        tsbds_0 = timeseries_by_data_states[0]
        tsbds_4 = timeseries_by_data_states[4]
        dummy_ts_id = 42

        ds_id = 1
        assert all(tsbds.data_state_id == ds_id for tsbds in timeseries_by_data_states)

        start_dt = dt.datetime(2020, 1, 1, tzinfo=dt.timezone.utc)
        end_dt = start_dt + dt.timedelta(hours=24 * 3)

        # Create DB data
        for i in range(24 * 3):
            timestamp = start_dt + dt.timedelta(hours=i)
            db.session.add(
                TimeseriesData(
                    timestamp=timestamp, timeseries_by_data_state_id=tsbds_0.id, value=i
                )
            )
        for i in range(24 * 2):
            timestamp = start_dt + dt.timedelta(hours=i)
            db.session.add(
                TimeseriesData(
                    timestamp=timestamp,
                    timeseries_by_data_state_id=tsbds_4.id,
                    value=10 + 2 * i,
                )
            )
        db.session.commit()

        with CurrentUser(admin_user):
            # Export CSV: UTC avg
            data = tsdcsvio.export_csv_bucket(
                start_dt, end_dt, [ts_0.id, ts_2.id, ts_4.id], ds_id, "1 day"
            )
            assert data == (
                f"Datetime,{ts_0.id},{ts_2.id},{ts_4.id}\n"
                "2020-01-01T00:00:00+0000,11.5,,33.0\n"
                "2020-01-02T00:00:00+0000,35.5,,81.0\n"
                "2020-01-03T00:00:00+0000,59.5,,\n"
            )

            # Export CSV: local TZ avg
            data = tsdcsvio.export_csv_bucket(
                start_dt,
                end_dt,
                (ts_0.id, ts_2.id, ts_4.id),
                ds_id,
                "P1D",
                timezone="Europe/Paris",
            )
            assert data == (
                f"Datetime,{ts_0.id},{ts_2.id},{ts_4.id}\n"
                "2019-12-31T23:00:00+0000,11.0,,32.0\n"
                "2020-01-01T23:00:00+0000,34.5,,79.0\n"
                "2020-01-02T23:00:00+0000,58.5,,104.0\n"
                "2020-01-03T23:00:00+0000,71.0,,\n"
            )

            # Export CSV: UTC sum
            data = tsdcsvio.export_csv_bucket(
                start_dt,
                end_dt,
                [ts_0.id, ts_2.id, ts_4.id],
                ds_id,
                "1 day",
                aggregation="sum",
            )
            assert data == (
                f"Datetime,{ts_0.id},{ts_2.id},{ts_4.id}\n"
                "2020-01-01T00:00:00+0000,276.0,,792.0\n"
                "2020-01-02T00:00:00+0000,852.0,,1944.0\n"
                "2020-01-03T00:00:00+0000,1428.0,,\n"
            )

            # Export CSV: UTC min
            data = tsdcsvio.export_csv_bucket(
                start_dt,
                end_dt,
                [ts_0.id, ts_2.id, ts_4.id],
                ds_id,
                "1 day",
                aggregation="min",
            )
            assert data == (
                f"Datetime,{ts_0.id},{ts_2.id},{ts_4.id}\n"
                "2020-01-01T00:00:00+0000,0.0,,10.0\n"
                "2020-01-02T00:00:00+0000,24.0,,58.0\n"
                "2020-01-03T00:00:00+0000,48.0,,\n"
            )

            # Export CSV: UTC max
            data = tsdcsvio.export_csv_bucket(
                start_dt,
                end_dt,
                [ts_0.id, ts_2.id, ts_4.id],
                ds_id,
                "1 day",
                aggregation="max",
            )
            assert data == (
                f"Datetime,{ts_0.id},{ts_2.id},{ts_4.id}\n"
                "2020-01-01T00:00:00+0000,23.0,,56.0\n"
                "2020-01-02T00:00:00+0000,47.0,,104.0\n"
                "2020-01-03T00:00:00+0000,71.0,,\n"
            )

            # Export CSV: invalid aggregation
            with pytest.raises(ValueError):
                tsdcsvio.export_csv_bucket(
                    start_dt,
                    end_dt,
                    [ts_0.id, ts_2.id, ts_4.id],
                    ds_id,
                    "1 day",
                    aggregation="lol",
                )

            # Unknown TS ID
            with pytest.raises(TimeseriesDataCSVIOError):
                tsdcsvio.export_csv_bucket(
                    start_dt,
                    end_dt,
                    [ts_0.id, ts_2.id, ts_4.id, dummy_ts_id],
                    ds_id,
                    "1 day",
                )

    @pytest.mark.parametrize("timeseries", (5,), indirect=True)
    @pytest.mark.parametrize("timeseries_by_data_states", (5,), indirect=True)
    @pytest.mark.usefixtures("users_by_user_groups")
    @pytest.mark.usefixtures("user_groups_by_campaign_scopes")
    def test_timeseries_data_io_export_csv_bucket_as_user(
        self, users, timeseries, timeseries_by_data_states
    ):
        user_1 = users[1]
        assert not user_1.is_admin
        ts_0 = timeseries[0]
        ts_1 = timeseries[1]
        ts_2 = timeseries[2]
        ts_3 = timeseries[3]
        ts_4 = timeseries[4]
        tsbds_1 = timeseries_by_data_states[1]
        tsbds_3 = timeseries_by_data_states[3]

        ds_id = 1
        assert all(tsbds.data_state_id == ds_id for tsbds in timeseries_by_data_states)

        start_dt = dt.datetime(2020, 1, 1, tzinfo=dt.timezone.utc)
        end_dt = start_dt + dt.timedelta(hours=24 * 3)

        # Create DB data
        for i in range(24 * 3):
            timestamp = start_dt + dt.timedelta(hours=i)
            db.session.add(
                TimeseriesData(
                    timestamp=timestamp, timeseries_by_data_state_id=tsbds_1.id, value=i
                )
            )
        for i in range(24 * 2):
            timestamp = start_dt + dt.timedelta(hours=i)
            db.session.add(
                TimeseriesData(
                    timestamp=timestamp,
                    timeseries_by_data_state_id=tsbds_3.id,
                    value=10 + 2 * i,
                )
            )
        db.session.commit()

        with CurrentUser(user_1):
            with pytest.raises(BEMServerAuthorizationError):
                data = tsdcsvio.export_csv_bucket(
                    start_dt, end_dt, [ts_0.id, ts_2.id, ts_4.id], ds_id, "1 day"
                )

        with CurrentUser(user_1):
            # Export CSV: UTC avg
            data = tsdcsvio.export_csv_bucket(
                start_dt, end_dt, [ts_1.id, ts_3.id], ds_id, "1 day"
            )
            assert data == (
                f"Datetime,{ts_1.id},{ts_3.id}\n"
                "2020-01-01T00:00:00+0000,11.5,33.0\n"
                "2020-01-02T00:00:00+0000,35.5,81.0\n"
                "2020-01-03T00:00:00+0000,59.5,\n"
            )

    @pytest.mark.usefixtures("timeseries_by_data_states")
    def test_timeseries_data_io_export_csv_bucket_data_state_error(
        self, users, timeseries
    ):
        admin_user = users[0]
        assert admin_user.is_admin
        ts_0 = timeseries[0]
        dummy_ds_id = 42

        start_dt = dt.datetime(2020, 1, 1, tzinfo=dt.timezone.utc)
        end_dt = start_dt + dt.timedelta(hours=3)

        with CurrentUser(admin_user):
            with pytest.raises(TimeseriesDataCSVIOError):
                tsdcsvio.export_csv_bucket(
                    start_dt, end_dt, (ts_0.id,), dummy_ds_id, "1 day"
                )
