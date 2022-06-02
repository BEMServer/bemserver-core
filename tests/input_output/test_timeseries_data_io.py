"""Timeseries CSV I/O tests"""
import io
import datetime as dt

import pytest

from bemserver_core.model import (
    TimeseriesData,
    TimeseriesDataState,
    TimeseriesByDataState,
)
from bemserver_core.input_output import tsdcsvio
from bemserver_core.database import db
from bemserver_core.authorization import CurrentUser, OpenBar
from bemserver_core.exceptions import (
    BEMServerAuthorizationError,
    TimeseriesDataIOUnknownDataStateError,
    TimeseriesDataIOUnknownTimeseriesError,
    TimeseriesDataIOInvalidAggregationError,
    TimeseriesDataCSVIOError,
)


class TestTimeseriesDataCSVIO:
    @pytest.mark.parametrize("timeseries", (3,), indirect=True)
    @pytest.mark.parametrize("mode", ("str", "textiobase"))
    @pytest.mark.parametrize("for_campaign", (True, False))
    def test_timeseries_data_io_import_csv_as_admin(
        self, users, campaigns, timeseries, mode, for_campaign
    ):
        admin_user = users[0]
        assert admin_user.is_admin
        ts_0 = timeseries[0]
        ts_2 = timeseries[2]
        campaign = campaigns[0] if for_campaign else None
        ds_id = 1

        assert not db.session.query(TimeseriesByDataState).all()
        assert not db.session.query(TimeseriesData).all()

        if for_campaign:
            header = f"Datetime,{ts_0.name},{ts_2.name}\n"
        else:
            header = f"Datetime,{ts_0.id},{ts_2.id}\n"

        csv_file = header + (
            "2020-01-01T00:00:00+00:00,0,10\n"
            "2020-01-01T01:00:00+00:00,1,11\n"
            "2020-01-01T02:00:00+00:00,2,12\n"
            "2020-01-01T03:00:00+00:00,3,13\n"
        )

        if mode == "textiobase":
            csv_file = io.StringIO(csv_file)

        with CurrentUser(admin_user):
            tsdcsvio.import_csv(csv_file, ds_id, campaign)

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
    @pytest.mark.usefixtures("user_groups_by_campaigns")
    @pytest.mark.usefixtures("user_groups_by_campaign_scopes")
    @pytest.mark.parametrize("for_campaign", (True, False))
    def test_timeseries_data_io_import_csv_as_user(
        self, users, timeseries, campaigns, for_campaign
    ):
        user_1 = users[1]
        assert not user_1.is_admin
        ts_0 = timeseries[0]
        ts_1 = timeseries[1]
        ts_2 = timeseries[2]
        ds_id = 1

        assert not db.session.query(TimeseriesData).all()

        if for_campaign:
            campaign = campaigns[0]
            header = f"Datetime,{ts_0.name},{ts_2.name}\n"
        else:
            campaign = None
            header = f"Datetime,{ts_0.id},{ts_2.id}\n"

        csv_file = header + (
            "2020-01-01T00:00:00+00:00,0,10\n"
            "2020-01-01T01:00:00+00:00,1,11\n"
            "2020-01-01T02:00:00+00:00,2,12\n"
            "2020-01-01T03:00:00+00:00,3,13\n"
        )

        with CurrentUser(user_1):
            with pytest.raises(BEMServerAuthorizationError):
                tsdcsvio.import_csv(csv_file, ds_id, campaign)

        if for_campaign:
            campaign = campaigns[1]
            header = f"Datetime,{ts_1.name}\n"
        else:
            campaign = None
            header = f"Datetime,{ts_1.id}\n"

        csv_file = header + (
            "2020-01-01T00:00:00+00:00,0\n"
            "2020-01-01T01:00:00+00:00,1\n"
            "2020-01-01T02:00:00+00:00,2\n"
            "2020-01-01T03:00:00+00:00,3\n"
        )

        with CurrentUser(user_1):
            tsdcsvio.import_csv(csv_file, ds_id, campaign)

    @pytest.mark.parametrize(
        "file_error",
        (
            ("", TimeseriesDataCSVIOError),
            ("Dummy,\n", TimeseriesDataCSVIOError),
            ("Datetime,1324564", TimeseriesDataIOUnknownTimeseriesError),
        ),
    )
    @pytest.mark.parametrize("for_campaign", (True, False))
    def test_timeseries_data_io_import_error(
        self, users, campaigns, for_campaign, file_error
    ):
        admin_user = users[0]
        assert admin_user.is_admin
        campaign = campaigns[0] if for_campaign else None
        ds_id = 1
        csv_file, exc_cls = file_error

        with CurrentUser(admin_user):
            with pytest.raises(exc_cls):
                tsdcsvio.import_csv(io.StringIO(csv_file), ds_id, campaign)

    @pytest.mark.parametrize(
        "row",
        (
            "2020-01-01T00:00:00+00:00",
            "2020-01-01T00:00:00+00:00,",
            "2020-01-01T00:00:00+00:00,a",
            "dummy,1",
        ),
    )
    @pytest.mark.usefixtures("timeseries")
    @pytest.mark.parametrize("for_campaign", (True, False))
    def test_timeseries_data_io_import_csv_error(
        self, users, campaigns, for_campaign, row
    ):
        admin_user = users[0]
        assert admin_user.is_admin
        campaign = campaigns[0] if for_campaign else None
        ds_id = 1

        header = "Datetime,Timeseries 0\n" if for_campaign else "Datetime,1\n"
        csv_file = header + row

        with CurrentUser(admin_user):
            with pytest.raises(TimeseriesDataCSVIOError):
                tsdcsvio.import_csv(io.StringIO(csv_file), ds_id, campaign)

    @pytest.mark.usefixtures("timeseries")
    @pytest.mark.parametrize("for_campaign", (True, False))
    def test_timeseries_data_io_import_csv_data_state_error(
        self, users, timeseries, campaigns, for_campaign
    ):
        admin_user = users[0]
        assert admin_user.is_admin
        ts_1 = timeseries[1]
        campaign = campaigns[0] if for_campaign else None
        dummy_ds_id = 42

        if for_campaign:
            header = f"Datetime,{ts_1.name}\n"
        else:
            header = f"Datetime,{ts_1.id}\n"

        csv_file = header + (
            "2020-01-01T00:00:00+00:00,0\n"
            "2020-01-01T01:00:00+00:00,1\n"
            "2020-01-01T02:00:00+00:00,2\n"
            "2020-01-01T03:00:00+00:00,3\n"
        )

        with CurrentUser(admin_user):
            with pytest.raises(TimeseriesDataIOUnknownDataStateError):
                tsdcsvio.import_csv(io.StringIO(csv_file), dummy_ds_id, campaign)

    @pytest.mark.parametrize("timeseries", (5,), indirect=True)
    @pytest.mark.parametrize("for_campaign", (True, False))
    def test_timeseries_data_io_export_csv_as_admin(
        self, users, timeseries, campaigns, for_campaign
    ):
        admin_user = users[0]
        assert admin_user.is_admin
        ts_0 = timeseries[0]
        ts_2 = timeseries[2]
        ts_4 = timeseries[4]
        campaign = campaigns[0] if for_campaign else None
        dummy_ts_id = 42
        dummy_ts_name = "dummy"

        start_dt = dt.datetime(2020, 1, 1, tzinfo=dt.timezone.utc)
        end_dt = start_dt + dt.timedelta(hours=3)

        # Create DB data
        with OpenBar():
            ds_1 = TimeseriesDataState.get(name="Raw").first()
            tsbds_0 = ts_0.get_timeseries_by_data_state(ds_1)
            tsbds_4 = ts_4.get_timeseries_by_data_state(ds_1)
            for i in range(3):
                timestamp = start_dt + dt.timedelta(hours=i)
                db.session.add(
                    TimeseriesData(
                        timestamp=timestamp,
                        timeseries_by_data_state_id=tsbds_0.id,
                        value=i,
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

            if for_campaign:
                ts_l = (ts_0.name, ts_2.name, ts_4.name)
                header = f"Datetime,{ts_0.name},{ts_2.name},{ts_4.name}\n"
            else:
                ts_l = (ts_0.id, ts_2.id, ts_4.id)
                header = f"Datetime,{ts_0.id},{ts_2.id},{ts_4.id}\n"

            data = tsdcsvio.export_csv(start_dt, end_dt, ts_l, ds_1.id, campaign)

            assert data == header + (
                "2020-01-01T00:00:00+0000,0.0,,10.0\n"
                "2020-01-01T01:00:00+0000,1.0,,12.0\n"
                "2020-01-01T02:00:00+0000,2.0,,\n"
            )

            if for_campaign:
                ts_l = (ts_0.name, ts_2.name, ts_4.name, dummy_ts_name)
            else:
                ts_l = (ts_0.id, ts_2.id, ts_4.id, dummy_ts_id)

            # Unknown TS ID
            with pytest.raises(TimeseriesDataIOUnknownTimeseriesError):
                tsdcsvio.export_csv(
                    start_dt,
                    end_dt,
                    ts_l,
                    ds_1.id,
                    campaign=campaign,
                )

    @pytest.mark.parametrize("timeseries", (5,), indirect=True)
    @pytest.mark.usefixtures("users_by_user_groups")
    @pytest.mark.usefixtures("user_groups_by_campaigns")
    @pytest.mark.usefixtures("user_groups_by_campaign_scopes")
    @pytest.mark.parametrize("for_campaign", (True, False))
    def test_timeseries_data_io_export_csv_as_user(
        self, users, campaigns, timeseries, for_campaign
    ):
        user_1 = users[1]
        assert not user_1.is_admin
        ts_0 = timeseries[0]
        ts_1 = timeseries[1]
        ts_2 = timeseries[2]
        ts_3 = timeseries[3]
        ts_4 = timeseries[4]

        start_dt = dt.datetime(2020, 1, 1, tzinfo=dt.timezone.utc)
        end_dt = start_dt + dt.timedelta(hours=3)

        # Create DB data
        with OpenBar():
            ds_1 = TimeseriesDataState.get(name="Raw").first()
            tsbds_1 = ts_1.get_timeseries_by_data_state(ds_1)
            tsbds_3 = ts_3.get_timeseries_by_data_state(ds_1)
            for i in range(3):
                timestamp = start_dt + dt.timedelta(hours=i)
                db.session.add(
                    TimeseriesData(
                        timestamp=timestamp,
                        timeseries_by_data_state_id=tsbds_1.id,
                        value=i,
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

            if for_campaign:
                campaign = campaigns[0]
                ts_l = (ts_0.name, ts_2.name, ts_4.name)
            else:
                campaign = None
                ts_l = (ts_0.id, ts_2.id, ts_4.id)

            with pytest.raises(BEMServerAuthorizationError):
                data = tsdcsvio.export_csv(
                    start_dt, end_dt, ts_l, ds_1.id, campaign=campaign
                )

            if for_campaign:
                campaign = campaigns[1]
                ts_l = (ts_1.name, ts_3.name)
                header = f"Datetime,{ts_1.name},{ts_3.name}\n"
            else:
                campaign = None
                ts_l = (ts_1.id, ts_3.id)
                header = f"Datetime,{ts_1.id},{ts_3.id}\n"

            data = tsdcsvio.export_csv(
                start_dt, end_dt, ts_l, ds_1.id, campaign=campaign
            )

            assert data == header + (
                "2020-01-01T00:00:00+0000,0.0,10.0\n"
                "2020-01-01T01:00:00+0000,1.0,12.0\n"
                "2020-01-01T02:00:00+0000,2.0,\n"
            )

    @pytest.mark.parametrize("for_campaign", (True, False))
    def test_timeseries_data_io_export_csv_data_state_error(
        self, users, campaigns, timeseries, for_campaign
    ):
        admin_user = users[0]
        assert admin_user.is_admin
        ts_1 = timeseries[0]
        dummy_ds_id = 42

        start_dt = dt.datetime(2020, 1, 1, tzinfo=dt.timezone.utc)
        end_dt = start_dt + dt.timedelta(hours=3)

        if for_campaign:
            campaign = campaigns[0]
            ts_l = (ts_1.name,)
        else:
            campaign = None
            ts_l = (ts_1.id,)

        with CurrentUser(admin_user):
            with pytest.raises(TimeseriesDataIOUnknownDataStateError):
                tsdcsvio.export_csv(
                    start_dt, end_dt, ts_l, dummy_ds_id, campaign=campaign
                )

    @pytest.mark.parametrize("timeseries", (5,), indirect=True)
    @pytest.mark.parametrize("for_campaign", (True, False))
    def test_timeseries_data_io_export_csv_bucket_as_admin(
        self, users, campaigns, timeseries, for_campaign
    ):
        admin_user = users[0]
        assert admin_user.is_admin
        campaign = campaigns[0] if for_campaign else None
        ts_0 = timeseries[0]
        ts_2 = timeseries[2]
        ts_4 = timeseries[4]
        dummy_ts_id = 42
        dummy_ts_name = "dummy"

        start_dt = dt.datetime(2020, 1, 1, tzinfo=dt.timezone.utc)
        end_dt = start_dt + dt.timedelta(hours=24 * 3)

        # Create DB data
        with OpenBar():
            ds_1 = TimeseriesDataState.get(name="Raw").first()
            tsbds_0 = ts_0.get_timeseries_by_data_state(ds_1)
            tsbds_4 = ts_4.get_timeseries_by_data_state(ds_1)
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

            if for_campaign:
                ts_l = (ts_0.name, ts_2.name, ts_4.name)
                header = f"Datetime,{ts_0.name},{ts_2.name},{ts_4.name}\n"
            else:
                ts_l = (ts_0.id, ts_2.id, ts_4.id)
                header = f"Datetime,{ts_0.id},{ts_2.id},{ts_4.id}\n"

            # Export CSV: UTC avg
            data = tsdcsvio.export_csv_bucket(
                start_dt, end_dt, ts_l, ds_1.id, "1 day", campaign=campaign
            )
            assert data == header + (
                "2020-01-01T00:00:00+0000,11.5,,33.0\n"
                "2020-01-02T00:00:00+0000,35.5,,81.0\n"
                "2020-01-03T00:00:00+0000,59.5,,\n"
            )

            # Export CSV: local TZ avg
            data = tsdcsvio.export_csv_bucket(
                start_dt,
                end_dt,
                ts_l,
                ds_1.id,
                "P1D",
                timezone="Europe/Paris",
                campaign=campaign,
            )
            assert data == header + (
                "2019-12-31T23:00:00+0000,11.0,,32.0\n"
                "2020-01-01T23:00:00+0000,34.5,,79.0\n"
                "2020-01-02T23:00:00+0000,58.5,,104.0\n"
                "2020-01-03T23:00:00+0000,71.0,,\n"
            )

            # Export CSV: UTC sum
            data = tsdcsvio.export_csv_bucket(
                start_dt,
                end_dt,
                ts_l,
                ds_1.id,
                "1 day",
                aggregation="sum",
                campaign=campaign,
            )
            assert data == header + (
                "2020-01-01T00:00:00+0000,276.0,,792.0\n"
                "2020-01-02T00:00:00+0000,852.0,,1944.0\n"
                "2020-01-03T00:00:00+0000,1428.0,,\n"
            )

            # Export CSV: UTC min
            data = tsdcsvio.export_csv_bucket(
                start_dt,
                end_dt,
                ts_l,
                ds_1.id,
                "1 day",
                aggregation="min",
                campaign=campaign,
            )
            assert data == header + (
                "2020-01-01T00:00:00+0000,0.0,,10.0\n"
                "2020-01-02T00:00:00+0000,24.0,,58.0\n"
                "2020-01-03T00:00:00+0000,48.0,,\n"
            )

            # Export CSV: UTC max
            data = tsdcsvio.export_csv_bucket(
                start_dt,
                end_dt,
                ts_l,
                ds_1.id,
                "1 day",
                aggregation="max",
                campaign=campaign,
            )
            assert data == header + (
                "2020-01-01T00:00:00+0000,23.0,,56.0\n"
                "2020-01-02T00:00:00+0000,47.0,,104.0\n"
                "2020-01-03T00:00:00+0000,71.0,,\n"
            )

            # Export CSV: invalid aggregation
            with pytest.raises(TimeseriesDataIOInvalidAggregationError):
                tsdcsvio.export_csv_bucket(
                    start_dt,
                    end_dt,
                    ts_l,
                    ds_1.id,
                    "1 day",
                    aggregation="lol",
                    campaign=campaign,
                )

            # Unknown TS ID

            if for_campaign:
                ts_l = (ts_0.name, ts_2.name, ts_4.name, dummy_ts_name)
            else:
                ts_l = (ts_0.id, ts_2.id, ts_4.id, dummy_ts_id)

            with pytest.raises(TimeseriesDataIOUnknownTimeseriesError):
                tsdcsvio.export_csv_bucket(
                    start_dt,
                    end_dt,
                    ts_l,
                    ds_1.id,
                    "1 day",
                    campaign=campaign,
                )

    @pytest.mark.parametrize("timeseries", (5,), indirect=True)
    @pytest.mark.usefixtures("users_by_user_groups")
    @pytest.mark.usefixtures("user_groups_by_campaigns")
    @pytest.mark.usefixtures("user_groups_by_campaign_scopes")
    @pytest.mark.parametrize("for_campaign", (True, False))
    def test_timeseries_data_io_export_csv_bucket_as_user(
        self, users, campaigns, timeseries, for_campaign
    ):
        user_1 = users[1]
        assert not user_1.is_admin
        ts_0 = timeseries[0]
        ts_1 = timeseries[1]
        ts_2 = timeseries[2]
        ts_3 = timeseries[3]
        ts_4 = timeseries[4]

        start_dt = dt.datetime(2020, 1, 1, tzinfo=dt.timezone.utc)
        end_dt = start_dt + dt.timedelta(hours=24 * 3)

        # Create DB data
        with OpenBar():
            ds_1 = TimeseriesDataState.get(name="Raw").first()
            tsbds_1 = ts_1.get_timeseries_by_data_state(ds_1)
            tsbds_3 = ts_3.get_timeseries_by_data_state(ds_1)
            for i in range(24 * 3):
                timestamp = start_dt + dt.timedelta(hours=i)
                db.session.add(
                    TimeseriesData(
                        timestamp=timestamp,
                        timeseries_by_data_state_id=tsbds_1.id,
                        value=i,
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

            if for_campaign:
                campaign = campaigns[0]
                ts_l = (ts_0.name, ts_2.name, ts_4.name)
            else:
                campaign = None
                ts_l = (ts_0.id, ts_2.id, ts_4.id)

            with pytest.raises(BEMServerAuthorizationError):
                data = tsdcsvio.export_csv_bucket(
                    start_dt, end_dt, ts_l, ds_1.id, "1 day", campaign=campaign
                )

            # Export CSV: UTC avg

            if for_campaign:
                campaign = campaigns[1]
                ts_l = (ts_1.name, ts_3.name)
                header = f"Datetime,{ts_1.name},{ts_3.name}\n"
            else:
                campaign = None
                ts_l = (ts_1.id, ts_3.id)
                header = f"Datetime,{ts_1.id},{ts_3.id}\n"

            data = tsdcsvio.export_csv_bucket(
                start_dt, end_dt, ts_l, ds_1.id, "1 day", campaign=campaign
            )
            assert data == header + (
                "2020-01-01T00:00:00+0000,11.5,33.0\n"
                "2020-01-02T00:00:00+0000,35.5,81.0\n"
                "2020-01-03T00:00:00+0000,59.5,\n"
            )

    @pytest.mark.parametrize("for_campaign", (True, False))
    def test_timeseries_data_io_export_csv_bucket_data_state_error(
        self, users, campaigns, timeseries, for_campaign
    ):
        admin_user = users[0]
        assert admin_user.is_admin
        ts_1 = timeseries[0]
        dummy_ds_id = 42

        start_dt = dt.datetime(2020, 1, 1, tzinfo=dt.timezone.utc)
        end_dt = start_dt + dt.timedelta(hours=3)

        if for_campaign:
            campaign = campaigns[0]
            ts_l = (ts_1.name,)
        else:
            campaign = None
            ts_l = (ts_1.id,)

        with CurrentUser(admin_user):
            with pytest.raises(TimeseriesDataIOUnknownDataStateError):
                tsdcsvio.export_csv_bucket(
                    start_dt, end_dt, ts_l, dummy_ds_id, "1 day", campaign=campaign
                )
