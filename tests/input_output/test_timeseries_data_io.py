"""Timeseries CSV I/O tests"""
import io
import datetime as dt
from zoneinfo import ZoneInfo

import sqlalchemy as sqla
import pandas as pd
import numpy as np

import pytest

from bemserver_core.model import (
    TimeseriesData,
    TimeseriesDataState,
    TimeseriesByDataState,
)
from bemserver_core.input_output import tsdio, tsdcsvio
from bemserver_core.database import db
from bemserver_core.authorization import CurrentUser, OpenBar
from bemserver_core.exceptions import (
    BEMServerAuthorizationError,
    TimeseriesDataIOInvalidAggregationError,
    TimeseriesDataCSVIOError,
    TimeseriesNotFoundError,
)


class TestTimeseriesDataIO:
    @pytest.mark.parametrize("timeseries", (3,), indirect=True)
    @pytest.mark.parametrize("for_campaign", (True, False))
    def test_timeseries_data_io_set_timeseries_data_as_admin(
        self, users, campaigns, timeseries, for_campaign
    ):
        admin_user = users[0]
        assert admin_user.is_admin
        ts_0 = timeseries[0]
        ts_2 = timeseries[2]
        campaign = campaigns[0] if for_campaign else None

        assert not db.session.query(TimeseriesByDataState).all()
        assert not db.session.query(TimeseriesData).all()

        with OpenBar():
            ds_1 = TimeseriesDataState.get(name="Raw").first()

        index = pd.DatetimeIndex(
            [
                "2020-01-01T00:00:00+00:00",
                "2020-01-01T01:00:00+00:00",
                "2020-01-01T02:00:00+00:00",
                "2020-01-01T03:00:00+00:00",
            ],
            name="timestamp",
        )
        val_0 = [0, 1, 2, 3]
        val_2 = [10, 11, 12, np.nan]
        data_df = pd.DataFrame(
            {
                ts_0.name if for_campaign else ts_0.id: val_0,
                ts_2.name if for_campaign else ts_2.id: val_2,
            },
            index=index,
        )

        with CurrentUser(admin_user):
            tsdio.set_timeseries_data(data_df, ds_1, campaign)

        # Rollback then query to ensure data is actually written
        db.session.rollback()

        # Check TSBDS are correctly auto-created
        tsbds_l = (
            db.session.query(TimeseriesByDataState)
            .order_by(TimeseriesByDataState.id)
            .all()
        )
        assert all(tsbds.data_state_id == ds_1.id for tsbds in tsbds_l)
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
            for idx, timestamp in enumerate(timestamps[:-1])
        ]

        assert data == expected

    @pytest.mark.parametrize("timeseries", (3,), indirect=True)
    @pytest.mark.usefixtures("users_by_user_groups")
    @pytest.mark.usefixtures("user_groups_by_campaigns")
    @pytest.mark.usefixtures("user_groups_by_campaign_scopes")
    @pytest.mark.parametrize("for_campaign", (True, False))
    def test_timeseries_data_io_set_timeseries_data_as_user(
        self, users, timeseries, campaigns, for_campaign
    ):
        user_1 = users[1]
        assert not user_1.is_admin
        ts_0 = timeseries[0]
        ts_1 = timeseries[1]
        ts_2 = timeseries[2]

        assert not db.session.query(TimeseriesData).all()

        with OpenBar():
            ds_1 = TimeseriesDataState.get(name="Raw").first()

        index = pd.DatetimeIndex(
            [
                "2020-01-01T00:00:00+00:00",
                "2020-01-01T01:00:00+00:00",
                "2020-01-01T02:00:00+00:00",
                "2020-01-01T03:00:00+00:00",
            ],
            name="timestamp",
        )
        val_0 = [0, 1, 2, 3]
        val_2 = [10, 11, 12, 13]

        data_df = pd.DataFrame(
            {
                ts_0.name if for_campaign else ts_0.id: val_0,
                ts_2.name if for_campaign else ts_2.id: val_2,
            },
            index=index,
        )

        if for_campaign:
            campaign = campaigns[0]
        else:
            campaign = None

        with CurrentUser(user_1):
            with pytest.raises(BEMServerAuthorizationError):
                tsdio.set_timeseries_data(data_df, ds_1, campaign)

        data_df = pd.DataFrame(
            {
                ts_1.name if for_campaign else ts_1.id: val_0,
            },
            index=index,
        )

        if for_campaign:
            campaign = campaigns[1]
        else:
            campaign = None

        with CurrentUser(user_1):
            tsdio.set_timeseries_data(data_df, ds_1, campaign)

    @pytest.mark.parametrize("for_campaign", (True, False))
    def test_timeseries_data_io_import_set_timeseries_data_timeseries_error(
        self, users, campaigns, for_campaign
    ):
        admin_user = users[0]
        assert admin_user.is_admin
        campaign = campaigns[0] if for_campaign else None

        with OpenBar():
            ds_1 = TimeseriesDataState.get(name="Raw").first()

        index = pd.DatetimeIndex(
            [
                "2020-01-01T00:00:00+00:00",
                "2020-01-01T01:00:00+00:00",
                "2020-01-01T02:00:00+00:00",
                "2020-01-01T03:00:00+00:00",
            ],
            name="timestamp",
        )
        val_0 = [0, 1, 2, 3]

        data_df = pd.DataFrame(
            {"Timeseries 0" if for_campaign else 1: val_0},
            index=index,
        )

        with CurrentUser(admin_user):
            with pytest.raises(TimeseriesNotFoundError):
                tsdio.set_timeseries_data(data_df, ds_1, campaign)

    @pytest.mark.parametrize("timeseries", (5,), indirect=True)
    @pytest.mark.parametrize("col_label", ("id", "name"))
    def test_timeseries_data_io_get_timeseries_data_as_admin(
        self, users, timeseries, col_label
    ):
        admin_user = users[0]
        assert admin_user.is_admin
        ts_0 = timeseries[0]
        ts_2 = timeseries[2]
        ts_4 = timeseries[4]

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

            ts_l = (ts_0, ts_2, ts_4)
            data_df = tsdio.get_timeseries_data(
                start_dt, end_dt, ts_l, ds_1, col_label=col_label
            )

        index = pd.DatetimeIndex(
            [
                "2020-01-01T00:00:00+00:00",
                "2020-01-01T01:00:00+00:00",
                "2020-01-01T02:00:00+00:00",
            ],
            name="timestamp",
        )
        val_0 = [0.0, 1.0, 2.0]
        val_2 = [np.nan, np.nan, np.nan]
        val_4 = [10.0, 12.0, np.nan]
        expected_data_df = pd.DataFrame(
            {
                ts_0.name if col_label == "name" else ts_0.id: val_0,
                ts_2.name if col_label == "name" else ts_2.id: val_2,
                ts_4.name if col_label == "name" else ts_4.id: val_4,
            },
            index=index,
        )
        assert data_df.equals(expected_data_df)

    @pytest.mark.parametrize("timeseries", (5,), indirect=True)
    @pytest.mark.usefixtures("users_by_user_groups")
    @pytest.mark.usefixtures("user_groups_by_campaigns")
    @pytest.mark.usefixtures("user_groups_by_campaign_scopes")
    @pytest.mark.parametrize("col_label", ("id", "name"))
    def test_timeseries_data_io_get_timeseries_data_as_user(
        self, users, timeseries, col_label
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

            ts_l = (ts_0, ts_2, ts_4)

            with pytest.raises(BEMServerAuthorizationError):
                tsdio.get_timeseries_data(
                    start_dt, end_dt, ts_l, ds_1, col_label=col_label
                )

            ts_l = (ts_1, ts_3)
            data_df = tsdio.get_timeseries_data(
                start_dt, end_dt, ts_l, ds_1, col_label=col_label
            )

        index = pd.DatetimeIndex(
            [
                "2020-01-01T00:00:00+00:00",
                "2020-01-01T01:00:00+00:00",
                "2020-01-01T02:00:00+00:00",
            ],
            name="timestamp",
        )
        val_1 = [0.0, 1.0, 2.0]
        val_3 = [10.0, 12.0, np.nan]
        expected_data_df = pd.DataFrame(
            {
                ts_1.name if col_label == "name" else ts_1.id: val_1,
                ts_3.name if col_label == "name" else ts_3.id: val_3,
            },
            index=index,
        )
        assert data_df.equals(expected_data_df)

    @pytest.mark.parametrize("timeseries", (5,), indirect=True)
    def test_timeseries_data_io_get_timeseries_buckets_data_fixed_size_as_admin(
        self, users, timeseries
    ):
        admin_user = users[0]
        assert admin_user.is_admin
        ts_0 = timeseries[0]
        ts_2 = timeseries[2]
        ts_4 = timeseries[4]

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

            ts_l = (ts_0, ts_2, ts_4)

            # Export CSV: UTC count 1 day
            data_df = tsdio.get_timeseries_buckets_data(
                start_dt,
                end_dt,
                ts_l,
                ds_1,
                "1 day",
                "count",
                col_label="name",
            )

            index = pd.DatetimeIndex(
                [
                    "2020-01-01T00:00:00",
                    "2020-01-02T00:00:00",
                    "2020-01-03T00:00:00",
                ],
                name="timestamp",
                tz="UTC",
            )
            expected_data_df = pd.DataFrame(
                {
                    ts_0.name: [24, 24, 24],
                    ts_2.name: [0, 0, 0],
                    ts_4.name: [24, 24, 0],
                },
                index=index,
            )
            assert data_df.equals(expected_data_df)

            # Export CSV: UTC count 2 days
            data_df = tsdio.get_timeseries_buckets_data(
                # Also check that start_dt TZ doesn't matter
                start_dt.astimezone(dt.timezone(dt.timedelta(hours=12))),
                end_dt,
                ts_l,
                ds_1,
                "2 day",
                "count",
                col_label="name",
            )

            index = pd.DatetimeIndex(
                [
                    "2020-01-01T00:00:00",
                    "2020-01-03T00:00:00",
                ],
                name="timestamp",
                tz="UTC",
            )
            expected_data_df = pd.DataFrame(
                {
                    ts_0.name: [48, 24],
                    ts_2.name: [0, 0],
                    ts_4.name: [48, 0],
                },
                index=index,
            )

            assert data_df.equals(expected_data_df)

            # Export CSV: UTC count 2 days with gapfill
            data_df = tsdio.get_timeseries_buckets_data(
                start_dt,
                end_dt + dt.timedelta(days=3),
                ts_l,
                ds_1,
                "2 day",
                "count",
                col_label="name",
            )

            index = pd.DatetimeIndex(
                [
                    "2020-01-01T00:00:00",
                    "2020-01-03T00:00:00",
                    "2020-01-05T00:00:00",
                ],
                name="timestamp",
                tz="UTC",
            )
            expected_data_df = pd.DataFrame(
                {
                    ts_0.name: [48, 24, 0],
                    ts_2.name: [0, 0, 0],
                    ts_4.name: [48, 0, 0],
                },
                index=index,
            )

            assert data_df.equals(expected_data_df)

            # Export CSV: UTC count 1 day, 3 hour (and a half) offset
            data_df = tsdio.get_timeseries_buckets_data(
                start_dt + dt.timedelta(hours=3, minutes=30),
                end_dt,
                ts_l,
                ds_1,
                "1 day",
                "count",
                col_label="name",
            )

            index = pd.DatetimeIndex(
                [
                    "2020-01-01T03:30:00",
                    "2020-01-02T03:30:00",
                    "2020-01-03T03:30:00",
                ],
                name="timestamp",
                tz="UTC",
            )
            expected_data_df = pd.DataFrame(
                {
                    ts_0.name: [24, 24, 20],
                    ts_2.name: [0, 0, 0],
                    ts_4.name: [24, 20, 0],
                },
                index=index,
            )

            assert data_df.equals(expected_data_df)

            # Export CSV: local TZ count 1 week
            data_df = tsdio.get_timeseries_buckets_data(
                # Check start_dt TZ doesn't change alignment to 00:00 local TZ
                dt.datetime(2020, 1, 1, tzinfo=dt.timezone.utc),
                dt.datetime(2020, 1, 9, tzinfo=ZoneInfo("Europe/Paris")),
                ts_l,
                ds_1,
                "1 week",
                "count",
                col_label="name",
                timezone="Europe/Paris",
            )

            index = pd.DatetimeIndex(
                [
                    "2019-12-30T00:00:00",
                    "2020-01-06T00:00:00",
                ],
                name="timestamp",
                tz="Europe/Paris",
            )
            expected_data_df = pd.DataFrame(
                {
                    ts_0.name: [72, 0],
                    ts_2.name: [0, 0],
                    ts_4.name: [48, 0],
                },
                index=index,
            )

            assert data_df.equals(expected_data_df)

            # Export CSV: UTC count 12 hours
            data_df = tsdio.get_timeseries_buckets_data(
                start_dt,
                end_dt,
                ts_l,
                ds_1,
                "12 hour",
                "count",
                col_label="name",
            )

            index = pd.DatetimeIndex(
                [
                    "2020-01-01T00:00:00",
                    "2020-01-01T12:00:00",
                    "2020-01-02T00:00:00",
                    "2020-01-02T12:00:00",
                    "2020-01-03T00:00:00",
                    "2020-01-03T12:00:00",
                ],
                name="timestamp",
                tz="UTC",
            )
            expected_data_df = pd.DataFrame(
                {
                    ts_0.name: 6 * [12],
                    ts_2.name: 6 * [0],
                    ts_4.name: 4 * [12] + 2 * [0],
                },
                index=index,
            )
            assert data_df.equals(expected_data_df)

            # Export CSV: local TZ avg
            data_df = tsdio.get_timeseries_buckets_data(
                start_dt.replace(tzinfo=ZoneInfo("Europe/Paris")),
                end_dt.replace(tzinfo=ZoneInfo("Europe/Paris")),
                ts_l,
                ds_1,
                "1 day",
                timezone="Europe/Paris",
                col_label="name",
            )

            index = pd.DatetimeIndex(
                [
                    "2020-01-01T00:00:00",
                    "2020-01-02T00:00:00",
                    "2020-01-03T00:00:00",
                ],
                name="timestamp",
                tz="Europe/Paris",
            )
            expected_data_df = pd.DataFrame(
                {
                    ts_0.name: [11.0, 34.5, 58.5],
                    ts_2.name: [np.nan, np.nan, np.nan],
                    ts_4.name: [32.0, 79.0, 104.0],
                },
                index=index,
            )

            assert data_df.equals(expected_data_df)

            # Export CSV: UTC sum, with gapfill
            data_df = tsdio.get_timeseries_buckets_data(
                start_dt,
                end_dt + dt.timedelta(days=1),
                ts_l,
                ds_1,
                "1 day",
                "sum",
                col_label="name",
            )

            index = pd.DatetimeIndex(
                [
                    "2020-01-01T00:00:00",
                    "2020-01-02T00:00:00",
                    "2020-01-03T00:00:00",
                    "2020-01-04T00:00:00",
                ],
                name="timestamp",
                tz="UTC",
            )
            expected_data_df = pd.DataFrame(
                {
                    ts_0.name: [276.0, 852.0, 1428.0, np.nan],
                    ts_2.name: [np.nan, np.nan, np.nan, np.nan],
                    ts_4.name: [792.0, 1944.0, np.nan, np.nan],
                },
                index=index,
            )

            assert data_df.equals(expected_data_df)

            # Export CSV: UTC min, with gapfill
            data_df = tsdio.get_timeseries_buckets_data(
                start_dt,
                end_dt + dt.timedelta(days=1),
                ts_l,
                ds_1,
                "1 day",
                "min",
                col_label="name",
            )

            index = pd.DatetimeIndex(
                [
                    "2020-01-01T00:00:00",
                    "2020-01-02T00:00:00",
                    "2020-01-03T00:00:00",
                    "2020-01-04T00:00:00",
                ],
                name="timestamp",
                tz="UTC",
            )
            expected_data_df = pd.DataFrame(
                {
                    ts_0.name: [0.0, 24.0, 48.0, np.nan],
                    ts_2.name: [np.nan, np.nan, np.nan, np.nan],
                    ts_4.name: [10.0, 58.0, np.nan, np.nan],
                },
                index=index,
            )

            assert data_df.equals(expected_data_df)

            # Export CSV: UTC max, with gapfill
            data_df = tsdio.get_timeseries_buckets_data(
                start_dt,
                end_dt + dt.timedelta(days=1),
                ts_l,
                ds_1,
                "1 day",
                "max",
                col_label="name",
            )

            index = pd.DatetimeIndex(
                [
                    "2020-01-01T00:00:00",
                    "2020-01-02T00:00:00",
                    "2020-01-03T00:00:00",
                    "2020-01-04T00:00:00",
                ],
                name="timestamp",
                tz="UTC",
            )
            expected_data_df = pd.DataFrame(
                {
                    ts_0.name: [23.0, 47.0, 71.0, np.nan],
                    ts_2.name: [np.nan, np.nan, np.nan, np.nan],
                    ts_4.name: [56.0, 104.0, np.nan, np.nan],
                },
                index=index,
            )

            assert data_df.equals(expected_data_df)

            # Export CSV: UTC count 1 day by ID
            data_df = tsdio.get_timeseries_buckets_data(
                start_dt,
                end_dt,
                ts_l,
                ds_1,
                "1 day",
                "count",
                col_label="id",
            )

            index = pd.DatetimeIndex(
                [
                    "2020-01-01T00:00:00",
                    "2020-01-02T00:00:00",
                    "2020-01-03T00:00:00",
                ],
                name="timestamp",
                tz="UTC",
            )
            expected_data_df = pd.DataFrame(
                {
                    ts_0.id: [24, 24, 24],
                    ts_2.id: [0, 0, 0],
                    ts_4.id: [24, 24, 0],
                },
                index=index,
            )
            assert data_df.equals(expected_data_df)

            # Export CSV: invalid aggregation
            with pytest.raises(TimeseriesDataIOInvalidAggregationError):
                tsdio.get_timeseries_buckets_data(
                    start_dt,
                    end_dt,
                    ts_l,
                    ds_1,
                    "1 day",
                    "lol",
                )

    def test_timeseries_data_io_get_timeseries_buckets_data_fixed_size_dst_as_admin(
        self, users, timeseries
    ):
        """Check bucketing in local TZ correctly handles DST"""
        admin_user = users[0]
        assert admin_user.is_admin
        ts_0 = timeseries[0]

        # Use UTC for start_dt otherwise start_dt + timedelta does surprising things
        # https://blog.ganssle.io/articles/2018/02/aware-datetime-arithmetic.html
        start_dt_1 = dt.datetime(
            2020, 3, 28, tzinfo=ZoneInfo("Europe/Paris")
        ).astimezone(dt.timezone.utc)
        end_dt_1 = dt.datetime(2020, 3, 30, tzinfo=ZoneInfo("Europe/Paris"))
        start_dt_2 = dt.datetime(
            2020, 10, 24, tzinfo=ZoneInfo("Europe/Paris")
        ).astimezone(dt.timezone.utc)
        end_dt_2 = dt.datetime(2020, 10, 26, tzinfo=ZoneInfo("Europe/Paris"))

        # Create DB data
        with OpenBar():
            ds_1 = TimeseriesDataState.get(name="Raw").first()
            tsbds_0 = ts_0.get_timeseries_by_data_state(ds_1)
        for i in range(24 * 3):
            timestamp_1 = start_dt_1 + dt.timedelta(hours=i)
            db.session.add(
                TimeseriesData(
                    timestamp=timestamp_1,
                    timeseries_by_data_state_id=tsbds_0.id,
                    value=i,
                )
            )
            timestamp_2 = start_dt_2 + dt.timedelta(hours=i)
            db.session.add(
                TimeseriesData(
                    timestamp=timestamp_2,
                    timeseries_by_data_state_id=tsbds_0.id,
                    value=i,
                )
            )
        db.session.commit()

        with CurrentUser(admin_user):

            args = [(ts_0,), ds_1, "1 day", "count"]
            kwargs = {"timezone": "Europe/Paris", "col_label": "name"}

            # local TZ count 1 day - Spring forward
            data_df = tsdio.get_timeseries_buckets_data(
                start_dt_1, end_dt_1, *args, **kwargs
            )
            index = pd.DatetimeIndex(
                ["2020-03-28T00:00:00", "2020-03-29T00:00:00"],
                name="timestamp",
                tz="Europe/Paris",
            )
            expected_data_df = pd.DataFrame({ts_0.name: [24, 23]}, index=index)
            assert data_df.equals(expected_data_df)

            # local TZ count 1 day - Fall back
            data_df = tsdio.get_timeseries_buckets_data(
                start_dt_2, end_dt_2, *args, **kwargs
            )
            index = pd.DatetimeIndex(
                ["2020-10-24T00:00:00", "2020-10-25T00:00:00"],
                name="timestamp",
                tz="Europe/Paris",
            )
            expected_data_df = pd.DataFrame({ts_0.name: [24, 25]}, index=index)
            assert data_df.equals(expected_data_df)

    @pytest.mark.parametrize("timeseries", (5,), indirect=True)
    def test_timeseries_data_io_get_timeseries_buckets_data_variable_size_as_admin(
        self, users, timeseries
    ):
        admin_user = users[0]
        assert admin_user.is_admin
        ts_0 = timeseries[0]
        ts_2 = timeseries[2]
        ts_4 = timeseries[4]

        start_dt = dt.datetime(2020, 1, 1, tzinfo=dt.timezone.utc)
        start_dt_plus_3_months = dt.datetime(2020, 4, 1, tzinfo=dt.timezone.utc)
        end_dt = start_dt + dt.timedelta(days=366 + 365)

        # Create DB data
        with OpenBar():
            ds_1 = TimeseriesDataState.get(name="Raw").first()
            tsbds_0 = ts_0.get_timeseries_by_data_state(ds_1)
            tsbds_4 = ts_4.get_timeseries_by_data_state(ds_1)

            timestamps = pd.date_range(
                start=start_dt, end=end_dt, inclusive="left", freq="H"
            )
            values = range(24 * (366 + 365))
            in_df = pd.DataFrame(
                {
                    tsbds_0.id: values,
                    tsbds_4.id: values,
                },
                index=pd.DatetimeIndex(timestamps, name="timestamp"),
            )
            in_df[tsbds_4.id] = 10 + 2 * in_df[tsbds_4.id]
            in_df.loc[in_df.index.year != 2020, tsbds_4.id] = np.nan
            in_df = in_df.melt(
                value_vars=in_df.columns,
                var_name="timeseries_by_data_state_id",
                ignore_index=False,
            )
            data_rows = [
                row
                for row in in_df.reset_index().to_dict(orient="records")
                if pd.notna(row["value"])
            ]
            query = (
                sqla.dialects.postgresql.insert(TimeseriesData)
                .values(data_rows)
                .on_conflict_do_nothing()
            )
            db.session.execute(query)
            db.session.commit()

        with CurrentUser(admin_user):

            ts_l = (ts_0, ts_2, ts_4)

            # Export CSV: UTC count year
            data_df = tsdio.get_timeseries_buckets_data(
                start_dt, end_dt, ts_l, ds_1, "1 year", "count", col_label="name"
            )

            index = pd.DatetimeIndex(
                [
                    "2020-01-01T00:00:00",
                    "2021-01-01T00:00:00",
                ],
                name="timestamp",
                tz="UTC",
            )
            expected_data_df = pd.DataFrame(
                {
                    ts_0.name: [24 * 366, 24 * 365],
                    ts_2.name: [0, 0],
                    ts_4.name: [24 * 366, 0],
                },
                index=index,
            )

            assert data_df.equals(expected_data_df)

            # Export CSV: UTC count year, with gapfill
            data_df = tsdio.get_timeseries_buckets_data(
                start_dt,
                end_dt + dt.timedelta(days=100),
                ts_l,
                ds_1,
                "1 year",
                "count",
                col_label="name",
            )

            index = pd.DatetimeIndex(
                [
                    "2020-01-01T00:00:00",
                    "2021-01-01T00:00:00",
                    "2022-01-01T00:00:00",
                ],
                name="timestamp",
                tz="UTC",
            )
            expected_data_df = pd.DataFrame(
                {
                    ts_0.name: [24 * 366, 24 * 365, 0],
                    ts_2.name: [0, 0, 0],
                    ts_4.name: [24 * 366, 0, 0],
                },
                index=index,
            )

            assert data_df.equals(expected_data_df)

            # Export CSV: UTC count 2 year
            data_df = tsdio.get_timeseries_buckets_data(
                start_dt, end_dt, ts_l, ds_1, "2 year", "count", col_label="name"
            )

            index = pd.DatetimeIndex(
                [
                    "2020-01-01T00:00:00",
                ],
                name="timestamp",
                tz="UTC",
            )
            expected_data_df = pd.DataFrame(
                {
                    ts_0.name: [24 * 366 + 24 * 365],
                    ts_2.name: [0],
                    ts_4.name: [24 * 366],
                },
                index=index,
            )

            assert data_df.equals(expected_data_df)

            # Export CSV: local TZ count year (first bucket incomplete)
            data_df = tsdio.get_timeseries_buckets_data(
                start_dt_plus_3_months.replace(tzinfo=ZoneInfo("Europe/Paris")),
                end_dt.replace(tzinfo=ZoneInfo("Europe/Paris")),
                ts_l,
                ds_1,
                "1 year",
                "count",
                timezone="Europe/Paris",
                col_label="name",
            )

            index = pd.DatetimeIndex(
                [
                    "2020-01-01T00:00:00",
                    "2021-01-01T00:00:00",
                ],
                name="timestamp",
                tz="Europe/Paris",
            )
            expected_data_df = pd.DataFrame(
                {
                    ts_0.name: [
                        # Apr 2020 UTC+2 -> Jan 2021 UTC+1
                        24 * (30 + 31 + 30 + 31 + 31 + 30 + 31 + 30 + 31) + 1,
                        # Apr 2021 UTC+2 -> Jan 2022 UTC+1
                        24 * 365,
                    ],
                    ts_2.name: [0, 0],
                    ts_4.name: [
                        # Apr 2020 UTC+2 -> Jan 2021 UTC+1
                        24 * (30 + 31 + 30 + 31 + 31 + 30 + 31 + 30 + 31) + 1,
                        # Jan 2021 UTC+1 -> Jan 2022 UTC
                        1,
                    ],
                },
                index=index,
            )

            assert data_df.equals(expected_data_df)

            # Export CSV: UTC avg month, with gapfill
            data_df = tsdio.get_timeseries_buckets_data(
                # Exact start day does not matter, data is aligned to month
                start_dt - dt.timedelta(days=20),
                start_dt_plus_3_months,
                ts_l,
                ds_1,
                "1 month",
                "avg",
                col_label="name",
            )

            index = pd.DatetimeIndex(
                [
                    "2019-12-01T00:00:00",
                    "2020-01-01T00:00:00",
                    "2020-02-01T00:00:00",
                    "2020-03-01T00:00:00",
                ],
                name="timestamp",
                tz="UTC",
            )
            expected_data_df = pd.DataFrame(
                {
                    ts_0.name: [np.nan, 371.5, 1091.5, 1811.5],
                    ts_2.name: [np.nan, np.nan, np.nan, np.nan],
                    ts_4.name: [np.nan, 753.0, 2193.0, 3633.0],
                },
                index=index,
            )

            assert data_df.equals(expected_data_df)

            # Export CSV: UTC avg month with start offset
            # Bins are aligned to month, only first bin differs
            data_df = tsdio.get_timeseries_buckets_data(
                start_dt + dt.timedelta(days=3),
                start_dt_plus_3_months,
                ts_l,
                ds_1,
                "1 month",
                "avg",
                col_label="name",
            )

            index = pd.DatetimeIndex(
                [
                    "2020-01-01T00:00:00",
                    "2020-02-01T00:00:00",
                    "2020-03-01T00:00:00",
                ],
                name="timestamp",
                tz="UTC",
            )
            expected_data_df = pd.DataFrame(
                {
                    ts_0.name: [407.5, 1091.5, 1811.5],
                    ts_2.name: [np.nan, np.nan, np.nan],
                    ts_4.name: [825.0, 2193.0, 3633.0],
                },
                index=index,
            )

            assert data_df.equals(expected_data_df)

            # Export CSV: UTC avg 2 month
            data_df = tsdio.get_timeseries_buckets_data(
                start_dt,
                start_dt_plus_3_months,
                ts_l,
                ds_1,
                "2 month",
                "avg",
                col_label="name",
            )

            index = pd.DatetimeIndex(
                [
                    "2020-01-01T00:00:00",
                    "2020-03-01T00:00:00",
                ],
                name="timestamp",
                tz="UTC",
            )
            expected_data_df = pd.DataFrame(
                {
                    ts_0.name: [731.5, 1811.5],
                    ts_2.name: [np.nan, np.nan],
                    ts_4.name: [1473.0, 3633.0],
                },
                index=index,
            )

            assert data_df.equals(expected_data_df)

            # Export CSV: local TZ avg month
            data_df = tsdio.get_timeseries_buckets_data(
                start_dt,
                start_dt_plus_3_months,
                ts_l,
                ds_1,
                "1 month",
                "avg",
                timezone="Europe/Paris",
                col_label="name",
            )

            index = pd.DatetimeIndex(
                [
                    "2020-01-01T00:00:00",
                    "2020-02-01T00:00:00",
                    "2020-03-01T00:00:00",
                    "2020-04-01T00:00:00",
                ],
                name="timestamp",
                tz="Europe/Paris",
            )
            expected_data_df = pd.DataFrame(
                {
                    ts_0.name: [371.0, 1090.5, 1810.0, 2182.5],
                    ts_2.name: [np.nan, np.nan, np.nan, np.nan],
                    ts_4.name: [752.0, 2191.0, 3630.0, 4375.0],
                },
                index=index,
            )

            assert data_df.equals(expected_data_df)

            # Export CSV: UTC sum month
            data_df = tsdio.get_timeseries_buckets_data(
                start_dt,
                start_dt_plus_3_months,
                ts_l,
                ds_1,
                "1 month",
                "sum",
                col_label="name",
            )

            index = pd.DatetimeIndex(
                [
                    "2020-01-01T00:00:00",
                    "2020-02-01T00:00:00",
                    "2020-03-01T00:00:00",
                ],
                name="timestamp",
                tz="UTC",
            )
            expected_data_df = pd.DataFrame(
                {
                    ts_0.name: [276396.0, 759684.0, 1347756.0],
                    ts_2.name: [np.nan, np.nan, np.nan],
                    ts_4.name: [560232.0, 1526328.0, 2702952.0],
                },
                index=index,
            )

            assert data_df.equals(expected_data_df)

            # Export CSV: UTC min month
            data_df = tsdio.get_timeseries_buckets_data(
                start_dt,
                start_dt_plus_3_months,
                ts_l,
                ds_1,
                "1 month",
                "min",
                col_label="name",
            )

            index = pd.DatetimeIndex(
                [
                    "2020-01-01T00:00:00",
                    "2020-02-01T00:00:00",
                    "2020-03-01T00:00:00",
                ],
                name="timestamp",
                tz="UTC",
            )
            expected_data_df = pd.DataFrame(
                {
                    ts_0.name: [0.0, 744.0, 1440.0],
                    ts_2.name: [np.nan, np.nan, np.nan],
                    ts_4.name: [10.0, 1498.0, 2890.0],
                },
                index=index,
            )

            assert data_df.equals(expected_data_df)

            # Export CSV: UTC max month
            data_df = tsdio.get_timeseries_buckets_data(
                start_dt,
                start_dt_plus_3_months,
                ts_l,
                ds_1,
                "1 month",
                "max",
                col_label="name",
            )

            index = pd.DatetimeIndex(
                [
                    "2020-01-01T00:00:00",
                    "2020-02-01T00:00:00",
                    "2020-03-01T00:00:00",
                ],
                name="timestamp",
                tz="UTC",
            )
            expected_data_df = pd.DataFrame(
                {
                    ts_0.name: [743.0, 1439.0, 2183.0],
                    ts_2.name: [np.nan, np.nan, np.nan],
                    ts_4.name: [1496.0, 2888.0, 4376.0],
                },
                index=index,
            )

            assert data_df.equals(expected_data_df)

            # Export CSV: UTC count year by ID
            data_df = tsdio.get_timeseries_buckets_data(
                start_dt, end_dt, ts_l, ds_1, "1 year", "count", col_label="id"
            )

            index = pd.DatetimeIndex(
                [
                    "2020-01-01T00:00:00",
                    "2021-01-01T00:00:00",
                ],
                name="timestamp",
                tz="UTC",
            )
            expected_data_df = pd.DataFrame(
                {
                    ts_0.id: [24 * 366, 24 * 365],
                    ts_2.id: [0, 0],
                    ts_4.id: [24 * 366, 0],
                },
                index=index,
            )

            assert data_df.equals(expected_data_df)

    @pytest.mark.parametrize("timeseries", (5,), indirect=True)
    @pytest.mark.usefixtures("users_by_user_groups")
    @pytest.mark.usefixtures("user_groups_by_campaigns")
    @pytest.mark.usefixtures("user_groups_by_campaign_scopes")
    @pytest.mark.parametrize("col_label", ("id", "name"))
    def test_timeseries_data_io_export_csv_bucket_as_user(
        self, users, timeseries, col_label
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

            ts_l = (ts_0, ts_2, ts_4)

            with pytest.raises(BEMServerAuthorizationError):
                tsdio.get_timeseries_buckets_data(
                    start_dt, end_dt, ts_l, ds_1, "1 day", col_label=col_label
                )

            # Export CSV: UTC avg

            ts_l = (ts_1, ts_3)

            data_df = tsdio.get_timeseries_buckets_data(
                start_dt, end_dt, ts_l, ds_1, "1 day", col_label=col_label
            )

            index = pd.DatetimeIndex(
                [
                    "2020-01-01T00:00:00",
                    "2020-01-02T00:00:00",
                    "2020-01-03T00:00:00",
                ],
                name="timestamp",
                tz="UTC",
            )
            expected_data_df = pd.DataFrame(
                {
                    ts_1.name if col_label == "name" else ts_1.id: [11.5, 35.5, 59.5],
                    ts_3.name if col_label == "name" else ts_3.id: [33.0, 81.0, np.nan],
                },
                index=index,
            )

            assert data_df.equals(expected_data_df)

    @pytest.mark.parametrize("timeseries", (5,), indirect=True)
    def test_timeseries_data_io_delete_as_admin(
        self,
        users,
        timeseries,
    ):
        admin_user = users[0]
        assert admin_user.is_admin
        ts_0 = timeseries[0]
        ts_2 = timeseries[2]

        start_dt = dt.datetime(2020, 1, 1, tzinfo=dt.timezone.utc)
        end_dt = start_dt + dt.timedelta(hours=3)

        # Create DB data
        with OpenBar():
            ds_1 = TimeseriesDataState.get(name="Raw").first()
            tsbds_0 = ts_0.get_timeseries_by_data_state(ds_1)
            for i in range(3):
                timestamp = start_dt + dt.timedelta(hours=i)
                db.session.add(
                    TimeseriesData(
                        timestamp=timestamp,
                        timeseries_by_data_state_id=tsbds_0.id,
                        value=i,
                    )
                )
            db.session.commit()

            assert db.session.query(TimeseriesData).all()

        ts_l = (ts_0, ts_2)

        with CurrentUser(admin_user):
            tsdio.delete(start_dt, end_dt, ts_l, ds_1)
            # Rollback then query to ensure data is actually deleted
            db.session.rollback()
            assert not db.session.query(TimeseriesData).all()

    @pytest.mark.parametrize("timeseries", (4,), indirect=True)
    @pytest.mark.usefixtures("users_by_user_groups")
    @pytest.mark.usefixtures("user_groups_by_campaigns")
    @pytest.mark.usefixtures("user_groups_by_campaign_scopes")
    def test_timeseries_data_io_delete_as_user(self, users, timeseries):
        user_1 = users[1]
        assert not user_1.is_admin
        ts_0 = timeseries[0]
        ts_1 = timeseries[1]
        ts_2 = timeseries[2]
        ts_3 = timeseries[3]

        start_dt = dt.datetime(2020, 1, 1, tzinfo=dt.timezone.utc)
        end_dt = start_dt + dt.timedelta(hours=3)

        # Create DB data
        with OpenBar():
            ds_1 = TimeseriesDataState.get(name="Raw").first()
            tsbds_0 = ts_0.get_timeseries_by_data_state(ds_1)
            tsbds_1 = ts_1.get_timeseries_by_data_state(ds_1)
            for i in range(3):
                timestamp = start_dt + dt.timedelta(hours=i)
                db.session.add(
                    TimeseriesData(
                        timestamp=timestamp,
                        timeseries_by_data_state_id=tsbds_0.id,
                        value=i,
                    )
                )
                db.session.add(
                    TimeseriesData(
                        timestamp=timestamp,
                        timeseries_by_data_state_id=tsbds_1.id,
                        value=i,
                    )
                )
            db.session.commit()

            assert db.session.query(TimeseriesData).all()

        ts_l = (ts_0, ts_2)

        with CurrentUser(user_1):
            with pytest.raises(BEMServerAuthorizationError):
                tsdio.delete(start_dt, end_dt, ts_l, ds_1)

        ts_l = (ts_1, ts_3)

        with CurrentUser(user_1):
            tsdio.delete(start_dt, end_dt, ts_l, ds_1)
            # Rollback then query to ensure data is actually deleted
            db.session.rollback()
            assert (
                not db.session.query(TimeseriesData)
                .filter_by(timeseries_by_data_state_id=tsbds_1.id)
                .all()
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

        assert not db.session.query(TimeseriesByDataState).all()
        assert not db.session.query(TimeseriesData).all()

        with OpenBar():
            ds_1 = TimeseriesDataState.get(name="Raw").first()

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
            tsdcsvio.import_csv(csv_file, ds_1, campaign)

        # Check TSBDS are correctly auto-created
        tsbds_l = (
            db.session.query(TimeseriesByDataState)
            .order_by(TimeseriesByDataState.id)
            .all()
        )
        assert all(tsbds.data_state_id == ds_1.id for tsbds in tsbds_l)
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

        assert not db.session.query(TimeseriesData).all()

        with OpenBar():
            ds_1 = TimeseriesDataState.get(name="Raw").first()

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
                tsdcsvio.import_csv(csv_file, ds_1, campaign)

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
            tsdcsvio.import_csv(csv_file, ds_1, campaign)

    @pytest.mark.parametrize(
        "file_error",
        (
            # Empty file
            ("", TimeseriesDataCSVIOError),
            # Empty TS name
            ("Datetime,1,\n", TimeseriesDataCSVIOError),
            # Unknown TS
            ("Datetime,1324564", TimeseriesNotFoundError),
        ),
    )
    @pytest.mark.parametrize("for_campaign", (True, False))
    def test_timeseries_data_io_import_csv_file_error(
        self, users, campaigns, for_campaign, file_error
    ):
        admin_user = users[0]
        assert admin_user.is_admin
        campaign = campaigns[0] if for_campaign else None
        csv_file, exc_cls = file_error

        with OpenBar():
            ds_1 = TimeseriesDataState.get(name="Raw").first()

        with CurrentUser(admin_user):
            with pytest.raises(exc_cls):
                tsdcsvio.import_csv(io.StringIO(csv_file), ds_1.id, campaign)

    @pytest.mark.parametrize(
        "row",
        (
            "2020-01-01T00:00:00+00:00,a",
            "dummy,1",
        ),
    )
    @pytest.mark.usefixtures("timeseries")
    @pytest.mark.parametrize("for_campaign", (True, False))
    def test_timeseries_data_io_import_csv_row_error(
        self, users, campaigns, for_campaign, row
    ):
        admin_user = users[0]
        assert admin_user.is_admin
        campaign = campaigns[0] if for_campaign else None

        with OpenBar():
            ds_1 = TimeseriesDataState.get(name="Raw").first()

        header = "Datetime,Timeseries 0\n" if for_campaign else "Datetime,1\n"
        csv_file = header + row

        with CurrentUser(admin_user):
            with pytest.raises(TimeseriesDataCSVIOError):
                tsdcsvio.import_csv(io.StringIO(csv_file), ds_1, campaign)

    @pytest.mark.usefixtures("timeseries")
    def test_timeseries_data_io_import_invalid_ts_id(self, users):
        """Check timeseries IDs provided as (non-decimal) strings instead of integers"""
        admin_user = users[0]
        assert admin_user.is_admin

        with OpenBar():
            ds_1 = TimeseriesDataState.get(name="Raw").first()

        csv_file = "Datetime,Timeseries 0\n2020-01-01T00:00:00+00:00,1"

        with CurrentUser(admin_user):
            with pytest.raises(TimeseriesDataCSVIOError):
                tsdcsvio.import_csv(io.StringIO(csv_file), ds_1.id)

    @pytest.mark.parametrize("timeseries", (5,), indirect=True)
    @pytest.mark.parametrize("col_label", ("id", "name"))
    def test_timeseries_data_io_export_csv_as_admin(self, users, timeseries, col_label):
        admin_user = users[0]
        assert admin_user.is_admin
        ts_0 = timeseries[0]
        ts_2 = timeseries[2]
        ts_4 = timeseries[4]

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

            if col_label == "name":
                header = f"Datetime,{ts_0.name},{ts_2.name},{ts_4.name}\n"
            else:
                header = f"Datetime,{ts_0.id},{ts_2.id},{ts_4.id}\n"

            ts_l = (ts_0, ts_2, ts_4)

            data = tsdcsvio.export_csv(start_dt, end_dt, ts_l, ds_1, col_label)

            assert data == header + (
                "2020-01-01T00:00:00+0000,0.0,,10.0\n"
                "2020-01-01T01:00:00+0000,1.0,,12.0\n"
                "2020-01-01T02:00:00+0000,2.0,,\n"
            )

    @pytest.mark.parametrize("timeseries", (5,), indirect=True)
    @pytest.mark.usefixtures("users_by_user_groups")
    @pytest.mark.usefixtures("user_groups_by_campaigns")
    @pytest.mark.usefixtures("user_groups_by_campaign_scopes")
    @pytest.mark.parametrize("col_label", ("id", "name"))
    def test_timeseries_data_io_export_csv_as_user(self, users, timeseries, col_label):
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

            ts_l = (ts_0, ts_2, ts_4)

            with pytest.raises(BEMServerAuthorizationError):
                data = tsdcsvio.export_csv(
                    start_dt, end_dt, ts_l, ds_1, col_label=col_label
                )

            if col_label == "name":
                header = f"Datetime,{ts_1.name},{ts_3.name}\n"
            else:
                ts_l = (ts_1.id, ts_3.id)
                header = f"Datetime,{ts_1.id},{ts_3.id}\n"

            ts_l = (ts_1, ts_3)

            data = tsdcsvio.export_csv(
                start_dt, end_dt, ts_l, ds_1, col_label=col_label
            )

            assert data == header + (
                "2020-01-01T00:00:00+0000,0.0,10.0\n"
                "2020-01-01T01:00:00+0000,1.0,12.0\n"
                "2020-01-01T02:00:00+0000,2.0,\n"
            )

    @pytest.mark.parametrize("timeseries", (5,), indirect=True)
    @pytest.mark.parametrize("col_label", ("id", "name"))
    def test_timeseries_data_io_export_csv_bucket_as_admin(
        self, users, timeseries, col_label
    ):
        admin_user = users[0]
        assert admin_user.is_admin
        ts_0 = timeseries[0]
        ts_2 = timeseries[2]
        ts_4 = timeseries[4]

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

            if col_label == "name":
                header = f"Datetime,{ts_0.name},{ts_2.name},{ts_4.name}\n"
            else:
                header = f"Datetime,{ts_0.id},{ts_2.id},{ts_4.id}\n"

            ts_l = (ts_0, ts_2, ts_4)

            # Export CSV: UTC avg
            data = tsdcsvio.export_csv_bucket(
                start_dt, end_dt, ts_l, ds_1, "1 day", col_label=col_label
            )
            assert data == header + (
                "2020-01-01T00:00:00+0000,11.5,,33.0\n"
                "2020-01-02T00:00:00+0000,35.5,,81.0\n"
                "2020-01-03T00:00:00+0000,59.5,,\n"
            )

            # Export CSV: local TZ avg
            data = tsdcsvio.export_csv_bucket(
                start_dt.replace(tzinfo=ZoneInfo("Europe/Paris")),
                end_dt.replace(tzinfo=ZoneInfo("Europe/Paris")),
                ts_l,
                ds_1,
                "1 day",
                timezone="Europe/Paris",
                col_label=col_label,
            )
            assert data == header + (
                "2020-01-01T00:00:00+0100,11.0,,32.0\n"
                "2020-01-02T00:00:00+0100,34.5,,79.0\n"
                "2020-01-03T00:00:00+0100,58.5,,104.0\n"
            )

            # Export CSV: UTC sum
            data = tsdcsvio.export_csv_bucket(
                start_dt,
                end_dt,
                ts_l,
                ds_1,
                "1 day",
                "sum",
                col_label=col_label,
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
                ds_1,
                "1 day",
                "min",
                col_label=col_label,
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
                ds_1,
                "1 day",
                "max",
                col_label=col_label,
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
                    ds_1,
                    "1 day",
                    "lol",
                    col_label=col_label,
                )

    @pytest.mark.parametrize("timeseries", (5,), indirect=True)
    @pytest.mark.usefixtures("users_by_user_groups")
    @pytest.mark.usefixtures("user_groups_by_campaigns")
    @pytest.mark.usefixtures("user_groups_by_campaign_scopes")
    @pytest.mark.parametrize("col_label", ("id", "name"))
    def test_timeseries_data_io_export_csv_bucket_as_user(
        self, users, timeseries, col_label
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

            ts_l = (ts_0, ts_2, ts_4)

            with pytest.raises(BEMServerAuthorizationError):
                data = tsdcsvio.export_csv_bucket(
                    start_dt, end_dt, ts_l, ds_1, "1 day", col_label=col_label
                )

            # Export CSV: UTC avg

            if col_label == "name":
                header = f"Datetime,{ts_1.name},{ts_3.name}\n"
            else:
                header = f"Datetime,{ts_1.id},{ts_3.id}\n"

            ts_l = (ts_1, ts_3)

            data = tsdcsvio.export_csv_bucket(
                start_dt, end_dt, ts_l, ds_1, "1 day", col_label=col_label
            )
            assert data == header + (
                "2020-01-01T00:00:00+0000,11.5,33.0\n"
                "2020-01-02T00:00:00+0000,35.5,81.0\n"
                "2020-01-03T00:00:00+0000,59.5,\n"
            )
