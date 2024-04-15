"""Timeseries I/O tests"""

import datetime as dt
import json
import math
from zoneinfo import ZoneInfo

import pytest

import numpy as np
import pandas as pd
from pandas.testing import assert_frame_equal

from bemserver_core.authorization import CurrentUser, OpenBar
from bemserver_core.database import db
from bemserver_core.exceptions import (
    BEMServerAuthorizationError,
    BEMServerCoreDimensionalityError,
    BEMServerCorePeriodError,
    BEMServerCoreUndefinedUnitError,
    TimeseriesDataCSVIOError,
    TimeseriesDataIODatetimeError,
    TimeseriesDataIOInvalidAggregationError,
    TimeseriesDataIOInvalidBucketWidthError,
    TimeseriesDataIOInvalidTimeseriesIDTypeError,
    TimeseriesDataJSONIOError,
    TimeseriesNotFoundError,
)
from bemserver_core.input_output import tsdcsvio, tsdio, tsdjsonio
from bemserver_core.model import (
    TimeseriesByDataState,
    TimeseriesData,
    TimeseriesDataState,
)
from tests.utils import create_timeseries_data


class TestTimeseriesDataIO:
    @pytest.mark.parametrize("campaigns", (2,), indirect=True)
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

        # Test conversions
        with OpenBar():
            ts_2.unit_symbol = "meter"
            tsdio.delete(
                dt.datetime(2020, 1, 1),
                dt.datetime(2020, 1, 2),
                (ts_0, ts_2),
                ds_1,
            )

        with CurrentUser(admin_user):
            tsdio.set_timeseries_data(
                data_df,
                ds_1,
                campaign,
                convert_from={ts_2.name if for_campaign else ts_2.id: "km"},
            )

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
            (timestamp, tsbds_2.id, (float(idx) + 10) * 1000)
            for idx, timestamp in enumerate(timestamps[:-1])
        ]

        assert data == expected

        # Undefined: dummy
        with CurrentUser(admin_user):
            with pytest.raises(BEMServerCoreUndefinedUnitError):
                tsdio.set_timeseries_data(
                    data_df,
                    ds_1,
                    campaign,
                    convert_from={ts_2.name if for_campaign else ts_2.id: "dummy"},
                )

        # Wrong dimension: kW vs. m
        with CurrentUser(admin_user):
            with pytest.raises(BEMServerCoreDimensionalityError):
                tsdio.set_timeseries_data(
                    data_df,
                    ds_1,
                    campaign,
                    convert_from={ts_2.name if for_campaign else ts_2.id: "kW"},
                )

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

    @pytest.mark.parametrize("for_campaign", (True, False))
    def test_timeseries_data_io_import_set_timeseries_data_columns_type_error(
        self, users, campaigns, timeseries, for_campaign
    ):
        admin_user = users[0]
        assert admin_user.is_admin
        campaign = campaigns[0] if for_campaign else None
        ts_0 = timeseries[0]

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
            # Purposely pass wrong types
            {ts_0.id if for_campaign else ts_0.name: val_0},
            index=index,
        )

        # Passing int instead of string results in int being cast to str. The
        # query is fine but the TS are not found.
        # Passing str instead of int results in cast error before query.
        expected_exc = (
            TimeseriesNotFoundError
            if for_campaign
            else TimeseriesDataIOInvalidTimeseriesIDTypeError
        )
        with CurrentUser(admin_user):
            with pytest.raises(expected_exc):
                tsdio.set_timeseries_data(data_df, ds_1, campaign)

    @pytest.mark.parametrize("for_campaign", (True, False))
    def test_timeseries_data_io_import_set_timeseries_data_empty_dataframe(
        self, users, campaigns, timeseries, for_campaign
    ):
        admin_user = users[0]
        assert admin_user.is_admin
        campaign = campaigns[0] if for_campaign else None
        ts_0 = timeseries[0]

        with OpenBar():
            ds_1 = TimeseriesDataState.get(name="Raw").first()

        # Empty dataframe
        index = pd.DatetimeIndex([])
        data_df = pd.DataFrame({}, index=index)

        # Nothing happens. No crash.
        with CurrentUser(admin_user):
            tsdio.set_timeseries_data(data_df, ds_1, campaign)

        index = pd.DatetimeIndex(["2020-01-01T00:00:00+00:00"])
        val_0 = [np.nan]

        # Not exactly empty but NaN-only dataframe
        data_df = pd.DataFrame(
            {ts_0.name if for_campaign else ts_0.id: val_0},
            index=index,
        )

        # Nothing happens. No crash.
        with CurrentUser(admin_user):
            tsdio.set_timeseries_data(data_df, ds_1, campaign)

    @pytest.mark.parametrize("timezone", ("UTC", "Europe/Paris"))
    @pytest.mark.parametrize("timeseries", (5,), indirect=True)
    def test_timeseries_data_io_get_last_as_admin(self, users, timeseries, timezone):
        admin_user = users[0]
        assert admin_user.is_admin
        ts_0 = timeseries[0]
        ts_2 = timeseries[2]
        ts_4 = timeseries[4]

        with OpenBar():
            ds_1 = TimeseriesDataState.get(name="Raw").first()

        start_dt = dt.datetime(2020, 1, 1, tzinfo=ZoneInfo(timezone))
        h1_dt = start_dt + dt.timedelta(hours=1)
        h2_dt = start_dt + dt.timedelta(hours=2)
        end_dt = start_dt + dt.timedelta(hours=3)

        ts_l = (ts_0, ts_2, ts_4)

        # No data (by col name)
        with CurrentUser(admin_user):
            data_df = tsdio.get_last(
                None, None, ts_l, ds_1, col_label="name", timezone=timezone
            )
            no_data_df = pd.DataFrame(
                index=pd.Index([ts.name for ts in ts_l], name="name"),
                columns=[
                    "timestamp",
                    "value",
                ],
            ).astype({"timestamp": f"datetime64[ns, {timezone}]", "value": float})
            assert_frame_equal(data_df, no_data_df)

        # Create data
        timestamps = pd.date_range(
            start=start_dt, end=end_dt, inclusive="left", freq="h"
        )
        create_timeseries_data(ts_0, ds_1, timestamps, [None, 0, 2])
        create_timeseries_data(ts_4, ds_1, timestamps, [12, 10, None])

        with CurrentUser(admin_user):
            # No time interval
            data_df = tsdio.get_last(None, None, ts_l, ds_1, timezone=timezone)
            expected_data_df = pd.DataFrame(
                {
                    "timestamp": [h2_dt, pd.NaT, h1_dt],
                    "value": [2.0, np.nan, 10.0],
                },
                index=pd.Index([ts.id for ts in ts_l], name="id"),
            ).astype({"timestamp": f"datetime64[ns, {timezone}]", "value": float})
            assert_frame_equal(data_df, expected_data_df)

            # Time interval, inclusive=left (default)
            data_df = tsdio.get_last(h1_dt, h2_dt, ts_l, ds_1, timezone=timezone)
            expected_data_df = pd.DataFrame(
                {
                    "timestamp": [h1_dt, pd.NaT, h1_dt],
                    "value": [0.0, np.nan, 10.0],
                },
                index=pd.Index([ts.id for ts in ts_l], name="id"),
            ).astype({"timestamp": f"datetime64[ns, {timezone}]", "value": float})
            assert_frame_equal(data_df, expected_data_df)

            # Time interval, inclusive=both
            data_df = tsdio.get_last(
                h1_dt, h2_dt, ts_l, ds_1, timezone=timezone, inclusive="both"
            )
            expected_data_df = pd.DataFrame(
                {
                    "timestamp": [h2_dt, pd.NaT, h1_dt],
                    "value": [2.0, np.nan, 10.0],
                },
                index=pd.Index([ts.id for ts in ts_l], name="id"),
            ).astype({"timestamp": f"datetime64[ns, {timezone}]", "value": float})
            assert_frame_equal(data_df, expected_data_df)

            # Time interval, inclusive=neither (no data in this case)
            data_df = tsdio.get_last(
                h1_dt,
                h2_dt,
                ts_l,
                ds_1,
                timezone=timezone,
                inclusive="neither",
                col_label="name",
            )
            assert_frame_equal(data_df, no_data_df)

            # Time interval, start_dt only
            data_df = tsdio.get_last(h2_dt, None, ts_l, ds_1, timezone=timezone)
            expected_data_df = pd.DataFrame(
                {
                    "timestamp": [h2_dt, pd.NaT, pd.NaT],
                    "value": [2.0, np.nan, np.nan],
                },
                index=pd.Index([ts.id for ts in ts_l], name="id"),
            ).astype({"timestamp": f"datetime64[ns, {timezone}]", "value": float})
            assert_frame_equal(data_df, expected_data_df)

            # Time interval, end_dt only
            data_df = tsdio.get_last(None, h1_dt, ts_l, ds_1, timezone=timezone)
            expected_data_df = pd.DataFrame(
                {
                    "timestamp": [pd.NaT, pd.NaT, start_dt],
                    "value": [np.nan, np.nan, 12.0],
                },
                index=pd.Index([ts.id for ts in ts_l], name="id"),
            ).astype({"timestamp": f"datetime64[ns, {timezone}]", "value": float})
            assert_frame_equal(data_df, expected_data_df)

    @pytest.mark.parametrize("campaigns", (2,), indirect=True)
    @pytest.mark.parametrize("timeseries", (5,), indirect=True)
    @pytest.mark.usefixtures("users_by_user_groups")
    @pytest.mark.usefixtures("user_groups_by_campaigns")
    @pytest.mark.usefixtures("user_groups_by_campaign_scopes")
    @pytest.mark.parametrize("timezone", ("UTC", "Europe/Paris"))
    def test_timeseries_data_io_get_last_as_user(self, users, timeseries, timezone):
        user_1 = users[1]
        assert not user_1.is_admin
        ts_0 = timeseries[0]
        ts_1 = timeseries[1]
        ts_2 = timeseries[2]
        ts_3 = timeseries[3]
        ts_4 = timeseries[4]

        with OpenBar():
            ds_1 = TimeseriesDataState.get(name="Raw").first()

        start_dt = dt.datetime(2020, 1, 1, tzinfo=ZoneInfo(timezone))
        h1_dt = start_dt + dt.timedelta(hours=1)
        h2_dt = start_dt + dt.timedelta(hours=2)
        end_dt = start_dt + dt.timedelta(hours=3)

        # Create data
        timestamps = pd.date_range(
            start=start_dt, end=end_dt, inclusive="left", freq="h"
        )
        create_timeseries_data(ts_1, ds_1, timestamps, [None, 0, 2])
        create_timeseries_data(ts_3, ds_1, timestamps, [12, 10, None])

        with CurrentUser(user_1):
            ts_l = (ts_0, ts_2, ts_4)
            with pytest.raises(BEMServerAuthorizationError):
                tsdio.get_timeseries_stats(ts_l, ds_1, timezone=timezone)

            ts_l = (ts_1, ts_3)
            data_df = tsdio.get_last(None, None, ts_l, ds_1, timezone=timezone)

            expected_data_df = pd.DataFrame(
                {
                    "timestamp": [h2_dt, h1_dt],
                    "value": [2.0, 10.0],
                },
                index=pd.Index([ts.id for ts in ts_l], name="id"),
            ).astype(
                {
                    "timestamp": f"datetime64[ns, {timezone}]",
                    "value": float,
                }
            )
            assert_frame_equal(data_df, expected_data_df)

    @pytest.mark.parametrize("timezone", ("UTC", "Europe/Paris"))
    @pytest.mark.parametrize("timeseries", (5,), indirect=True)
    def test_timeseries_data_io_get_timeseries_stats_as_admin(
        self, users, timeseries, timezone
    ):
        admin_user = users[0]
        assert admin_user.is_admin
        ts_0 = timeseries[0]
        ts_2 = timeseries[2]
        ts_4 = timeseries[4]

        with OpenBar():
            ds_1 = TimeseriesDataState.get(name="Raw").first()

        start_dt = dt.datetime(2020, 1, 1, tzinfo=ZoneInfo(timezone))
        h1_dt = start_dt + dt.timedelta(hours=1)
        h2_dt = start_dt + dt.timedelta(hours=2)
        end_dt = start_dt + dt.timedelta(hours=3)

        ts_l = (ts_0, ts_2, ts_4)

        # No data (by col name)
        with CurrentUser(admin_user):
            data_df = tsdio.get_timeseries_stats(
                ts_l, ds_1, col_label="name", timezone=timezone
            )
            no_data_df = pd.DataFrame(
                index=pd.Index([ts.name for ts in ts_l], name="name"),
                columns=[
                    "first_timestamp",
                    "last_timestamp",
                    "count",
                    "min",
                    "max",
                    "avg",
                    "stddev",
                ],
            )
            no_data_df["count"] = no_data_df["count"].fillna(0)
            no_data_df = no_data_df.astype(
                {
                    "first_timestamp": f"datetime64[ns, {timezone}]",
                    "last_timestamp": f"datetime64[ns, {timezone}]",
                    "count": int,
                    "min": float,
                    "max": float,
                    "avg": float,
                    "stddev": float,
                }
            )
            assert_frame_equal(data_df, no_data_df)

        # Create data
        timestamps = pd.date_range(
            start=start_dt, end=end_dt, inclusive="left", freq="h"
        )
        create_timeseries_data(ts_0, ds_1, timestamps, [None, 0, 2])
        create_timeseries_data(ts_4, ds_1, timestamps, [10, 12, None])

        with CurrentUser(admin_user):
            data_df = tsdio.get_timeseries_stats(ts_l, ds_1, timezone=timezone)
            expected_data_df = pd.DataFrame(
                {
                    "first_timestamp": [h1_dt, pd.NaT, start_dt],
                    "last_timestamp": [h2_dt, pd.NaT, h1_dt],
                    "count": [2, 0, 2],
                    "min": [0.0, np.nan, 10.0],
                    "max": [2.0, np.nan, 12.0],
                    "avg": [1.0, np.nan, 11.0],
                    "stddev": [math.sqrt(2), np.nan, math.sqrt(2)],
                },
                index=pd.Index([ts.id for ts in ts_l], name="id"),
            ).astype(
                {
                    "first_timestamp": f"datetime64[ns, {timezone}]",
                    "last_timestamp": f"datetime64[ns, {timezone}]",
                    "count": int,
                    "min": float,
                    "max": float,
                    "avg": float,
                    "stddev": float,
                }
            )
            assert_frame_equal(data_df, expected_data_df)

    @pytest.mark.parametrize("campaigns", (2,), indirect=True)
    @pytest.mark.parametrize("timeseries", (5,), indirect=True)
    @pytest.mark.usefixtures("users_by_user_groups")
    @pytest.mark.usefixtures("user_groups_by_campaigns")
    @pytest.mark.usefixtures("user_groups_by_campaign_scopes")
    @pytest.mark.parametrize("timezone", ("UTC", "Europe/Paris"))
    def test_timeseries_data_io_get_timeseries_stats_as_user(
        self, users, timeseries, timezone
    ):
        user_1 = users[1]
        assert not user_1.is_admin
        ts_0 = timeseries[0]
        ts_1 = timeseries[1]
        ts_2 = timeseries[2]
        ts_3 = timeseries[3]
        ts_4 = timeseries[4]

        with OpenBar():
            ds_1 = TimeseriesDataState.get(name="Raw").first()

        start_dt = dt.datetime(2020, 1, 1, tzinfo=ZoneInfo(timezone))
        h1_dt = start_dt + dt.timedelta(hours=1)
        h2_dt = start_dt + dt.timedelta(hours=2)
        end_dt = start_dt + dt.timedelta(hours=3)

        # Create data
        timestamps = pd.date_range(
            start=start_dt, end=end_dt, inclusive="left", freq="h"
        )
        create_timeseries_data(ts_1, ds_1, timestamps, [None, 0, 2])
        create_timeseries_data(ts_3, ds_1, timestamps, [10, 12, None])

        with CurrentUser(user_1):
            ts_l = (ts_0, ts_2, ts_4)
            with pytest.raises(BEMServerAuthorizationError):
                tsdio.get_timeseries_stats(ts_l, ds_1, timezone=timezone)

            ts_l = (ts_1, ts_3)
            data_df = tsdio.get_timeseries_stats(ts_l, ds_1, timezone=timezone)

            expected_data_df = pd.DataFrame(
                {
                    "first_timestamp": [h1_dt, start_dt],
                    "last_timestamp": [h2_dt, h1_dt],
                    "count": [2, 2],
                    "min": [0.0, 10.0],
                    "max": [2.0, 12.0],
                    "avg": [1.0, 11.0],
                    "stddev": [math.sqrt(2), math.sqrt(2)],
                },
                index=pd.Index([ts.id for ts in ts_l], name="id"),
            ).astype(
                {
                    "first_timestamp": f"datetime64[ns, {timezone}]",
                    "last_timestamp": f"datetime64[ns, {timezone}]",
                    "count": int,
                    "min": float,
                    "max": float,
                    "avg": float,
                    "stddev": float,
                }
            )
            assert_frame_equal(data_df, expected_data_df)

    @pytest.mark.parametrize("timeseries", (5,), indirect=True)
    def test_timeseries_data_io_get_timeseries_data_as_admin(
        self,
        users,
        timeseries,
    ):
        admin_user = users[0]
        assert admin_user.is_admin
        ts_0 = timeseries[0]
        ts_2 = timeseries[2]
        ts_4 = timeseries[4]

        with OpenBar():
            ds_1 = TimeseriesDataState.get(name="Raw").first()

        start_dt = dt.datetime(2020, 1, 1, tzinfo=dt.timezone.utc)
        h1_dt = start_dt + dt.timedelta(hours=1)
        h2_dt = start_dt + dt.timedelta(hours=2)
        end_dt = start_dt + dt.timedelta(hours=3)

        ts_l = (ts_0, ts_2, ts_4)

        # No data (by col name)
        with CurrentUser(admin_user):
            data_df = tsdio.get_timeseries_data(
                start_dt, end_dt, ts_l, ds_1, col_label="name"
            )
            index = pd.DatetimeIndex([], name="timestamp", tz="UTC")
            no_data_df = pd.DataFrame(
                {ts_0.name: [], ts_2.name: [], ts_4.name: []},
                index=index,
            )
            assert data_df.equals(no_data_df)

        # Create data
        timestamps = pd.date_range(
            start=start_dt, end=end_dt, inclusive="left", freq="h"
        )
        values_1 = range(3)
        create_timeseries_data(ts_0, ds_1, timestamps, values_1)
        values_2 = [10 + 2 * i for i in range(2)]
        create_timeseries_data(ts_4, ds_1, timestamps[:2], values_2)

        with CurrentUser(admin_user):
            data_df = tsdio.get_timeseries_data(start_dt, end_dt, ts_l, ds_1)
            index = pd.DatetimeIndex(
                [
                    "2020-01-01T00:00:00+00:00",
                    "2020-01-01T01:00:00+00:00",
                    "2020-01-01T02:00:00+00:00",
                ],
                name="timestamp",
                tz="UTC",
            )
            val_0 = [0.0, 1.0, 2.0]
            val_2 = [np.nan, np.nan, np.nan]
            val_4 = [10.0, 12.0, np.nan]
            expected_data_df = pd.DataFrame(
                {ts_0.id: val_0, ts_2.id: val_2, ts_4.id: val_4},
                index=index,
            )
            assert data_df.equals(expected_data_df)

            # Get with no start_date
            data_df = tsdio.get_timeseries_data(None, end_dt, ts_l, ds_1)
            assert data_df.equals(expected_data_df)

            # Get with no end date
            data_df = tsdio.get_timeseries_data(start_dt, None, ts_l, ds_1)
            assert data_df.equals(expected_data_df)

            # Get with no start/end date
            data_df = tsdio.get_timeseries_data(None, None, ts_l, ds_1)
            assert data_df.equals(expected_data_df)

            # Get outside data range: no data
            data_df = tsdio.get_timeseries_data(
                end_dt, None, ts_l, ds_1, col_label="name"
            )
            assert data_df.equals(no_data_df)

            # Get with TZ
            data_df = tsdio.get_timeseries_data(
                start_dt,
                end_dt,
                ts_l,
                ds_1,
                timezone="Europe/Paris",
            )
            expected_data_df.index = pd.DatetimeIndex(
                [
                    "2020-01-01T00:00:00+00:00",
                    "2020-01-01T01:00:00+00:00",
                    "2020-01-01T02:00:00+00:00",
                ],
                name="timestamp",
                tz=ZoneInfo("Europe/Paris"),
            )
            assert data_df.equals(expected_data_df)

            # Test conversions
            with OpenBar():
                ts_0.unit_symbol = "meter"

            data_df = tsdio.get_timeseries_data(
                start_dt,
                end_dt,
                ts_l,
                ds_1,
                convert_to={ts_0.id: "mm"},
            )
            index = pd.DatetimeIndex(
                [
                    "2020-01-01T00:00:00+00:00",
                    "2020-01-01T01:00:00+00:00",
                    "2020-01-01T02:00:00+00:00",
                ],
                name="timestamp",
                tz="UTC",
            )
            val_0 = [1000 * 0.0, 1000 * 1.0, 1000 * 2.0]
            val_2 = [np.nan, np.nan, np.nan]
            val_4 = [10.0, 12.0, np.nan]
            expected_data_df = pd.DataFrame(
                {ts_0.id: val_0, ts_2.id: val_2, ts_4.id: val_4},
                index=index,
            )
            assert data_df.equals(expected_data_df)
            # Undefined: dummy
            with pytest.raises(BEMServerCoreUndefinedUnitError):
                data_df = tsdio.get_timeseries_data(
                    start_dt,
                    end_dt,
                    ts_l,
                    ds_1,
                    convert_to={ts_0.id: "dummy"},
                )
            # Wrong dimension: kW vs. m
            with pytest.raises(BEMServerCoreDimensionalityError):
                data_df = tsdio.get_timeseries_data(
                    start_dt,
                    end_dt,
                    ts_l,
                    ds_1,
                    convert_to={ts_0.id: "kW"},
                )

            # Purposely set order different than ID order to check output ordering
            ts_l = (ts_2, ts_0, ts_4)

            # Test inclusive
            index = pd.DatetimeIndex(
                [
                    "2020-01-01T01:00:00+00:00",
                    "2020-01-01T02:00:00+00:00",
                ],
                name="timestamp",
                tz="UTC",
            )
            val_0 = [1.0, 2.0]
            val_2 = [np.nan, np.nan]
            val_4 = [12.0, np.nan]
            expected_data_df = pd.DataFrame(
                {ts_2.id: val_2, ts_0.id: val_0, ts_4.id: val_4},
                index=index,
            )
            data_df = tsdio.get_timeseries_data(
                h1_dt, h2_dt, ts_l, ds_1, inclusive="both"
            )
            assert data_df.equals(expected_data_df)
            data_df = tsdio.get_timeseries_data(
                h1_dt, h2_dt, ts_l, ds_1, inclusive="neither"
            )
            mask = (expected_data_df.index > h1_dt) & (expected_data_df.index < h2_dt)
            assert data_df.equals(expected_data_df.loc[mask])
            data_df = tsdio.get_timeseries_data(
                h1_dt, h2_dt, ts_l, ds_1, inclusive="left"
            )
            mask = (expected_data_df.index >= h1_dt) & (expected_data_df.index < h2_dt)
            assert data_df.equals(expected_data_df.loc[mask])
            data_df = tsdio.get_timeseries_data(
                h1_dt, h2_dt, ts_l, ds_1, inclusive="right"
            )
            mask = (expected_data_df.index > h1_dt) & (expected_data_df.index <= h2_dt)
            assert data_df.equals(expected_data_df.loc[mask])

    @pytest.mark.parametrize("campaigns", (2,), indirect=True)
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

        with OpenBar():
            ds_1 = TimeseriesDataState.get(name="Raw").first()

        start_dt = dt.datetime(2020, 1, 1, tzinfo=dt.timezone.utc)
        end_dt = start_dt + dt.timedelta(hours=3)

        timestamps = pd.date_range(
            start=start_dt, end=end_dt, inclusive="left", freq="h"
        )
        values_1 = range(3)
        create_timeseries_data(ts_1, ds_1, timestamps, values_1)
        values_2 = [10 + 2 * i for i in range(2)]
        create_timeseries_data(ts_3, ds_1, timestamps[:2], values_2)

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
            tz="UTC",
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

        with OpenBar():
            ds_1 = TimeseriesDataState.get(name="Raw").first()

        start_dt = dt.datetime(2020, 1, 1, tzinfo=dt.timezone.utc)
        end_dt = start_dt + dt.timedelta(hours=24 * 3)

        timestamps = pd.date_range(
            start=start_dt, end=end_dt, inclusive="left", freq="h"
        )

        ts_l = (ts_0, ts_2, ts_4)

        with CurrentUser(admin_user):
            # No data
            data_df = tsdio.get_timeseries_buckets_data(
                start_dt,
                end_dt,
                ts_l,
                ds_1,
                1,
                "day",
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
                    ts_0.id: [np.nan, np.nan, np.nan],
                    ts_2.id: [np.nan, np.nan, np.nan],
                    ts_4.id: [np.nan, np.nan, np.nan],
                },
                index=index,
            )
            assert data_df.equals(expected_data_df)

        values_1 = range(24 * 3)
        create_timeseries_data(ts_0, ds_1, timestamps, values_1)
        values_2 = [10 + 2 * i for i in range(24 * 2)]
        create_timeseries_data(ts_4, ds_1, timestamps[: 24 * 2], values_2)

        with CurrentUser(admin_user):
            # UTC count 1 day
            data_df = tsdio.get_timeseries_buckets_data(
                start_dt,
                end_dt,
                ts_l,
                ds_1,
                1,
                "day",
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

            # UTC count 1 day with gapfill
            data_df = tsdio.get_timeseries_buckets_data(
                start_dt,
                end_dt + dt.timedelta(days=3),
                ts_l,
                ds_1,
                1,
                "day",
                "count",
                col_label="name",
            )

            index = pd.DatetimeIndex(
                [
                    "2020-01-01T00:00:00",
                    "2020-01-02T00:00:00",
                    "2020-01-03T00:00:00",
                    "2020-01-04T00:00:00",
                    "2020-01-05T00:00:00",
                    "2020-01-06T00:00:00",
                ],
                name="timestamp",
                tz="UTC",
            )
            expected_data_df = pd.DataFrame(
                {
                    ts_0.name: [24, 24, 24, 0, 0, 0],
                    ts_2.name: [0, 0, 0, 0, 0, 0],
                    ts_4.name: [24, 24, 0, 0, 0, 0],
                },
                index=index,
            )
            assert data_df.equals(expected_data_df)

            # UTC count 1 day, 3 hour (and a half) offset
            # start time is floored to round to interval
            data_df = tsdio.get_timeseries_buckets_data(
                start_dt + dt.timedelta(hours=3, minutes=30),
                end_dt,
                ts_l,
                ds_1,
                1,
                "day",
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

            # Local TZ count 1 week
            data_df = tsdio.get_timeseries_buckets_data(
                # Check start_dt TZ doesn't change alignment to 00:00 local TZ
                dt.datetime(2020, 1, 1, tzinfo=dt.timezone.utc),
                dt.datetime(2020, 1, 9, tzinfo=ZoneInfo("Europe/Paris")),
                ts_l,
                ds_1,
                1,
                "week",
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
                tz=ZoneInfo("Europe/Paris"),
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

            # UTC count 12 hours
            data_df = tsdio.get_timeseries_buckets_data(
                start_dt,
                end_dt,
                ts_l,
                ds_1,
                12,
                "hour",
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

            # Local TZ avg
            data_df = tsdio.get_timeseries_buckets_data(
                start_dt.replace(tzinfo=ZoneInfo("Europe/Paris")),
                end_dt.replace(tzinfo=ZoneInfo("Europe/Paris")),
                ts_l,
                ds_1,
                1,
                "day",
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
                tz=ZoneInfo("Europe/Paris"),
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

            # UTC sum, with gapfill
            data_df = tsdio.get_timeseries_buckets_data(
                start_dt,
                end_dt + dt.timedelta(days=1),
                ts_l,
                ds_1,
                1,
                "day",
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

            # UTC min, with gapfill
            data_df = tsdio.get_timeseries_buckets_data(
                start_dt,
                end_dt + dt.timedelta(days=1),
                ts_l,
                ds_1,
                1,
                "day",
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

            # UTC max, with gapfill
            data_df = tsdio.get_timeseries_buckets_data(
                start_dt,
                end_dt + dt.timedelta(days=1),
                ts_l,
                ds_1,
                1,
                "day",
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

            # Test conversions
            with OpenBar():
                ts_0.unit_symbol = "meter"

            data_df = tsdio.get_timeseries_buckets_data(
                start_dt,
                end_dt,
                ts_l,
                ds_1,
                1,
                "day",
                convert_to={ts_0.id: "mm"},
            )
            index = pd.DatetimeIndex(
                [
                    "2020-01-01T00:00:00+00:00",
                    "2020-01-02T00:00:00+00:00",
                    "2020-01-03T00:00:00+00:00",
                ],
                name="timestamp",
                tz="UTC",
            )
            val_0 = [1000 * 11.5, 1000 * 35.5, 1000 * 59.5]
            val_2 = [np.nan, np.nan, np.nan]
            val_4 = [33.0, 81.0, np.nan]
            expected_data_df = pd.DataFrame(
                {ts_0.id: val_0, ts_2.id: val_2, ts_4.id: val_4},
                index=index,
            )
            assert data_df.equals(expected_data_df)
            # Undefined: dummy
            with pytest.raises(BEMServerCoreUndefinedUnitError):
                data_df = tsdio.get_timeseries_buckets_data(
                    start_dt,
                    end_dt,
                    ts_l,
                    ds_1,
                    1,
                    "day",
                    convert_to={ts_0.id: "dummy"},
                )
            # Wrong dimension: kW vs. m
            with pytest.raises(BEMServerCoreDimensionalityError):
                data_df = tsdio.get_timeseries_buckets_data(
                    start_dt,
                    end_dt,
                    ts_l,
                    ds_1,
                    1,
                    "day",
                    convert_to={ts_0.id: "kW"},
                )
            # Wrong dimension: mm vs. count
            with pytest.raises(BEMServerCoreDimensionalityError):
                data_df = tsdio.get_timeseries_buckets_data(
                    start_dt,
                    end_dt,
                    ts_l,
                    ds_1,
                    1,
                    "day",
                    "count",
                    convert_to={ts_0.id: "mm"},
                )
            # count vs. count OK (data unchanged)
            data_df = tsdio.get_timeseries_buckets_data(
                start_dt,
                end_dt,
                ts_l,
                ds_1,
                1,
                "day",
                "count",
                convert_to={ts_0.id: "count"},
            )
            index = pd.DatetimeIndex(
                [
                    "2020-01-01T00:00:00+00:00",
                    "2020-01-02T00:00:00+00:00",
                    "2020-01-03T00:00:00+00:00",
                ],
                name="timestamp",
                tz="UTC",
            )
            val_0 = [24, 24, 24]
            val_2 = [0, 0, 0]
            val_4 = [24, 24, 0]
            expected_data_df = pd.DataFrame(
                {ts_0.id: val_0, ts_2.id: val_2, ts_4.id: val_4},
                index=index,
            )
            assert data_df.equals(expected_data_df)

            # Purposely set order different than ID order to check output ordering
            ts_l = (ts_2, ts_0, ts_4)

            # UTC count 1 day by ID
            data_df = tsdio.get_timeseries_buckets_data(
                start_dt,
                end_dt,
                ts_l,
                ds_1,
                1,
                "day",
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
                    ts_2.id: [0, 0, 0],
                    ts_0.id: [24, 24, 24],
                    ts_4.id: [24, 24, 0],
                },
                index=index,
            )
            assert data_df.equals(expected_data_df)

            # No timeseries
            data_df = tsdio.get_timeseries_buckets_data(
                start_dt, end_dt, [], ds_1, 1, "day"
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
            expected_data_df = pd.DataFrame({}, index=index)
            assert data_df.equals(expected_data_df)

            # Invalid aggregation
            with pytest.raises(TimeseriesDataIOInvalidAggregationError):
                tsdio.get_timeseries_buckets_data(
                    start_dt,
                    end_dt,
                    ts_l,
                    ds_1,
                    1,
                    "day",
                    "dummy",
                )

            with pytest.raises(TimeseriesDataIOInvalidBucketWidthError):
                tsdio.get_timeseries_buckets_data(
                    start_dt,
                    end_dt,
                    ts_l,
                    ds_1,
                    -1,
                    "day",
                    "avg",
                )

            with pytest.raises(TimeseriesDataIOInvalidBucketWidthError):
                tsdio.get_timeseries_buckets_data(
                    start_dt,
                    end_dt,
                    ts_l,
                    ds_1,
                    1,
                    "dummy",
                    "avg",
                )

            with pytest.raises(BEMServerCorePeriodError):
                # 2 weeks
                tsdio.get_timeseries_buckets_data(
                    start_dt, end_dt, ts_l, ds_1, 2, "week", "count"
                )

            with pytest.raises(BEMServerCorePeriodError):
                # 2 days
                tsdio.get_timeseries_buckets_data(
                    start_dt, end_dt, ts_l, ds_1, 2, "day", "count"
                )

    def test_timeseries_data_io_get_timeseries_buckets_data_dst_as_admin(
        self, users, timeseries
    ):
        """Non-regression test for issue with ambiguous datetimes on DST change"""
        admin_user = users[0]
        assert admin_user.is_admin
        ts_0 = timeseries[0]

        with OpenBar():
            ds_1 = TimeseriesDataState.get(name="Raw").first()

        start_dt = dt.datetime(2020, 10, 25, tzinfo=dt.timezone.utc)
        end_dt = dt.datetime(2020, 10, 26, tzinfo=dt.timezone.utc)

        timestamps = pd.date_range(
            start=start_dt, end=end_dt, inclusive="left", freq="h"
        )

        ts_l = (ts_0,)

        values_1 = range(24)
        create_timeseries_data(ts_0, ds_1, timestamps, values_1)

        with CurrentUser(admin_user):
            # Local TZ count 1 hour
            data_df = tsdio.get_timeseries_buckets_data(
                dt.datetime(2020, 10, 25, 0, 0, tzinfo=dt.timezone.utc),
                dt.datetime(2020, 10, 25, 2, 0, tzinfo=dt.timezone.utc),
                ts_l,
                ds_1,
                1,
                "hour",
                "count",
                timezone="Europe/Paris",
                col_label="name",
            )

            index = pd.DatetimeIndex(
                [
                    "2020-10-25T00:00:00",
                    "2020-10-25T01:00:00",
                ],
                name="timestamp",
                tz="UTC",
            ).tz_convert(ZoneInfo("Europe/Paris"))
            expected_data_df = pd.DataFrame({ts_0.name: [1, 1]}, index=index)

            assert data_df.equals(expected_data_df)

    def test_timeseries_data_io_get_timeseries_buckets_data_fixed_size_dst_as_admin(
        self, users, timeseries
    ):
        """Check bucketing in local TZ correctly handles DST"""
        admin_user = users[0]
        assert admin_user.is_admin
        ts_0 = timeseries[0]

        with OpenBar():
            ds_1 = TimeseriesDataState.get(name="Raw").first()

        # Use UTC for start_dt otherwise start_dt + timedelta does surprising things
        # https://blog.ganssle.io/articles/2018/02/aware-datetime-arithmetic.html
        # In fact, use UTC for all for date_range
        start_dt_1 = dt.datetime(
            2020, 3, 28, tzinfo=ZoneInfo("Europe/Paris")
        ).astimezone(dt.timezone.utc)
        end_dt_1 = dt.datetime(2020, 3, 30, tzinfo=ZoneInfo("Europe/Paris")).astimezone(
            dt.timezone.utc
        )
        start_dt_2 = dt.datetime(
            2020, 10, 24, tzinfo=ZoneInfo("Europe/Paris")
        ).astimezone(dt.timezone.utc)
        end_dt_2 = dt.datetime(
            2020, 10, 26, tzinfo=ZoneInfo("Europe/Paris")
        ).astimezone(dt.timezone.utc)

        timestamps_1 = pd.date_range(
            start=start_dt_1, end=end_dt_1, inclusive="left", freq="h"
        )
        values_1 = range(len(timestamps_1))
        create_timeseries_data(ts_0, ds_1, timestamps_1, values_1)
        timestamps_2 = pd.date_range(
            start=start_dt_2, end=end_dt_2, inclusive="left", freq="h"
        )
        values_2 = [10 + 2 * i for i in range(len(timestamps_2))]
        create_timeseries_data(ts_0, ds_1, timestamps_2, values_2)

        with CurrentUser(admin_user):
            args = [(ts_0,), ds_1, 1, "day", "count"]
            kwargs = {"timezone": "Europe/Paris", "col_label": "name"}

            # local TZ count 1 day - Spring forward
            data_df = tsdio.get_timeseries_buckets_data(
                start_dt_1, end_dt_1, *args, **kwargs
            )
            index = pd.DatetimeIndex(
                ["2020-03-28T00:00:00", "2020-03-29T00:00:00"],
                name="timestamp",
                tz=ZoneInfo("Europe/Paris"),
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
                tz=ZoneInfo("Europe/Paris"),
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

        with OpenBar():
            ds_1 = TimeseriesDataState.get(name="Raw").first()

        start_dt = dt.datetime(2020, 1, 1, tzinfo=dt.timezone.utc)
        start_dt_plus_3_months = dt.datetime(2020, 4, 1, tzinfo=dt.timezone.utc)
        end_dt = start_dt + dt.timedelta(days=366 + 365)

        timestamps = pd.date_range(
            start=start_dt, end=end_dt, inclusive="left", freq="h"
        )
        values_1 = range(24 * (366 + 365))
        create_timeseries_data(ts_0, ds_1, timestamps, values_1)
        values_2 = [10 + 2 * i for i in range(24 * 366)]
        create_timeseries_data(ts_4, ds_1, timestamps[: 24 * 366], values_2)

        ts_l = (ts_0, ts_2, ts_4)

        with CurrentUser(admin_user):
            # UTC count year
            data_df = tsdio.get_timeseries_buckets_data(
                start_dt, end_dt, ts_l, ds_1, 1, "year", "count", col_label="name"
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

            # UTC count year, with gapfill
            data_df = tsdio.get_timeseries_buckets_data(
                start_dt,
                end_dt + dt.timedelta(days=100),
                ts_l,
                ds_1,
                1,
                "year",
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

            # Local TZ count year - start_dt floored
            data_df = tsdio.get_timeseries_buckets_data(
                start_dt_plus_3_months.replace(tzinfo=ZoneInfo("Europe/Paris")),
                end_dt.replace(tzinfo=ZoneInfo("Europe/Paris")),
                ts_l,
                ds_1,
                1,
                "year",
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
                tz=ZoneInfo("Europe/Paris"),
            )
            expected_data_df = pd.DataFrame(
                {
                    ts_0.name: [
                        # 1st sample missing because UTC+1
                        24 * 366 - 1,
                        24 * 365,
                    ],
                    ts_2.name: [0, 0],
                    ts_4.name: [
                        # 1st sample missing because UTC+1
                        24 * 366 - 1,
                        1,
                    ],
                },
                index=index,
            )
            assert data_df.equals(expected_data_df)

            # UTC avg month, with gapfill
            data_df = tsdio.get_timeseries_buckets_data(
                # Exact start day does not matter, data is aligned to month
                start_dt - dt.timedelta(days=20),
                start_dt_plus_3_months,
                ts_l,
                ds_1,
                1,
                "month",
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

            # UTC avg month
            data_df = tsdio.get_timeseries_buckets_data(
                start_dt + dt.timedelta(days=3),
                start_dt_plus_3_months,
                ts_l,
                ds_1,
                1,
                "month",
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
                    ts_0.name: [371.5, 1091.5, 1811.5],
                    ts_2.name: [np.nan, np.nan, np.nan],
                    ts_4.name: [753.0, 2193.0, 3633.0],
                },
                index=index,
            )

            assert data_df.equals(expected_data_df)

            # Local TZ avg month
            data_df = tsdio.get_timeseries_buckets_data(
                start_dt,
                start_dt_plus_3_months,
                ts_l,
                ds_1,
                1,
                "month",
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
                tz=ZoneInfo("Europe/Paris"),
            )
            expected_data_df = pd.DataFrame(
                {
                    ts_0.name: [371.0, 1090.5, 1810.0, 2541.5],
                    ts_2.name: [np.nan, np.nan, np.nan, np.nan],
                    ts_4.name: [752.0, 2191.0, 3630.0, 5093.0],
                },
                index=index,
            )

            assert data_df.equals(expected_data_df)

            # UTC sum month
            data_df = tsdio.get_timeseries_buckets_data(
                start_dt,
                start_dt_plus_3_months,
                ts_l,
                ds_1,
                1,
                "month",
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

            # UTC min month
            data_df = tsdio.get_timeseries_buckets_data(
                start_dt,
                start_dt_plus_3_months,
                ts_l,
                ds_1,
                1,
                "month",
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

            # UTC max month
            data_df = tsdio.get_timeseries_buckets_data(
                start_dt,
                start_dt_plus_3_months,
                ts_l,
                ds_1,
                1,
                "month",
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

            # UTC count year by ID
            data_df = tsdio.get_timeseries_buckets_data(
                start_dt, end_dt, ts_l, ds_1, 1, "year", "count", col_label="id"
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

            # No timeseries
            data_df = tsdio.get_timeseries_buckets_data(
                start_dt, end_dt, [], ds_1, 1, "year"
            )

            index = pd.DatetimeIndex(
                [
                    "2020-01-01T00:00:00",
                    "2021-01-01T00:00:00",
                ],
                name="timestamp",
                tz="UTC",
            )
            expected_data_df = pd.DataFrame({}, index=index)
            assert data_df.equals(expected_data_df)

            with pytest.raises(BEMServerCorePeriodError):
                # 2 years
                tsdio.get_timeseries_buckets_data(
                    start_dt, end_dt, ts_l, ds_1, 2, "year", "count"
                )

            with pytest.raises(BEMServerCorePeriodError):
                # 2 months
                tsdio.get_timeseries_buckets_data(
                    start_dt, end_dt, ts_l, ds_1, 2, "month"
                )

    @pytest.mark.parametrize("campaigns", (2,), indirect=True)
    @pytest.mark.parametrize("timeseries", (5,), indirect=True)
    @pytest.mark.usefixtures("users_by_user_groups")
    @pytest.mark.usefixtures("user_groups_by_campaigns")
    @pytest.mark.usefixtures("user_groups_by_campaign_scopes")
    @pytest.mark.parametrize("col_label", ("id", "name"))
    def test_timeseries_data_io_get_timeseries_buckets_data_as_user(
        self, users, timeseries, col_label
    ):
        user_1 = users[1]
        assert not user_1.is_admin
        ts_0 = timeseries[0]
        ts_1 = timeseries[1]
        ts_2 = timeseries[2]
        ts_3 = timeseries[3]
        ts_4 = timeseries[4]

        with OpenBar():
            ds_1 = TimeseriesDataState.get(name="Raw").first()

        start_dt = dt.datetime(2020, 1, 1, tzinfo=dt.timezone.utc)
        end_dt = start_dt + dt.timedelta(hours=24 * 3)

        timestamps = pd.date_range(
            start=start_dt, end=end_dt, inclusive="left", freq="h"
        )
        values_1 = range(24 * 3)
        create_timeseries_data(ts_1, ds_1, timestamps, values_1)
        values_2 = [10 + 2 * i for i in range(24 * 2)]
        create_timeseries_data(ts_3, ds_1, timestamps[: 24 * 2], values_2)

        with CurrentUser(user_1):
            ts_l = (ts_0, ts_2, ts_4)

            with pytest.raises(BEMServerAuthorizationError):
                tsdio.get_timeseries_buckets_data(
                    start_dt, end_dt, ts_l, ds_1, 1, "day", col_label=col_label
                )

            # UTC avg

            ts_l = (ts_1, ts_3)

            data_df = tsdio.get_timeseries_buckets_data(
                start_dt, end_dt, ts_l, ds_1, 1, "day", col_label=col_label
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

        with OpenBar():
            ds_1 = TimeseriesDataState.get(name="Raw").first()

        start_dt = dt.datetime(2020, 1, 1, tzinfo=dt.timezone.utc)
        end_dt = start_dt + dt.timedelta(hours=3)

        timestamps = pd.date_range(
            start=start_dt, end=end_dt, inclusive="left", freq="h"
        )
        values = range(3)
        create_timeseries_data(ts_0, ds_1, timestamps, values)

        assert db.session.query(TimeseriesData).all()

        ts_l = (ts_0, ts_2)

        with CurrentUser(admin_user):
            tsdio.delete(start_dt, end_dt, ts_l, ds_1)
            assert not db.session.query(TimeseriesData).all()

    @pytest.mark.parametrize("campaigns", (2,), indirect=True)
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

        with OpenBar():
            ds_1 = TimeseriesDataState.get(name="Raw").first()
            tsbds_1 = ts_1.get_timeseries_by_data_state(ds_1)

        start_dt = dt.datetime(2020, 1, 1, tzinfo=dt.timezone.utc)
        end_dt = start_dt + dt.timedelta(hours=3)

        timestamps = pd.date_range(
            start=start_dt, end=end_dt, inclusive="left", freq="h"
        )
        values = range(3)
        create_timeseries_data(ts_0, ds_1, timestamps, values)
        create_timeseries_data(ts_1, ds_1, timestamps, values)

        assert db.session.query(TimeseriesData).all()

        ts_l = (ts_0, ts_2)

        with CurrentUser(user_1):
            with pytest.raises(BEMServerAuthorizationError):
                tsdio.delete(start_dt, end_dt, ts_l, ds_1)

        ts_l = (ts_1, ts_3)

        with CurrentUser(user_1):
            tsdio.delete(start_dt, end_dt, ts_l, ds_1)
            assert (
                not db.session.query(TimeseriesData)
                .filter_by(timeseries_by_data_state_id=tsbds_1.id)
                .all()
            )


class TestTimeseriesDataCSVIO:
    @pytest.mark.parametrize("campaigns", (2,), indirect=True)
    @pytest.mark.parametrize("timeseries", (3,), indirect=True)
    @pytest.mark.parametrize("for_campaign", (True, False))
    def test_timeseries_data_io_import_csv_as_admin(
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

        if for_campaign:
            header = f"Datetime,{ts_0.name},{ts_2.name}\n"
        else:
            header = f"Datetime,{ts_0.id},{ts_2.id}\n"

        csv_data = header + (
            "2020-01-01T00:00:00+00:00,0,10\n"
            "2020-01-01T01:00:00+00:00,1,11\n"
            "2020-01-01T02:00:00+00:00,2,12\n"
            # Test TZ mix
            "2020-01-01T04:00:00+01:00,3,13\n"
        )

        with CurrentUser(admin_user):
            tsdcsvio.import_csv(csv_data, ds_1, campaign)

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

        # Test conversions
        with OpenBar():
            ts_2.unit_symbol = "meter"
            tsdio.delete(
                dt.datetime(2020, 1, 1),
                dt.datetime(2020, 1, 2),
                (ts_0, ts_2),
                ds_1,
            )

        with CurrentUser(admin_user):
            tsdcsvio.import_csv(
                csv_data,
                ds_1,
                convert_from={ts_2.name if for_campaign else ts_2.id: "km"},
                campaign=campaign,
            )

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
            (timestamp, tsbds_2.id, (float(idx) + 10) * 1000)
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

        csv_data = header + (
            "2020-01-01T00:00:00+00:00,0,10\n"
            "2020-01-01T01:00:00+00:00,1,11\n"
            "2020-01-01T02:00:00+00:00,2,12\n"
            "2020-01-01T03:00:00+00:00,3,13\n"
        )

        with CurrentUser(user_1):
            with pytest.raises(BEMServerAuthorizationError):
                tsdcsvio.import_csv(csv_data, ds_1, campaign)

        if for_campaign:
            campaign = campaigns[1]
            header = f"Datetime,{ts_1.name}\n"
        else:
            campaign = None
            header = f"Datetime,{ts_1.id}\n"

        csv_data = header + (
            "2020-01-01T00:00:00+00:00,0\n"
            "2020-01-01T01:00:00+00:00,1\n"
            "2020-01-01T02:00:00+00:00,2\n"
            "2020-01-01T03:00:00+00:00,3\n"
        )

        with CurrentUser(user_1):
            tsdcsvio.import_csv(csv_data, ds_1, campaign)

    @pytest.mark.parametrize(
        "data_error",
        (
            # Empty file
            ("", TimeseriesDataCSVIOError),
            # Missing headers
            ("\n", TimeseriesDataCSVIOError),
            # Wrong (e.g. JSON) file format
            ('{"1": {"2020-01-01T00:00:00+00:00": 0}}', TimeseriesDataCSVIOError),
            # Empty TS name
            ("Datetime,1,\n", TimeseriesDataCSVIOError),
            # Unknown TS
            ("Datetime,1324564", TimeseriesNotFoundError),
        ),
    )
    @pytest.mark.parametrize("for_campaign", (True, False))
    def test_timeseries_data_io_import_csv_error(
        self, users, campaigns, for_campaign, data_error
    ):
        admin_user = users[0]
        assert admin_user.is_admin
        campaign = campaigns[0] if for_campaign else None
        csv_data, exc_cls = data_error

        with OpenBar():
            ds_1 = TimeseriesDataState.get(name="Raw").first()

        with CurrentUser(admin_user):
            with pytest.raises(exc_cls):
                tsdcsvio.import_csv(csv_data, ds_1, campaign)

    @pytest.mark.parametrize(
        "row_error",
        (
            # Value not float
            ("2020-01-01T00:00:00+00:00,a", TimeseriesDataCSVIOError),
            # Too many columns
            ("2500-01-01T00:00:00+00:00,1,2,3,4,5", TimeseriesDataCSVIOError),
            # Naive datetime
            ("2020-01-01T00:00:00,12", TimeseriesDataIODatetimeError),
            # Invalid timestamp
            ("dummy,1", TimeseriesDataIODatetimeError),
            ("0,1", TimeseriesDataIODatetimeError),
            ("2500-01-01T00:00:00+00:00,12", TimeseriesDataIODatetimeError),
        ),
    )
    @pytest.mark.usefixtures("timeseries")
    @pytest.mark.parametrize("for_campaign", (True, False))
    def test_timeseries_data_io_import_csv_row_error(
        self, users, campaigns, for_campaign, row_error
    ):
        admin_user = users[0]
        assert admin_user.is_admin
        campaign = campaigns[0] if for_campaign else None
        row, exc_cls = row_error

        with OpenBar():
            ds_1 = TimeseriesDataState.get(name="Raw").first()

        header = "Datetime,Timeseries 0\n" if for_campaign else "Datetime,1\n"
        csv_data = header + row

        with CurrentUser(admin_user):
            with pytest.raises(exc_cls):
                tsdcsvio.import_csv(csv_data, ds_1, campaign)

    @pytest.mark.usefixtures("timeseries")
    def test_timeseries_data_io_import_csv_invalid_ts_id(self, users):
        """Check timeseries IDs provided as (non-decimal) strings instead of integers"""
        admin_user = users[0]
        assert admin_user.is_admin

        with OpenBar():
            ds_1 = TimeseriesDataState.get(name="Raw").first()

        csv_data = "Datetime,Timeseries 0\n2020-01-01T00:00:00+00:00,1"

        with CurrentUser(admin_user):
            with pytest.raises(TimeseriesDataIOInvalidTimeseriesIDTypeError):
                tsdcsvio.import_csv(csv_data, ds_1)

    @pytest.mark.parametrize("timeseries", (5,), indirect=True)
    @pytest.mark.parametrize("col_label", ("id", "name"))
    def test_timeseries_data_io_export_csv_as_admin(self, users, timeseries, col_label):
        admin_user = users[0]
        assert admin_user.is_admin
        ts_0 = timeseries[0]
        ts_2 = timeseries[2]
        ts_4 = timeseries[4]

        with OpenBar():
            ds_1 = TimeseriesDataState.get(name="Raw").first()
            ts_0.unit_symbol = "meter"

        start_dt = dt.datetime(2020, 1, 1, tzinfo=dt.timezone.utc)
        end_dt = start_dt + dt.timedelta(hours=3)

        timestamps = pd.date_range(
            start=start_dt, end=end_dt, inclusive="left", freq="h"
        )
        values_1 = range(3)
        create_timeseries_data(ts_0, ds_1, timestamps, values_1)
        values_2 = [10 + 2 * i for i in range(2)]
        create_timeseries_data(ts_4, ds_1, timestamps[:2], values_2)

        ts_l = (ts_0, ts_2, ts_4)

        with CurrentUser(admin_user):
            if col_label == "name":
                header = f"Datetime,{ts_0.name},{ts_2.name},{ts_4.name}\n"
            else:
                header = f"Datetime,{ts_0.id},{ts_2.id},{ts_4.id}\n"

            data = tsdcsvio.export_csv(
                start_dt,
                end_dt,
                ts_l,
                ds_1,
                col_label=col_label,
            )
            assert data == header + (
                "2020-01-01T00:00:00+0000,0.0,,10.0\n"
                "2020-01-01T01:00:00+0000,1.0,,12.0\n"
                "2020-01-01T02:00:00+0000,2.0,,\n"
            )

            # No start/end date
            data = tsdcsvio.export_csv(
                None,
                None,
                ts_l,
                ds_1,
                col_label=col_label,
            )
            assert data == header + (
                "2020-01-01T00:00:00+0000,0.0,,10.0\n"
                "2020-01-01T01:00:00+0000,1.0,,12.0\n"
                "2020-01-01T02:00:00+0000,2.0,,\n"
            )

            # Local TZ
            data = tsdcsvio.export_csv(
                start_dt,
                end_dt,
                ts_l,
                ds_1,
                timezone="Europe/Paris",
                col_label=col_label,
            )
            assert data == header + (
                "2020-01-01T01:00:00+0100,0.0,,10.0\n"
                "2020-01-01T02:00:00+0100,1.0,,12.0\n"
                "2020-01-01T03:00:00+0100,2.0,,\n"
            )

            # Unit conversions
            data = tsdcsvio.export_csv(
                start_dt,
                end_dt,
                ts_l,
                ds_1,
                convert_to={ts_0.name if col_label == "name" else ts_0.id: "mm"},
                col_label=col_label,
            )
            assert data == header + (
                "2020-01-01T00:00:00+0000,0.0,,10.0\n"
                "2020-01-01T01:00:00+0000,1000.0,,12.0\n"
                "2020-01-01T02:00:00+0000,2000.0,,\n"
            )

    @pytest.mark.parametrize("campaigns", (2,), indirect=True)
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

        with OpenBar():
            ds_1 = TimeseriesDataState.get(name="Raw").first()

        start_dt = dt.datetime(2020, 1, 1, tzinfo=dt.timezone.utc)
        end_dt = start_dt + dt.timedelta(hours=3)

        timestamps = pd.date_range(
            start=start_dt, end=end_dt, inclusive="left", freq="h"
        )
        values_1 = range(3)
        create_timeseries_data(ts_1, ds_1, timestamps, values_1)
        values_2 = [10 + 2 * i for i in range(2)]
        create_timeseries_data(ts_3, ds_1, timestamps[:2], values_2)

        with CurrentUser(user_1):
            ts_l = (ts_0, ts_2, ts_4)

            with pytest.raises(BEMServerAuthorizationError):
                data = tsdcsvio.export_csv(
                    start_dt, end_dt, ts_l, ds_1, col_label=col_label
                )

            if col_label == "name":
                header = f"Datetime,{ts_1.name},{ts_3.name}\n"
            else:
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

        with OpenBar():
            ds_1 = TimeseriesDataState.get(name="Raw").first()
            ts_0.unit_symbol = "meter"

        start_dt = dt.datetime(2020, 1, 1, tzinfo=dt.timezone.utc)
        end_dt = start_dt + dt.timedelta(hours=24 * 3)

        timestamps = pd.date_range(
            start=start_dt, end=end_dt, inclusive="left", freq="h"
        )
        values_1 = range(24 * 3)
        create_timeseries_data(ts_0, ds_1, timestamps, values_1)
        values_2 = [10 + 2 * i for i in range(24 * 2)]
        create_timeseries_data(ts_4, ds_1, timestamps[: 24 * 2], values_2)

        ts_l = (ts_0, ts_2, ts_4)

        with CurrentUser(admin_user):
            if col_label == "name":
                header = f"Datetime,{ts_0.name},{ts_2.name},{ts_4.name}\n"
            else:
                header = f"Datetime,{ts_0.id},{ts_2.id},{ts_4.id}\n"

            # Export CSV: UTC avg
            data = tsdcsvio.export_csv_bucket(
                start_dt, end_dt, ts_l, ds_1, 1, "day", col_label=col_label
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
                1,
                "day",
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
                1,
                "day",
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
                1,
                "day",
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
                1,
                "day",
                "max",
                col_label=col_label,
            )
            assert data == header + (
                "2020-01-01T00:00:00+0000,23.0,,56.0\n"
                "2020-01-02T00:00:00+0000,47.0,,104.0\n"
                "2020-01-03T00:00:00+0000,71.0,,\n"
            )

            # Export CSV: conversions
            data = tsdcsvio.export_csv_bucket(
                start_dt,
                end_dt,
                ts_l,
                ds_1,
                1,
                "day",
                convert_to={ts_0.name if col_label == "name" else ts_0.id: "mm"},
                col_label=col_label,
            )
            assert data == header + (
                "2020-01-01T00:00:00+0000,11500.0,,33.0\n"
                "2020-01-02T00:00:00+0000,35500.0,,81.0\n"
                "2020-01-03T00:00:00+0000,59500.0,,\n"
            )

            # Export CSV: no timeseries
            data = tsdcsvio.export_csv_bucket(
                start_dt,
                end_dt,
                [],
                ds_1,
                1,
                "day",
                col_label=col_label,
            )
            assert data == (
                "Datetime\n"
                "2020-01-01T00:00:00+0000\n"
                "2020-01-02T00:00:00+0000\n"
                "2020-01-03T00:00:00+0000\n"
            )

            # Export CSV: invalid aggregation
            with pytest.raises(TimeseriesDataIOInvalidAggregationError):
                tsdcsvio.export_csv_bucket(
                    start_dt,
                    end_dt,
                    ts_l,
                    ds_1,
                    1,
                    "day",
                    "lol",
                    col_label=col_label,
                )

    @pytest.mark.parametrize("campaigns", (2,), indirect=True)
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

        with OpenBar():
            ds_1 = TimeseriesDataState.get(name="Raw").first()

        start_dt = dt.datetime(2020, 1, 1, tzinfo=dt.timezone.utc)
        end_dt = start_dt + dt.timedelta(hours=24 * 3)

        timestamps = pd.date_range(
            start=start_dt, end=end_dt, inclusive="left", freq="h"
        )
        values_1 = range(24 * 3)
        create_timeseries_data(ts_1, ds_1, timestamps, values_1)
        values_2 = [10 + 2 * i for i in range(24 * 2)]
        create_timeseries_data(ts_3, ds_1, timestamps[: 24 * 2], values_2)

        with CurrentUser(user_1):
            ts_l = (ts_0, ts_2, ts_4)

            with pytest.raises(BEMServerAuthorizationError):
                data = tsdcsvio.export_csv_bucket(
                    start_dt, end_dt, ts_l, ds_1, 1, "day", col_label=col_label
                )

            if col_label == "name":
                header = f"Datetime,{ts_1.name},{ts_3.name}\n"
            else:
                header = f"Datetime,{ts_1.id},{ts_3.id}\n"

            ts_l = (ts_1, ts_3)

            # Export CSV: UTC avg
            data = tsdcsvio.export_csv_bucket(
                start_dt, end_dt, ts_l, ds_1, 1, "day", col_label=col_label
            )
            assert data == header + (
                "2020-01-01T00:00:00+0000,11.5,33.0\n"
                "2020-01-02T00:00:00+0000,35.5,81.0\n"
                "2020-01-03T00:00:00+0000,59.5,\n"
            )


class TestTimeseriesDataJSONIO:
    @pytest.mark.parametrize("campaigns", (2,), indirect=True)
    @pytest.mark.parametrize("timeseries", (3,), indirect=True)
    @pytest.mark.parametrize("for_campaign", (True, False))
    def test_timeseries_data_io_import_json_as_admin(
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

        ts_l = [ts_0, ts_2]

        if for_campaign:
            labels = [ts.name for ts in ts_l]
        else:
            labels = [ts.id for ts in ts_l]

        json_data = {
            labels[0]: {
                "2020-01-01T00:00:00+00:00": 0,
                "2020-01-01T01:00:00+00:00": 1,
                "2020-01-01T02:00:00+00:00": 2,
                "2020-01-01T03:00:00+00:00": 3,
            },
            labels[1]: {
                "2020-01-01T00:00:00+00:00": 10,
                "2020-01-01T01:00:00+00:00": 11,
                "2020-01-01T02:00:00+00:00": 12,
                "2020-01-01T03:00:00+00:00": 13,
            },
        }
        json_data = json.dumps(json_data)

        with CurrentUser(admin_user):
            tsdjsonio.import_json(json_data, ds_1, campaign)

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

        # Test conversions
        with OpenBar():
            ts_2.unit_symbol = "meter"
            tsdio.delete(
                dt.datetime(2020, 1, 1),
                dt.datetime(2020, 1, 2),
                (ts_0, ts_2),
                ds_1,
            )

        with CurrentUser(admin_user):
            tsdjsonio.import_json(
                json_data,
                ds_1,
                convert_from={ts_2.name if for_campaign else ts_2.id: "km"},
                campaign=campaign,
            )

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
            (timestamp, tsbds_2.id, (float(idx) + 10) * 1000)
            for idx, timestamp in enumerate(timestamps)
        ]

        assert data == expected

    @pytest.mark.parametrize("timeseries", (3,), indirect=True)
    @pytest.mark.usefixtures("users_by_user_groups")
    @pytest.mark.usefixtures("user_groups_by_campaigns")
    @pytest.mark.usefixtures("user_groups_by_campaign_scopes")
    @pytest.mark.parametrize("for_campaign", (True, False))
    def test_timeseries_data_io_import_json_as_user(
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

        ts_l = [ts_0, ts_2]

        if for_campaign:
            campaign = campaigns[0]
            labels = [ts.name for ts in ts_l]
        else:
            campaign = None
            labels = [ts.id for ts in ts_l]

        json_data = {
            labels[0]: {
                "2020-01-01T00:00:00+00:00": 0,
                "2020-01-01T01:00:00+00:00": 1,
                "2020-01-01T02:00:00+00:00": 2,
                "2020-01-01T03:00:00+00:00": 3,
            },
            labels[1]: {
                "2020-01-01T00:00:00+00:00": 10,
                "2020-01-01T01:00:00+00:00": 11,
                "2020-01-01T02:00:00+00:00": 12,
                "2020-01-01T03:00:00+00:00": 13,
            },
        }
        json_data = json.dumps(json_data)

        with CurrentUser(user_1):
            with pytest.raises(BEMServerAuthorizationError):
                tsdjsonio.import_json(json_data, ds_1, campaign)

        ts_l = [ts_1]

        if for_campaign:
            campaign = campaigns[1]
            labels = [ts.name for ts in ts_l]
        else:
            campaign = None
            labels = [ts.id for ts in ts_l]

        json_data = {
            labels[0]: {
                "2020-01-01T00:00:00+00:00": 0,
                "2020-01-01T01:00:00+00:00": 1,
                "2020-01-01T02:00:00+00:00": 2,
                "2020-01-01T03:00:00+00:00": 3,
            },
        }
        json_data = json.dumps(json_data)

        with CurrentUser(user_1):
            tsdjsonio.import_json(json_data, ds_1, campaign)

    @pytest.mark.parametrize(
        "data_error",
        (
            # Empty file
            ("", TimeseriesDataJSONIOError),
            # Invalid file
            ("dummy", TimeseriesDataJSONIOError),
            # Empty TS name
            ('{"": []}', TimeseriesDataJSONIOError),
            # Unknown TS
            ('{"1324564": []}', TimeseriesNotFoundError),
            # Invalid timestamp
            ('{"1": {"dummy": 1}}', TimeseriesDataIODatetimeError),
            ('{"1": [{"dummy": 1}]}', TimeseriesDataIODatetimeError),
            ('{"1": {"2500-01-01T00:00:00+00:00": 1}}', TimeseriesDataIODatetimeError),
            # Invalid value
            ('{"1": {"2020-01-01T00:00:00+00:00": "lol"}}', TimeseriesDataJSONIOError),
        ),
    )
    @pytest.mark.usefixtures("timeseries")
    def test_timeseries_data_io_import_json_error(self, users, data_error):
        admin_user = users[0]
        assert admin_user.is_admin
        json_data, exc_cls = data_error

        with OpenBar():
            ds_1 = TimeseriesDataState.get(name="Raw").first()

        with CurrentUser(admin_user):
            with pytest.raises(exc_cls):
                tsdjsonio.import_json(json_data, ds_1)

    @pytest.mark.usefixtures("timeseries")
    def test_timeseries_data_io_import_json_invalid_ts_id(self, users):
        """Check timeseries IDs provided as (non-decimal) strings instead of integers"""
        admin_user = users[0]
        assert admin_user.is_admin

        with OpenBar():
            ds_1 = TimeseriesDataState.get(name="Raw").first()

        json_data = '{"Timeseries 0": {"2020-01-01T00:00:00+00:00": 1}}'

        with CurrentUser(admin_user):
            with pytest.raises(TimeseriesDataIOInvalidTimeseriesIDTypeError):
                tsdjsonio.import_json(json_data, ds_1)

    @pytest.mark.parametrize("timeseries", (5,), indirect=True)
    @pytest.mark.parametrize("col_label", ("id", "name"))
    def test_timeseries_data_io_export_json_as_admin(
        self, users, timeseries, col_label
    ):
        admin_user = users[0]
        assert admin_user.is_admin
        ts_0 = timeseries[0]
        ts_2 = timeseries[2]
        ts_4 = timeseries[4]

        with OpenBar():
            ds_1 = TimeseriesDataState.get(name="Raw").first()
            ts_0.unit_symbol = "meter"

        start_dt = dt.datetime(2020, 1, 1, tzinfo=dt.timezone.utc)
        end_dt = start_dt + dt.timedelta(hours=3)

        timestamps = pd.date_range(
            start=start_dt, end=end_dt, inclusive="left", freq="h"
        )
        values_1 = range(3)
        create_timeseries_data(ts_0, ds_1, timestamps, values_1)
        values_2 = [10 + 2 * i for i in range(2)]
        create_timeseries_data(ts_4, ds_1, timestamps[:2], values_2)

        ts_l = (ts_0, ts_2, ts_4)

        with CurrentUser(admin_user):
            labels = [str(getattr(ts, col_label)) for ts in ts_l]

            data = tsdjsonio.export_json(
                start_dt,
                end_dt,
                ts_l,
                ds_1,
                col_label=col_label,
            )
            expected = {
                labels[0]: {
                    "2020-01-01T00:00:00+00:00": 0.0,
                    "2020-01-01T01:00:00+00:00": 1.0,
                    "2020-01-01T02:00:00+00:00": 2.0,
                },
                labels[2]: {
                    "2020-01-01T00:00:00+00:00": 10.0,
                    "2020-01-01T01:00:00+00:00": 12.0,
                },
            }
            assert json.loads(data) == expected

            # No start/end date
            data = tsdjsonio.export_json(
                None,
                None,
                ts_l,
                ds_1,
                col_label=col_label,
            )
            expected = {
                labels[0]: {
                    "2020-01-01T00:00:00+00:00": 0.0,
                    "2020-01-01T01:00:00+00:00": 1.0,
                    "2020-01-01T02:00:00+00:00": 2.0,
                },
                labels[2]: {
                    "2020-01-01T00:00:00+00:00": 10.0,
                    "2020-01-01T01:00:00+00:00": 12.0,
                },
            }
            assert json.loads(data) == expected

            # Local TZ
            data = tsdjsonio.export_json(
                start_dt,
                end_dt,
                ts_l,
                ds_1,
                timezone="Europe/Paris",
                col_label=col_label,
            )
            expected = {
                labels[0]: {
                    "2020-01-01T01:00:00+01:00": 0.0,
                    "2020-01-01T02:00:00+01:00": 1.0,
                    "2020-01-01T03:00:00+01:00": 2.0,
                },
                labels[2]: {
                    "2020-01-01T01:00:00+01:00": 10.0,
                    "2020-01-01T02:00:00+01:00": 12.0,
                },
            }
            assert json.loads(data) == expected

            # Unit conversions
            data = tsdjsonio.export_json(
                start_dt,
                end_dt,
                ts_l,
                ds_1,
                convert_to={ts_0.name if col_label == "name" else ts_0.id: "mm"},
                col_label=col_label,
            )
            expected = {
                labels[0]: {
                    "2020-01-01T00:00:00+00:00": 0.0,
                    "2020-01-01T01:00:00+00:00": 1000.0,
                    "2020-01-01T02:00:00+00:00": 2000.0,
                },
                labels[2]: {
                    "2020-01-01T00:00:00+00:00": 10.0,
                    "2020-01-01T01:00:00+00:00": 12.0,
                },
            }
            assert json.loads(data) == expected

    @pytest.mark.parametrize("campaigns", (2,), indirect=True)
    @pytest.mark.parametrize("timeseries", (5,), indirect=True)
    @pytest.mark.usefixtures("users_by_user_groups")
    @pytest.mark.usefixtures("user_groups_by_campaigns")
    @pytest.mark.usefixtures("user_groups_by_campaign_scopes")
    @pytest.mark.parametrize("col_label", ("id", "name"))
    def test_timeseries_data_io_export_json_as_user(self, users, timeseries, col_label):
        user_1 = users[1]
        assert not user_1.is_admin
        ts_0 = timeseries[0]
        ts_1 = timeseries[1]
        ts_2 = timeseries[2]
        ts_3 = timeseries[3]
        ts_4 = timeseries[4]

        with OpenBar():
            ds_1 = TimeseriesDataState.get(name="Raw").first()

        start_dt = dt.datetime(2020, 1, 1, tzinfo=dt.timezone.utc)
        end_dt = start_dt + dt.timedelta(hours=3)

        timestamps = pd.date_range(
            start=start_dt, end=end_dt, inclusive="left", freq="h"
        )
        values_1 = range(3)
        create_timeseries_data(ts_1, ds_1, timestamps, values_1)
        values_2 = [10 + 2 * i for i in range(2)]
        create_timeseries_data(ts_3, ds_1, timestamps[:2], values_2)

        with CurrentUser(user_1):
            ts_l = (ts_0, ts_2, ts_4)

            with pytest.raises(BEMServerAuthorizationError):
                data = tsdjsonio.export_json(
                    start_dt, end_dt, ts_l, ds_1, col_label=col_label
                )

            ts_l = (ts_1, ts_3)

            labels = [str(getattr(ts, col_label)) for ts in ts_l]

            data = tsdjsonio.export_json(
                start_dt, end_dt, ts_l, ds_1, col_label=col_label
            )
            expected = {
                labels[0]: {
                    "2020-01-01T00:00:00+00:00": 0.0,
                    "2020-01-01T01:00:00+00:00": 1.0,
                    "2020-01-01T02:00:00+00:00": 2.0,
                },
                labels[1]: {
                    "2020-01-01T00:00:00+00:00": 10.0,
                    "2020-01-01T01:00:00+00:00": 12.0,
                },
            }
            assert json.loads(data) == expected

    @pytest.mark.parametrize("timeseries", (5,), indirect=True)
    @pytest.mark.parametrize("col_label", ("id", "name"))
    def test_timeseries_data_io_export_json_bucket_as_admin(
        self, users, timeseries, col_label
    ):
        admin_user = users[0]
        assert admin_user.is_admin
        ts_0 = timeseries[0]
        ts_2 = timeseries[2]
        ts_4 = timeseries[4]

        with OpenBar():
            ds_1 = TimeseriesDataState.get(name="Raw").first()
            ts_0.unit_symbol = "meter"

        start_dt = dt.datetime(2020, 1, 1, tzinfo=dt.timezone.utc)
        end_dt = start_dt + dt.timedelta(hours=24 * 3)

        timestamps = pd.date_range(
            start=start_dt, end=end_dt, inclusive="left", freq="h"
        )
        values_1 = range(24 * 3)
        create_timeseries_data(ts_0, ds_1, timestamps, values_1)
        values_2 = [10 + 2 * i for i in range(24 * 2)]
        create_timeseries_data(ts_4, ds_1, timestamps[: 24 * 2], values_2)

        ts_l = (ts_0, ts_2, ts_4)

        labels = [str(getattr(ts, col_label)) for ts in ts_l]

        with CurrentUser(admin_user):
            # Export JSON: UTC avg
            data = tsdjsonio.export_json_bucket(
                start_dt, end_dt, ts_l, ds_1, 1, "day", col_label=col_label
            )
            expected = {
                labels[0]: {
                    "2020-01-01T00:00:00+00:00": 11.5,
                    "2020-01-02T00:00:00+00:00": 35.5,
                    "2020-01-03T00:00:00+00:00": 59.5,
                },
                labels[1]: {
                    "2020-01-01T00:00:00+00:00": None,
                    "2020-01-02T00:00:00+00:00": None,
                    "2020-01-03T00:00:00+00:00": None,
                },
                labels[2]: {
                    "2020-01-01T00:00:00+00:00": 33.0,
                    "2020-01-02T00:00:00+00:00": 81.0,
                    "2020-01-03T00:00:00+00:00": None,
                },
            }
            assert json.loads(data) == expected

            # Export JSON: local TZ avg
            data = tsdjsonio.export_json_bucket(
                start_dt.replace(tzinfo=ZoneInfo("Europe/Paris")),
                end_dt.replace(tzinfo=ZoneInfo("Europe/Paris")),
                ts_l,
                ds_1,
                1,
                "day",
                timezone="Europe/Paris",
                col_label=col_label,
            )
            expected = {
                labels[0]: {
                    "2020-01-01T00:00:00+01:00": 11.0,
                    "2020-01-02T00:00:00+01:00": 34.5,
                    "2020-01-03T00:00:00+01:00": 58.5,
                },
                labels[1]: {
                    "2020-01-01T00:00:00+01:00": None,
                    "2020-01-02T00:00:00+01:00": None,
                    "2020-01-03T00:00:00+01:00": None,
                },
                labels[2]: {
                    "2020-01-01T00:00:00+01:00": 32.0,
                    "2020-01-02T00:00:00+01:00": 79.0,
                    "2020-01-03T00:00:00+01:00": 104.0,
                },
            }
            assert json.loads(data) == expected

            # Export JSON: UTC sum
            data = tsdjsonio.export_json_bucket(
                start_dt,
                end_dt,
                ts_l,
                ds_1,
                1,
                "day",
                "sum",
                col_label=col_label,
            )
            expected = {
                labels[0]: {
                    "2020-01-01T00:00:00+00:00": 276.0,
                    "2020-01-02T00:00:00+00:00": 852.0,
                    "2020-01-03T00:00:00+00:00": 1428.0,
                },
                labels[1]: {
                    "2020-01-01T00:00:00+00:00": None,
                    "2020-01-02T00:00:00+00:00": None,
                    "2020-01-03T00:00:00+00:00": None,
                },
                labels[2]: {
                    "2020-01-01T00:00:00+00:00": 792.0,
                    "2020-01-02T00:00:00+00:00": 1944.0,
                    "2020-01-03T00:00:00+00:00": None,
                },
            }
            assert json.loads(data) == expected

            # Export JSON: UTC min
            data = tsdjsonio.export_json_bucket(
                start_dt,
                end_dt,
                ts_l,
                ds_1,
                1,
                "day",
                "min",
                col_label=col_label,
            )
            expected = {
                labels[0]: {
                    "2020-01-01T00:00:00+00:00": 0.0,
                    "2020-01-02T00:00:00+00:00": 24.0,
                    "2020-01-03T00:00:00+00:00": 48.0,
                },
                labels[1]: {
                    "2020-01-01T00:00:00+00:00": None,
                    "2020-01-02T00:00:00+00:00": None,
                    "2020-01-03T00:00:00+00:00": None,
                },
                labels[2]: {
                    "2020-01-01T00:00:00+00:00": 10.0,
                    "2020-01-02T00:00:00+00:00": 58.0,
                    "2020-01-03T00:00:00+00:00": None,
                },
            }
            assert json.loads(data) == expected

            # Export JSON: UTC max
            data = tsdjsonio.export_json_bucket(
                start_dt,
                end_dt,
                ts_l,
                ds_1,
                1,
                "day",
                "max",
                col_label=col_label,
            )
            expected = {
                labels[0]: {
                    "2020-01-01T00:00:00+00:00": 23.0,
                    "2020-01-02T00:00:00+00:00": 47.0,
                    "2020-01-03T00:00:00+00:00": 71.0,
                },
                labels[1]: {
                    "2020-01-01T00:00:00+00:00": None,
                    "2020-01-02T00:00:00+00:00": None,
                    "2020-01-03T00:00:00+00:00": None,
                },
                labels[2]: {
                    "2020-01-01T00:00:00+00:00": 56.0,
                    "2020-01-02T00:00:00+00:00": 104.0,
                    "2020-01-03T00:00:00+00:00": None,
                },
            }
            assert json.loads(data) == expected

            # Export JSON: unit converions
            data = tsdjsonio.export_json_bucket(
                start_dt,
                end_dt,
                ts_l,
                ds_1,
                1,
                "day",
                convert_to={ts_0.name if col_label == "name" else ts_0.id: "mm"},
                col_label=col_label,
            )
            expected = {
                labels[0]: {
                    "2020-01-01T00:00:00+00:00": 11500.0,
                    "2020-01-02T00:00:00+00:00": 35500.0,
                    "2020-01-03T00:00:00+00:00": 59500.0,
                },
                labels[1]: {
                    "2020-01-01T00:00:00+00:00": None,
                    "2020-01-02T00:00:00+00:00": None,
                    "2020-01-03T00:00:00+00:00": None,
                },
                labels[2]: {
                    "2020-01-01T00:00:00+00:00": 33.0,
                    "2020-01-02T00:00:00+00:00": 81.0,
                    "2020-01-03T00:00:00+00:00": None,
                },
            }
            assert json.loads(data) == expected

            # Export JSON: no timeseries
            data = tsdjsonio.export_json_bucket(
                start_dt,
                end_dt,
                [],
                ds_1,
                1,
                "day",
                col_label=col_label,
            )
            expected = {}
            assert json.loads(data) == expected

            # Export JSON: invalid aggregation
            with pytest.raises(TimeseriesDataIOInvalidAggregationError):
                tsdjsonio.export_json_bucket(
                    start_dt,
                    end_dt,
                    ts_l,
                    ds_1,
                    1,
                    "day",
                    "lol",
                    col_label=col_label,
                )

    @pytest.mark.parametrize("campaigns", (2,), indirect=True)
    @pytest.mark.parametrize("timeseries", (5,), indirect=True)
    @pytest.mark.usefixtures("users_by_user_groups")
    @pytest.mark.usefixtures("user_groups_by_campaigns")
    @pytest.mark.usefixtures("user_groups_by_campaign_scopes")
    @pytest.mark.parametrize("col_label", ("id", "name"))
    def test_timeseries_data_io_export_json_bucket_as_user(
        self, users, timeseries, col_label
    ):
        user_1 = users[1]
        assert not user_1.is_admin
        ts_0 = timeseries[0]
        ts_1 = timeseries[1]
        ts_2 = timeseries[2]
        ts_3 = timeseries[3]
        ts_4 = timeseries[4]

        with OpenBar():
            ds_1 = TimeseriesDataState.get(name="Raw").first()

        start_dt = dt.datetime(2020, 1, 1, tzinfo=dt.timezone.utc)
        end_dt = start_dt + dt.timedelta(hours=24 * 3)

        timestamps = pd.date_range(
            start=start_dt, end=end_dt, inclusive="left", freq="h"
        )
        values_1 = range(24 * 3)
        create_timeseries_data(ts_1, ds_1, timestamps, values_1)
        values_2 = [10 + 2 * i for i in range(24 * 2)]
        create_timeseries_data(ts_3, ds_1, timestamps[: 24 * 2], values_2)

        with CurrentUser(user_1):
            ts_l = (ts_0, ts_2, ts_4)

            with pytest.raises(BEMServerAuthorizationError):
                data = tsdjsonio.export_json_bucket(
                    start_dt, end_dt, ts_l, ds_1, 1, "day", col_label=col_label
                )

            ts_l = (ts_1, ts_3)

            labels = [str(getattr(ts, col_label)) for ts in ts_l]

            # Export JSON: UTC avg
            data = tsdjsonio.export_json_bucket(
                start_dt, end_dt, ts_l, ds_1, 1, "day", col_label=col_label
            )
            expected = {
                labels[0]: {
                    "2020-01-01T00:00:00+00:00": 11.5,
                    "2020-01-02T00:00:00+00:00": 35.5,
                    "2020-01-03T00:00:00+00:00": 59.5,
                },
                labels[1]: {
                    "2020-01-01T00:00:00+00:00": 33.0,
                    "2020-01-02T00:00:00+00:00": 81.0,
                    "2020-01-03T00:00:00+00:00": None,
                },
            }
            assert json.loads(data) == expected
