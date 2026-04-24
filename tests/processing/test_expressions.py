"""Expressions tests"""

import datetime as dt

import pytest

import pandas as pd
from pandas.testing import assert_frame_equal

from bemserver_core.authorization import CurrentUser, OpenBar
from bemserver_core.database import db
from bemserver_core.input_output import tsdio
from bemserver_core.model import Expression, ExpressionVariable, TimeseriesDataState
from bemserver_core.processing.expressions import evaluate
from tests.utils import create_timeseries_data


class TestExpressionsEvaluateProcessing:
    @pytest.mark.parametrize("campaigns", (2,), indirect=True)
    @pytest.mark.parametrize("timeseries", (3,), indirect=True)
    def test_evaluate(self, users, timeseries, campaign_scopes):
        admin_user = users[0]
        assert admin_user.is_admin
        cs_1 = campaign_scopes[0]
        ts_1 = timeseries[0]
        ts_3 = timeseries[2]

        with OpenBar():
            ds_clean = TimeseriesDataState.get(name="Clean").first()
            expr_1 = Expression.new(
                campaign_scope_id=cs_1.id,
                expr="2*a",
                timeseries_id=ts_1.id,
            )
            expr_2 = Expression.new(
                campaign_scope_id=cs_1.id,
                expr="a**2",
                timeseries_id=ts_1.id,
            )
            db.session.flush()
            ExpressionVariable.new(
                campaign_scope_id=cs_1.id,
                expression_id=expr_1.id,
                name="a",
                timeseries_id=ts_3.id,
                aggregation="avg",
            )
            ExpressionVariable.new(
                campaign_scope_id=cs_1.id,
                expression_id=expr_2.id,
                name="a",
                timeseries_id=ts_3.id,
                aggregation="avg",
            )
            db.session.flush()

        start_dt = dt.datetime(2020, 1, 1, tzinfo=dt.UTC)
        end_dt = dt.datetime(2020, 1, 2, tzinfo=dt.UTC)
        timestamps = pd.date_range(start_dt, end_dt, inclusive="left", freq="6h")
        values_2 = [0, 2, 4, 8]
        create_timeseries_data(ts_3, ds_clean, timestamps, values_2)

        with CurrentUser(admin_user):
            # Expression 1 : TS0 = 2 * TS1
            evaluate(expr_1, start_dt, end_dt, 6, "hour")
            data_df = tsdio.get_timeseries_data(start_dt, end_dt, [ts_1], ds_clean)
            expected_df = pd.DataFrame(
                {ts_1.id: [0, 4, 8, 16]},
                pd.DatetimeIndex(timestamps, name="timestamp", freq="6h").as_unit("us"),
                dtype=float,
            )
            expected_df.columns.name = "id"
            expected_df.index.freq = None
            assert_frame_equal(data_df, expected_df)

            # Expression 2 : TS0 = TS1**2
            tsdio.delete(start_dt, end_dt, [ts_1], ds_clean)
            evaluate(expr_2, start_dt, end_dt, 6, "hour")
            data_df = tsdio.get_timeseries_data(start_dt, end_dt, [ts_1], ds_clean)
            expected_df = pd.DataFrame(
                {ts_1.id: [0, 4, 16, 64]},
                pd.DatetimeIndex(timestamps, name="timestamp", freq="6h").as_unit("us"),
                dtype=float,
            )
            expected_df.columns.name = "id"
            expected_df.index.freq = None
            assert_frame_equal(data_df, expected_df)

            # Expression 1 : TS0 = 2 * TS1, aggreg avg
            tsdio.delete(start_dt, end_dt, [ts_1], ds_clean)
            evaluate(expr_1, start_dt, end_dt, 12, "hour")
            data_df = tsdio.get_timeseries_data(start_dt, end_dt, [ts_1], ds_clean)
            expected_df = pd.DataFrame(
                {ts_1.id: [2, 12]},
                pd.DatetimeIndex(
                    pd.date_range(start_dt, end_dt, inclusive="left", freq="12h"),
                    name="timestamp",
                    freq="12h",
                ).as_unit("us"),
                dtype=float,
            )
            expected_df.columns.name = "id"
            expected_df.index.freq = None
            assert_frame_equal(data_df, expected_df)
