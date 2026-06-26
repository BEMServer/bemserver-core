"""Expressions tests"""

import datetime as dt

import pandas as pd
from pandas.testing import assert_series_equal

from bemserver_core.authorization import CurrentUser, OpenBar
from bemserver_core.database import db
from bemserver_core.model import Expression, ExpressionVariable, TimeseriesDataState
from bemserver_core.processing.expressions import (
    evaluate,
    evaluate_from_dict,
    get_expression_variable_values,
)
from tests.utils import create_timeseries_data


class TestExpressionsEvaluateProcessing:
    def test_get_expression_variable_values(self, users, timeseries, campaign_scopes):
        admin_user = users[0]
        assert admin_user.is_admin
        cs_1 = campaign_scopes[0]
        ts_1 = timeseries[0]

        with OpenBar():
            ds_clean = TimeseriesDataState.get(name="Clean").first()
            expr_1 = Expression.new(
                campaign_scope_id=cs_1.id,
                name="2 times a",
                expr="2*a",
            )
            db.session.flush()
            expr_var_1 = ExpressionVariable.new(
                campaign_scope_id=cs_1.id,
                expression_id=expr_1.id,
                name="a",
                timeseries_id=ts_1.id,
                aggregation="avg",
            )
            db.session.flush()

        start_dt = dt.datetime(2020, 1, 1, tzinfo=dt.UTC)
        end_dt = dt.datetime(2020, 1, 2, tzinfo=dt.UTC)
        timestamps = pd.date_range(start_dt, end_dt, inclusive="left", freq="6h")
        values_2 = [0, 2, 4, 8]
        create_timeseries_data(ts_1, ds_clean, timestamps, values_2)

        with CurrentUser(admin_user):
            data_s = get_expression_variable_values(
                expr_var_1,
                start_dt,
                end_dt,
                ds_clean,
                6,
                "hour",
                timezone="UTC",
            )
            expected_s = pd.Series(
                [0, 2, 4, 8],
                index=pd.DatetimeIndex(timestamps, name="timestamp", freq="6h").as_unit(
                    "us"
                ),
                name=ts_1.id,
                dtype=float,
            )
            assert_series_equal(data_s, expected_s)

    def test_evaluate(self, users, timeseries, campaign_scopes):
        admin_user = users[0]
        assert admin_user.is_admin
        cs_1 = campaign_scopes[0]
        ts_1 = timeseries[0]

        with OpenBar():
            ds_clean = TimeseriesDataState.get(name="Clean").first()
            expr_1 = Expression.new(
                campaign_scope_id=cs_1.id,
                name="2 times a",
                expr="2*a",
            )
            expr_2 = Expression.new(
                campaign_scope_id=cs_1.id,
                name="a squared",
                expr="a**2",
            )
            db.session.flush()
            ExpressionVariable.new(
                campaign_scope_id=cs_1.id,
                expression_id=expr_1.id,
                name="a",
                timeseries_id=ts_1.id,
                aggregation="avg",
            )
            ExpressionVariable.new(
                campaign_scope_id=cs_1.id,
                expression_id=expr_2.id,
                name="a",
                timeseries_id=ts_1.id,
                aggregation="avg",
            )
            db.session.flush()

        start_dt = dt.datetime(2020, 1, 1, tzinfo=dt.UTC)
        end_dt = dt.datetime(2020, 1, 2, tzinfo=dt.UTC)
        timestamps = pd.date_range(start_dt, end_dt, inclusive="left", freq="6h")
        values_2 = [0, 2, 4, 8]
        create_timeseries_data(ts_1, ds_clean, timestamps, values_2)

        with CurrentUser(admin_user):
            data_s = evaluate(expr_1, start_dt, end_dt, ds_clean, 6, "hour")
            expected_s = pd.Series(
                [0, 4, 8, 16],
                pd.DatetimeIndex(timestamps, name="timestamp", freq="6h").as_unit("us"),
                name="2 times a",
                dtype=float,
            )
            assert_series_equal(data_s, expected_s)

            data_s = evaluate(expr_2, start_dt, end_dt, ds_clean, 6, "hour")
            expected_s = pd.Series(
                [0, 4, 16, 64],
                pd.DatetimeIndex(timestamps, name="timestamp", freq="6h").as_unit("us"),
                name="a squared",
                dtype=float,
            )
            assert_series_equal(data_s, expected_s)

            data_s = evaluate(expr_1, start_dt, end_dt, ds_clean, 12, "hour")
            expected_s = pd.Series(
                [2, 12],
                pd.DatetimeIndex(
                    pd.date_range(start_dt, end_dt, inclusive="left", freq="12h"),
                    name="timestamp",
                    freq="12h",
                ).as_unit("us"),
                name="2 times a",
                dtype=float,
            )
            assert_series_equal(data_s, expected_s)

    def test_evaluate_from_dict(self, users, timeseries, campaign_scopes):
        admin_user = users[0]
        assert admin_user.is_admin
        cs_1 = campaign_scopes[0]
        ts_1 = timeseries[0]

        with OpenBar():
            ds_clean = TimeseriesDataState.get(name="Clean").first()
            expr_1 = Expression.new(
                campaign_scope_id=cs_1.id,
                name="2 times a",
                expr="2*a",
            )
            expr_2 = Expression.new(
                campaign_scope_id=cs_1.id,
                name="a squared",
                expr="a**2",
            )
            db.session.flush()
            ExpressionVariable.new(
                campaign_scope_id=cs_1.id,
                expression_id=expr_1.id,
                name="a",
                timeseries_id=ts_1.id,
                aggregation="avg",
            )
            ExpressionVariable.new(
                campaign_scope_id=cs_1.id,
                expression_id=expr_2.id,
                name="a",
                timeseries_id=ts_1.id,
                aggregation="avg",
            )
            db.session.flush()

        start_dt = dt.datetime(2020, 1, 1, tzinfo=dt.UTC)
        end_dt = dt.datetime(2020, 1, 2, tzinfo=dt.UTC)
        timestamps = pd.date_range(start_dt, end_dt, inclusive="left", freq="6h")
        values_2 = [0, 2, 4, 8]
        create_timeseries_data(ts_1, ds_clean, timestamps, values_2)

        with CurrentUser(admin_user):
            data_s = evaluate_from_dict(
                expr_1.to_dict(), start_dt, end_dt, ds_clean, 6, "hour"
            )
            expected_s = pd.Series(
                [0, 4, 8, 16],
                pd.DatetimeIndex(timestamps, name="timestamp", freq="6h").as_unit("us"),
                name="2 times a",
                dtype=float,
            )
            assert_series_equal(data_s, expected_s)

            data_s = evaluate_from_dict(
                expr_2.to_dict(), start_dt, end_dt, ds_clean, 6, "hour"
            )
            expected_s = pd.Series(
                [0, 4, 16, 64],
                pd.DatetimeIndex(timestamps, name="timestamp", freq="6h").as_unit("us"),
                name="a squared",
                dtype=float,
            )
            assert_series_equal(data_s, expected_s)

            data_s = evaluate_from_dict(
                expr_1.to_dict(), start_dt, end_dt, ds_clean, 12, "hour"
            )
            expected_s = pd.Series(
                [2, 12],
                pd.DatetimeIndex(
                    pd.date_range(start_dt, end_dt, inclusive="left", freq="12h"),
                    name="timestamp",
                    freq="12h",
                ).as_unit("us"),
                name="2 times a",
                dtype=float,
            )
            assert_series_equal(data_s, expected_s)
