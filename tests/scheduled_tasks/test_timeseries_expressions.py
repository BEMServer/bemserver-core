"""TimeseriesExpression task tests"""

import datetime as dt

import pytest

import pandas as pd
from pandas.testing import assert_frame_equal

from bemserver_core.authorization import OpenBar
from bemserver_core.database import db
from bemserver_core.input_output import tsdio
from bemserver_core.model import TimeseriesDataState, TimeseriesExpression
from bemserver_core.tasks.timeseries_expressions import compute_timeseries_expressions
from tests.utils import create_timeseries_data


class TestTimeseriesExpressionScheduledTask:
    @pytest.mark.parametrize("timeseries", (4,), indirect=True)
    def test_compute_timeseries_expression(
        self, users, timeseries, campaigns, campaign_scopes, expressions
    ):
        admin_user = users[0]
        assert admin_user.is_admin
        ts_0 = timeseries[0]
        ts_3 = timeseries[3]
        expr_1 = expressions[0]
        campaign_1 = campaigns[0]
        cs_1 = campaign_scopes[0]

        with OpenBar():
            ds_clean = TimeseriesDataState.get(name="Clean").first()
            TimeseriesExpression.new(
                campaign_scope_id=cs_1.id,
                expression_id=expr_1.id,
                timeseries_id=ts_3.id,
                bucket_width_value=1,
                bucket_width_unit="day",
            )
            db.session.flush()

        start_dt = dt.datetime(2020, 1, 1, tzinfo=dt.UTC)
        end_dt = dt.datetime(2020, 1, 5, tzinfo=dt.UTC)
        timestamps = pd.date_range(start_dt, end_dt, inclusive="left", freq="1D")
        values = [0, 1, 2, 3]
        create_timeseries_data(ts_0, ds_clean, timestamps, values)

        with OpenBar():
            compute_timeseries_expressions(campaign_1, start_dt, end_dt)
            data_df = tsdio.get_timeseries_data(start_dt, end_dt, (ts_3,), ds_clean)
            index = pd.DatetimeIndex(
                pd.date_range(start_dt, end_dt, inclusive="left", freq="1D"),
                name="timestamp",
                tz="UTC",
                freq=None,
            ).as_unit("us")
            expected_data_df = pd.DataFrame(
                {ts_3.id: [0, 2, 4, 6]},
                index=index,
            ).astype(float)
            expected_data_df.columns.name = "id"
            assert_frame_equal(data_df, expected_data_df)
