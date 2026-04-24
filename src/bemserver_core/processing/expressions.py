"""Expressions

Evaluate expressions
"""

import pandas as pd

from bemserver_core import expression_eval
from bemserver_core.input_output import tsdio
from bemserver_core.model import TimeseriesDataState


def evaluate(
    expression,
    start_dt,
    end_dt,
    bucket_width_value,
    bucket_width_unit,
    timezone="UTC",
):
    namespace = {}

    ds_clean = TimeseriesDataState.get(name="Clean").first()

    for expr_var in expression.variables:
        namespace[expr_var.name] = tsdio.get_timeseries_buckets_data(
            start_dt,
            end_dt,
            [expr_var.timeseries],
            ds_clean,
            bucket_width_value,
            bucket_width_unit,
            aggregation=expr_var.aggregation,
            convert_to=expr_var.unit_symbol,
            timezone=timezone,
            col_label="id",
        )[expr_var.timeseries_id]

    data_s = expression_eval.evaluate(expression.expr, namespace)
    data_df = pd.DataFrame({expression.timeseries_id: data_s}, data_s.index)

    tsdio.set_timeseries_data(data_df, ds_clean, convert_from=expression.unit_symbol)
