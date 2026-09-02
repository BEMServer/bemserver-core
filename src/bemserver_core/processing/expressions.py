"""Expressions

Evaluate expressions
"""

from bemserver_core import expression_eval
from bemserver_core.exceptions import TimeseriesNotFoundError
from bemserver_core.input_output import tsdio
from bemserver_core.model import Timeseries


def get_expression_variable_values(
    expression_variable,
    start_dt,
    end_dt,
    data_state,
    bucket_width_value,
    bucket_width_unit,
    timezone="UTC",
):
    return tsdio.get_timeseries_buckets_data(
        start_dt,
        end_dt,
        [expression_variable.timeseries],
        data_state,
        bucket_width_value,
        bucket_width_unit,
        aggregation=expression_variable.aggregation,
        convert_to=expression_variable.unit_symbol,
        timezone=timezone,
        col_label="id",
    )[expression_variable.timeseries_id]


def evaluate(
    expression,
    start_dt,
    end_dt,
    data_state,
    bucket_width_value,
    bucket_width_unit,
    timezone="UTC",
):
    namespace = {}

    for expr_var in expression.variables:
        namespace[expr_var.name] = get_expression_variable_values(
            expr_var,
            start_dt,
            end_dt,
            data_state,
            bucket_width_value,
            bucket_width_unit,
            timezone=timezone,
        )

    return expression_eval.evaluate(expression.expr, namespace).rename(expression.name)


def evaluate_from_dict(
    expression,
    start_dt,
    end_dt,
    data_state,
    bucket_width_value,
    bucket_width_unit,
    timezone="UTC",
):
    namespace = {}

    for expr_var in expression["variables"]:
        timeseries = Timeseries.get_by_id(expr_var["timeseries_id"])
        if timeseries is None:
            raise TimeseriesNotFoundError(
                f"Unknown timeseries: {expr_var['timeseries_id']}"
            )
        namespace[expr_var["name"]] = tsdio.get_timeseries_buckets_data(
            start_dt,
            end_dt,
            [timeseries],
            data_state,
            bucket_width_value,
            bucket_width_unit,
            aggregation=expr_var["aggregation"],
            convert_to=expr_var["unit_symbol"],
            timezone=timezone,
            col_label="id",
        )[expr_var["timeseries_id"]]

    return expression_eval.evaluate(expression["expr"], namespace).rename(
        expression["name"]
    )
