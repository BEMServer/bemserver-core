"""Expressions

Evaluate expressions
"""

from bemserver_core import expression_eval
from bemserver_core.input_output import tsdio


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
