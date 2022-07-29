"""Custom operation

User-defined operations on timeseries

Use of ast literals inspired by https://stackoverflow.com/a/9558001
"""
import operator
import ast
from contextvars import ContextVar

from bemserver_core.model import Timeseries
from bemserver_core.input_output import tsdio
from bemserver_core.utils import ContextVarManager


UNARY_OPERATORS = {
    ast.USub: operator.neg,
}

BINARY_OPERATORS = {
    ast.Add: operator.add,
    ast.Sub: operator.sub,
    ast.Mult: operator.mul,
    ast.Div: operator.truediv,
    ast.Pow: operator.pow,
}

TIMESERIES = ContextVar("timeseries")


def eval_(node):
    """Recursively evaluate literal expression node"""
    print(node)
    if isinstance(node, ast.Num):
        return node.n
    if isinstance(node, ast.UnaryOp):
        return UNARY_OPERATORS[type(node.op)](eval_(node.operand))
    if isinstance(node, ast.BinOp):
        return BINARY_OPERATORS[type(node.op)](eval_(node.left), eval_(node.right))
    if isinstance(node, ast.Name):
        return TIMESERIES.get()[node.id]
    raise TypeError(node)


# TODO: let the user pass aliases in the string along with an alias -> TS mapping


# TODO: aggregate
def evaluate(start_dt, end_dt, data_state, operation):
    """Evaluate operation defined as string

    Returns operation result as a pandas Series.
    """
    # TODO: IDs are ints so they should be prefixed to be treated as names
    ts_ids = [
        cn.id for cn in ast.walk(ast.parse(operation)) if isinstance(cn, ast.Name)
    ]
    timeseries = Timeseries.get_many_by_id(ts_ids)

    # Get source data
    data_df = tsdio.get_timeseries_data(
        start_dt,
        end_dt,
        timeseries,
        data_state,
    )

    # TODO: catch inf (div by 0)

    with ContextVarManager(TIMESERIES, {ts_id: data_df[ts_id] for ts_id in ts_ids}):
        return eval_(ast.parse(operation, mode="eval").body)
