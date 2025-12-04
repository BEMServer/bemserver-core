"""Expressions"""

import sqlalchemy as sqla

from bemserver_core import expression_eval
from bemserver_core.authorization import AuthMixin
from bemserver_core.database import Base
from bemserver_core.input_output.timeseries_data_io import AGGREGATION_FUNCTIONS, tsdio


class Expression(AuthMixin, Base):
    __tablename__ = "expressions"

    id = sqla.Column(sqla.Integer, primary_key=True)
    expr = sqla.Column(sqla.String, nullable=False)

    def validate(self):
        expression_eval.validate(self.expr, [v.name for v in self.variables])

    def evaluate(
        self,
        start_dt,
        end_dt,
        data_state,
        bucket_width_value,
        bucket_width_unit,
        timezone="UTC",
    ):
        namespace = {}
        for expr_var in self.variables:
            namespace[expr_var["name"]] = tsdio.get_timeseries_buckets_data(
                start_dt,
                end_dt,
                [expr_var.timeseries],
                data_state,
                bucket_width_value,
                bucket_width_unit,
                aggregation=expr_var.aggregation,
                convert_to=expr_var.unit_symbol,
                timezone=timezone,
                col_label="id",
            )[expr_var.timeseries_id]
        return expression_eval.evaluate(self.expr, namespace)


class ExpressionVariable(AuthMixin, Base):
    __tablename__ = "expr_vars"
    __table_args__ = (sqla.UniqueConstraint("expr_id", "name"),)

    id = sqla.Column(sqla.Integer, primary_key=True)
    expr_id = sqla.Column(sqla.ForeignKey("expressions.id"), nullable=False)

    name = sqla.Column(sqla.String, nullable=False)
    timeseries_id = sqla.Column(sqla.ForeignKey("timeseries.id"), nullable=False)
    aggregation = sqla.Column(
        sqla.Enum(AGGREGATION_FUNCTIONS), nullable=False, default="avg"
    )
    # TODO: validate?
    unit_symbol = sqla.Column(sqla.String(20))

    # TODO: cascade delete?
    expression = sqla.orm.relationship(
        Expression,
        backref=sqla.orm.backref("variables", cascade="all, delete-orphan"),
    )
    timeseries = sqla.orm.relationship(
        "Timeseries",
        backref=sqla.orm.backref("expression_variables", cascade="all, delete-orphan"),
    )
