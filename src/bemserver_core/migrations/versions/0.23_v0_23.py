"""v0.23

Revision ID: 0.23
Revises: 0.21
Create Date: 2026-09-03 17:36:06.857267

"""

import enum

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision = "0.23"
down_revision = "0.21"
branch_labels = None
depends_on = None


class PeriodEnum(enum.Enum):
    second = "second"
    minute = "minute"
    hour = "hour"
    day = "day"
    week = "week"
    month = "month"
    year = "year"


class AggregationFunctionsEnum(enum.StrEnum):
    AVG = "avg"
    SUM = "sum"
    MIN = "min"
    MAX = "max"
    COUNT = "count"


def upgrade():
    period_enum = postgresql.ENUM(PeriodEnum, name="periodenum", create_type=False)
    agg_funcs_enum = postgresql.ENUM(AggregationFunctionsEnum, name="aggfuncsenum")

    op.create_table(
        "expressions",
        sa.Column("id", sa.Integer(), nullable=False),
        sa.Column("campaign_scope_id", sa.Integer(), nullable=False),
        sa.Column("name", sa.String(), nullable=False),
        sa.Column("expr", sa.String(), nullable=False),
        sa.Column("unit_symbol", sa.String(length=20), nullable=True),
        sa.ForeignKeyConstraint(
            ["campaign_scope_id"],
            ["c_scopes.id"],
            name=op.f("fk_expressions_campaign_scope_id_c_scopes"),
        ),
        sa.PrimaryKeyConstraint("id", name=op.f("pk_expressions")),
    )
    op.create_table(
        "expr_vars",
        sa.Column("id", sa.Integer(), nullable=False),
        sa.Column("campaign_scope_id", sa.Integer(), nullable=False),
        sa.Column("expression_id", sa.Integer(), nullable=False),
        sa.Column("name", sa.String(), nullable=False),
        sa.Column("timeseries_id", sa.Integer(), nullable=False),
        sa.Column("aggregation", agg_funcs_enum, nullable=False),
        sa.Column("unit_symbol", sa.String(length=20), nullable=True),
        sa.ForeignKeyConstraint(
            ["campaign_scope_id"],
            ["c_scopes.id"],
            name=op.f("fk_expr_vars_campaign_scope_id_c_scopes"),
        ),
        sa.ForeignKeyConstraint(
            ["expression_id"],
            ["expressions.id"],
            name=op.f("fk_expr_vars_expression_id_expressions"),
        ),
        sa.ForeignKeyConstraint(
            ["timeseries_id"],
            ["timeseries.id"],
            name=op.f("fk_expr_vars_timeseries_id_timeseries"),
        ),
        sa.PrimaryKeyConstraint("id", name=op.f("pk_expr_vars")),
        sa.UniqueConstraint(
            "expression_id", "name", name=op.f("uq_expr_vars_expression_id")
        ),
    )
    op.create_table(
        "ts_expressions",
        sa.Column("id", sa.Integer(), nullable=False),
        sa.Column("campaign_scope_id", sa.Integer(), nullable=False),
        sa.Column("expression_id", sa.Integer(), nullable=False),
        sa.Column("timeseries_id", sa.Integer(), nullable=False),
        sa.Column("src_data_state_id", sa.Integer(), nullable=False),
        sa.Column("dest_data_state_id", sa.Integer(), nullable=False),
        sa.Column("bucket_width_value", sa.Integer(), nullable=False),
        sa.Column("bucket_width_unit", period_enum, nullable=False),
        sa.Column("timezone", sa.String(length=40), nullable=False),
        sa.ForeignKeyConstraint(
            ["campaign_scope_id"],
            ["c_scopes.id"],
            name=op.f("fk_ts_expressions_campaign_scope_id_c_scopes"),
        ),
        sa.ForeignKeyConstraint(
            ["dest_data_state_id"],
            ["ts_data_states.id"],
            name=op.f("fk_ts_expressions_dest_data_state_id_ts_data_states"),
        ),
        sa.ForeignKeyConstraint(
            ["expression_id"],
            ["expressions.id"],
            name=op.f("fk_ts_expressions_expression_id_expressions"),
        ),
        sa.ForeignKeyConstraint(
            ["src_data_state_id"],
            ["ts_data_states.id"],
            name=op.f("fk_ts_expressions_src_data_state_id_ts_data_states"),
        ),
        sa.ForeignKeyConstraint(
            ["timeseries_id"],
            ["timeseries.id"],
            name=op.f("fk_ts_expressions_timeseries_id_timeseries"),
        ),
        sa.PrimaryKeyConstraint("id", name=op.f("pk_ts_expressions")),
    )


def downgrade():
    op.drop_table("ts_expressions")
    op.drop_table("expr_vars")
    op.drop_table("expressions")
