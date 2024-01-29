"""v0.3

Revision ID: 0.3
Revises: 0.1
Create Date: 2022-12-05 17:11:29.663991

"""

from textwrap import dedent

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = "0.3"
down_revision = "0.1"
branch_labels = None
depends_on = None


def gen_ddl_trigger_ro(table_name, col_name):
    return sa.DDL(
        dedent(
            f"""
            CREATE TRIGGER
                {table_name}_trigger_update_readonly_{col_name}
            BEFORE UPDATE
                OF {col_name} ON {table_name}
            FOR EACH ROW
                WHEN (
                    NEW.{col_name} IS DISTINCT FROM OLD.{col_name}
                )
                EXECUTE FUNCTION column_update_not_allowed({col_name});
            """
        )
    )


def upgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    op.create_table(
        "event_categs",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("name", sa.String(length=80), nullable=False),
        sa.Column("description", sa.String(length=250), nullable=True),
        sa.PrimaryKeyConstraint("id", name=op.f("pk_event_categs")),
        sa.UniqueConstraint("name", name=op.f("uq_event_categs_name")),
    )
    op.create_table(
        "event_levels",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("name", sa.String(length=80), nullable=False),
        sa.Column("description", sa.String(length=250), nullable=True),
        sa.PrimaryKeyConstraint("id", name=op.f("pk_event_levels")),
        sa.UniqueConstraint("name", name=op.f("uq_event_levels_name")),
    )
    op.create_table(
        "st_check_missing_by_campaigns",
        sa.Column("id", sa.Integer(), nullable=False),
        sa.Column("campaign_id", sa.Integer(), nullable=False),
        sa.Column("is_enabled", sa.Boolean(), nullable=False),
        sa.ForeignKeyConstraint(
            ["campaign_id"],
            ["campaigns.id"],
            name=op.f("fk_st_check_missing_by_campaigns_campaign_id_campaigns"),
        ),
        sa.PrimaryKeyConstraint("id", name=op.f("pk_st_check_missing_by_campaigns")),
        sa.UniqueConstraint(
            "campaign_id", name=op.f("uq_st_check_missing_by_campaigns_campaign_id")
        ),
    )
    op.create_table(
        "events",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("campaign_scope_id", sa.Integer(), nullable=False),
        sa.Column("category_id", sa.Integer(), nullable=False),
        sa.Column("level_id", sa.Integer(), nullable=False),
        sa.Column("timestamp", sa.DateTime(timezone=True), nullable=False),
        sa.Column("source", sa.String(), nullable=False),
        sa.Column("description", sa.String(), nullable=True),
        sa.ForeignKeyConstraint(
            ["campaign_scope_id"],
            ["c_scopes.id"],
            name=op.f("fk_events_campaign_scope_id_c_scopes"),
        ),
        sa.ForeignKeyConstraint(
            ["category_id"],
            ["event_categs.id"],
            name=op.f("fk_events_category_id_event_categs"),
        ),
        sa.ForeignKeyConstraint(
            ["level_id"],
            ["event_levels.id"],
            name=op.f("fk_events_level_id_event_levels"),
        ),
        sa.PrimaryKeyConstraint("id", name=op.f("pk_events")),
    )
    op.create_table(
        "ts_by_events",
        sa.Column("id", sa.Integer(), nullable=False),
        sa.Column("timeseries_id", sa.Integer(), nullable=False),
        sa.Column("event_id", sa.Integer(), nullable=False),
        sa.ForeignKeyConstraint(
            ["event_id"], ["events.id"], name=op.f("fk_ts_by_events_event_id_events")
        ),
        sa.ForeignKeyConstraint(
            ["timeseries_id"],
            ["timeseries.id"],
            name=op.f("fk_ts_by_events_timeseries_id_timeseries"),
        ),
        sa.PrimaryKeyConstraint("id", name=op.f("pk_ts_by_events")),
        sa.UniqueConstraint(
            "event_id", "timeseries_id", name=op.f("uq_ts_by_events_event_id")
        ),
    )
    # ### end Alembic commands ###

    op.execute(gen_ddl_trigger_ro("events", "timestamp"))
    op.execute(gen_ddl_trigger_ro("events", "campaign_scope_id"))

    event_categories_table = sa.sql.table(
        "event_categs",
        sa.sql.column("id", sa.Integer),
        sa.sql.column("name", sa.String),
        sa.sql.column("description", sa.String),
    )

    op.bulk_insert(
        event_categories_table,
        [
            {"name": "Data missing"},
            {"name": "Data present"},
            {"name": "Data outliers"},
        ],
    )

    event_levels_table = sa.sql.table(
        "event_levels",
        sa.sql.column("id", sa.Integer),
        sa.sql.column("name", sa.String),
        sa.sql.column("description", sa.String),
    )

    op.bulk_insert(
        event_levels_table,
        [
            {"name": "INFO", "description": "Information"},
            {"name": "WARNING", "description": "Warning"},
            {"name": "ERROR", "description": "Error"},
            {"name": "CRITICAL", "description": "Critical"},
        ],
    )


def downgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    op.drop_table("ts_by_events")
    op.drop_table("events")
    op.drop_table("st_check_missing_by_campaigns")
    op.drop_table("event_levels")
    op.drop_table("event_categs")
    # ### end Alembic commands ###
