"""v0.14

Revision ID: 0.14
Revises: 0.13
Create Date: 2023-05-05 09:39:43.892682

"""

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "0.14"
down_revision = "0.13"
branch_labels = None
depends_on = None


wtbs_table = sa.sql.table("weather_ts_by_site", sa.sql.column("forecast"))


def upgrade():
    op.add_column(
        "weather_ts_by_site", sa.Column("forecast", sa.Boolean(), nullable=True)
    )
    op.execute(wtbs_table.update().values(forecast=False))
    op.alter_column("weather_ts_by_site", "forecast", nullable=False)
    op.drop_constraint(
        "uq_weather_ts_by_site_site_id", "weather_ts_by_site", type_="unique"
    )
    op.create_unique_constraint(
        op.f("uq_weather_ts_by_site_site_id"),
        "weather_ts_by_site",
        ["site_id", "parameter", "forecast"],
    )


def downgrade():
    op.drop_constraint(
        op.f("uq_weather_ts_by_site_site_id"), "weather_ts_by_site", type_="unique"
    )
    op.execute(wtbs_table.delete().where(wtbs_table.c.forecast.is_(True)))
    op.create_unique_constraint(
        "uq_weather_ts_by_site_site_id", "weather_ts_by_site", ["site_id", "parameter"]
    )
    op.drop_column("weather_ts_by_site", "forecast")
