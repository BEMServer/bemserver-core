"""v0.12

Revision ID: 0.12
Revises: 0.11
Create Date: 2023-03-14 10:01:24.080592

"""

import enum

from alembic import op
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision = "0.12"
down_revision = "0.11"
branch_labels = None
depends_on = None


class WeatherParameterEnumOld(enum.Enum):
    AIR_TEMPERATURE = "air temperature"
    DEWPOINT_TEMPERATURE = "dewpoint temperature"
    WETBULB_TEMPERATURE = "wetbulb temperature"
    WIND_SPEED = "wind speed"
    WIND_DIRECTION = "wind direction"
    SURFACE_SOLAR_RADIATION = "surface solar radiation"
    DIRECT_NORMAL_SOLAR_RADIATION = "direct normal solar radiation"
    RELATIVE_HUMIDITY = "relative humidity"
    SURFACE_PRESSURE = "surface pressure"
    TOTAL_PRECIPITATION = "total precipitation"


class WeatherParameterEnum(enum.Enum):
    AIR_TEMPERATURE = "air temperature"
    DEWPOINT_TEMPERATURE = "dewpoint temperature"
    WETBULB_TEMPERATURE = "wetbulb temperature"
    WIND_SPEED = "wind speed"
    WIND_DIRECTION = "wind direction"
    SURFACE_SOLAR_RADIATION = "surface solar radiation"
    SURFACE_DIRECT_SOLAR_RADIATION = "surface direct solar radiation"
    SURFACE_DIFFUSE_SOLAR_RADIATION = "surface diffuse solar radiation"
    DIRECT_NORMAL_SOLAR_RADIATION = "direct normal solar radiation"
    RELATIVE_HUMIDITY = "relative humidity"
    SURFACE_PRESSURE = "surface pressure"
    TOTAL_PRECIPITATION = "total precipitation"


def upgrade():
    op.execute("ALTER TYPE weatherparameterenum RENAME TO weatherparameterenum_old")
    weatherparameterenum = postgresql.ENUM(
        WeatherParameterEnum, name="weatherparameterenum"
    )
    weatherparameterenum.create(op.get_bind(), checkfirst=True)
    op.execute(
        "ALTER TABLE weather_ts_by_site ALTER COLUMN parameter "
        "TYPE weatherparameterenum USING parameter::text::weatherparameterenum"
    )
    op.execute("DROP TYPE weatherparameterenum_old")


def downgrade():
    op.execute("ALTER TYPE weatherparameterenum RENAME TO weatherparameterenum_old")
    weatherparameterenum = postgresql.ENUM(
        WeatherParameterEnumOld, name="weatherparameterenum"
    )
    weatherparameterenum.create(op.get_bind(), checkfirst=True)
    op.execute(
        "ALTER TABLE weather_ts_by_site ALTER COLUMN parameter "
        "TYPE weatherparameterenum USING parameter::text::weatherparameterenum"
    )
    op.execute("DROP TYPE weatherparameterenum_old")
