"""Weather data"""

import enum

import sqlalchemy as sqla

from bemserver_core.authorization import AuthMixin, Relation, auth
from bemserver_core.database import Base


class WeatherParameterEnum(enum.Enum):
    """Weather parameter enum"""

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


class WeatherTimeseriesBySite(AuthMixin, Base):
    __tablename__ = "weather_ts_by_site"
    __table_args__ = (sqla.UniqueConstraint("site_id", "parameter", "forecast"),)

    id = sqla.Column(sqla.Integer, primary_key=True)
    site_id = sqla.Column(sqla.ForeignKey("sites.id"), nullable=False)
    parameter = sqla.Column(sqla.Enum(WeatherParameterEnum), nullable=False)
    timeseries_id = sqla.Column(sqla.ForeignKey("timeseries.id"), nullable=False)
    forecast = sqla.Column(sqla.Boolean(), nullable=False)

    site = sqla.orm.relationship(
        "Site",
        backref=sqla.orm.backref(
            "weather_timeseries_by_sites", cascade="all, delete-orphan"
        ),
    )
    timeseries = sqla.orm.relationship(
        "Timeseries",
        backref=sqla.orm.backref(
            "weather_timeseries_by_sites", cascade="all, delete-orphan"
        ),
    )

    @classmethod
    def register_class(cls):
        auth.register_class(
            cls,
            fields={
                "timeseries": Relation(
                    kind="one",
                    other_type="Timeseries",
                    my_field="timeseries_id",
                    other_field="id",
                ),
            },
        )
