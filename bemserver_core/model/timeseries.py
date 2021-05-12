"""Timeseries"""

import datetime as dt
import sqlalchemy as sqla

from bemserver_core.database import Base


class Timeseries(Base):
    __tablename__ = "timeseries"

    id = sqla.Column(sqla.Integer, primary_key=True)
    name = sqla.Column(sqla.String(80), unique=True, nullable=False)
    description = sqla.Column(sqla.String(500))
    unit = sqla.Column(sqla.String(20))
    min_value = sqla.Column(sqla.Float)
    max_value = sqla.Column(sqla.Float)
