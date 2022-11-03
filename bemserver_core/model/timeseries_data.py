"""Timeseries data"""
import sqlalchemy as sqla

from bemserver_core.database import Base


class TimeseriesData(Base):
    __tablename__ = "ts_data"
    __table_args__ = (sqla.PrimaryKeyConstraint("ts_by_data_state_id", "timestamp"),)

    timestamp = sqla.Column(sqla.DateTime(timezone=True))
    timeseries_by_data_state_id = sqla.Column(
        "ts_by_data_state_id",
        sqla.Integer,
        sqla.ForeignKey("ts_by_data_states.id"),
        nullable=False,
    )
    value = sqla.Column(sqla.Float)

    timeseries_by_data_state = sqla.orm.relationship(
        "TimeseriesByDataState",
        backref=sqla.orm.backref("timeseries_data", cascade="all, delete-orphan"),
    )
