"""Timeseries data"""
import sqlalchemy as sqla

from bemserver_core.database import Base


class TimeseriesData(Base):
    __tablename__ = "timeseries_data"
    __table_args__ = (
        sqla.PrimaryKeyConstraint("timeseries_by_data_state_id", "timestamp"),
    )

    timestamp = sqla.Column(sqla.DateTime(timezone=True))
    timeseries_by_data_state_id = sqla.Column(
        sqla.Integer,
        sqla.ForeignKey("timeseries_by_data_states.id"),
        nullable=False,
    )
    value = sqla.Column(sqla.Float)

    timeseries_by_data_state = sqla.orm.relationship(
        "TimeseriesByDataState",
        backref=sqla.orm.backref("timeseries_data", cascade="all, delete-orphan"),
    )


sqla.event.listen(
    TimeseriesData.__table__,
    "after_create",
    sqla.DDL(
        "SELECT create_hypertable("
        "  '%(table)s',"
        "  'timestamp',"
        "  create_default_indexes => False"
        ");"
    ),
)
