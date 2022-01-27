"""Timeseries data"""
import sqlalchemy as sqla

from bemserver_core.database import Base
from bemserver_core.model.timeseries import Timeseries
from bemserver_core.authorization import auth, AuthMixin, get_current_user


class TimeseriesData(AuthMixin, Base):
    __tablename__ = "timeseries_data"
    __table_args__ = (sqla.PrimaryKeyConstraint("timeseries_id", "timestamp"),)

    timestamp = sqla.Column(sqla.DateTime(timezone=True))
    timeseries_id = sqla.Column(
        sqla.Integer,
        sqla.ForeignKey("timeseries.id"),
        nullable=False,
    )
    timeseries = sqla.orm.relationship("Timeseries")
    value = sqla.Column(sqla.Float)

    @classmethod
    def check_can_export(cls, start_dt, end_dt, timeseries):
        current_user = get_current_user()
        # TODO: test non-existent timeseries
        for ts_id in timeseries:
            ts = Timeseries.get_by_id(ts_id)
            auth.authorize(current_user, "read_data", ts)

    @classmethod
    def check_can_import(cls, start_dt, end_dt, timeseries):
        current_user = get_current_user()
        # TODO: test non-existent timeseries
        for ts_id in timeseries:
            ts = Timeseries.get_by_id(ts_id)
            auth.authorize(current_user, "write_data", ts)


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
