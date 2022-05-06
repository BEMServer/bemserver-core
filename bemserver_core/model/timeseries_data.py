"""Timeseries data"""
import sqlalchemy as sqla

from bemserver_core.database import Base, db


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


def init_db_timeseries_data():
    """Create timescale hypertable

    This function is meant to be used for tests or dev setups after create_all.
    Production setups should rely on migration scripts.
    """
    db.session.execute(
        sqla.DDL(
            "SELECT create_hypertable("
            f"  '{TimeseriesData.__table__}',"
            "  'timestamp',"
            "  create_default_indexes => False"
            ");"
        )
    )
    db.session.commit()
