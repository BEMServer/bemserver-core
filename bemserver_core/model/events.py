"""Event"""

import datetime as dt
import sqlalchemy as sqla
import sqlalchemy.orm as sqlaorm

from bemserver_core.database import Base, db
from bemserver_core.model.exceptions import EventError


class EventChannel(Base):
    __tablename__ = "event_channels"

    id = sqla.Column(sqla.Integer, primary_key=True)
    name = sqla.Column(sqla.String(80))


class EventCategory(Base):
    __tablename__ = "event_categories"

    id = sqla.Column(sqla.String(80), primary_key=True, nullable=False)
    description = sqla.Column(sqla.String(250))
    parent = sqla.Column(
        sqla.String,
        sqla.ForeignKey("event_categories.id"),
        nullable=True
    )


class EventState(Base):
    __tablename__ = "event_states"

    id = sqla.Column(sqla.String(80), primary_key=True, nullable=False)
    description = sqla.Column(sqla.String(250))


class EventLevel(Base):
    __tablename__ = "event_levels"

    id = sqla.Column(sqla.String(80), primary_key=True, nullable=False)
    description = sqla.Column(sqla.String(250))


@sqlaorm.declarative_mixin
class Event:
    """Abstract base class for event classes"""

    id = sqla.Column(
        sqla.Integer, primary_key=True, autoincrement=True, nullable=False)

    @sqlaorm.declared_attr
    def channel_id(cls):
        return sqla.Column(
            sqla.Integer,
            sqla.ForeignKey("event_channels.id"),
            nullable=False
        )

    @sqlaorm.declared_attr
    def category(cls):
        return sqla.Column(
            sqla.String,
            sqla.ForeignKey("event_categories.id"),
            nullable=False
        )

    @sqlaorm.declared_attr
    def level(cls):
        return sqla.Column(
            sqla.String,
            sqla.ForeignKey("event_levels.id"),
            nullable=False
        )

    @sqlaorm.declared_attr
    def state(cls):
        return sqla.Column(
            sqla.String,
            sqla.ForeignKey("event_states.id"),
            nullable=False
        )

    timestamp = sqla.Column(sqla.DateTime(timezone=True), nullable=False)

    source = sqla.Column(sqla.String, nullable=False)

    description = sqla.Column(sqla.String(250))

    @classmethod
    def open(
            cls, channel_id, category, source, level="ERROR",
            timestamp=None, description=None
    ):
        """Create a NEW event.

        :param int channel_id: The channel ID of the event. See `EventChannel`.
        :param string category: The category of the event. See `EventCategory`.
        :param string source: The source name of the event (service name...).
        :param string level: (optional, default "ERROR")
            The level name of the event. See `EventLevel`.
        :param datetime timestamp: (optional, default None)
            Time (tz-aware) of the event. Set to now if None.
        :param string description: (optional, default None)
            Text to describe the event.
        :returns Event: The instance of the event created.
        """
        ts_now = dt.datetime.now(dt.timezone.utc)
        return cls.new(
            channel_id=channel_id,
            category=category, source=source, level=level, state="NEW",
            timestamp=timestamp or ts_now,
            description=description,
        )

    @classmethod
    def list_by_state(
        cls, states=("NEW", "ONGOING",), channel_id=None,
        category=None, source=None, level="ERROR"
    ):
        if states is None or len(states) <= 0:
            raise EventError("Missing `state` filter.")
        state_conditions = tuple((cls.state == x) for x in states)
        stmt = sqla.select(cls).filter(sqla.or_(*state_conditions))
        if channel_id is not None:
            stmt = stmt.filter(cls.channel_id == channel_id)
        if category is not None:
            stmt = stmt.filter(cls.category == category)
        if source is not None:
            stmt = stmt.filter(cls.source == source)
        if level is not None:
            stmt = stmt.filter(cls.level == level)
        return db.session.execute(stmt).all()


class TimeseriesEvent(Event, Base):
    __tablename__ = "timeseries_events"


class TimeseriesEventByTimeseries(Base):
    """TimeseriesEvent x Timeseries associations"""
    __tablename__ = "timeseries_events_by_timeseries"
    __table_args__ = (
        sqla.UniqueConstraint("timeseries_event_id", "timeseries_id"),
    )

    id = sqla.Column(sqla.Integer, primary_key=True)
    timeseries_event_id = sqla.Column(
        sqla.Integer,
        sqla.ForeignKey("timeseries_events.id"),
        nullable=True
    )
    timeseries_id = sqla.Column(
        sqla.Integer,
        sqla.ForeignKey("timeseries.id"),
        nullable=True
    )


# TODO: maybe this is something the concerned service could fill
@sqla.event.listens_for(EventCategory.__table__, "after_create")
def _insert_initial_event_categories(target, connection, **kwargs):
    # add default event categories
    connection.execute(
        target.insert(),
        {
            "id": "ABNORMAL_TIMESTAMPS",
            "description": "Abnormal timestamps in timeseries",
        }
    )
    connection.execute(
        target.insert(),
        {
            "id": "observation_missing",
            "parent": "ABNORMAL_TIMESTAMPS",
            "description": "Observation timestamp is missing",
        }
    )
    connection.execute(
        target.insert(),
        {
            "id": "observation_interval_too_large",
            "parent": "ABNORMAL_TIMESTAMPS",
            "description": (
                "Observation timestamp interval is too large"
                " compared to the timeseries observation interval"
            ),
        }
    )
    connection.execute(
        target.insert(),
        {
            "id": "observation_interval_too_short",
            "parent": "ABNORMAL_TIMESTAMPS",
            "description": (
                "Observation timestamp interval is too short"
                " compared to the timeseries observation interval"
            ),
        }
    )
    connection.execute(
        target.insert(),
        {
            "id": "reception_interval_too_large",
            "parent": "ABNORMAL_TIMESTAMPS",
            "description": (
                "Reception timestamp interval is too large"
                " compared to the timeseries reception interval"
            ),
        }
    )
    connection.execute(
        target.insert(),
        {
            "id": "reception_interval_too_short",
            "parent": "ABNORMAL_TIMESTAMPS",
            "description": (
                "Reception timestamp interval is too short"
                " compared to the timeseries reception interval"
            ),
        }
    )
    connection.execute(
        target.insert(),
        {
            "id": "ABNORMAL_MEASURE_VALUES",
            "description": "Abnormal measure values in timeseries",
        }
    )
    connection.execute(
        target.insert(),
        {
            "id": "out_of_range",
            "parent": "ABNORMAL_MEASURE_VALUES",
            "description": "Measure value is out of range",
        }
    )


@sqla.event.listens_for(EventLevel.__table__, "after_create")
def _insert_initial_event_levels(target, connection, **kwargs):
    # add the 3 default event levels
    connection.execute(
        target.insert(),
        {"id": "INFO", "description": "Information"}
    )
    connection.execute(
        target.insert(),
        {"id": "WARNING", "description": "Warning"}
    )
    connection.execute(
        target.insert(),
        {"id": "ERROR", "description": "Error"}
    )
    connection.execute(
        target.insert(),
        {"id": "CRITICAL", "description": "Critical"}
    )


@sqla.event.listens_for(EventState.__table__, "after_create")
def _insert_initial_event_states(target, connection, **kwargs):
    # add the 3 default event states
    connection.execute(
        target.insert(),
        {"id": "NEW", "description": "New event"},
    )
    connection.execute(
        target.insert(),
        {"id": "ONGOING", "description": "Ongoing event"},
    )
    connection.execute(
        target.insert(),
        {"id": "CLOSED", "description": "Closed event"},
    )
