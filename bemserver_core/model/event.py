"""Event"""

import datetime as dt
import sqlalchemy as sqla

from bemserver_core.database import Base, BaseMixin, db
from bemserver_core.model.exceptions import EventError


class EventCategory(Base, BaseMixin):
    __tablename__ = "event_category"

    id = sqla.Column(sqla.String(80), primary_key=True, nullable=False)
    description = sqla.Column(sqla.String(250))
    parent = sqla.Column(
        sqla.String,
        sqla.ForeignKey("event_category.id"),
        nullable=True
    )


class EventState(Base, BaseMixin):
    __tablename__ = "event_state"

    id = sqla.Column(sqla.String(80), primary_key=True, nullable=False)
    description = sqla.Column(sqla.String(250))


class EventLevel(Base, BaseMixin):
    __tablename__ = "event_level"

    id = sqla.Column(sqla.String(80), primary_key=True, nullable=False)
    description = sqla.Column(sqla.String(250))


class EventTarget(Base, BaseMixin):
    __tablename__ = "event_target"

    id = sqla.Column(sqla.String(80), primary_key=True, nullable=False)
    description = sqla.Column(sqla.String(250))


# Event state could be deduced on the fly:
#   1/ if no timestamp_end is defined then it can be NEW or ONGOING:
#       a/ if time elapsed between timestamp_start and timestamp_last_update
#           is less than an amount of seconds (maybe 30) then the event is NEW
#       b/ else the event is older than 30 seconds and its state is ONGOING
#   2/ else timestamp_end is defined and the event state is CLOSED
# These deductions have to be done in the code and SQL request when needed.
# Instead of this we choose to manually set the event state and store it in
#  the database ("state" column of "Event" table). It will probably help us
#  later when requesting events from the database (by making things easier as
#  "state" will just be an additional filter criteria).
class Event(Base, BaseMixin):
    __tablename__ = "event"

    id = sqla.Column(
        sqla.Integer, primary_key=True, autoincrement=True, nullable=False)

    category = sqla.Column(
        sqla.String,
        sqla.ForeignKey("event_category.id"),
        nullable=False
    )

    level = sqla.Column(
        sqla.String,
        sqla.ForeignKey("event_level.id"),
        nullable=False
    )

    timestamp_start = sqla.Column(sqla.DateTime(timezone=True), nullable=False)
    timestamp_end = sqla.Column(sqla.DateTime(timezone=True))

    source = sqla.Column(sqla.String, nullable=False)
    target_type = sqla.Column(
        sqla.String,
        sqla.ForeignKey("event_target.id"),
        nullable=False
    )
    target_id = sqla.Column(sqla.Integer, nullable=False)

    state = sqla.Column(
        sqla.String,
        sqla.ForeignKey("event_state.id"),
        nullable=False
    )

    timestamp_last_update = sqla.Column(
        sqla.DateTime(timezone=True), nullable=False)

    description = sqla.Column(sqla.String(250))

    @property
    def duration(self):
        if self.timestamp_start is not None:
            # event is CLOSED
            if self.timestamp_end is not None:
                return self.timestamp_end - self.timestamp_start
            # event is NEW or ONGOING
            if self.timestamp_last_update is not None:
                return self.timestamp_last_update - self.timestamp_start
            else:
                return dt.datetime.now(dt.timezone.utc) - self.timestamp_start
        return None

    def extend(self):
        """Change the state of the event to ONGOING:
            - a NEW event will be updated to ONGOING
            - an ONGOING event will still ONGOING
            - a CLOSED event can not be extended (an EventError is raised)

        Extending a NEW or ONGOING event also updates the timestamp_last_update
        field value.

        :raises EventError: When trying to extend a CLOSED event.
        """
        if self.state == "CLOSED":
            raise EventError("A closed event can not be extended.")
        if self.state != "ONGOING":
            self.state = "ONGOING"
        self.timestamp_last_update = dt.datetime.now(dt.timezone.utc)
        self.save()

    def close(self, timestamp_end=None):
        """Change the state of the event to CLOSED (if not CLOSED yet) and
        update the timestamp_last_update field value.

        Note that a NEW event can be CLOSED without being ONGOING before.

        :param datetime timestamp_end: (optional, default None)
            Time (tz-aware) of when the event is CLOSED. Set to NOW if None.
        """
        # TODO: warn if event is already closed?
        if self.state != "CLOSED":
            self.state = "CLOSED"
            ts_now = dt.datetime.now(dt.timezone.utc)
            self.timestamp_last_update = ts_now
            self.timestamp_end = timestamp_end or ts_now
            self.save()

    @classmethod
    def open(
            cls, category, source, target_type, target_id, level="ERROR",
            timestamp_start=None, description=None):
        """Create a NEW event.

        :param string category: The category of the event. See `EventCategory`.
        :param string source: The source name of the event (service name...).
        :param string target_type: The target type of the event.
            Can be "TIMESERIES" for a timeseries target. See `EventTarget`.
        :param int target_id: The target unique ID (can be a timeseries ID).
        :param string level: (optional, default "ERROR")
            The level name of the event. See `EventLevel`.
        :param datetime timestamp_start: (optional, default None)
            Time (tz-aware) of when the event is opened. Set to NOW if None.
        :param string description: (optional, default None)
            Text to describe the event.
        :returns Event: The instance of the event created.
        """
        ts_now = dt.datetime.now(dt.timezone.utc)
        evt = cls(
            category=category, source=source, level=level, state="NEW",
            target_type=target_type, target_id=target_id,
            timestamp_start=timestamp_start or ts_now,
            timestamp_last_update=ts_now, description=description)
        evt.save()
        return evt

    @classmethod
    def list_by_state(
            cls, states=("NEW", "ONGOING",), category=None, source=None,
            level="ERROR", target_type=None, target_id=None):
        if states is None or len(states) <= 0:
            raise EventError("Missing `state` filter.")
        state_conditions = tuple((cls.state == x) for x in states)
        stmt = sqla.select(cls).filter(sqla.or_(*state_conditions))
        if category is not None:
            stmt = stmt.filter(cls.category == category)
        if source is not None:
            stmt = stmt.filter(cls.source == source)
        if level is not None:
            stmt = stmt.filter(cls.level == level)
        if target_type is not None:
            stmt = stmt.filter(cls.target_type == target_type)
        if target_id is not None:
            stmt = stmt.filter(cls.target_id == target_id)
        return db.session.execute(stmt).all()


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


@sqla.event.listens_for(EventTarget.__table__, "after_create")
def _insert_initial_event_targets(target, connection, **kwargs):
    # add default event targets
    connection.execute(
        target.insert(),
        {"id": "TIMESERIES", "description": "Timeseries"}
    )
    connection.execute(
        target.insert(),
        {"id": "SITE", "description": "Site"}
    )
    connection.execute(
        target.insert(),
        {"id": "BUILDING", "description": "Building"}
    )
    connection.execute(
        target.insert(),
        {"id": "FLOOR", "description": "Floor"}
    )
    connection.execute(
        target.insert(),
        {"id": "SPACE", "description": "Space"}
    )
    connection.execute(
        target.insert(),
        {"id": "SENSOR", "description": "Sensor"}
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
