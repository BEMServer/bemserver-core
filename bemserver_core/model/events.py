"""Event"""
import sqlalchemy as sqla

from bemserver_core.database import Base, db, make_columns_read_only
from bemserver_core.authorization import auth, AuthMixin, Relation
from bemserver_core.exceptions import BEMServerCoreCampaignScopeError
from .timeseries import Timeseries


class EventCategory(AuthMixin, Base):
    __tablename__ = "event_categs"

    id = sqla.Column(sqla.Integer, primary_key=True, autoincrement=True, nullable=False)
    name = sqla.Column(sqla.String(80), unique=True, nullable=False)
    description = sqla.Column(sqla.String(250))


class EventLevel(AuthMixin, Base):
    __tablename__ = "event_levels"

    id = sqla.Column(sqla.Integer, primary_key=True, autoincrement=True, nullable=False)
    name = sqla.Column(sqla.String(80), unique=True, nullable=False)
    description = sqla.Column(sqla.String(250))


class Event(AuthMixin, Base):
    __tablename__ = "events"

    id = sqla.Column(sqla.Integer, primary_key=True, autoincrement=True, nullable=False)
    campaign_scope_id = sqla.Column(sqla.ForeignKey("c_scopes.id"), nullable=False)
    category_id = sqla.Column(sqla.ForeignKey("event_categs.id"), nullable=False)
    level_id = sqla.Column(sqla.ForeignKey("event_levels.id"), nullable=False)
    timestamp = sqla.Column(sqla.DateTime(timezone=True), nullable=False)
    source = sqla.Column(sqla.String, nullable=False)
    description = sqla.Column(sqla.String())

    category = sqla.orm.relationship(
        "EventCategory",
        backref=sqla.orm.backref("events", cascade="all, delete-orphan"),
    )
    level = sqla.orm.relationship(
        "EventLevel", backref=sqla.orm.backref("events", cascade="all, delete-orphan")
    )
    campaign_scope = sqla.orm.relationship(
        "CampaignScope", backref=sqla.orm.backref("sites", cascade="all, delete-orphan")
    )

    @classmethod
    def register_class(cls):
        auth.register_class(
            cls,
            fields={
                "campaign_scope": Relation(
                    kind="one",
                    other_type="CampaignScope",
                    my_field="campaign_scope_id",
                    other_field="id",
                ),
            },
        )


class TimeseriesByEvent(AuthMixin, Base):
    __tablename__ = "ts_by_events"
    __table_args__ = (sqla.UniqueConstraint("event_id", "timeseries_id"),)

    id = sqla.Column(sqla.Integer, primary_key=True)
    timeseries_id = sqla.Column(sqla.ForeignKey("timeseries.id"), nullable=False)
    event_id = sqla.Column(sqla.ForeignKey("events.id"), nullable=False)

    event = sqla.orm.relationship(
        "Event",
        backref=sqla.orm.backref("timeseries_by_event", cascade="all, delete-orphan"),
    )
    timeseries = sqla.orm.relationship(
        "Timeseries",
        backref=sqla.orm.backref("timeseries_by_event", cascade="all, delete-orphan"),
    )

    def _before_flush(self):
        # Ensure TS and Event are in same Campaign scope
        if self.timeseries_id and self.event_id:
            timeseries = Timeseries.get_by_id(self.timeseries_id)
            event = Event.get_by_id(self.event_id)
            if timeseries.campaign_scope != event.campaign_scope:
                raise BEMServerCoreCampaignScopeError(
                    "Event and timeseries must be in same campaign scope"
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
                "event": Relation(
                    kind="one",
                    other_type="Event",
                    my_field="event_id",
                    other_field="id",
                ),
            },
        )


def init_db_events_triggers():
    """Create triggers to protect some columns from update.

    This function is meant to be used for tests or dev setups after create_all.
    Production setups should rely on migration scripts.
    """
    make_columns_read_only(
        Event.timestamp,
        Event.campaign_scope_id,
    )
    db.session.commit()


def init_db_events():
    """Create default event levels

    This function is meant to be used for tests or dev setups after create_all.
    Production setups should rely on migration scripts.
    """
    db.session.add_all(
        [
            EventCategory(name="Data missing"),
            EventCategory(name="Data present"),
            EventCategory(name="Data outliers"),
            EventLevel(name="INFO", description="Information"),
            EventLevel(name="WARNING", description="Warning"),
            EventLevel(name="ERROR", description="Error"),
            EventLevel(name="CRITICAL", description="Critical"),
        ]
    )
    db.session.commit()
