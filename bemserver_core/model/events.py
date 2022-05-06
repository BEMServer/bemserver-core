"""Event"""

import sqlalchemy as sqla
import sqlalchemy.orm as sqlaorm
from sqlalchemy.ext.hybrid import hybrid_property

from bemserver_core.database import Base
from bemserver_core.authorization import auth, AuthMixin, Relation


class EventCategory(AuthMixin, Base):
    __tablename__ = "event_categories"

    id = sqla.Column(sqla.String(80), primary_key=True, nullable=False)
    description = sqla.Column(sqla.String(250))
    parent = sqla.Column(
        sqla.String, sqla.ForeignKey("event_categories.id"), nullable=True
    )


class EventState(AuthMixin, Base):
    __tablename__ = "event_states"

    id = sqla.Column(sqla.String(80), primary_key=True, nullable=False)
    description = sqla.Column(sqla.String(250))


class EventLevel(AuthMixin, Base):
    __tablename__ = "event_levels"

    id = sqla.Column(sqla.String(80), primary_key=True, nullable=False)
    description = sqla.Column(sqla.String(250))


class Event(AuthMixin, Base):

    __tablename__ = "events"

    id = sqla.Column(sqla.Integer, primary_key=True, autoincrement=True, nullable=False)

    # Use getter/setter to prevent modifying campaign_scope after commit
    @sqlaorm.declared_attr
    def _campaign_scope_id(cls):
        return sqla.Column(
            sqla.Integer, sqla.ForeignKey("campaign_scopes.id"), nullable=False
        )

    @hybrid_property
    def campaign_scope_id(self):
        return self._campaign_scope_id

    @campaign_scope_id.setter
    def campaign_scope_id(self, campaign_scope_id):
        if self.id is not None:
            raise AttributeError("campaign_scope_id cannot be modified")
        self._campaign_scope_id = campaign_scope_id

    @sqlaorm.declared_attr
    def category(cls):
        return sqla.Column(
            sqla.String, sqla.ForeignKey("event_categories.id"), nullable=False
        )

    @sqlaorm.declared_attr
    def level(cls):
        return sqla.Column(
            sqla.String, sqla.ForeignKey("event_levels.id"), nullable=False
        )

    @sqlaorm.declared_attr
    def state(cls):
        return sqla.Column(
            sqla.String, sqla.ForeignKey("event_states.id"), nullable=False
        )

    # Use getter/setter to prevent modifying timestamp after commit
    _timestamp = sqla.Column(sqla.DateTime(timezone=True), nullable=False)

    @hybrid_property
    def timestamp(self):
        return self._timestamp

    @timestamp.setter
    def timestamp(self, timestamp):
        if self.id is not None:
            raise AttributeError("timestamp cannot be modified")
        self._timestamp = timestamp

    source = sqla.Column(sqla.String, nullable=False)

    description = sqla.Column(sqla.String(250))

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


@sqla.event.listens_for(EventLevel.__table__, "after_create")
def _insert_initial_event_levels(target, connection, **kwargs):
    # add the 3 default event levels
    connection.execute(target.insert(), {"id": "INFO", "description": "Information"})
    connection.execute(target.insert(), {"id": "WARNING", "description": "Warning"})
    connection.execute(target.insert(), {"id": "ERROR", "description": "Error"})
    connection.execute(target.insert(), {"id": "CRITICAL", "description": "Critical"})


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
