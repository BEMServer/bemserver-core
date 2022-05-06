"""Event"""

import sqlalchemy as sqla
import sqlalchemy.orm as sqlaorm
from sqlalchemy.ext.hybrid import hybrid_property

from bemserver_core.database import Base, db
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


def init_db_events():
    """Create default event levels and states

    This function is meant to be used for tests or dev setups after create_all.
    Production setups should rely on migration scripts.
    """
    db.session.add_all(
        [
            EventLevel(id="INFO", description="Information"),
            EventLevel(id="WARNING", description="Warning"),
            EventLevel(id="ERROR", description="Error"),
            EventLevel(id="CRITICAL", description="Critical"),
            EventState(id="NEW", description="New event"),
            EventState(id="ONGOING", description="Ongoing event"),
            EventState(id="CLOSED", description="Closed event"),
        ]
    )
    db.session.commit()
