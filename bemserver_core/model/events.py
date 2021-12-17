"""Event"""

import sqlalchemy as sqla
import sqlalchemy.orm as sqlaorm
from sqlalchemy.ext.associationproxy import association_proxy
from sqlalchemy.ext.hybrid import hybrid_property

from bemserver_core.database import Base, db
from bemserver_core.authorization import (
    auth,
    AuthMixin,
    Relation,
    get_current_user,
    get_current_campaign,
)
from bemserver_core.exceptions import BEMServerCoreMissingCampaignError


class EventChannel(AuthMixin, Base):
    __tablename__ = "event_channels"

    id = sqla.Column(sqla.Integer, primary_key=True)
    name = sqla.Column(sqla.String(80))

    @classmethod
    def register_class(cls):
        auth.register_class(
            cls,
            fields={
                "event_channels_by_campaigns": Relation(
                    kind="many",
                    other_type="EventChannelByCampaign",
                    my_field="id",
                    other_field="event_channel_id",
                ),
            },
        )

    @classmethod
    def get(cls, *, campaign_id=None, **kwargs):
        query = super().get(**kwargs)
        if campaign_id:
            query = query.join(EventChannelByCampaign).filter_by(
                campaign_id=campaign_id
            )
        return query


class EventChannelByCampaign(AuthMixin, Base):
    """EventChannel x Campaign associations

    Event channels associated with a campaign can be read by all campaign
    users for the campaign time range.
    """

    __tablename__ = "event_channels_by_campaigns"
    __table_args__ = (sqla.UniqueConstraint("campaign_id", "event_channel_id"),)

    id = sqla.Column(sqla.Integer, primary_key=True)
    campaign_id = sqla.Column(sqla.ForeignKey("campaigns.id"))
    event_channel_id = sqla.Column(sqla.ForeignKey("event_channels.id"))

    @classmethod
    def register_class(cls):
        auth.register_class(
            cls,
            fields={
                "campaign": Relation(
                    kind="one",
                    other_type="Campaign",
                    my_field="campaign_id",
                    other_field="id",
                ),
            },
        )


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


@sqlaorm.declarative_mixin
class Event:
    """Abstract base class for event classes

    Channel and timestamp can't be changed after commit. There is no real use
    case for modifying these and it would screw up the auth layer as these are
    used by the authorization rules.
    """

    id = sqla.Column(sqla.Integer, primary_key=True, autoincrement=True, nullable=False)

    # Use getter/setter to prevent modifying channel after commit
    @sqlaorm.declared_attr
    def _channel_id(cls):
        return sqla.Column(
            sqla.Integer, sqla.ForeignKey("event_channels.id"), nullable=False
        )

    @hybrid_property
    def channel_id(self):
        return self._channel_id

    @channel_id.setter
    def channel_id(self, channel_id):
        if self.id is not None:
            raise AttributeError("channel_id cannot be modified")
        self._channel_id = channel_id

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
    def list_by_state(
        cls,
        states=(
            "NEW",
            "ONGOING",
        ),
        channel_id=None,
        category=None,
        source=None,
        level="ERROR",
    ):
        current_campaign = get_current_campaign()
        if current_campaign is None:
            raise BEMServerCoreMissingCampaignError
        if states is None or len(states) <= 0:
            raise ValueError("Missing `state` filter.")
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
        if current_campaign.start_time:
            stmt = stmt.filter(cls.timestamp >= current_campaign.start_time)
        if current_campaign.end_time:
            stmt = stmt.filter(cls.timestamp <= current_campaign.end_time)
        return db.session.execute(stmt).all()

    @classmethod
    def register_class(cls):
        auth.register_class(
            cls,
            fields={
                "channel": Relation(
                    kind="one",
                    other_type="EventChannel",
                    my_field="channel_id",
                    other_field="id",
                ),
            },
        )

    @classmethod
    def get(cls, **kwargs):
        current_campaign = get_current_campaign()
        if current_campaign is None:
            raise BEMServerCoreMissingCampaignError
        auth.authorize(get_current_user(), "read", current_campaign)
        query = super().get(**kwargs)
        if current_campaign.start_time:
            query = query.filter(cls.timestamp >= current_campaign.start_time)
        if current_campaign.end_time:
            query = query.filter(cls.timestamp <= current_campaign.end_time)
        return query

    @classmethod
    def new(cls, *args, timestamp, **kwargs):
        current_campaign = get_current_campaign()
        if current_campaign is None:
            raise BEMServerCoreMissingCampaignError
        current_campaign.auth_dates((timestamp,))
        return super().new(*args, timestamp=timestamp, **kwargs)

    @classmethod
    def get_by_id(cls, item_id, **kwargs):
        current_campaign = get_current_campaign()
        if current_campaign is None:
            raise BEMServerCoreMissingCampaignError
        return super().get_by_id(item_id, **kwargs)

    def update(self, **kwargs):
        current_campaign = get_current_campaign()
        if current_campaign is None:
            raise BEMServerCoreMissingCampaignError
        return super().update(**kwargs)

    def delete(self):
        current_campaign = get_current_campaign()
        if current_campaign is None:
            raise BEMServerCoreMissingCampaignError
        return super().delete()


class TimeseriesEvent(Event, AuthMixin, Base):
    __tablename__ = "timeseries_events"

    timeseries_ids = association_proxy(
        "timeseries",
        "timeseries_id",
        creator=lambda ts_id: TimeseriesEventByTimeseries(timeseries_id=ts_id),
    )


class TimeseriesEventByTimeseries(AuthMixin, Base):
    """TimeseriesEvent x Timeseries associations"""

    __tablename__ = "timeseries_events_by_timeseries"
    __table_args__ = (sqla.UniqueConstraint("timeseries_event_id", "timeseries_id"),)

    timeseries_event_id = sqla.Column(
        sqla.Integer,
        sqla.ForeignKey("timeseries_events.id", ondelete="CASCADE"),
        primary_key=True,
    )
    timeseries_event = sqla.orm.relationship(
        "TimeseriesEvent",
        backref=sqla.orm.backref(
            "timeseries", passive_deletes=True, cascade="all, delete-orphan"
        ),
    )
    timeseries_id = sqla.Column(
        sqla.Integer,
        sqla.ForeignKey("timeseries.id", ondelete="CASCADE"),
        primary_key=True,
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
        },
    )
    connection.execute(
        target.insert(),
        {
            "id": "observation_missing",
            "parent": "ABNORMAL_TIMESTAMPS",
            "description": "Observation timestamp is missing",
        },
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
        },
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
        },
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
        },
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
        },
    )
    connection.execute(
        target.insert(),
        {
            "id": "ABNORMAL_MEASURE_VALUES",
            "description": "Abnormal measure values in timeseries",
        },
    )
    connection.execute(
        target.insert(),
        {
            "id": "out_of_range",
            "parent": "ABNORMAL_MEASURE_VALUES",
            "description": "Measure value is out of range",
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
