"""Event"""

import datetime as dt
import enum
from functools import total_ordering

import sqlalchemy as sqla

from bemserver_core.authorization import AuthMixin, Relation, auth
from bemserver_core.celery import BEMServerCoreSystemTask, celery, logger
from bemserver_core.database import Base, db, make_columns_read_only
from bemserver_core.exceptions import (
    BEMServerCoreCampaignError,
    BEMServerCoreCampaignScopeError,
    BEMServerCoreTaskError,
)
from bemserver_core.model.campaigns import (
    Campaign,
    CampaignScope,
    UserGroupByCampaignScope,
)
from bemserver_core.model.sites import Building, Site, Space, Storey, Zone
from bemserver_core.model.users import User, UserByUserGroup, UserGroup


@total_ordering
class EventLevelEnum(enum.Enum):
    """Event levels enum"""

    DEBUG = 10
    INFO = 20
    WARNING = 30
    ERROR = 40
    CRITICAL = 50

    def __lt__(self, other):
        if self.__class__ is other.__class__:
            return self.value < other.value
        return NotImplemented


DEFAULT_NOTIFICATION_EVENT_LEVEL = EventLevelEnum.WARNING


class EventCategory(AuthMixin, Base):
    __tablename__ = "event_categs"

    id = sqla.Column(sqla.Integer, primary_key=True, autoincrement=True, nullable=False)
    name = sqla.Column(sqla.String(80), unique=True, nullable=False)
    description = sqla.Column(sqla.String(250))


class Event(AuthMixin, Base):
    __tablename__ = "events"

    id = sqla.Column(sqla.Integer, primary_key=True, autoincrement=True, nullable=False)
    campaign_scope_id = sqla.Column(sqla.ForeignKey("c_scopes.id"), nullable=False)
    category_id = sqla.Column(sqla.ForeignKey("event_categs.id"), nullable=False)
    timestamp = sqla.Column(sqla.DateTime(timezone=True), nullable=False)
    source = sqla.Column(sqla.String, nullable=False)
    description = sqla.Column(sqla.String())
    level = sqla.Column(sqla.Enum(EventLevelEnum), nullable=False)

    category = sqla.orm.relationship(
        "EventCategory",
        backref=sqla.orm.backref("events", cascade="all, delete-orphan"),
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

    @classmethod
    def get(
        cls,
        campaign_id=None,
        user_id=None,
        timeseries_id=None,
        site_id=None,
        recurse_site_id=None,
        building_id=None,
        recurse_building_id=None,
        storey_id=None,
        recurse_storey_id=None,
        space_id=None,
        zone_id=None,
        **kwargs,
    ):
        if "campaign_scope_id" in kwargs:
            CampaignScope.get_by_id(kwargs["campaign_scope_id"])
        query = super().get(**kwargs)
        if campaign_id is not None:
            Campaign.get_by_id(campaign_id)
            cs = sqla.orm.aliased(CampaignScope)
            query = query.join(cs, cls.campaign_scope_id == cs.id).filter(
                cs.campaign_id == campaign_id
            )
        if user_id is not None:
            User.get_by_id(user_id)
            cs = sqla.orm.aliased(CampaignScope)
            ubug = sqla.orm.aliased(UserByUserGroup)
            query = (
                query.join(cs, cls.campaign_scope_id == cs.id)
                .join(sqla.orm.aliased(UserGroupByCampaignScope))
                .join(sqla.orm.aliased(UserGroup))
                .join(ubug)
                .filter(ubug.user_id == user_id)
            )
        if timeseries_id is not None:
            from .timeseries import Timeseries  # noqa: avoid cyclic import

            Timeseries.get_by_id(timeseries_id)
            query = query.join(TimeseriesByEvent).filter(
                TimeseriesByEvent.timeseries_id == timeseries_id
            )
        if site_id is not None:
            if recurse_site_id is not None:
                raise ValueError(
                    "site_id and recurse_site_id are mutually exclusive arguments."
                )
            query = cls._filter_by_site(query, site_id)
        if recurse_site_id is not None:
            query = cls._filter_by_site(query, recurse_site_id, recurse=True)
        if building_id is not None:
            if recurse_building_id is not None:
                raise ValueError(
                    "building_id and recurse_building_id "
                    "are mutually exclusive arguments."
                )
            query = cls._filter_by_building(query, building_id)
        if recurse_building_id is not None:
            query = cls._filter_by_building(query, recurse_building_id, recurse=True)
        if storey_id is not None:
            if recurse_storey_id is not None:
                raise ValueError(
                    "storey_id and recurse_storey_id are mutually exclusive arguments."
                )
            query = cls._filter_by_storey(query, storey_id)
        if recurse_storey_id is not None:
            query = cls._filter_by_storey(query, recurse_storey_id, recurse=True)
        if space_id is not None:
            query = cls._filter_by_space(query, space_id)
        if zone_id is not None:
            query = cls._filter_by_zone(query, zone_id)
        return query

    @staticmethod
    def _filter_by_site(base_query, site_id, recurse=False):
        Site.get_by_id(site_id)

        query = base_query.join(EventBySite).join(Site).filter(Site.id == site_id)

        if recurse:
            site_alias = sqla.orm.aliased(Site)
            building_ts_q = (
                base_query.join(EventByBuilding)
                .join(Building)
                .join(site_alias, site_alias.id == Building.site_id)
                .filter(site_alias.id == site_id)
            )
            building_alias = sqla.orm.aliased(Building)
            site_alias = sqla.orm.aliased(Site)
            storey_ts_q = (
                base_query.join(EventByStorey)
                .join(Storey)
                .join(building_alias, building_alias.id == Storey.building_id)
                .join(site_alias, site_alias.id == building_alias.site_id)
                .filter(site_alias.id == site_id)
            )
            site_alias = sqla.orm.aliased(Site)
            building_alias = sqla.orm.aliased(Building)
            storey_alias = sqla.orm.aliased(Storey)
            space_ts_q = (
                base_query.join(EventBySpace)
                .join(Space)
                .join(storey_alias, storey_alias.id == Space.storey_id)
                .join(
                    building_alias,
                    building_alias.id == storey_alias.building_id,
                )
                .join(site_alias, site_alias.id == building_alias.site_id)
                .filter(site_alias.id == site_id)
            )
            query = query.union(building_ts_q).union(storey_ts_q).union(space_ts_q)

        return query

    @staticmethod
    def _filter_by_building(base_query, building_id, recurse=False):
        Building.get_by_id(building_id)

        query = (
            base_query.join(EventByBuilding)
            .join(Building)
            .filter(Building.id == building_id)
        )

        if recurse:
            building_alias = sqla.orm.aliased(Building)
            storey_ts_q = (
                base_query.join(EventByStorey)
                .join(Storey)
                .join(building_alias, building_alias.id == Storey.building_id)
                .filter(building_alias.id == building_id)
            )
            building_alias = sqla.orm.aliased(Building)
            storey_alias = sqla.orm.aliased(Storey)
            space_ts_q = (
                base_query.join(EventBySpace)
                .join(Space)
                .join(storey_alias, storey_alias.id == Space.storey_id)
                .join(
                    building_alias,
                    building_alias.id == storey_alias.building_id,
                )
                .filter(building_alias.id == building_id)
            )
            query = query.union(storey_ts_q).union(space_ts_q)

        return query

    @staticmethod
    def _filter_by_storey(base_query, storey_id, recurse=False):
        Storey.get_by_id(storey_id)

        query = (
            base_query.join(EventByStorey).join(Storey).filter(Storey.id == storey_id)
        )

        if recurse:
            storey_alias = sqla.orm.aliased(Storey)
            space_ts_q = (
                base_query.join(EventBySpace)
                .join(Space)
                .join(storey_alias, storey_alias.id == Space.storey_id)
                .filter(storey_alias.id == storey_id)
            )
            query = query.union(space_ts_q)

        return query

    @staticmethod
    def _filter_by_space(base_query, space_id):
        Space.get_by_id(space_id)
        query = base_query.join(EventBySpace).join(Space).filter(Space.id == space_id)
        return query

    @staticmethod
    def _filter_by_zone(base_query, zone_id):
        Zone.get_by_id(zone_id)
        query = base_query.join(EventByZone).join(Zone).filter(Zone.id == zone_id)
        return query

    def notify(self, timestamp):
        from bemserver_core.model.notifications import (
            Notification,
        )

        ecbu_q = sqla.orm.aliased(
            EventCategoryByUser,
            alias=EventCategoryByUser.get(category_id=self.category_id).subquery(),
        )

        query = (
            db.session.query(User)
            .join(UserByUserGroup)
            .join(UserGroup)
            .join(UserGroupByCampaignScope)
            .join(CampaignScope, CampaignScope.id == self.campaign_scope_id)
            .outerjoin(ecbu_q)
        )

        if self.level >= DEFAULT_NOTIFICATION_EVENT_LEVEL:
            level_filter = sqla.or_(
                ecbu_q.notification_level <= self.level,
                ecbu_q.notification_level.is_(None),
            )
        else:
            level_filter = ecbu_q.notification_level <= self.level

        query = query.filter(level_filter)

        for user in list(query):
            Notification.new(user=user, event=self, timestamp=timestamp)


@sqla.event.listens_for(Event, "after_insert")
def event_after_insert(_mapper, _connection, target):
    """Callback executed on insert event

    Notify event. Since the notification is created in a separate thread, we
    want to ensure the event is committed to database, so we register a callback
    to run on the next commit event of the session.

    https://stackoverflow.com/questions/25078815/
    """
    timestamp = dt.datetime.now(tz=dt.timezone.utc)

    @sqla.event.listens_for(db.session, "after_commit", once=True)
    def event_after_commit_after_insert(_session):
        """Callback executed on commit event after an insert"""
        # Ensure the event has not been deleted or rolled-back since flush
        if sqla.inspect(target).persistent:
            notify.delay(target.id, timestamp)


@celery.task(name="Notify", base=BEMServerCoreSystemTask)
def notify(event_id, timestamp):
    """Create notifications for an event with a given timestamp"""
    logger.info("Notify event %s", event_id)
    event = Event.get_by_id(event_id)
    if event is None:
        raise BEMServerCoreTaskError(f"Unknown event ID {event_id}")
    event.notify(timestamp)
    db.session.commit()


class EventCategoryByUser(AuthMixin, Base):
    """EventCategory x User associations"""

    __tablename__ = "event_categs_by_users"
    __table_args__ = (sqla.UniqueConstraint("user_id", "category_id"),)

    id = sqla.Column(sqla.Integer, primary_key=True)
    user_id = sqla.Column(sqla.ForeignKey("users.id"), nullable=False)
    category_id = sqla.Column(sqla.ForeignKey("event_categs.id"), nullable=False)
    notification_level = sqla.Column(sqla.Enum(EventLevelEnum), nullable=False)

    user = sqla.orm.relationship(
        "User",
        backref=sqla.orm.backref(
            "event_categories_by_users", cascade="all, delete-orphan"
        ),
    )
    category = sqla.orm.relationship(
        "EventCategory",
        backref=sqla.orm.backref(
            "event_categories_by_users", cascade="all, delete-orphan"
        ),
    )

    @classmethod
    def register_class(cls):
        auth.register_class(
            cls,
            fields={
                "user": Relation(
                    kind="one",
                    other_type="User",
                    my_field="user_id",
                    other_field="id",
                ),
                "event_category": Relation(
                    kind="one",
                    other_type="EventCategory",
                    my_field="category_id",
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
        backref=sqla.orm.backref("timeseries_by_events", cascade="all, delete-orphan"),
    )
    timeseries = sqla.orm.relationship(
        "Timeseries",
        backref=sqla.orm.backref("timeseries_by_events", cascade="all, delete-orphan"),
    )

    def _before_flush(self):
        # Ensure TS and Event are in same Campaign scope
        if self.timeseries_id and self.event_id:
            from .timeseries import Timeseries  # noqa: avoid cyclic import

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


class EventBySite(AuthMixin, Base):
    __tablename__ = "events_by_sites"
    __table_args__ = (sqla.UniqueConstraint("site_id", "event_id"),)

    id = sqla.Column(sqla.Integer, primary_key=True)
    event_id = sqla.Column(sqla.ForeignKey("events.id"), nullable=False)
    site_id = sqla.Column(sqla.ForeignKey("sites.id"), nullable=False)

    site = sqla.orm.relationship(
        "Site",
        backref=sqla.orm.backref("events_by_sites", cascade="all, delete-orphan"),
    )
    event = sqla.orm.relationship(
        "Event",
        backref=sqla.orm.backref("events_by_sites", cascade="all, delete-orphan"),
    )

    def _before_flush(self):
        # Ensure Site and Event are in same Campaign
        if self.site_id and self.event_id:
            site = Site.get_by_id(self.site_id)
            event = Event.get_by_id(self.event_id)
            if site.campaign != event.campaign_scope.campaign:
                raise BEMServerCoreCampaignError(
                    "Event and site must be in same campaign"
                )

    @classmethod
    def register_class(cls):
        auth.register_class(
            cls,
            fields={
                "event": Relation(
                    kind="one",
                    other_type="Event",
                    my_field="event_id",
                    other_field="id",
                ),
                "site": Relation(
                    kind="one",
                    other_type="Site",
                    my_field="site_id",
                    other_field="id",
                ),
            },
        )


class EventByBuilding(AuthMixin, Base):
    __tablename__ = "events_by_buildings"
    __table_args__ = (sqla.UniqueConstraint("building_id", "event_id"),)

    id = sqla.Column(sqla.Integer, primary_key=True)
    event_id = sqla.Column(sqla.ForeignKey("events.id"), nullable=False)
    building_id = sqla.Column(sqla.ForeignKey("buildings.id"), nullable=False)

    building = sqla.orm.relationship(
        "Building",
        backref=sqla.orm.backref("events_by_buildings", cascade="all, delete-orphan"),
    )
    event = sqla.orm.relationship(
        "Event",
        backref=sqla.orm.backref("events_by_buildings", cascade="all, delete-orphan"),
    )

    def _before_flush(self):
        # Ensure Building and Event are in same Campaign
        if self.building_id and self.event_id:
            building = Building.get_by_id(self.building_id)
            event = Event.get_by_id(self.event_id)
            if building.site.campaign != event.campaign_scope.campaign:
                raise BEMServerCoreCampaignError(
                    "Event and building must be in same campaign"
                )

    @classmethod
    def register_class(cls):
        auth.register_class(
            cls,
            fields={
                "event": Relation(
                    kind="one",
                    other_type="Event",
                    my_field="event_id",
                    other_field="id",
                ),
                "building": Relation(
                    kind="one",
                    other_type="Building",
                    my_field="building_id",
                    other_field="id",
                ),
            },
        )


class EventByStorey(AuthMixin, Base):
    __tablename__ = "events_by_storeys"
    __table_args__ = (sqla.UniqueConstraint("storey_id", "event_id"),)

    id = sqla.Column(sqla.Integer, primary_key=True)
    event_id = sqla.Column(sqla.ForeignKey("events.id"), nullable=False)
    storey_id = sqla.Column(sqla.ForeignKey("storeys.id"), nullable=False)

    storey = sqla.orm.relationship(
        "Storey",
        backref=sqla.orm.backref("events_by_storeys", cascade="all, delete-orphan"),
    )
    event = sqla.orm.relationship(
        "Event",
        backref=sqla.orm.backref("events_by_storeys", cascade="all, delete-orphan"),
    )

    def _before_flush(self):
        # Ensure Storey and Event are in same Campaign
        if self.storey_id and self.event_id:
            storey = Storey.get_by_id(self.storey_id)
            event = Event.get_by_id(self.event_id)
            if storey.building.site.campaign != event.campaign_scope.campaign:
                raise BEMServerCoreCampaignError(
                    "Event and storey must be in same campaign"
                )

    @classmethod
    def register_class(cls):
        auth.register_class(
            cls,
            fields={
                "event": Relation(
                    kind="one",
                    other_type="Event",
                    my_field="event_id",
                    other_field="id",
                ),
                "storey": Relation(
                    kind="one",
                    other_type="Storey",
                    my_field="storey_id",
                    other_field="id",
                ),
            },
        )


class EventBySpace(AuthMixin, Base):
    __tablename__ = "events_by_spaces"
    __table_args__ = (sqla.UniqueConstraint("space_id", "event_id"),)

    id = sqla.Column(sqla.Integer, primary_key=True)
    event_id = sqla.Column(sqla.ForeignKey("events.id"), nullable=False)
    space_id = sqla.Column(sqla.ForeignKey("spaces.id"), nullable=False)

    space = sqla.orm.relationship(
        "Space",
        backref=sqla.orm.backref("events_by_spaces", cascade="all, delete-orphan"),
    )
    event = sqla.orm.relationship(
        "Event",
        backref=sqla.orm.backref("events_by_spaces", cascade="all, delete-orphan"),
    )

    def _before_flush(self):
        # Ensure Space and Event are in same Campaign
        if self.space_id and self.event_id:
            space = Space.get_by_id(self.space_id)
            event = Event.get_by_id(self.event_id)
            if space.storey.building.site.campaign != event.campaign_scope.campaign:
                raise BEMServerCoreCampaignError(
                    "Event and space must be in same campaign"
                )

    @classmethod
    def register_class(cls):
        auth.register_class(
            cls,
            fields={
                "event": Relation(
                    kind="one",
                    other_type="Event",
                    my_field="event_id",
                    other_field="id",
                ),
                "space": Relation(
                    kind="one",
                    other_type="Space",
                    my_field="space_id",
                    other_field="id",
                ),
            },
        )


class EventByZone(AuthMixin, Base):
    __tablename__ = "events_by_zones"
    __table_args__ = (sqla.UniqueConstraint("zone_id", "event_id"),)

    id = sqla.Column(sqla.Integer, primary_key=True)
    event_id = sqla.Column(sqla.ForeignKey("events.id"), nullable=False)
    zone_id = sqla.Column(sqla.ForeignKey("zones.id"), nullable=False)

    zone = sqla.orm.relationship(
        "Zone",
        backref=sqla.orm.backref("events_by_zones", cascade="all, delete-orphan"),
    )
    event = sqla.orm.relationship(
        "Event",
        backref=sqla.orm.backref("events_by_zones", cascade="all, delete-orphan"),
    )

    def _before_flush(self):
        # Ensure Zone and Event are in same Campaign
        if self.zone_id and self.event_id:
            zone = Zone.get_by_id(self.zone_id)
            event = Event.get_by_id(self.event_id)
            if zone.campaign != event.campaign_scope.campaign:
                raise BEMServerCoreCampaignError(
                    "Event and zone must be in same campaign"
                )

    @classmethod
    def register_class(cls):
        auth.register_class(
            cls,
            fields={
                "event": Relation(
                    kind="one",
                    other_type="Event",
                    my_field="event_id",
                    other_field="id",
                ),
                "zone": Relation(
                    kind="one",
                    other_type="Zone",
                    my_field="zone_id",
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
        EventCategoryByUser.user_id,
    )


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
            EventCategory(name="No data outliers"),
        ]
    )
