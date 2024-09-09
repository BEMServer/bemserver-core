"""Timeseries"""

import sqlalchemy as sqla

from bemserver_core.authorization import AuthMixin, Relation, auth
from bemserver_core.common import PropertyType, ureg
from bemserver_core.database import Base, db, make_columns_read_only
from bemserver_core.exceptions import TimeseriesNotFoundError
from bemserver_core.model.campaigns import (
    Campaign,
    CampaignScope,
    UserGroupByCampaignScope,
)
from bemserver_core.model.events import Event, TimeseriesByEvent
from bemserver_core.model.sites import Building, Site, Space, Storey, Zone
from bemserver_core.model.users import User, UserByUserGroup, UserGroup


class TimeseriesProperty(AuthMixin, Base):
    __tablename__ = "ts_props"

    id = sqla.Column(sqla.Integer, primary_key=True)
    name = sqla.Column(sqla.String(80), unique=True, nullable=False)
    description = sqla.Column(sqla.String(250))
    value_type = sqla.Column(
        sqla.Enum(PropertyType),
        default=PropertyType.string,
        nullable=False,
    )
    unit_symbol = sqla.Column(sqla.String(20))


class TimeseriesDataState(AuthMixin, Base):
    __tablename__ = "ts_data_states"

    id = sqla.Column(sqla.Integer, primary_key=True)
    name = sqla.Column(sqla.String(80), unique=True, nullable=False)


class Timeseries(AuthMixin, Base):
    __tablename__ = "timeseries"
    __table_args__ = (sqla.UniqueConstraint("campaign_id", "name"),)

    id = sqla.Column(sqla.Integer, primary_key=True)
    name = sqla.Column(sqla.String(80), nullable=False)
    description = sqla.Column(sqla.String(500))
    unit_symbol = sqla.Column(sqla.String(20), default="", nullable=False)
    campaign_id = sqla.Column(sqla.ForeignKey("campaigns.id"), nullable=False)
    campaign_scope_id = sqla.Column(sqla.ForeignKey("c_scopes.id"), nullable=False)

    campaign = sqla.orm.relationship(
        "Campaign", backref=sqla.orm.backref("timeseries", cascade="all, delete-orphan")
    )
    campaign_scope = sqla.orm.relationship(
        "CampaignScope",
        backref=sqla.orm.backref("timeseries", cascade="all, delete-orphan"),
    )

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
                "campaign_scope": Relation(
                    kind="one",
                    other_type="CampaignScope",
                    my_field="campaign_scope_id",
                    other_field="id",
                ),
            },
        )

    def _before_flush(self):
        if self.unit_symbol is not None:
            ureg.validate_unit(self.unit_symbol)

    @classmethod
    def get(
        cls,
        user_id=None,
        site_id=None,
        recurse_site_id=None,
        building_id=None,
        recurse_building_id=None,
        storey_id=None,
        recurse_storey_id=None,
        space_id=None,
        zone_id=None,
        event_id=None,
        properties=None,
        **kwargs,
    ):
        if "campaign_id" in kwargs:
            Campaign.get_by_id(kwargs["campaign_id"])
        if "campaign_scope_id" in kwargs:
            CampaignScope.get_by_id(kwargs["campaign_scope_id"])
        query = super().get(**kwargs)
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
        if event_id is not None:
            query = cls._filter_by_event(query, event_id)
        if properties is not None:
            query = cls._filter_by_properties(query, properties)
        return query

    @staticmethod
    def _filter_by_site(base_query, site_id, recurse=False):
        Site.get_by_id(site_id)

        query = base_query.join(TimeseriesBySite).join(Site).filter(Site.id == site_id)

        if recurse:
            site_alias = sqla.orm.aliased(Site)
            building_ts_q = (
                base_query.join(TimeseriesByBuilding)
                .join(Building)
                .join(site_alias, site_alias.id == Building.site_id)
                .filter(site_alias.id == site_id)
            )
            building_alias = sqla.orm.aliased(Building)
            site_alias = sqla.orm.aliased(Site)
            storey_ts_q = (
                base_query.join(TimeseriesByStorey)
                .join(Storey)
                .join(building_alias, building_alias.id == Storey.building_id)
                .join(site_alias, site_alias.id == building_alias.site_id)
                .filter(site_alias.id == site_id)
            )
            site_alias = sqla.orm.aliased(Site)
            building_alias = sqla.orm.aliased(Building)
            storey_alias = sqla.orm.aliased(Storey)
            space_ts_q = (
                base_query.join(TimeseriesBySpace)
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
            base_query.join(TimeseriesByBuilding)
            .join(Building)
            .filter(Building.id == building_id)
        )

        if recurse:
            building_alias = sqla.orm.aliased(Building)
            storey_ts_q = (
                base_query.join(TimeseriesByStorey)
                .join(Storey)
                .join(building_alias, building_alias.id == Storey.building_id)
                .filter(building_alias.id == building_id)
            )
            building_alias = sqla.orm.aliased(Building)
            storey_alias = sqla.orm.aliased(Storey)
            space_ts_q = (
                base_query.join(TimeseriesBySpace)
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
            base_query.join(TimeseriesByStorey)
            .join(Storey)
            .filter(Storey.id == storey_id)
        )

        if recurse:
            storey_alias = sqla.orm.aliased(Storey)
            space_ts_q = (
                base_query.join(TimeseriesBySpace)
                .join(Space)
                .join(storey_alias, storey_alias.id == Space.storey_id)
                .filter(storey_alias.id == storey_id)
            )
            query = query.union(space_ts_q)

        return query

    @staticmethod
    def _filter_by_space(base_query, space_id):
        Space.get_by_id(space_id)
        query = (
            base_query.join(TimeseriesBySpace).join(Space).filter(Space.id == space_id)
        )
        return query

    @staticmethod
    def _filter_by_zone(base_query, zone_id):
        Zone.get_by_id(zone_id)
        query = base_query.join(TimeseriesByZone).join(Zone).filter(Zone.id == zone_id)
        return query

    @staticmethod
    def _filter_by_event(base_query, event_id):
        Event.get_by_id(event_id)
        query = (
            base_query.join(TimeseriesByEvent).join(Event).filter(Event.id == event_id)
        )
        return query

    @staticmethod
    def _filter_by_properties(query, properties):
        for prop_name, value in properties.items():
            tspd = sqla.orm.aliased(TimeseriesPropertyData)
            tsp = sqla.orm.aliased(TimeseriesProperty)
            query = query.join(
                tspd, (tspd.timeseries_id == Timeseries.id) & (tspd.value == value)
            ).join(tsp, (tspd.property_id == tsp.id) & (tsp.name == prop_name))
        return query

    def get_timeseries_by_data_state(self, data_state):
        """Return timeseries x data state association for a given data state

        Create the timeseries x data state association on the fly if needed
        """
        tsbds = TimeseriesByDataState.get(
            timeseries=self,
            data_state=data_state,
        ).first()
        if tsbds is None:
            # Create tsbds on the fly if needed
            tsbds = TimeseriesByDataState.new(
                timeseries_id=self.id,
                data_state_id=data_state.id,
            )
            # Flush to allow the use of tsbds.id right away
            db.session.flush()
        return tsbds

    @classmethod
    def get_by_name(cls, campaign, name):
        """Get timeseries by name for a given campaign

        :param Campaign campaign: Timeseries campaign
        :param str name: Timeseries name
        """
        return Timeseries.get(name=name, campaign_id=campaign.id).first()

    @classmethod
    def get_many_by_id(cls, timeseries):
        """Get a list of timeseries by ID

        :param list timeseries: List of timeseries IDs
        """
        ts_d = {ts_id: Timeseries.get_by_id(ts_id) for ts_id in timeseries}
        if None in ts_d.values():
            raise TimeseriesNotFoundError(
                f"Unknown timeseries: {[k for k in ts_d.keys() if ts_d[k] is None]}"
            )
        return list(ts_d.values())

    @classmethod
    def get_many_by_name(cls, campaign, timeseries):
        """Get a list of timeseries by name for a given campaign

        :param list timeseries: List of timeseries names
        :param Campaign campaign: Campaign
        """
        ts_d = {ts_id: Timeseries.get_by_name(campaign, ts_id) for ts_id in timeseries}
        if None in ts_d.values():
            raise TimeseriesNotFoundError(
                f"Unknown timeseries: {[k for k in ts_d.keys() if ts_d[k] is None]}"
            )
        return list(ts_d.values())

    def get_property_value(self, property_name):
        """Get propery value for a given property name

        :param str property_name: Property name

        Returns the value property cast to the property type
        """
        stmt = (
            sqla.select(TimeseriesPropertyData, TimeseriesProperty)
            .join(
                TimeseriesProperty,
                TimeseriesPropertyData.property_id == TimeseriesProperty.id,
            )
            .filter(TimeseriesPropertyData.timeseries_id == self.id)
            .filter(TimeseriesProperty.name == property_name)
        )
        ret = db.session.execute(stmt).first()

        # If none found, return None
        if ret is None:
            return None

        # Value is stored in DB as string, cast to property type
        prop_data, se_property = ret
        return se_property.value_type.value(prop_data.value)

    @classmethod
    def get_property_for_many_timeseries(cls, timeseries, prop_name):
        """Get property by name for a list of timeseries

        :param list timeseries: List of timeseries IDs
        """
        subq = (
            sqla.select(TimeseriesPropertyData)
            .join(TimeseriesProperty)
            .filter(TimeseriesProperty.name == prop_name)
        ).subquery()
        stmt = (
            sqla.select(Timeseries.id, subq.c.value)
            .outerjoin(subq)
            .filter(Timeseries.id.in_(timeseries))
        )
        # Thanks to the outer join, the query produces a list of of (TS.id, prop) tuples
        # where prop is None if not defined
        # prop list is of the form [(id_1, "value"), (id_2, None), ..., (id_N, "value")]
        # prop dict is of the form {id_1: "value", id_2: None, ..., id_N: "value"}
        return dict(list(db.session.execute(stmt)))


class TimeseriesPropertyData(AuthMixin, Base):
    """Timeseries property data"""

    __tablename__ = "ts_prop_data"
    __table_args__ = (sqla.UniqueConstraint("timeseries_id", "property_id"),)

    id = sqla.Column(sqla.Integer, primary_key=True)
    timeseries_id = sqla.Column(sqla.ForeignKey("timeseries.id"), nullable=False)
    property_id = sqla.Column(sqla.ForeignKey("ts_props.id"), nullable=False)
    value = sqla.Column(sqla.String(100), nullable=False)

    timeseries = sqla.orm.relationship(
        "Timeseries",
        backref=sqla.orm.backref(
            "timeseries_property_data", cascade="all, delete-orphan"
        ),
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
            },
        )

    def _before_flush(self):
        # Get property type and try to parse value to ensure its type validity.
        if (prop := TimeseriesProperty.get_by_id(self.property_id)) is not None:
            prop.value_type.verify(self.value)


class TimeseriesByDataState(AuthMixin, Base):
    __tablename__ = "ts_by_data_states"
    __table_args__ = (sqla.UniqueConstraint("timeseries_id", "data_state_id"),)

    id = sqla.Column(sqla.Integer, primary_key=True)
    timeseries_id = sqla.Column(sqla.ForeignKey("timeseries.id"), nullable=False)
    data_state_id = sqla.Column(sqla.ForeignKey("ts_data_states.id"), nullable=False)

    timeseries = sqla.orm.relationship(
        "Timeseries",
        backref=sqla.orm.backref(
            "timeseries_by_data_states", cascade="all, delete-orphan"
        ),
    )
    data_state = sqla.orm.relationship(
        "TimeseriesDataState",
        backref=sqla.orm.backref(
            "timeseries_by_data_states", cascade="all, delete-orphan"
        ),
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
            },
        )


class TimeseriesBySite(AuthMixin, Base):
    __tablename__ = "ts_by_sites"
    __table_args__ = (sqla.UniqueConstraint("site_id", "timeseries_id"),)

    id = sqla.Column(sqla.Integer, primary_key=True)
    timeseries_id = sqla.Column(sqla.ForeignKey("timeseries.id"), nullable=False)
    site_id = sqla.Column(sqla.ForeignKey("sites.id"), nullable=False)

    site = sqla.orm.relationship(
        "Site",
        backref=sqla.orm.backref("timeseries_by_sites", cascade="all, delete-orphan"),
    )
    timeseries = sqla.orm.relationship(
        "Timeseries",
        backref=sqla.orm.backref("timeseries_by_sites", cascade="all, delete-orphan"),
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
                "site": Relation(
                    kind="one",
                    other_type="Site",
                    my_field="site_id",
                    other_field="id",
                ),
            },
        )


class TimeseriesByBuilding(AuthMixin, Base):
    __tablename__ = "ts_by_buildings"
    __table_args__ = (sqla.UniqueConstraint("building_id", "timeseries_id"),)

    id = sqla.Column(sqla.Integer, primary_key=True)
    timeseries_id = sqla.Column(sqla.ForeignKey("timeseries.id"), nullable=False)
    building_id = sqla.Column(sqla.ForeignKey("buildings.id"), nullable=False)

    building = sqla.orm.relationship(
        "Building",
        backref=sqla.orm.backref(
            "timeseries_by_buildings", cascade="all, delete-orphan"
        ),
    )
    timeseries = sqla.orm.relationship(
        "Timeseries",
        backref=sqla.orm.backref(
            "timeseries_by_buildings", cascade="all, delete-orphan"
        ),
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
                "building": Relation(
                    kind="one",
                    other_type="Building",
                    my_field="building_id",
                    other_field="id",
                ),
            },
        )


class TimeseriesByStorey(AuthMixin, Base):
    __tablename__ = "ts_by_storeys"
    __table_args__ = (sqla.UniqueConstraint("storey_id", "timeseries_id"),)

    id = sqla.Column(sqla.Integer, primary_key=True)
    timeseries_id = sqla.Column(sqla.ForeignKey("timeseries.id"), nullable=False)
    storey_id = sqla.Column(sqla.ForeignKey("storeys.id"), nullable=False)

    storey = sqla.orm.relationship(
        "Storey",
        backref=sqla.orm.backref("timeseries_by_storeys", cascade="all, delete-orphan"),
    )
    timeseries = sqla.orm.relationship(
        "Timeseries",
        backref=sqla.orm.backref("timeseries_by_storeys", cascade="all, delete-orphan"),
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
                "storey": Relation(
                    kind="one",
                    other_type="Storey",
                    my_field="storey_id",
                    other_field="id",
                ),
            },
        )


class TimeseriesBySpace(AuthMixin, Base):
    __tablename__ = "ts_by_spaces"
    __table_args__ = (sqla.UniqueConstraint("space_id", "timeseries_id"),)

    id = sqla.Column(sqla.Integer, primary_key=True)
    timeseries_id = sqla.Column(sqla.ForeignKey("timeseries.id"), nullable=False)
    space_id = sqla.Column(sqla.ForeignKey("spaces.id"), nullable=False)

    space = sqla.orm.relationship(
        "Space",
        backref=sqla.orm.backref("timeseries_by_spaces", cascade="all, delete-orphan"),
    )
    timeseries = sqla.orm.relationship(
        "Timeseries",
        backref=sqla.orm.backref("timeseries_by_spaces", cascade="all, delete-orphan"),
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
                "space": Relation(
                    kind="one",
                    other_type="Space",
                    my_field="space_id",
                    other_field="id",
                ),
            },
        )


class TimeseriesByZone(AuthMixin, Base):
    __tablename__ = "ts_by_zones"
    __table_args__ = (sqla.UniqueConstraint("zone_id", "timeseries_id"),)

    id = sqla.Column(sqla.Integer, primary_key=True)
    timeseries_id = sqla.Column(sqla.ForeignKey("timeseries.id"), nullable=False)
    zone_id = sqla.Column(sqla.ForeignKey("zones.id"), nullable=False)

    zone = sqla.orm.relationship(
        "Zone",
        backref=sqla.orm.backref("timeseries_by_zones", cascade="all, delete-orphan"),
    )
    timeseries = sqla.orm.relationship(
        "Timeseries",
        backref=sqla.orm.backref("timeseries_by_zones", cascade="all, delete-orphan"),
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
                "zone": Relation(
                    kind="one",
                    other_type="Zone",
                    my_field="zone_id",
                    other_field="id",
                ),
            },
        )


def init_db_timeseries_triggers():
    """Create triggers to protect some columns from update.

    This function is meant to be used for tests or dev setups after create_all.
    Production setups should rely on migration scripts.
    """
    make_columns_read_only(
        Timeseries.campaign_id,
        Timeseries.campaign_scope_id,
        TimeseriesProperty.value_type,
        TimeseriesPropertyData.timeseries_id,
        TimeseriesPropertyData.property_id,
    )


def init_db_timeseries():
    """Create default timeseries data states

    This function is meant to be used for tests or dev setups after create_all.
    Production setups should rely on migration scripts.
    """
    db.session.add_all(
        [
            TimeseriesProperty(
                name="Min",
                description="Minimum expected value",
                value_type=PropertyType.float,
            ),
            TimeseriesProperty(
                name="Max",
                description="Maximum expected value",
                value_type=PropertyType.float,
            ),
            TimeseriesProperty(
                name="Interval",
                description="Expected interval",
                value_type=PropertyType.float,
                unit_symbol="s",
            ),
            TimeseriesDataState(name="Raw"),
            TimeseriesDataState(name="Clean"),
        ]
    )
