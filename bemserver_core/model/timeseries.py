"""Timeseries"""
import sqlalchemy as sqla

from bemserver_core.database import Base, db, make_columns_read_only
from bemserver_core.authorization import AuthMixin, auth, Relation
from bemserver_core.model.users import User, UserGroup, UserByUserGroup
from bemserver_core.model.campaigns import (
    Campaign,
    CampaignScope,
    UserGroupByCampaignScope,
)
from bemserver_core.model.sites import Site, Building, Storey, Space, Zone
from bemserver_core.common import PropertyType
from bemserver_core.exceptions import TimeseriesNotFoundError


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
    unit_symbol = sqla.Column(sqla.String(20))
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

    @classmethod
    def get(
        cls,
        *,
        user_id=None,
        site_id=None,
        building_id=None,
        storey_id=None,
        space_id=None,
        zone_id=None,
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
            Site.get_by_id(site_id)
            site = sqla.orm.aliased(Site)
            building = sqla.orm.aliased(Building)
            site_building = sqla.orm.aliased(Site)
            storey = sqla.orm.aliased(Storey)
            building_storey = sqla.orm.aliased(Building)
            site_storey = sqla.orm.aliased(Site)
            space = sqla.orm.aliased(Space)
            site_space = sqla.orm.aliased(Site)
            building_space = sqla.orm.aliased(Building)
            storey_space = sqla.orm.aliased(Storey)
            query = (
                query.join(TimeseriesBySite)
                .join(site)
                .filter(site.id == site_id)
                .union(
                    query.join(TimeseriesByBuilding)
                    .join(building)
                    .join(site_building, site_building.id == building.site_id)
                    .filter(site_building.id == site_id)
                    .union(
                        query.join(TimeseriesByStorey)
                        .join(storey)
                        .join(building_storey, building_storey.id == storey.building_id)
                        .join(site_storey, site_storey.id == building_storey.site_id)
                        .filter(site_storey.id == site_id)
                        .union(
                            query.join(TimeseriesBySpace)
                            .join(space)
                            .join(storey_space, storey_space.id == space.storey_id)
                            .join(
                                building_space,
                                building_space.id == storey_space.building_id,
                            )
                            .join(site_space, site_space.id == building_space.site_id)
                            .filter(site_space.id == site_id)
                        )
                    )
                )
            )
        if building_id is not None:
            Building.get_by_id(building_id)
            building = sqla.orm.aliased(Building)
            storey = sqla.orm.aliased(Storey)
            building_storey = sqla.orm.aliased(Building)
            space = sqla.orm.aliased(Space)
            building_space = sqla.orm.aliased(Building)
            storey_space = sqla.orm.aliased(Storey)
            query = (
                query.join(TimeseriesByBuilding)
                .join(building)
                .filter(building.id == building_id)
                .union(
                    query.join(TimeseriesByStorey)
                    .join(storey)
                    .join(building_storey, building_storey.id == storey.building_id)
                    .filter(building_storey.id == storey_id)
                    .union(
                        query.join(TimeseriesBySpace)
                        .join(space)
                        .join(storey_space, storey_space.id == space.storey_id)
                        .join(
                            building_space,
                            building_space.id == storey_space.building_id,
                        )
                        .filter(building_space.id == building_id)
                    )
                )
            )
        if storey_id is not None:
            Storey.get_by_id(storey_id)
            storey = sqla.orm.aliased(Storey)
            space = sqla.orm.aliased(Space)
            storey_space = sqla.orm.aliased(Storey)
            query = (
                query.join(TimeseriesByStorey)
                .join(storey)
                .filter(storey.id == storey_id)
                .union(
                    query.join(TimeseriesBySpace)
                    .join(space)
                    .join(storey_space, storey_space.id == space.storey_id)
                    .filter(storey_space.id == storey_id)
                )
            )
        if space_id is not None:
            Space.get_by_id(space_id)
            space = sqla.orm.aliased(Space)
            query = (
                query.join(TimeseriesBySpace).join(space).filter(space.id == space_id)
            )
        if zone_id is not None:
            Zone.get_by_id(zone_id)
            zone = sqla.orm.aliased(Zone)
            query = query.join(TimeseriesByZone).join(zone).filter(zone.id == zone_id)
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
        backref=sqla.orm.backref("timeseries_by_site", cascade="all, delete-orphan"),
    )
    timeseries = sqla.orm.relationship(
        "Timeseries",
        backref=sqla.orm.backref("timeseries_by_site", cascade="all, delete-orphan"),
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
            "timeseries_by_building", cascade="all, delete-orphan"
        ),
    )
    timeseries = sqla.orm.relationship(
        "Timeseries",
        backref=sqla.orm.backref(
            "timeseries_by_building", cascade="all, delete-orphan"
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
        backref=sqla.orm.backref("timeseries_by_storey", cascade="all, delete-orphan"),
    )
    timeseries = sqla.orm.relationship(
        "Timeseries",
        backref=sqla.orm.backref("timeseries_by_storey", cascade="all, delete-orphan"),
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
        backref=sqla.orm.backref("timeseries_by_space", cascade="all, delete-orphan"),
    )
    timeseries = sqla.orm.relationship(
        "Timeseries",
        backref=sqla.orm.backref("timeseries_by_space", cascade="all, delete-orphan"),
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
    __tablename__ = "timeseries_by_zones"
    __table_args__ = (sqla.UniqueConstraint("zone_id", "timeseries_id"),)

    id = sqla.Column(sqla.Integer, primary_key=True)
    timeseries_id = sqla.Column(sqla.ForeignKey("timeseries.id"), nullable=False)
    zone_id = sqla.Column(sqla.ForeignKey("zones.id"), nullable=False)

    zone = sqla.orm.relationship(
        "Zone",
        backref=sqla.orm.backref("timeseries_by_zone", cascade="all, delete-orphan"),
    )
    timeseries = sqla.orm.relationship(
        "Timeseries",
        backref=sqla.orm.backref("timeseries_by_zone", cascade="all, delete-orphan"),
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
    db.session.commit()


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
    db.session.commit()
