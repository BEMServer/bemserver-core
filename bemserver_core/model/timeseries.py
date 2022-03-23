"""Timeseries"""
import sqlalchemy as sqla
import sqlalchemy.orm as sqlaorm
from sqlalchemy.ext.hybrid import hybrid_property

from bemserver_core.database import Base
from bemserver_core.authorization import AuthMixin, auth, Relation
from bemserver_core.model.users import User, UserGroup, UserByUserGroup
from bemserver_core.model.campaigns import (
    Campaign,
    CampaignScope,
    UserGroupByCampaignScope,
)
from bemserver_core.model.sites import Site, Building, Storey, Space, Zone


class TimeseriesProperty(AuthMixin, Base):
    __tablename__ = "timeseries_properties"

    id = sqla.Column(sqla.Integer, primary_key=True)
    name = sqla.Column(sqla.String(80), unique=True, nullable=False)
    description = sqla.Column(sqla.String(250))


class TimeseriesDataState(AuthMixin, Base):
    __tablename__ = "timeseries_data_states"

    id = sqla.Column(sqla.Integer, primary_key=True)
    name = sqla.Column(sqla.String(80), unique=True, nullable=False)


class Timeseries(AuthMixin, Base):
    __tablename__ = "timeseries"
    __table_args__ = (sqla.UniqueConstraint("name", "_campaign_id"),)

    id = sqla.Column(sqla.Integer, primary_key=True)
    name = sqla.Column(sqla.String(80), nullable=False)
    description = sqla.Column(sqla.String(500))
    unit_symbol = sqla.Column(sqla.String(20))
    campaign = sqla.orm.relationship("Campaign")
    campaign_scope = sqla.orm.relationship("CampaignScope")
    timeseries_by_data_states = sqla.orm.relationship("TimeseriesByDataState")

    # Use getter/setter to prevent modifying campaign / campaign_scope after commit
    @sqlaorm.declared_attr
    def _campaign_id(cls):
        return sqla.Column(
            sqla.Integer, sqla.ForeignKey("campaigns.id"), nullable=False
        )

    @hybrid_property
    def campaign_id(self):
        return self._campaign_id

    @campaign_id.setter
    def campaign_id(self, campaign_id):
        if self.id is not None:
            raise AttributeError("campaign_id cannot be modified")
        self._campaign_id = campaign_id

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
        **kwargs
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

    def get_timeseries_by_data_states(self, data_state):
        return next(
            (
                ts
                for ts in self.timeseries_by_data_states
                if ts.data_state == data_state
            ),
            None,
        )


class TimeseriesPropertyData(AuthMixin, Base):
    """Timeseries property data"""

    __tablename__ = "timeseries_property_data"
    __table_args__ = (sqla.UniqueConstraint("timeseries_id", "property_id"),)

    id = sqla.Column(sqla.Integer, primary_key=True)
    timeseries_id = sqla.Column(sqla.ForeignKey("timeseries.id"), nullable=False)
    property_id = sqla.Column(
        sqla.ForeignKey("timeseries_properties.id"), nullable=False
    )
    value = sqla.Column(sqla.Float)

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


class TimeseriesByDataState(AuthMixin, Base):
    __tablename__ = "timeseries_by_data_states"
    __table_args__ = (sqla.UniqueConstraint("timeseries_id", "data_state_id"),)

    id = sqla.Column(sqla.Integer, primary_key=True)
    timeseries_id = sqla.Column(sqla.ForeignKey("timeseries.id"), nullable=False)
    timeseries = sqla.orm.relationship(
        "Timeseries", back_populates="timeseries_by_data_states"
    )
    data_state_id = sqla.Column(
        sqla.ForeignKey("timeseries_data_states.id"), nullable=False
    )
    data_state = sqla.orm.relationship("TimeseriesDataState")

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
    __tablename__ = "timeseries_by_sites"
    __table_args__ = (sqla.UniqueConstraint("timeseries_id", "site_id"),)

    id = sqla.Column(sqla.Integer, primary_key=True)
    timeseries_id = sqla.Column(sqla.ForeignKey("timeseries.id"), nullable=False)
    site_id = sqla.Column(sqla.ForeignKey("sites.id"), nullable=False)

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
    __tablename__ = "timeseries_by_buildings"
    __table_args__ = (sqla.UniqueConstraint("timeseries_id", "building_id"),)

    id = sqla.Column(sqla.Integer, primary_key=True)
    timeseries_id = sqla.Column(sqla.ForeignKey("timeseries.id"), nullable=False)
    building_id = sqla.Column(sqla.ForeignKey("buildings.id"), nullable=False)

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
    __tablename__ = "timeseries_by_storeys"
    __table_args__ = (sqla.UniqueConstraint("timeseries_id", "storey_id"),)

    id = sqla.Column(sqla.Integer, primary_key=True)
    timeseries_id = sqla.Column(sqla.ForeignKey("timeseries.id"), nullable=False)
    storey_id = sqla.Column(sqla.ForeignKey("storeys.id"), nullable=False)

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
    __tablename__ = "timeseries_by_spaces"
    __table_args__ = (sqla.UniqueConstraint("timeseries_id", "space_id"),)

    id = sqla.Column(sqla.Integer, primary_key=True)
    timeseries_id = sqla.Column(sqla.ForeignKey("timeseries.id"), nullable=False)
    space_id = sqla.Column(sqla.ForeignKey("spaces.id"), nullable=False)

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
    __table_args__ = (sqla.UniqueConstraint("timeseries_id", "zone_id"),)

    id = sqla.Column(sqla.Integer, primary_key=True)
    timeseries_id = sqla.Column(sqla.ForeignKey("timeseries.id"), nullable=False)
    zone_id = sqla.Column(sqla.ForeignKey("zones.id"), nullable=False)

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


@sqla.event.listens_for(TimeseriesDataState.__table__, "after_create")
def _insert_initial_timeseries_data_states(target, connection, **kwargs):
    connection.execute(target.insert(), {"name": "Raw"})
    connection.execute(target.insert(), {"name": "Clean"})
