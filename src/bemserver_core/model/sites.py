"""Sites"""

import sqlalchemy as sqla

from bemserver_core.authorization import AuthMgrMixin, auth_mgr
from bemserver_core.common import PropertyType
from bemserver_core.database import Base, db, make_columns_read_only

from .campaigns import Campaign
from .users import User


class StructuralElementProperty(AuthMgrMixin, Base):
    __tablename__ = "struct_elem_props"

    id = sqla.Column(sqla.Integer, primary_key=True)
    name = sqla.Column(sqla.String(80), unique=True, nullable=False)
    description = sqla.Column(sqla.String(500))
    value_type = sqla.Column(
        sqla.Enum(PropertyType),
        default=PropertyType.string,
        nullable=False,
    )
    unit_symbol = sqla.Column(sqla.String(20))

    def authorize_read(self, actor):
        return True


class SiteProperty(AuthMgrMixin, Base):
    __tablename__ = "site_props"

    id = sqla.Column(sqla.Integer, primary_key=True)
    structural_element_property_id = sqla.Column(
        "struct_elem_prop_id",
        sqla.ForeignKey("struct_elem_props.id"),
        unique=True,
        nullable=False,
    )
    structural_element_property = sqla.orm.relationship(
        "StructuralElementProperty",
        backref=sqla.orm.backref("site_properties", cascade="all, delete-orphan"),
    )

    def authorize_read(self, actor):
        return True


class BuildingProperty(AuthMgrMixin, Base):
    __tablename__ = "building_props"

    id = sqla.Column(sqla.Integer, primary_key=True)
    structural_element_property_id = sqla.Column(
        "struct_elem_prop_id",
        sqla.ForeignKey("struct_elem_props.id"),
        unique=True,
        nullable=False,
    )
    structural_element_property = sqla.orm.relationship(
        "StructuralElementProperty",
        backref=sqla.orm.backref("building_properties", cascade="all, delete-orphan"),
    )

    def authorize_read(self, actor):
        return True


class StoreyProperty(AuthMgrMixin, Base):
    __tablename__ = "storey_props"

    id = sqla.Column(sqla.Integer, primary_key=True)
    structural_element_property_id = sqla.Column(
        "struct_elem_prop_id",
        sqla.ForeignKey("struct_elem_props.id"),
        unique=True,
        nullable=False,
    )
    structural_element_property = sqla.orm.relationship(
        "StructuralElementProperty",
        backref=sqla.orm.backref("storey_properties", cascade="all, delete-orphan"),
    )

    def authorize_read(self, actor):
        return True


class SpaceProperty(AuthMgrMixin, Base):
    __tablename__ = "space_props"

    id = sqla.Column(sqla.Integer, primary_key=True)
    structural_element_property_id = sqla.Column(
        "struct_elem_prop_id",
        sqla.ForeignKey("struct_elem_props.id"),
        unique=True,
        nullable=False,
    )
    structural_element_property = sqla.orm.relationship(
        "StructuralElementProperty",
        backref=sqla.orm.backref("space_properties", cascade="all, delete-orphan"),
    )

    def authorize_read(self, actor):
        return True


class ZoneProperty(AuthMgrMixin, Base):
    __tablename__ = "zone_props"

    id = sqla.Column(sqla.Integer, primary_key=True)
    structural_element_property_id = sqla.Column(
        "struct_elem_prop_id",
        sqla.ForeignKey("struct_elem_props.id"),
        unique=True,
        nullable=False,
    )
    structural_element_property = sqla.orm.relationship(
        "StructuralElementProperty",
        backref=sqla.orm.backref("zone_properties", cascade="all, delete-orphan"),
    )

    def authorize_read(self, actor):
        return True


class SitePropertyData(AuthMgrMixin, Base):
    __tablename__ = "site_prop_data"
    __table_args__ = (sqla.UniqueConstraint("site_id", "site_prop_id"),)

    id = sqla.Column(sqla.Integer, primary_key=True)
    site_id = sqla.Column(sqla.ForeignKey("sites.id"), nullable=False)
    site_property_id = sqla.Column(
        "site_prop_id", sqla.ForeignKey("site_props.id"), nullable=False
    )
    value = sqla.Column(sqla.String(100), nullable=False)

    site = sqla.orm.relationship(
        "Site",
        backref=sqla.orm.backref("site_property_data", cascade="all, delete-orphan"),
    )
    site_property = sqla.orm.relationship(
        "SiteProperty",
        backref=sqla.orm.backref("site_property_data", cascade="all, delete-orphan"),
    )

    def _before_flush(self):
        # Get property type and try to parse value to ensure its type validity.
        if (prop := SiteProperty.get_by_id(self.site_property_id)) is not None:
            prop.structural_element_property.value_type.verify(self.value)

    @classmethod
    def authorize_query(cls, actor, query):
        return Site.authorize_query(actor, query.join(Site))

    def authorize_read(self, actor):
        campaign = (
            db.session.query(Campaign).join(Site).filter(Site.id == self.site_id).one()
        )
        return campaign.is_member(actor)


class BuildingPropertyData(AuthMgrMixin, Base):
    __tablename__ = "building_prop_data"
    __table_args__ = (sqla.UniqueConstraint("building_id", "building_prop_id"),)

    id = sqla.Column(sqla.Integer, primary_key=True)
    building_id = sqla.Column(sqla.ForeignKey("buildings.id"), nullable=False)
    building_property_id = sqla.Column(
        "building_prop_id", sqla.ForeignKey("building_props.id"), nullable=False
    )
    value = sqla.Column(sqla.String(100), nullable=False)

    building = sqla.orm.relationship(
        "Building",
        backref=sqla.orm.backref(
            "building_property_data", cascade="all, delete-orphan"
        ),
    )
    building_property = sqla.orm.relationship(
        "BuildingProperty",
        backref=sqla.orm.backref(
            "building_property_data", cascade="all, delete-orphan"
        ),
    )

    def _before_flush(self):
        # Get property type and try to parse value to ensure its type validity.
        if (prop := BuildingProperty.get_by_id(self.building_property_id)) is not None:
            prop.structural_element_property.value_type.verify(self.value)

    @classmethod
    def authorize_query(cls, actor, query):
        return Building.authorize_query(actor, query.join(Building))

    def authorize_read(self, actor):
        campaign = (
            db.session.query(Campaign)
            .join(Site)
            .join(Building)
            .filter(Building.id == self.building_id)
            .one()
        )
        return campaign.is_member(actor)


class StoreyPropertyData(AuthMgrMixin, Base):
    __tablename__ = "storey_prop_data"
    __table_args__ = (sqla.UniqueConstraint("storey_id", "storey_prop_id"),)

    id = sqla.Column(sqla.Integer, primary_key=True)
    storey_id = sqla.Column(sqla.ForeignKey("storeys.id"), nullable=False)
    storey_property_id = sqla.Column(
        "storey_prop_id", sqla.ForeignKey("storey_props.id"), nullable=False
    )
    value = sqla.Column(sqla.String(100), nullable=False)

    storey = sqla.orm.relationship(
        "Storey",
        backref=sqla.orm.backref("storey_property_data", cascade="all, delete-orphan"),
    )
    storey_property = sqla.orm.relationship(
        "StoreyProperty",
        backref=sqla.orm.backref("storey_property_data", cascade="all, delete-orphan"),
    )

    def _before_flush(self):
        # Get property type and try to parse value to ensure its type validity.
        if (prop := StoreyProperty.get_by_id(self.storey_property_id)) is not None:
            prop.structural_element_property.value_type.verify(self.value)

    @classmethod
    def authorize_query(cls, actor, query):
        return Storey.authorize_query(actor, query.join(Storey))

    def authorize_read(self, actor):
        campaign = (
            db.session.query(Campaign)
            .join(Site)
            .join(Building)
            .join(Storey)
            .filter(Storey.id == self.storey_id)
        ).one()
        return campaign.is_member(actor)


class SpacePropertyData(AuthMgrMixin, Base):
    __tablename__ = "space_prop_data"
    __table_args__ = (sqla.UniqueConstraint("space_id", "space_prop_id"),)

    id = sqla.Column(sqla.Integer, primary_key=True)
    space_id = sqla.Column(sqla.ForeignKey("spaces.id"), nullable=False)
    space_property_id = sqla.Column(
        "space_prop_id", sqla.ForeignKey("space_props.id"), nullable=False
    )
    value = sqla.Column(sqla.String(100), nullable=False)

    space = sqla.orm.relationship(
        "Space",
        backref=sqla.orm.backref("space_property_data", cascade="all, delete-orphan"),
    )
    space_property = sqla.orm.relationship(
        "SpaceProperty",
        backref=sqla.orm.backref("space_property_data", cascade="all, delete-orphan"),
    )

    def _before_flush(self):
        # Get property type and try to parse value to ensure its type validity.
        if (prop := SpaceProperty.get_by_id(self.space_property_id)) is not None:
            prop.structural_element_property.value_type.verify(self.value)

    @classmethod
    def authorize_query(cls, actor, query):
        return Space.authorize_query(actor, query.join(Space))

    def authorize_read(self, actor):
        campaign = (
            db.session.query(Campaign)
            .join(Site)
            .join(Building)
            .join(Storey)
            .join(Space)
            .filter(Space.id == self.space_id)
        ).one()
        return campaign.is_member(actor)


class ZonePropertyData(AuthMgrMixin, Base):
    __tablename__ = "zone_prop_data"
    __table_args__ = (sqla.UniqueConstraint("zone_id", "zone_prop_id"),)

    id = sqla.Column(sqla.Integer, primary_key=True)
    zone_id = sqla.Column(sqla.ForeignKey("zones.id"), nullable=False)
    zone_property_id = sqla.Column(
        "zone_prop_id", sqla.ForeignKey("zone_props.id"), nullable=False
    )
    value = sqla.Column(sqla.String(100), nullable=False)

    zone = sqla.orm.relationship(
        "Zone",
        backref=sqla.orm.backref("zone_property_data", cascade="all, delete-orphan"),
    )
    zone_property = sqla.orm.relationship(
        "ZoneProperty",
        backref=sqla.orm.backref("zone_property_data", cascade="all, delete-orphan"),
    )

    def _before_flush(self):
        # Get property type and try to parse value to ensure its type validity.
        if (prop := ZoneProperty.get_by_id(self.zone_property_id)) is not None:
            prop.structural_element_property.value_type.verify(self.value)

    @classmethod
    def authorize_query(cls, actor, query):
        return Zone.authorize_query(actor, query.join(Zone))

    def authorize_read(self, actor):
        campaign = (
            db.session.query(Campaign).join(Zone).filter(Zone.id == self.zone_id)
        ).one()
        return campaign.is_member(actor)


PROPERTIES_MAPPING = {
    # Model class name: (lowercase name, property class, property data class)
    "Site": ("site", SiteProperty, SitePropertyData),
    "Building": ("building", BuildingProperty, BuildingPropertyData),
    "Storey": ("storey", StoreyProperty, StoreyPropertyData),
    "Space": ("space", SpaceProperty, SpacePropertyData),
    "Zone": ("zone", ZoneProperty, ZonePropertyData),
}


class StructuralElementBase(Base):
    __abstract__ = True

    def get_property_value(self, property_name):
        """Get propery value for a given property name

        :param str property_name: Property name

        Returns the value property cast to the property type
        """
        name, property_cls, property_data_cls = PROPERTIES_MAPPING[
            self.__class__.__name__
        ]

        stmt = (
            sqla.select(property_data_cls, StructuralElementProperty)
            .join(
                property_cls,
                getattr(property_data_cls, f"{name}_property_id") == property_cls.id,
            )
            .join(
                StructuralElementProperty,
                property_cls.structural_element_property_id
                == StructuralElementProperty.id,
            )
            .filter(getattr(property_data_cls, f"{name}_id") == self.id)
            .filter(StructuralElementProperty.name == property_name)
        )
        ret = db.session.execute(stmt).first()

        # If none found, return None
        if ret is None:
            return None

        # Value is stored in DB as string, cast to property type
        prop_data, se_property = ret
        return se_property.value_type.value(prop_data.value)


class Site(AuthMgrMixin, StructuralElementBase):
    __tablename__ = "sites"
    __table_args__ = (sqla.UniqueConstraint("campaign_id", "name"),)

    id = sqla.Column(sqla.Integer, primary_key=True)
    name = sqla.Column(sqla.String(80), nullable=False)
    description = sqla.Column(sqla.String(500))
    ifc_id = sqla.Column(sqla.String(22))
    campaign_id = sqla.Column(sqla.ForeignKey("campaigns.id"), nullable=False)
    latitude = sqla.Column(sqla.Float())
    longitude = sqla.Column(sqla.Float())

    campaign = sqla.orm.relationship(
        "Campaign", backref=sqla.orm.backref("sites", cascade="all, delete-orphan")
    )

    @classmethod
    def authorize_query(cls, actor, query):
        return Campaign.authorize_query(actor, query.join(Campaign))

    def authorize_read(self, actor):
        campaign = (
            db.session.query(Campaign).filter(Campaign.id == self.campaign_id).one()
        )
        return campaign.is_member(actor)


@auth_mgr.add_rule("get_weather_data")
def authorize_get_weather_data(actor: User, site: Site) -> bool:
    return False


class Building(AuthMgrMixin, StructuralElementBase):
    __tablename__ = "buildings"
    __table_args__ = (sqla.UniqueConstraint("site_id", "name"),)

    id = sqla.Column(sqla.Integer, primary_key=True)
    name = sqla.Column(sqla.String(80), nullable=False)
    description = sqla.Column(sqla.String(500))
    ifc_id = sqla.Column(sqla.String(22))
    site_id = sqla.Column(sqla.ForeignKey("sites.id"), nullable=False)

    site = sqla.orm.relationship(
        "Site", backref=sqla.orm.backref("buildings", cascade="all, delete-orphan")
    )

    @classmethod
    def get(cls, *, campaign_id=None, **kwargs):
        query = super().get(**kwargs)
        if campaign_id is not None:
            Campaign.get_by_id(campaign_id)
            site = sqla.orm.aliased(Site)
            query = query.join(site, cls.site_id == site.id).filter(
                site.campaign_id == campaign_id
            )
        return query

    @classmethod
    def authorize_query(cls, actor, query):
        return Site.authorize_query(actor, query.join(Site))

    def authorize_read(self, actor):
        campaign = (
            db.session.query(Campaign).join(Site).filter(Site.id == self.site_id).one()
        )
        return campaign.is_member(actor)


class Storey(AuthMgrMixin, StructuralElementBase):
    __tablename__ = "storeys"
    __table_args__ = (sqla.UniqueConstraint("building_id", "name"),)

    id = sqla.Column(sqla.Integer, primary_key=True)
    name = sqla.Column(sqla.String(80), nullable=False)
    description = sqla.Column(sqla.String(500))
    ifc_id = sqla.Column(sqla.String(22))
    building_id = sqla.Column(sqla.ForeignKey("buildings.id"), nullable=False)

    building = sqla.orm.relationship(
        "Building", backref=sqla.orm.backref("storeys", cascade="all, delete-orphan")
    )

    @classmethod
    def get(cls, *, campaign_id=None, site_id=None, **kwargs):
        query = super().get(**kwargs)
        if campaign_id is not None:
            Campaign.get_by_id(campaign_id)
            site = sqla.orm.aliased(Site)
            building = sqla.orm.aliased(Building)
            query = (
                query.join(building, cls.building_id == building.id)
                .join(site, building.site_id == site.id)
                .filter(site.campaign_id == campaign_id)
            )
        if site_id is not None:
            Site.get_by_id(site_id)
            building = sqla.orm.aliased(Building)
            query = query.join(building, cls.building_id == building.id).filter(
                building.site_id == site_id
            )
        return query

    @classmethod
    def authorize_query(cls, actor, query):
        return Building.authorize_query(actor, query.join(Building))

    def authorize_read(self, actor):
        campaign = (
            db.session.query(Campaign)
            .join(Site)
            .join(Building)
            .filter(Building.id == self.building_id)
            .one()
        )
        return campaign.is_member(actor)


class Space(AuthMgrMixin, StructuralElementBase):
    __tablename__ = "spaces"
    __table_args__ = (sqla.UniqueConstraint("storey_id", "name"),)

    id = sqla.Column(sqla.Integer, primary_key=True)
    name = sqla.Column(sqla.String(80), nullable=False)
    description = sqla.Column(sqla.String(500))
    ifc_id = sqla.Column(sqla.String(22))
    storey_id = sqla.Column(sqla.ForeignKey("storeys.id"), nullable=False)

    storey = sqla.orm.relationship(
        "Storey", backref=sqla.orm.backref("spaces", cascade="all, delete-orphan")
    )

    @classmethod
    def get(cls, *, campaign_id=None, site_id=None, building_id=None, **kwargs):
        query = super().get(**kwargs)
        if campaign_id is not None:
            Campaign.get_by_id(campaign_id)
            site = sqla.orm.aliased(Site)
            building = sqla.orm.aliased(Building)
            storey = sqla.orm.aliased(Storey)
            query = (
                query.join(storey, cls.storey_id == storey.id)
                .join(building, storey.building_id == building.id)
                .join(site, building.site_id == site.id)
                .filter(site.campaign_id == campaign_id)
            )
        if site_id is not None:
            Site.get_by_id(site_id)
            building = sqla.orm.aliased(Building)
            storey = sqla.orm.aliased(Storey)
            query = (
                query.join(storey, cls.storey_id == storey.id)
                .join(building, storey.building_id == building.id)
                .filter(building.site_id == site_id)
            )
        if building_id is not None:
            Building.get_by_id(building_id)
            storey = sqla.orm.aliased(Storey)
            query = query.join(storey, cls.storey_id == storey.id).filter(
                storey.building_id == building_id
            )
        return query

    @classmethod
    def authorize_query(cls, actor, query):
        return Storey.authorize_query(actor, query.join(Storey))

    def authorize_read(self, actor):
        campaign = (
            db.session.query(Campaign)
            .join(Site)
            .join(Building)
            .join(Storey)
            .filter(Storey.id == self.storey_id)
            .one()
        )
        return campaign.is_member(actor)


class Zone(AuthMgrMixin, StructuralElementBase):
    __tablename__ = "zones"
    __table_args__ = (sqla.UniqueConstraint("campaign_id", "name"),)

    id = sqla.Column(sqla.Integer, primary_key=True)
    name = sqla.Column(sqla.String(80), nullable=False)
    description = sqla.Column(sqla.String(500))
    ifc_id = sqla.Column(sqla.String(22))
    campaign_id = sqla.Column(sqla.ForeignKey("campaigns.id"), nullable=False)

    campaign = sqla.orm.relationship(
        "Campaign", backref=sqla.orm.backref("zones", cascade="all, delete-orphan")
    )

    @classmethod
    def authorize_query(cls, actor, query):
        return Campaign.authorize_query(actor, query.join(Campaign))

    def authorize_read(self, actor):
        campaign = (
            db.session.query(Campaign).filter(Campaign.id == self.campaign_id).one()
        )
        return campaign.is_member(actor)


def init_db_structural_elements_triggers():
    """Create triggers to protect some columns from update.

    This function is meant to be used for tests or dev setups after create_all.
    Production setups should rely on migration scripts.
    """
    make_columns_read_only(
        Site.campaign_id,
        Building.site_id,
        Storey.building_id,
        Space.storey_id,
        Zone.campaign_id,
        StructuralElementProperty.value_type,
        SiteProperty.structural_element_property_id,
        BuildingProperty.structural_element_property_id,
        StoreyProperty.structural_element_property_id,
        SpaceProperty.structural_element_property_id,
        ZoneProperty.structural_element_property_id,
        SitePropertyData.site_id,
        SitePropertyData.site_property_id,
        BuildingPropertyData.building_id,
        BuildingPropertyData.building_property_id,
        StoreyPropertyData.storey_id,
        StoreyPropertyData.storey_property_id,
        SpacePropertyData.space_id,
        SpacePropertyData.space_property_id,
        ZonePropertyData.zone_id,
        ZonePropertyData.zone_property_id,
    )
