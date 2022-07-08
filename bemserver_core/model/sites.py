"""Sites"""
import sqlalchemy as sqla
import sqlalchemy.orm as sqlaorm
from sqlalchemy.ext.hybrid import hybrid_property

from bemserver_core.database import Base
from bemserver_core.model import Campaign
from bemserver_core.authorization import AuthMixin, auth, Relation
from bemserver_core.common import PropertyType


class StructuralElementProperty(AuthMixin, Base):
    __tablename__ = "structural_element_properties"

    id = sqla.Column(sqla.Integer, primary_key=True)
    name = sqla.Column(sqla.String(80), unique=True, nullable=False)
    description = sqla.Column(sqla.String(500))
    _value_type = sqla.Column(
        sqla.Enum(PropertyType),
        default=PropertyType.string,
        nullable=False,
    )

    @hybrid_property
    def value_type(self):
        return self._value_type

    @value_type.setter
    def value_type(self, value_type):
        if self.id is not None:
            raise AttributeError("value_type cannot be modified")
        self._value_type = value_type


class SiteProperty(AuthMixin, Base):
    __tablename__ = "site_properties"

    id = sqla.Column(sqla.Integer, primary_key=True)
    _structural_element_property_id = sqla.Column(
        sqla.ForeignKey("structural_element_properties.id"), unique=True, nullable=False
    )
    structural_element_property = sqla.orm.relationship(
        "StructuralElementProperty",
        backref=sqla.orm.backref("site_properties", cascade="all, delete-orphan"),
    )

    @hybrid_property
    def structural_element_property_id(self):
        return self._structural_element_property_id

    @structural_element_property_id.setter
    def structural_element_property_id(self, structural_element_property_id):
        if self.id is not None:
            raise AttributeError("structural_element_property_id cannot be modified")
        self._structural_element_property_id = structural_element_property_id


class BuildingProperty(AuthMixin, Base):
    __tablename__ = "building_properties"

    id = sqla.Column(sqla.Integer, primary_key=True)
    _structural_element_property_id = sqla.Column(
        sqla.ForeignKey("structural_element_properties.id"), unique=True, nullable=False
    )
    structural_element_property = sqla.orm.relationship(
        "StructuralElementProperty",
        backref=sqla.orm.backref("building_properties", cascade="all, delete-orphan"),
    )

    @hybrid_property
    def structural_element_property_id(self):
        return self._structural_element_property_id

    @structural_element_property_id.setter
    def structural_element_property_id(self, structural_element_property_id):
        if self.id is not None:
            raise AttributeError("structural_element_property_id cannot be modified")
        self._structural_element_property_id = structural_element_property_id


class StoreyProperty(AuthMixin, Base):
    __tablename__ = "storey_properties"

    id = sqla.Column(sqla.Integer, primary_key=True)
    _structural_element_property_id = sqla.Column(
        sqla.ForeignKey("structural_element_properties.id"), unique=True, nullable=False
    )
    structural_element_property = sqla.orm.relationship(
        "StructuralElementProperty",
        backref=sqla.orm.backref("storey_properties", cascade="all, delete-orphan"),
    )

    @hybrid_property
    def structural_element_property_id(self):
        return self._structural_element_property_id

    @structural_element_property_id.setter
    def structural_element_property_id(self, structural_element_property_id):
        if self.id is not None:
            raise AttributeError("structural_element_property_id cannot be modified")
        self._structural_element_property_id = structural_element_property_id


class SpaceProperty(AuthMixin, Base):
    __tablename__ = "space_properties"

    id = sqla.Column(sqla.Integer, primary_key=True)
    _structural_element_property_id = sqla.Column(
        sqla.ForeignKey("structural_element_properties.id"), unique=True, nullable=False
    )
    structural_element_property = sqla.orm.relationship(
        "StructuralElementProperty",
        backref=sqla.orm.backref("space_properties", cascade="all, delete-orphan"),
    )

    @hybrid_property
    def structural_element_property_id(self):
        return self._structural_element_property_id

    @structural_element_property_id.setter
    def structural_element_property_id(self, structural_element_property_id):
        if self.id is not None:
            raise AttributeError("structural_element_property_id cannot be modified")
        self._structural_element_property_id = structural_element_property_id


class ZoneProperty(AuthMixin, Base):
    __tablename__ = "zone_properties"

    id = sqla.Column(sqla.Integer, primary_key=True)
    _structural_element_property_id = sqla.Column(
        sqla.ForeignKey("structural_element_properties.id"), unique=True, nullable=False
    )
    structural_element_property = sqla.orm.relationship(
        "StructuralElementProperty",
        backref=sqla.orm.backref("zone_properties", cascade="all, delete-orphan"),
    )

    @hybrid_property
    def structural_element_property_id(self):
        return self._structural_element_property_id

    @structural_element_property_id.setter
    def structural_element_property_id(self, structural_element_property_id):
        if self.id is not None:
            raise AttributeError("structural_element_property_id cannot be modified")
        self._structural_element_property_id = structural_element_property_id


class Site(AuthMixin, Base):
    __tablename__ = "sites"
    __table_args__ = (sqla.UniqueConstraint("_campaign_id", "name"),)

    id = sqla.Column(sqla.Integer, primary_key=True)
    name = sqla.Column(sqla.String(80), nullable=False)
    description = sqla.Column(sqla.String(500))
    ifc_id = sqla.Column(sqla.String(22))

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

    campaign = sqla.orm.relationship(
        "Campaign", backref=sqla.orm.backref("sites", cascade="all, delete-orphan")
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
            },
        )


class Building(AuthMixin, Base):
    __tablename__ = "buildings"
    __table_args__ = (sqla.UniqueConstraint("_site_id", "name"),)

    id = sqla.Column(sqla.Integer, primary_key=True)
    name = sqla.Column(sqla.String(80), nullable=False)
    description = sqla.Column(sqla.String(500))
    ifc_id = sqla.Column(sqla.String(22))

    @sqlaorm.declared_attr
    def _site_id(cls):
        return sqla.Column(sqla.Integer, sqla.ForeignKey("sites.id"), nullable=False)

    @hybrid_property
    def site_id(self):
        return self._site_id

    @site_id.setter
    def site_id(self, site_id):
        if self.id is not None:
            raise AttributeError("site_id cannot be modified")
        self._site_id = site_id

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
    def register_class(cls):
        auth.register_class(
            cls,
            fields={
                "site": Relation(
                    kind="one",
                    other_type="Site",
                    my_field="site_id",
                    other_field="id",
                ),
            },
        )


class Storey(AuthMixin, Base):
    __tablename__ = "storeys"
    __table_args__ = (sqla.UniqueConstraint("_building_id", "name"),)

    id = sqla.Column(sqla.Integer, primary_key=True)
    name = sqla.Column(sqla.String(80), nullable=False)
    description = sqla.Column(sqla.String(500))
    ifc_id = sqla.Column(sqla.String(22))

    @sqlaorm.declared_attr
    def _building_id(cls):
        return sqla.Column(
            sqla.Integer, sqla.ForeignKey("buildings.id"), nullable=False
        )

    @hybrid_property
    def building_id(self):
        return self._building_id

    @building_id.setter
    def building_id(self, building_id):
        if self.id is not None:
            raise AttributeError("building_id cannot be modified")
        self._building_id = building_id

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
    def register_class(cls):
        auth.register_class(
            cls,
            fields={
                "building": Relation(
                    kind="one",
                    other_type="Building",
                    my_field="building_id",
                    other_field="id",
                ),
            },
        )


class Space(AuthMixin, Base):
    __tablename__ = "spaces"
    __table_args__ = (sqla.UniqueConstraint("_storey_id", "name"),)

    id = sqla.Column(sqla.Integer, primary_key=True)
    name = sqla.Column(sqla.String(80), nullable=False)
    description = sqla.Column(sqla.String(500))
    ifc_id = sqla.Column(sqla.String(22))

    @sqlaorm.declared_attr
    def _storey_id(cls):
        return sqla.Column(sqla.Integer, sqla.ForeignKey("storeys.id"), nullable=False)

    @hybrid_property
    def storey_id(self):
        return self._storey_id

    @storey_id.setter
    def storey_id(self, storey_id):
        if self.id is not None:
            raise AttributeError("storey_id cannot be modified")
        self._storey_id = storey_id

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
    def register_class(cls):
        auth.register_class(
            cls,
            fields={
                "storey": Relation(
                    kind="one",
                    other_type="Storey",
                    my_field="storey_id",
                    other_field="id",
                ),
            },
        )


class Zone(AuthMixin, Base):
    __tablename__ = "zones"
    __table_args__ = (sqla.UniqueConstraint("_campaign_id", "name"),)

    id = sqla.Column(sqla.Integer, primary_key=True)
    name = sqla.Column(sqla.String(80), nullable=False)
    description = sqla.Column(sqla.String(500))
    ifc_id = sqla.Column(sqla.String(22))

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

    campaign = sqla.orm.relationship(
        "Campaign", backref=sqla.orm.backref("zones", cascade="all, delete-orphan")
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
            },
        )


class SitePropertyData(AuthMixin, Base):
    __tablename__ = "site_property_data"
    __table_args__ = (sqla.UniqueConstraint("site_id", "_site_property_id"),)

    id = sqla.Column(sqla.Integer, primary_key=True)
    site_id = sqla.Column(sqla.ForeignKey("sites.id"), nullable=False)
    _site_property_id = sqla.Column(
        sqla.ForeignKey("site_properties.id"), nullable=False
    )
    value = sqla.Column(sqla.String(100), nullable=False)

    @hybrid_property
    def site_property_id(self):
        return self._site_property_id

    @site_property_id.setter
    def site_property_id(self, site_property_id):
        if self.id is not None:
            raise AttributeError("site_property_id cannot be modified")
        self._site_property_id = site_property_id

    site = sqla.orm.relationship(
        "Site",
        backref=sqla.orm.backref("site_property_data", cascade="all, delete-orphan"),
    )
    site_property = sqla.orm.relationship(
        "SiteProperty",
        backref=sqla.orm.backref("site_property_data", cascade="all, delete-orphan"),
    )

    @classmethod
    def register_class(cls):
        auth.register_class(
            cls,
            fields={
                "site": Relation(
                    kind="one",
                    other_type="Site",
                    my_field="site_id",
                    other_field="id",
                ),
            },
        )

    def _before_flush(self):
        # Get property type and try to parse value to ensure its type validity.
        if (prop := SiteProperty.get_by_id(self.site_property_id)) is not None:
            prop.structural_element_property.value_type.verify(self.value)


class BuildingPropertyData(AuthMixin, Base):
    __tablename__ = "building_property_data"
    __table_args__ = (sqla.UniqueConstraint("building_id", "_building_property_id"),)

    id = sqla.Column(sqla.Integer, primary_key=True)
    building_id = sqla.Column(sqla.ForeignKey("buildings.id"), nullable=False)
    _building_property_id = sqla.Column(
        sqla.ForeignKey("building_properties.id"), nullable=False
    )
    value = sqla.Column(sqla.String(100), nullable=False)

    @hybrid_property
    def building_property_id(self):
        return self._building_property_id

    @building_property_id.setter
    def building_property_id(self, building_property_id):
        if self.id is not None:
            raise AttributeError("building_property_id cannot be modified")
        self._building_property_id = building_property_id

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

    @classmethod
    def register_class(cls):
        auth.register_class(
            cls,
            fields={
                "building": Relation(
                    kind="one",
                    other_type="Building",
                    my_field="building_id",
                    other_field="id",
                ),
            },
        )

    def _before_flush(self):
        # Get property type and try to parse value to ensure its type validity.
        if (prop := BuildingProperty.get_by_id(self.building_property_id)) is not None:
            prop.structural_element_property.value_type.verify(self.value)


class StoreyPropertyData(AuthMixin, Base):
    __tablename__ = "storey_property_data"
    __table_args__ = (sqla.UniqueConstraint("storey_id", "_storey_property_id"),)

    id = sqla.Column(sqla.Integer, primary_key=True)
    storey_id = sqla.Column(sqla.ForeignKey("storeys.id"), nullable=False)
    _storey_property_id = sqla.Column(
        sqla.ForeignKey("storey_properties.id"), nullable=False
    )
    value = sqla.Column(sqla.String(100), nullable=False)

    @hybrid_property
    def storey_property_id(self):
        return self._storey_property_id

    @storey_property_id.setter
    def storey_property_id(self, storey_property_id):
        if self.id is not None:
            raise AttributeError("storey_property_id cannot be modified")
        self._storey_property_id = storey_property_id

    storey = sqla.orm.relationship(
        "Storey",
        backref=sqla.orm.backref("storey_property_data", cascade="all, delete-orphan"),
    )
    storey_property = sqla.orm.relationship(
        "StoreyProperty",
        backref=sqla.orm.backref("storey_property_data", cascade="all, delete-orphan"),
    )

    @classmethod
    def register_class(cls):
        auth.register_class(
            cls,
            fields={
                "storey": Relation(
                    kind="one",
                    other_type="Storey",
                    my_field="storey_id",
                    other_field="id",
                ),
            },
        )

    def _before_flush(self):
        # Get property type and try to parse value to ensure its type validity.
        if (prop := StoreyProperty.get_by_id(self.storey_property_id)) is not None:
            prop.structural_element_property.value_type.verify(self.value)


class SpacePropertyData(AuthMixin, Base):
    __tablename__ = "space_property_data"
    __table_args__ = (sqla.UniqueConstraint("space_id", "_space_property_id"),)

    id = sqla.Column(sqla.Integer, primary_key=True)
    space_id = sqla.Column(sqla.ForeignKey("spaces.id"), nullable=False)
    _space_property_id = sqla.Column(
        sqla.ForeignKey("space_properties.id"), nullable=False
    )
    value = sqla.Column(sqla.String(100), nullable=False)

    @hybrid_property
    def space_property_id(self):
        return self._space_property_id

    @space_property_id.setter
    def space_property_id(self, space_property_id):
        if self.id is not None:
            raise AttributeError("space_property_id cannot be modified")
        self._space_property_id = space_property_id

    space = sqla.orm.relationship(
        "Space",
        backref=sqla.orm.backref("space_property_data", cascade="all, delete-orphan"),
    )
    space_property = sqla.orm.relationship(
        "SpaceProperty",
        backref=sqla.orm.backref("space_property_data", cascade="all, delete-orphan"),
    )

    @classmethod
    def register_class(cls):
        auth.register_class(
            cls,
            fields={
                "space": Relation(
                    kind="one",
                    other_type="Space",
                    my_field="space_id",
                    other_field="id",
                ),
            },
        )

    def _before_flush(self):
        # Get property type and try to parse value to ensure its type validity.
        if (prop := SpaceProperty.get_by_id(self.space_property_id)) is not None:
            prop.structural_element_property.value_type.verify(self.value)


class ZonePropertyData(AuthMixin, Base):
    __tablename__ = "zone_property_data"
    __table_args__ = (sqla.UniqueConstraint("zone_id", "_zone_property_id"),)

    id = sqla.Column(sqla.Integer, primary_key=True)
    zone_id = sqla.Column(sqla.ForeignKey("zones.id"), nullable=False)
    _zone_property_id = sqla.Column(
        sqla.ForeignKey("zone_properties.id"), nullable=False
    )
    value = sqla.Column(sqla.String(100), nullable=False)

    @hybrid_property
    def zone_property_id(self):
        return self._zone_property_id

    @zone_property_id.setter
    def zone_property_id(self, zone_property_id):
        if self.id is not None:
            raise AttributeError("zone_property_id cannot be modified")
        self._zone_property_id = zone_property_id

    zone = sqla.orm.relationship(
        "Zone",
        backref=sqla.orm.backref("zone_property_data", cascade="all, delete-orphan"),
    )
    zone_property = sqla.orm.relationship(
        "ZoneProperty",
        backref=sqla.orm.backref("zone_property_data", cascade="all, delete-orphan"),
    )

    @classmethod
    def register_class(cls):
        auth.register_class(
            cls,
            fields={
                "zone": Relation(
                    kind="one",
                    other_type="Zone",
                    my_field="zone_id",
                    other_field="id",
                ),
            },
        )

    def _before_flush(self):
        # Get property type and try to parse value to ensure its type validity.
        if (prop := ZoneProperty.get_by_id(self.zone_property_id)) is not None:
            prop.structural_element_property.value_type.verify(self.value)
