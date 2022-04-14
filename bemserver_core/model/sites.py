"""Sites"""
import sqlalchemy as sqla
import sqlalchemy.orm as sqlaorm
from sqlalchemy.ext.hybrid import hybrid_property

from bemserver_core.database import Base
from bemserver_core.authorization import AuthMixin, auth, Relation


class StructuralElementProperty(AuthMixin, Base):
    __tablename__ = "structural_element_properties"

    id = sqla.Column(sqla.Integer, primary_key=True)
    name = sqla.Column(sqla.String(80), unique=True, nullable=False)
    description = sqla.Column(sqla.String(500))


class SiteProperty(AuthMixin, Base):
    __tablename__ = "site_properties"

    id = sqla.Column(sqla.Integer, primary_key=True)
    structural_element_property_id = sqla.Column(
        sqla.ForeignKey("structural_element_properties.id"), unique=True, nullable=False
    )
    structural_element_property = sqla.orm.relationship("StructuralElementProperty")


class BuildingProperty(AuthMixin, Base):
    __tablename__ = "building_properties"

    id = sqla.Column(sqla.Integer, primary_key=True)
    structural_element_property_id = sqla.Column(
        sqla.ForeignKey("structural_element_properties.id"), unique=True, nullable=False
    )
    structural_element_property = sqla.orm.relationship("StructuralElementProperty")


class StoreyProperty(AuthMixin, Base):
    __tablename__ = "storey_properties"

    id = sqla.Column(sqla.Integer, primary_key=True)
    structural_element_property_id = sqla.Column(
        sqla.ForeignKey("structural_element_properties.id"), unique=True, nullable=False
    )
    structural_element_property = sqla.orm.relationship("StructuralElementProperty")


class SpaceProperty(AuthMixin, Base):
    __tablename__ = "space_properties"

    id = sqla.Column(sqla.Integer, primary_key=True)
    structural_element_property_id = sqla.Column(
        sqla.ForeignKey("structural_element_properties.id"), unique=True, nullable=False
    )
    structural_element_property = sqla.orm.relationship("StructuralElementProperty")


class ZoneProperty(AuthMixin, Base):
    __tablename__ = "zone_properties"

    id = sqla.Column(sqla.Integer, primary_key=True)
    structural_element_property_id = sqla.Column(
        sqla.ForeignKey("structural_element_properties.id"), unique=True, nullable=False
    )
    structural_element_property = sqla.orm.relationship("StructuralElementProperty")


class Site(AuthMixin, Base):
    __tablename__ = "sites"
    __table_args__ = (sqla.UniqueConstraint("_campaign_id", "name"),)

    id = sqla.Column(sqla.Integer, primary_key=True)
    name = sqla.Column(sqla.String(80), unique=True, nullable=False)
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
    name = sqla.Column(sqla.String(80), unique=True, nullable=False)
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
    name = sqla.Column(sqla.String(80), unique=True, nullable=False)
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
    name = sqla.Column(sqla.String(80), unique=True, nullable=False)
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
    name = sqla.Column(sqla.String(80), unique=True, nullable=False)
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
    __table_args__ = (sqla.UniqueConstraint("site_id", "site_property_id"),)

    id = sqla.Column(sqla.Integer, primary_key=True)
    site_id = sqla.Column(sqla.ForeignKey("sites.id"), nullable=False)
    site_property_id = sqla.Column(
        sqla.ForeignKey("site_properties.id"), nullable=False
    )
    value = sqla.Column(sqla.String(100), nullable=False)

    site = sqla.orm.relationship(
        "Site",
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


class BuildingPropertyData(AuthMixin, Base):
    __tablename__ = "building_property_data"
    __table_args__ = (sqla.UniqueConstraint("building_id", "building_property_id"),)

    id = sqla.Column(sqla.Integer, primary_key=True)
    building_id = sqla.Column(sqla.ForeignKey("buildings.id"), nullable=False)
    building_property_id = sqla.Column(
        sqla.ForeignKey("building_properties.id"), nullable=False
    )
    value = sqla.Column(sqla.String(100), nullable=False)

    building = sqla.orm.relationship(
        "Building",
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


class StoreyPropertyData(AuthMixin, Base):
    __tablename__ = "storey_property_data"
    __table_args__ = (sqla.UniqueConstraint("storey_id", "storey_property_id"),)

    id = sqla.Column(sqla.Integer, primary_key=True)
    storey_id = sqla.Column(sqla.ForeignKey("storeys.id"), nullable=False)
    storey_property_id = sqla.Column(
        sqla.ForeignKey("storey_properties.id"), nullable=False
    )
    value = sqla.Column(sqla.String(100), nullable=False)

    storey = sqla.orm.relationship(
        "Storey",
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


class SpacePropertyData(AuthMixin, Base):
    __tablename__ = "space_property_data"
    __table_args__ = (sqla.UniqueConstraint("space_id", "space_property_id"),)

    id = sqla.Column(sqla.Integer, primary_key=True)
    space_id = sqla.Column(sqla.ForeignKey("spaces.id"), nullable=False)
    space_property_id = sqla.Column(
        sqla.ForeignKey("space_properties.id"), nullable=False
    )
    value = sqla.Column(sqla.String(100), nullable=False)

    space = sqla.orm.relationship(
        "Space",
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


class ZonePropertyData(AuthMixin, Base):
    __tablename__ = "zone_property_data"
    __table_args__ = (sqla.UniqueConstraint("zone_id", "zone_property_id"),)

    id = sqla.Column(sqla.Integer, primary_key=True)
    zone_id = sqla.Column(sqla.ForeignKey("zones.id"), nullable=False)
    zone_property_id = sqla.Column(
        sqla.ForeignKey("zone_properties.id"), nullable=False
    )
    value = sqla.Column(sqla.String(100), nullable=False)

    zone = sqla.orm.relationship(
        "Zone",
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
