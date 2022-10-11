"""Sites"""
import sqlalchemy as sqla

from bemserver_core.database import Base, db, make_columns_read_only
from bemserver_core.model import Campaign
from bemserver_core.authorization import AuthMixin, auth, Relation
from bemserver_core.common import PropertyType


class StructuralElementProperty(AuthMixin, Base):
    __tablename__ = "structural_element_properties"

    id = sqla.Column(sqla.Integer, primary_key=True)
    name = sqla.Column(sqla.String(80), unique=True, nullable=False)
    description = sqla.Column(sqla.String(500))
    value_type = sqla.Column(
        sqla.Enum(PropertyType),
        default=PropertyType.string,
        nullable=False,
    )
    unit_symbol = sqla.Column(sqla.String(20))


class StructuralElement(AuthMixin, Base):
    __tablename__ = "structural_elements"
    __mapper_args__ = {
        "polymorphic_identity": "se",
        "polymorphic_on": "type_",
    }

    id = sqla.Column(sqla.Integer, primary_key=True)
    description = sqla.Column(sqla.String(500))
    ifc_id = sqla.Column(sqla.String(22))
    type_ = sqla.Column(sqla.String(50))

    @classmethod
    def register_class(cls):
        auth.register_class(
            cls,
            fields={
                "site": Relation(
                    kind="one", other_type="Site", my_field="id", other_field="id"
                ),
                "building": Relation(
                    kind="one", other_type="Building", my_field="id", other_field="id"
                ),
                "storey": Relation(
                    kind="one", other_type="Storey", my_field="id", other_field="id"
                ),
                "space": Relation(
                    kind="one", other_type="Space", my_field="id", other_field="id"
                ),
                "zone": Relation(
                    kind="one", other_type="Zone", my_field="id", other_field="id"
                ),
            },
        )


class Site(StructuralElement):
    __tablename__ = "sites"
    __table_args__ = (sqla.UniqueConstraint("campaign_id", "name"),)
    __mapper_args__ = {
        "polymorphic_identity": "site",
    }

    id = sqla.Column(sqla.ForeignKey("structural_elements.id"), primary_key=True)
    name = sqla.Column(sqla.String(80), nullable=False)
    campaign_id = sqla.Column(sqla.ForeignKey("campaigns.id"), nullable=False)

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


class Building(StructuralElement):
    __tablename__ = "buildings"
    __table_args__ = (sqla.UniqueConstraint("site_id", "name"),)
    __mapper_args__ = {
        "polymorphic_identity": "building",
    }

    id = sqla.Column(sqla.ForeignKey("structural_elements.id"), primary_key=True)
    name = sqla.Column(sqla.String(80), nullable=False)
    site_id = sqla.Column(sqla.ForeignKey("sites.id"), nullable=False)

    site = sqla.orm.relationship(
        "Site",
        backref=sqla.orm.backref("buildings", cascade="all, delete-orphan"),
        foreign_keys=site_id,
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


class Storey(StructuralElement):
    __tablename__ = "storeys"
    __table_args__ = (sqla.UniqueConstraint("building_id", "name"),)
    __mapper_args__ = {
        "polymorphic_identity": "storey",
    }

    id = sqla.Column(sqla.ForeignKey("structural_elements.id"), primary_key=True)
    name = sqla.Column(sqla.String(80), nullable=False)
    building_id = sqla.Column(sqla.ForeignKey("buildings.id"), nullable=False)

    building = sqla.orm.relationship(
        "Building",
        backref=sqla.orm.backref("storeys", cascade="all, delete-orphan"),
        foreign_keys=building_id,
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


class Space(StructuralElement):
    __tablename__ = "spaces"
    __table_args__ = (sqla.UniqueConstraint("storey_id", "name"),)
    __mapper_args__ = {
        "polymorphic_identity": "space",
    }

    id = sqla.Column(sqla.ForeignKey("structural_elements.id"), primary_key=True)
    name = sqla.Column(sqla.String(80), nullable=False)
    storey_id = sqla.Column(sqla.ForeignKey("storeys.id"), nullable=False)

    storey = sqla.orm.relationship(
        "Storey",
        backref=sqla.orm.backref("spaces", cascade="all, delete-orphan"),
        foreign_keys=storey_id,
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


class Zone(StructuralElement):
    __tablename__ = "zones"
    __table_args__ = (sqla.UniqueConstraint("campaign_id", "name"),)
    __mapper_args__ = {
        "polymorphic_identity": "zone",
    }

    id = sqla.Column(sqla.ForeignKey("structural_elements.id"), primary_key=True)
    name = sqla.Column(sqla.String(80), nullable=False)
    campaign_id = sqla.Column(sqla.ForeignKey("campaigns.id"), nullable=False)

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


class StructuralElementPropertyData(AuthMixin, Base):
    __tablename__ = "structural_element_property_data"
    __table_args__ = (
        sqla.UniqueConstraint(
            "structural_element_id", "structural_element_property_id"
        ),
    )

    id = sqla.Column(sqla.Integer, primary_key=True)
    structural_element_id = sqla.Column(
        sqla.ForeignKey("structural_elements.id"), nullable=False
    )
    structural_element_property_id = sqla.Column(
        sqla.ForeignKey("structural_element_properties.id"), nullable=False
    )
    value = sqla.Column(sqla.String(100), nullable=False)

    structural_element = sqla.orm.relationship(
        "StructuralElement",
        backref=sqla.orm.backref(
            "structural_element_property_data", cascade="all, delete-orphan"
        ),
    )
    structural_element_property = sqla.orm.relationship(
        "StructuralElementProperty",
        backref=sqla.orm.backref(
            "structural_element_property_data", cascade="all, delete-orphan"
        ),
    )

    @classmethod
    def register_class(cls):
        auth.register_class(
            cls,
            fields={
                "structural_element": Relation(
                    kind="one",
                    other_type="StructuralElement",
                    my_field="structural_element_id",
                    other_field="id",
                ),
            },
        )

    def _before_flush(self):
        # Get property type and try to parse value to ensure its type validity.
        if (
            prop := StructuralElementProperty.get_by_id(
                self.structural_element_property_id
            )
        ) is not None:
            prop.value_type.verify(self.value)


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
        StructuralElementPropertyData.structural_element_id,
        StructuralElementPropertyData.structural_element_property_id,
    )
    db.session.commit()
