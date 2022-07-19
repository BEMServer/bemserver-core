"""Sites"""
import sqlalchemy as sqla

from bemserver_core.database import Base, db, generate_ddl_trigger_readonly
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


class SiteProperty(AuthMixin, Base):
    __tablename__ = "site_properties"

    id = sqla.Column(sqla.Integer, primary_key=True)
    structural_element_property_id = sqla.Column(
        sqla.ForeignKey("structural_element_properties.id"), unique=True, nullable=False
    )
    structural_element_property = sqla.orm.relationship(
        "StructuralElementProperty",
        backref=sqla.orm.backref("site_properties", cascade="all, delete-orphan"),
    )


class BuildingProperty(AuthMixin, Base):
    __tablename__ = "building_properties"

    id = sqla.Column(sqla.Integer, primary_key=True)
    structural_element_property_id = sqla.Column(
        sqla.ForeignKey("structural_element_properties.id"), unique=True, nullable=False
    )
    structural_element_property = sqla.orm.relationship(
        "StructuralElementProperty",
        backref=sqla.orm.backref("building_properties", cascade="all, delete-orphan"),
    )


class StoreyProperty(AuthMixin, Base):
    __tablename__ = "storey_properties"

    id = sqla.Column(sqla.Integer, primary_key=True)
    structural_element_property_id = sqla.Column(
        sqla.ForeignKey("structural_element_properties.id"), unique=True, nullable=False
    )
    structural_element_property = sqla.orm.relationship(
        "StructuralElementProperty",
        backref=sqla.orm.backref("storey_properties", cascade="all, delete-orphan"),
    )


class SpaceProperty(AuthMixin, Base):
    __tablename__ = "space_properties"

    id = sqla.Column(sqla.Integer, primary_key=True)
    structural_element_property_id = sqla.Column(
        sqla.ForeignKey("structural_element_properties.id"), unique=True, nullable=False
    )
    structural_element_property = sqla.orm.relationship(
        "StructuralElementProperty",
        backref=sqla.orm.backref("space_properties", cascade="all, delete-orphan"),
    )


class ZoneProperty(AuthMixin, Base):
    __tablename__ = "zone_properties"

    id = sqla.Column(sqla.Integer, primary_key=True)
    structural_element_property_id = sqla.Column(
        sqla.ForeignKey("structural_element_properties.id"), unique=True, nullable=False
    )
    structural_element_property = sqla.orm.relationship(
        "StructuralElementProperty",
        backref=sqla.orm.backref("zone_properties", cascade="all, delete-orphan"),
    )


class Site(AuthMixin, Base):
    __tablename__ = "sites"
    __table_args__ = (sqla.UniqueConstraint("campaign_id", "name"),)

    id = sqla.Column(sqla.Integer, primary_key=True)
    name = sqla.Column(sqla.String(80), nullable=False)
    description = sqla.Column(sqla.String(500))
    ifc_id = sqla.Column(sqla.String(22))
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


class Building(AuthMixin, Base):
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


def init_db_structural_elements_triggers():
    """Create triggers to protect some columns from update.

    This function is meant to be used for tests or dev setups after create_all.
    Production setups should rely on migration scripts.
    """

    # Set "update read-only trigger" on:
    #  - campaign_id column for Site table
    #  - site_id column for Building table
    #  - building_id column for Storey table
    #  - storey_id column for Space table
    #  - campaign_id column for Zone table
    for struct_elmt_table, struct_elmt_column in [
        (Site, Site.campaign_id),
        (Building, Building.site_id),
        (Storey, Storey.building_id),
        (Space, Space.storey_id),
        (Zone, Zone.campaign_id),
    ]:
        db.session.execute(
            generate_ddl_trigger_readonly(
                struct_elmt_table.__table__,
                struct_elmt_column.key,
            )
        )

    # Set "update read-only trigger" on value_type column
    #  for structural element property table.
    db.session.execute(
        generate_ddl_trigger_readonly(
            StructuralElementProperty.__table__,
            StructuralElementProperty.value_type.key,
            row_name=StructuralElementProperty.name.key,
        )
    )

    # Set "update read-only trigger" on structural_element_property_id column
    #  for site/building/... property tables.
    for struct_elmt_prop_table in [
        SiteProperty,
        BuildingProperty,
        StoreyProperty,
        SpaceProperty,
        ZoneProperty,
    ]:
        db.session.execute(
            generate_ddl_trigger_readonly(
                struct_elmt_prop_table.__table__,
                struct_elmt_prop_table.structural_element_property_id.key,
            )
        )

    # Set "update read-only trigger" on:
    #  - site_id and site_property_id columns for SitePropertyData table
    #  - building_id and building_property_id columns for BuildingPropertyData table
    #  - storey_id and storey_property_id columns for StoreyPropertyData table
    #  - space_id and space_property_id columns for SpacePropertyData table
    #  - zone_id and zone_property_id columns for ZonePropertyData table
    for (
        struct_elmt_prop_data_table,
        struct_elmt_id_column,
        struct_elmt_prop_id_column,
    ) in [
        (SitePropertyData, SitePropertyData.site_id, SitePropertyData.site_property_id),
        (
            BuildingPropertyData,
            BuildingPropertyData.building_id,
            BuildingPropertyData.building_property_id,
        ),
        (
            StoreyPropertyData,
            StoreyPropertyData.storey_id,
            StoreyPropertyData.storey_property_id,
        ),
        (
            SpacePropertyData,
            SpacePropertyData.space_id,
            SpacePropertyData.space_property_id,
        ),
        (ZonePropertyData, ZonePropertyData.zone_id, ZonePropertyData.zone_property_id),
    ]:
        db.session.execute(
            generate_ddl_trigger_readonly(
                struct_elmt_prop_data_table.__table__,
                struct_elmt_id_column.key,
            )
        )
        db.session.execute(
            generate_ddl_trigger_readonly(
                struct_elmt_prop_data_table.__table__,
                struct_elmt_prop_id_column.key,
            )
        )

    db.session.commit()
