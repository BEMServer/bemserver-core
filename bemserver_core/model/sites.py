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


class BuildingProperty(AuthMixin, Base):
    __tablename__ = "building_properties"

    id = sqla.Column(sqla.Integer, primary_key=True)
    structural_element_property_id = sqla.Column(
        sqla.ForeignKey("structural_element_properties.id"), unique=True, nullable=False
    )


class StoreyProperty(AuthMixin, Base):
    __tablename__ = "storey_properties"

    id = sqla.Column(sqla.Integer, primary_key=True)
    structural_element_property_id = sqla.Column(
        sqla.ForeignKey("structural_element_properties.id"), unique=True, nullable=False
    )


class SpaceProperty(AuthMixin, Base):
    __tablename__ = "space_properties"

    id = sqla.Column(sqla.Integer, primary_key=True)
    structural_element_property_id = sqla.Column(
        sqla.ForeignKey("structural_element_properties.id"), unique=True, nullable=False
    )


class ZoneProperty(AuthMixin, Base):
    __tablename__ = "zone_properties"

    id = sqla.Column(sqla.Integer, primary_key=True)
    structural_element_property_id = sqla.Column(
        sqla.ForeignKey("structural_element_properties.id"), unique=True, nullable=False
    )


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
