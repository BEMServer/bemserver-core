"""Sites"""
import sqlalchemy as sqla

from bemserver_core.database import Base
from bemserver_core.authorization import AuthMixin, auth, Relation


class Site(AuthMixin, Base):
    __tablename__ = "sites"
    __table_args__ = (sqla.UniqueConstraint("campaign_id", "name"),)

    id = sqla.Column(sqla.Integer, primary_key=True)
    name = sqla.Column(sqla.String(80), unique=True, nullable=False)
    description = sqla.Column(sqla.String(500))
    campaign_id = sqla.Column(sqla.ForeignKey("campaigns.id"), nullable=False)
    ifc_id = sqla.Column(sqla.String(22))

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
    name = sqla.Column(sqla.String(80), unique=True, nullable=False)
    description = sqla.Column(sqla.String(500))
    site_id = sqla.Column(sqla.ForeignKey("sites.id"), nullable=False)
    ifc_id = sqla.Column(sqla.String(22))

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
    name = sqla.Column(sqla.String(80), unique=True, nullable=False)
    description = sqla.Column(sqla.String(500))
    building_id = sqla.Column(sqla.ForeignKey("buildings.id"), nullable=False)
    ifc_id = sqla.Column(sqla.String(22))

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
    name = sqla.Column(sqla.String(80), unique=True, nullable=False)
    description = sqla.Column(sqla.String(500))
    storey_id = sqla.Column(sqla.ForeignKey("spaces.id"), nullable=False)
    ifc_id = sqla.Column(sqla.String(22))

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
    name = sqla.Column(sqla.String(80), unique=True, nullable=False)
    description = sqla.Column(sqla.String(500))
    campaign_id = sqla.Column(sqla.ForeignKey("campaigns.id"), nullable=False)
    ifc_id = sqla.Column(sqla.String(22))

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
