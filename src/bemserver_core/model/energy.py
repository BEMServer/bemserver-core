"""Energy"""

import sqlalchemy as sqla

from bemserver_core.authorization import AuthMgrMixin
from bemserver_core.database import Base, db

from .timeseries import Timeseries


class Energy(AuthMgrMixin, Base):
    __tablename__ = "energies"

    id = sqla.Column(sqla.Integer, primary_key=True)
    name = sqla.Column(sqla.String(80), unique=True, nullable=False)

    def authorize_read(self, actor):
        return True


class EnergyEndUse(AuthMgrMixin, Base):
    __tablename__ = "ener_end_uses"

    id = sqla.Column(sqla.Integer, primary_key=True)
    name = sqla.Column(sqla.String(80), unique=True, nullable=False)

    def authorize_read(self, actor):
        return True


class EnergyProductionTechnology(AuthMgrMixin, Base):
    __tablename__ = "ener_prod_techs"

    id = sqla.Column(sqla.Integer, primary_key=True)
    name = sqla.Column(sqla.String(80), unique=True, nullable=False)

    def authorize_read(self, actor):
        return True


class EnergyConsumptionTimeseriesBySite(AuthMgrMixin, Base):
    __tablename__ = "ener_cons_ts_by_site"
    __table_args__ = (sqla.UniqueConstraint("site_id", "energy_id", "end_use_id"),)

    id = sqla.Column(sqla.Integer, primary_key=True)
    site_id = sqla.Column(sqla.ForeignKey("sites.id"), nullable=False)
    energy_id = sqla.Column(sqla.ForeignKey("energies.id"), nullable=False)
    end_use_id = sqla.Column(sqla.ForeignKey("ener_end_uses.id"), nullable=False)
    timeseries_id = sqla.Column(sqla.ForeignKey("timeseries.id"), nullable=False)

    site = sqla.orm.relationship(
        "Site",
        backref=sqla.orm.backref(
            "energy_consumption_timeseries_by_sites", cascade="all, delete-orphan"
        ),
    )
    timeseries = sqla.orm.relationship(
        "Timeseries",
        backref=sqla.orm.backref(
            "energy_consumption_timeseries_by_sites", cascade="all, delete-orphan"
        ),
    )
    energy = sqla.orm.relationship(
        "Energy",
        backref=sqla.orm.backref(
            "energy_consumption_timeseries_by_sites", cascade="all, delete-orphan"
        ),
    )
    end_use = sqla.orm.relationship(
        "EnergyEndUse",
        backref=sqla.orm.backref(
            "energy_consumption_timeseries_by_sites", cascade="all, delete-orphan"
        ),
    )

    @classmethod
    def authorize_query(cls, actor, query):
        return Timeseries.authorize_query(actor, query.join(Timeseries))

    def authorize_read(self, actor):
        timeseries = Timeseries.get_by_id(self.timeseries_id)
        return timeseries.authorize_read(actor)


class EnergyConsumptionTimeseriesByBuilding(AuthMgrMixin, Base):
    __tablename__ = "ener_cons_ts_by_building"
    __table_args__ = (sqla.UniqueConstraint("building_id", "energy_id", "end_use_id"),)

    id = sqla.Column(sqla.Integer, primary_key=True)
    building_id = sqla.Column(sqla.ForeignKey("buildings.id"), nullable=False)
    energy_id = sqla.Column(sqla.ForeignKey("energies.id"), nullable=False)
    end_use_id = sqla.Column(sqla.ForeignKey("ener_end_uses.id"), nullable=False)
    timeseries_id = sqla.Column(sqla.ForeignKey("timeseries.id"), nullable=False)

    building = sqla.orm.relationship(
        "Building",
        backref=sqla.orm.backref(
            "energy_consumption_timeseries_by_buildings", cascade="all, delete-orphan"
        ),
    )
    timeseries = sqla.orm.relationship(
        "Timeseries",
        backref=sqla.orm.backref(
            "energy_consumption_timeseries_by_buildings", cascade="all, delete-orphan"
        ),
    )
    energy = sqla.orm.relationship(
        "Energy",
        backref=sqla.orm.backref(
            "energy_consumption_timeseries_by_buildings", cascade="all, delete-orphan"
        ),
    )
    end_use = sqla.orm.relationship(
        "EnergyEndUse",
        backref=sqla.orm.backref(
            "energy_consumption_timeseries_by_buildings", cascade="all, delete-orphan"
        ),
    )

    @classmethod
    def authorize_query(cls, actor, query):
        return Timeseries.authorize_query(actor, query.join(Timeseries))

    def authorize_read(self, actor):
        timeseries = Timeseries.get_by_id(self.timeseries_id)
        return timeseries.authorize_read(actor)


class EnergyProductionTimeseriesBySite(AuthMgrMixin, Base):
    __tablename__ = "ener_prod_ts_by_site"
    __table_args__ = (sqla.UniqueConstraint("site_id", "energy_id", "prod_tech_id"),)

    id = sqla.Column(sqla.Integer, primary_key=True)
    site_id = sqla.Column(sqla.ForeignKey("sites.id"), nullable=False)
    energy_id = sqla.Column(sqla.ForeignKey("energies.id"), nullable=False)
    prod_tech_id = sqla.Column(sqla.ForeignKey("ener_prod_techs.id"), nullable=False)
    timeseries_id = sqla.Column(sqla.ForeignKey("timeseries.id"), nullable=False)

    site = sqla.orm.relationship(
        "Site",
        backref=sqla.orm.backref(
            "energy_production_timeseries_by_sites", cascade="all, delete-orphan"
        ),
    )
    timeseries = sqla.orm.relationship(
        "Timeseries",
        backref=sqla.orm.backref(
            "energy_production_timeseries_by_sites", cascade="all, delete-orphan"
        ),
    )
    energy = sqla.orm.relationship(
        "Energy",
        backref=sqla.orm.backref(
            "energy_production_timeseries_by_sites", cascade="all, delete-orphan"
        ),
    )
    prod_tech = sqla.orm.relationship(
        "EnergyProductionTechnology",
        backref=sqla.orm.backref(
            "energy_production_timeseries_by_sites", cascade="all, delete-orphan"
        ),
    )

    @classmethod
    def authorize_query(cls, actor, query):
        return Timeseries.authorize_query(actor, query.join(Timeseries))

    def authorize_read(self, actor):
        timeseries = Timeseries.get_by_id(self.timeseries_id)
        return timeseries.authorize_read(actor)


class EnergyProductionTimeseriesByBuilding(AuthMgrMixin, Base):
    __tablename__ = "ener_prod_ts_by_building"
    __table_args__ = (
        sqla.UniqueConstraint("building_id", "energy_id", "prod_tech_id"),
    )

    id = sqla.Column(sqla.Integer, primary_key=True)
    building_id = sqla.Column(sqla.ForeignKey("buildings.id"), nullable=False)
    energy_id = sqla.Column(sqla.ForeignKey("energies.id"), nullable=False)
    prod_tech_id = sqla.Column(sqla.ForeignKey("ener_prod_techs.id"), nullable=False)
    timeseries_id = sqla.Column(sqla.ForeignKey("timeseries.id"), nullable=False)

    building = sqla.orm.relationship(
        "Building",
        backref=sqla.orm.backref(
            "energy_production_timeseries_by_buildings", cascade="all, delete-orphan"
        ),
    )
    timeseries = sqla.orm.relationship(
        "Timeseries",
        backref=sqla.orm.backref(
            "energy_production_timeseries_by_buildings", cascade="all, delete-orphan"
        ),
    )
    energy = sqla.orm.relationship(
        "Energy",
        backref=sqla.orm.backref(
            "energy_production_timeseries_by_buildings", cascade="all, delete-orphan"
        ),
    )
    prod_tech = sqla.orm.relationship(
        "EnergyProductionTechnology",
        backref=sqla.orm.backref(
            "energy_production_timeseries_by_buildings", cascade="all, delete-orphan"
        ),
    )

    @classmethod
    def authorize_query(cls, actor, query):
        return Timeseries.authorize_query(actor, query.join(Timeseries))

    def authorize_read(self, actor):
        timeseries = Timeseries.get_by_id(self.timeseries_id)
        return timeseries.authorize_read(actor)


def init_db_energy():
    """Create default energy energys and end uses

    This function is meant to be used for tests or dev setups after create_all.
    Production setups should rely on migration scripts.
    """
    db.session.add_all(
        [
            Energy(name="all"),
            Energy(name="electricity"),
            Energy(name="natural gas"),
            Energy(name="propane gas"),
            Energy(name="heating oil"),
            Energy(name="wood log"),
            Energy(name="wood pellet"),
            Energy(name="wood chips"),
            Energy(name="heating network"),
            Energy(name="cooling network"),
            EnergyEndUse(name="all"),
            EnergyEndUse(name="heating"),
            EnergyEndUse(name="cooling"),
            EnergyEndUse(name="ventilation"),
            EnergyEndUse(name="lighting"),
            EnergyEndUse(name="appliances"),
            EnergyProductionTechnology(name="all"),
            EnergyProductionTechnology(name="PV panels"),
            EnergyProductionTechnology(name="wind turbines"),
            EnergyProductionTechnology(name="solar thermal collectors"),
        ]
    )
