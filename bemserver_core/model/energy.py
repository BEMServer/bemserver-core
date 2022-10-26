"""Energy"""
import sqlalchemy as sqla

from bemserver_core.database import Base, db
from bemserver_core.authorization import AuthMixin, auth, Relation


class EnergySource(AuthMixin, Base):
    __tablename__ = "ener_sources"

    id = sqla.Column(sqla.Integer, primary_key=True)
    name = sqla.Column(sqla.String(80), unique=True, nullable=False)


class EnergyEndUse(AuthMixin, Base):
    __tablename__ = "ener_end_uses"

    id = sqla.Column(sqla.Integer, primary_key=True)
    name = sqla.Column(sqla.String(80), unique=True, nullable=False)


class EnergyConsumptionTimeseriesBySite(AuthMixin, Base):
    __tablename__ = "ener_cons_ts_by_site"
    __table_args__ = (sqla.UniqueConstraint("site_id", "source_id", "end_use_id"),)

    id = sqla.Column(sqla.Integer, primary_key=True)
    site_id = sqla.Column(sqla.ForeignKey("sites.id"), nullable=False)
    source_id = sqla.Column(sqla.ForeignKey("ener_sources.id"), nullable=False)
    end_use_id = sqla.Column(sqla.ForeignKey("ener_end_uses.id"), nullable=False)
    timeseries_id = sqla.Column(sqla.ForeignKey("timeseries.id"), nullable=False)
    # Multiply TS values by wh_conversion_factor to get Wh
    wh_conversion_factor = sqla.Column(sqla.Float, nullable=False, default=1)

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
    source = sqla.orm.relationship(
        "EnergySource",
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


class EnergyConsumptionTimeseriesByBuilding(AuthMixin, Base):
    __tablename__ = "ener_cons_ts_by_building"
    __table_args__ = (sqla.UniqueConstraint("building_id", "source_id", "end_use_id"),)

    id = sqla.Column(sqla.Integer, primary_key=True)
    building_id = sqla.Column(sqla.ForeignKey("buildings.id"), nullable=False)
    source_id = sqla.Column(sqla.ForeignKey("ener_sources.id"), nullable=False)
    end_use_id = sqla.Column(sqla.ForeignKey("ener_end_uses.id"), nullable=False)
    timeseries_id = sqla.Column(sqla.ForeignKey("timeseries.id"), nullable=False)
    # Multiply TS values by kwh_conversion_factor to get Wh
    wh_conversion_factor = sqla.Column(sqla.Integer, nullable=False, default=1)

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
    source = sqla.orm.relationship(
        "EnergySource",
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


def init_db_energy():
    """Create default energy sources and end uses

    This function is meant to be used for tests or dev setups after create_all.
    Production setups should rely on migration scripts.
    """
    db.session.add_all(
        [
            EnergySource(name="all"),
            EnergySource(name="electricity"),
            EnergySource(name="natural gas"),
            EnergySource(name="propane gas"),
            EnergySource(name="heating oil"),
            EnergySource(name="wood log"),
            EnergySource(name="wood pellet"),
            EnergySource(name="wood chips"),
            EnergySource(name="heating network"),
            EnergySource(name="cooling network"),
            EnergyEndUse(name="all"),
            EnergyEndUse(name="heating"),
            EnergyEndUse(name="cooling"),
            EnergyEndUse(name="ventilation"),
            EnergyEndUse(name="lighting"),
            EnergyEndUse(name="appliances"),
        ]
    )
    db.session.commit()
