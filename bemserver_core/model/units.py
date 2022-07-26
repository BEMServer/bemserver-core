"""Units"""
import sqlalchemy as sqla
from sqlalchemy.ext.hybrid import hybrid_property

from bemserver_core.database import Base, db
from bemserver_core.authorization import AuthMixin


class Unit(AuthMixin, Base):
    __tablename__ = "units"
    __table_args__ = (
        sqla.UniqueConstraint("name", "symbol"),
        sqla.UniqueConstraint("symbol"),
    )

    id = sqla.Column(sqla.Integer, primary_key=True)
    name = sqla.Column(sqla.String(80), nullable=False)
    symbol = sqla.Column(sqla.String(20), nullable=False)
    is_default_si_unit = sqla.Column(sqla.Boolean, nullable=False, default=False)
    is_derived = sqla.Column(sqla.Boolean, nullable=False, default=False)

    @hybrid_property
    def label(self):
        if len(self.symbol) > 0:
            return f"{self.name} [{self.symbol}]"
        return self.name

    @classmethod
    def get_by_symbol(cls, symbol):
        """Get unit by symbol

        :param str symbol: Unit symbol
        """
        return cls.get(symbol=symbol).first()


# See https://en.wikipedia.org/wiki/List_of_physical_quantities
#  and https://en.wikipedia.org/wiki/International_System_of_Units
# All of following units are either SI or, on the sidelines, non-SI but acceptable.
def init_db_units():
    """Create default units

    This function is meant to be used for tests or dev setups after create_all.
    Production setups should rely on migration scripts.
    """

    # Base units
    db.session.add_all(
        [
            # Length: The one-dimensional extent of an object
            Unit(name="meter", symbol="m", is_default_si_unit=True),
            Unit(name="centi-meter", symbol="cm"),
            Unit(name="milli-meter", symbol="mm"),
            # Mass: A measure of resistance to acceleration
            Unit(name="gram", symbol="g"),
            Unit(name="kilo-gram", symbol="kg", is_default_si_unit=True),
            Unit(name="tonne", symbol="t"),
            # Time: The duration of an event
            unit_second := Unit(name="second", symbol="s", is_default_si_unit=True),
            Unit(name="minute", symbol="min"),
            Unit(name="hour", symbol="h"),
            Unit(name="day", symbol="d"),
            # Electric current: Rate of flow of electrical charge per unit time
            Unit(name="ampere", symbol="A", is_default_si_unit=True),
            Unit(name="milli-ampere", symbol="mA"),
            # Thermodynamic temperature:
            #  Average kinetic energy per degree of freedom of a system
            Unit(name="degree Celsius", symbol="°C"),
            Unit(name="degree Fahrenheit", symbol="°F"),
            Unit(name="kelvin", symbol="K", is_default_si_unit=True),
            # Luminosity intensity:
            #  Wavelength-weighted power of emitted light per unit solid angle
            Unit(name="candela", symbol="cd", is_default_si_unit=True),
            # Amount of substance:
            #  The quantity proportional to the number of particles in a sample,
            #  with the Avogadro constant as the proportionality constant
            Unit(name="mole", symbol="mol", is_default_si_unit=True),
        ]
    )

    # Derived units
    db.session.add_all(
        [
            # Acceleration (linear)
            Unit(
                name="meter per square second",
                symbol="m/s²",
                is_default_si_unit=True,
                is_derived=True,
            ),
            Unit(name="kilo-meter per square hour", symbol="km/h²", is_derived=True),
            # Acceleration (angular)
            Unit(
                name="radian per square second",
                symbol="rad/s²",
                is_default_si_unit=True,
                is_derived=True,
            ),
            # Velocity (linear)
            Unit(
                name="meter per second",
                symbol="m/s",
                is_default_si_unit=True,
                is_derived=True,
            ),
            Unit(name="kilo-meter per hour", symbol="km/h", is_derived=True),
            # Velocity (angular)
            Unit(
                name="radian per second",
                symbol="rad/s",
                is_default_si_unit=True,
                is_derived=True,
            ),
            # Area
            Unit(
                name="square meter",
                symbol="m²",
                is_default_si_unit=True,
                is_derived=True,
            ),
            Unit(name="square centi-meter", symbol="cm²", is_derived=True),
            Unit(name="square milli-meter", symbol="mm²", is_derived=True),
            Unit(name="hectare", symbol="ha", is_derived=True),
            # Area density
            Unit(
                name="kilo-gram per square meter",
                symbol="kg/m²",
                is_default_si_unit=True,
                is_derived=True,
            ),
            Unit(name="gram per square centi-meter", symbol="g/cm²", is_derived=True),
            # Capacitance
            Unit(name="farad", symbol="F", is_default_si_unit=True, is_derived=True),
            Unit(name="coulomb per volt", symbol="C/V", is_derived=True),
            # Centrifugal force
            Unit(
                name="newton radian",
                symbol="N⋅rad",
                is_default_si_unit=True,
                is_derived=True,
            ),
            # Dose equivalent
            Unit(name="sievert", symbol="Sv", is_default_si_unit=True, is_derived=True),
            # Dynamic viscosity
            Unit(
                name="pascal second",
                symbol="Pa⋅s",
                is_default_si_unit=True,
                is_derived=True,
            ),
            # Electric charge
            Unit(name="coulomb", symbol="C", is_default_si_unit=True, is_derived=True),
            Unit(name="ampere second", symbol="As", is_derived=True),
            Unit(name="ampere hour", symbol="Ah", is_derived=True),
            # Electric charge density
            Unit(
                name="coulomb per cubic meter",
                symbol="C/m³",
                is_default_si_unit=True,
                is_derived=True,
            ),
            # Electric current density
            Unit(
                name="ampere per square meter",
                symbol="A/m²",
                is_default_si_unit=True,
                is_derived=True,
            ),
            # Electric dipole moment
            Unit(
                name="coulomb meter",
                symbol="C⋅m",
                is_default_si_unit=True,
                is_derived=True,
            ),
            # Electric displacement field
            Unit(
                name="coulomb per square meter",
                symbol="C/m²",
                is_default_si_unit=True,
                is_derived=True,
            ),
            # Electric field strength
            Unit(
                name="volt per meter",
                symbol="V/m",
                is_default_si_unit=True,
                is_derived=True,
            ),
            Unit(name="newton per coulomb", symbol="N/C", is_derived=True),
            # Electrical conductance
            Unit(name="siemens", symbol="S", is_default_si_unit=True, is_derived=True),
            # Electrical conductivity
            Unit(
                name="siemens per meter",
                symbol="S/m",
                is_default_si_unit=True,
                is_derived=True,
            ),
            # Electric potential
            Unit(name="volt", symbol="V", is_default_si_unit=True, is_derived=True),
            Unit(name="kilo-volt", symbol="kV", is_derived=True),
            # Electrical resistance, Impedance
            Unit(name="ohm", symbol="Ω", is_default_si_unit=True, is_derived=True),
            Unit(name="kilo-ohm", symbol="kΩ", is_derived=True),
            # Electrical resistivity
            Unit(
                name="ohm per meter",
                symbol="Ω/m",
                is_default_si_unit=True,
                is_derived=True,
            ),
            # Energy, Heat
            Unit(name="joule", symbol="J", is_default_si_unit=True, is_derived=True),
            Unit(name="watt hour", symbol="Wh", is_derived=True),
            Unit(name="kilo-watt hour", symbol="kWh", is_derived=True),
            Unit(name="mega-watt hour", symbol="MWh", is_derived=True),
            # Energy density
            Unit(
                name="joule per cubic meter",
                symbol="J/m³",
                is_default_si_unit=True,
                is_derived=True,
            ),
            Unit(name="watt hour per cubic meter", symbol="Wh/m³", is_derived=True),
            # Entropy
            Unit(
                name="joule per kelvin",
                symbol="J/K",
                is_default_si_unit=True,
                is_derived=True,
            ),
            # Force, Weight
            Unit(name="newton", symbol="N", is_default_si_unit=True, is_derived=True),
            # Frequency
            Unit(name="hertz", symbol="Hz", is_default_si_unit=True, is_derived=True),
            Unit(name="kilo-hertz", symbol="kHz", is_derived=True),
            # Illuminance
            Unit(name="lux", symbol="lx", is_default_si_unit=True, is_derived=True),
            Unit(name="lumen per square meter", symbol="lm/m²", is_derived=True),
            Unit(name="lumen per square centi-meter", symbol="lm/cm²", is_derived=True),
            # Impulse
            Unit(
                name="newton second",
                symbol="N⋅s",
                is_default_si_unit=True,
                is_derived=True,
            ),
            # Inductance
            Unit(name="henry", symbol="H", is_default_si_unit=True, is_derived=True),
            # Irradiance, Intensity, Heat flux density
            Unit(
                name="watt per square meter",
                symbol="W/m²",
                is_default_si_unit=True,
                is_derived=True,
            ),
            Unit(name="kilo-watt per square meter", symbol="kW/m²", is_derived=True),
            # Linear density
            Unit(
                name="kilo-gram per meter",
                symbol="kg/m",
                is_default_si_unit=True,
                is_derived=True,
            ),
            # Luminance
            Unit(
                name="candela per square meter",
                symbol="cd/m²",
                is_default_si_unit=True,
                is_derived=True,
            ),
            Unit(
                name="candela per square centi-meter",
                symbol="cd/cm²",
                is_derived=True,
            ),
            # Luminous flux
            Unit(name="lumen", symbol="lm", is_default_si_unit=True, is_derived=True),
            # Magnetic field strength, Magnetization
            Unit(
                name="ampere per meter",
                symbol="A/m",
                is_default_si_unit=True,
                is_derived=True,
            ),
            # Magnetic flux
            Unit(name="weber", symbol="Wb", is_default_si_unit=True, is_derived=True),
            # Magnetic flux density
            Unit(name="tesla", symbol="T", is_default_si_unit=True, is_derived=True),
            # Mass fraction
            Unit(
                name="kilo-gram per kilo-gram",
                symbol="kg/kg",
                is_default_si_unit=True,
                is_derived=True,
            ),
            # Mass density
            Unit(
                name="kilo-gram per cubic meter",
                symbol="kg/m³",
                is_default_si_unit=True,
                is_derived=True,
            ),
            # Molar concentration
            Unit(
                name="mole per cubic meter",
                symbol="mol/m³",
                is_default_si_unit=True,
                is_derived=True,
            ),
            # Molar energy, Chemical potential
            Unit(
                name="joule per mole",
                symbol="J/mol",
                is_default_si_unit=True,
                is_derived=True,
            ),
            # Molar entropy, Molar heat capacity
            Unit(
                name="joule per kelvin mole",
                symbol="J/(K⋅mol)",
                is_default_si_unit=True,
                is_derived=True,
            ),
            # Optical power
            Unit(
                name="dioptre",
                symbol="dpt",
                is_default_si_unit=True,
                is_derived=True,
            ),
            # Permeability
            Unit(
                name="henry per meter",
                symbol="H/m",
                is_default_si_unit=True,
                is_derived=True,
            ),
            Unit(name="newton per square ampere", symbol="N/A²", is_derived=True),
            # Permittivity
            Unit(
                name="farad per meter",
                symbol="F/m",
                is_default_si_unit=True,
                is_derived=True,
            ),
            # Plane angle
            Unit(name="degree", symbol="°", is_derived=True),
            Unit(name="radian", symbol="rad", is_default_si_unit=True, is_derived=True),
            Unit(name="arcminute", symbol="′", is_derived=True),
            Unit(name="arcsecond", symbol="″", is_derived=True),
            # Power
            Unit(name="watt", symbol="W", is_default_si_unit=True, is_derived=True),
            Unit(name="kilo-watt", symbol="kW", is_derived=True),
            Unit(name="mega-watt", symbol="MW", is_derived=True),
            Unit(name="volt ampere", symbol="VA", is_derived=True),
            Unit(name="kilo-volt ampere", symbol="kVA", is_derived=True),
            # Pressure, Stress
            Unit(name="pascal", symbol="Pa", is_default_si_unit=True, is_derived=True),
            Unit(name="hectopascal", symbol="hPa", is_derived=True),
            Unit(name="milli-meter of mercury", symbol="mm Hg", is_derived=True),
            # Radioactive activity
            Unit(
                name="becquerel",
                symbol="Bq",
                is_default_si_unit=True,
                is_derived=True,
            ),
            # Radioactive dose
            Unit(name="gray", symbol="Gy", is_default_si_unit=True, is_derived=True),
            # Radiance
            Unit(
                name="watt per square meter steradian",
                symbol="W/(m²⋅sr)",
                is_default_si_unit=True,
                is_derived=True,
            ),
            # Radiant intensity
            Unit(
                name="watt per square meter",
                symbol="W/m",
                is_default_si_unit=True,
                is_derived=True,
            ),
            # Reaction rate
            Unit(
                name="mole per cubic meter second",
                symbol="mol/(m³⋅s)",
                is_default_si_unit=True,
                is_derived=True,
            ),
            # Reluctance
            Unit(
                name="reverse henry",
                symbol="1/H",
                is_default_si_unit=True,
                is_derived=True,
            ),
            # Solid angle
            Unit(
                name="steradian",
                symbol="sr",
                is_default_si_unit=True,
                is_derived=True,
            ),
            # Specific energy
            Unit(
                name="joule per kilo-gram",
                symbol="J/kg",
                is_default_si_unit=True,
                is_derived=True,
            ),
            # Specific heat capacity
            Unit(
                name="joule per kelvin per kilo-gram",
                symbol="J/(K⋅kg)",
                is_default_si_unit=True,
                is_derived=True,
            ),
            # Specific volume
            Unit(
                name="cubic meter per kilo-gram",
                symbol="m³/kg",
                is_default_si_unit=True,
                is_derived=True,
            ),
            # Surface tension
            Unit(
                name="newton per meter",
                symbol="N/m",
                is_default_si_unit=True,
                is_derived=True,
            ),
            Unit(name="joule per square meter", symbol="J/m²", is_derived=True),
            # Temperature gradient
            Unit(
                name="kelvin per meter",
                symbol="K/m",
                is_default_si_unit=True,
                is_derived=True,
            ),
            # Thermal conductance
            Unit(
                name="watt per kelvin",
                symbol="W/K",
                is_default_si_unit=True,
                is_derived=True,
            ),
            # Thermal conductivity
            Unit(
                name="watt per meter kelvin",
                symbol="W/(m⋅K)",
                is_default_si_unit=True,
                is_derived=True,
            ),
            # Thermal resistance
            Unit(
                name="kelvin per watt",
                symbol="K/W",
                is_default_si_unit=True,
                is_derived=True,
            ),
            # Thermal resistivity
            Unit(
                name="kelvin meter per watt",
                symbol="(K⋅m)/W",
                is_default_si_unit=True,
                is_derived=True,
            ),
            # Volume
            Unit(name="liter", symbol="L", is_derived=True),
            Unit(
                name="cubic meter",
                symbol="m³",
                is_default_si_unit=True,
                is_derived=True,
            ),
            Unit(name="stere", symbol="st", is_derived=True),
            # Volumetric flow rate
            Unit(name="liter per second", symbol="L/s", is_derived=True),
            Unit(name="liter per minute", symbol="L/min", is_derived=True),
            Unit(name="liter per hour", symbol="L/h", is_derived=True),
            Unit(
                name="cubic meter per second",
                symbol="m³/s",
                is_default_si_unit=True,
                is_derived=True,
            ),
            Unit(name="cubic meter per minute", symbol="m³/min", is_derived=True),
            Unit(name="cubic meter per hour", symbol="m³/h", is_derived=True),
            # Ratio
            Unit(name="decibel", symbol="dB", is_derived=True),
            Unit(name="percentage", symbol="%", is_derived=True),
        ]
    )

    db.session.commit()

    return {
        unit_second.symbol: unit_second,
    }
