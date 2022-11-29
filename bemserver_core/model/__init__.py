"""Model"""
from pathlib import Path

from .users import User, UserGroup, UserByUserGroup
from .campaigns import (
    Campaign,
    CampaignScope,
    UserGroupByCampaign,
    UserGroupByCampaignScope,
)
from .sites import (
    StructuralElementProperty,
    SiteProperty,
    BuildingProperty,
    StoreyProperty,
    SpaceProperty,
    ZoneProperty,
    Site,
    Building,
    Storey,
    Space,
    Zone,
    SitePropertyData,
    BuildingPropertyData,
    StoreyPropertyData,
    SpacePropertyData,
    ZonePropertyData,
)
from .timeseries import (
    TimeseriesDataState,
    TimeseriesProperty,
    Timeseries,
    TimeseriesPropertyData,
    TimeseriesByDataState,
    TimeseriesBySite,
    TimeseriesByBuilding,
    TimeseriesByStorey,
    TimeseriesBySpace,
    TimeseriesByZone,
)
from .timeseries_data import TimeseriesData  # noqa
from .events import (  # noqa
    EventCategory,
    EventLevel,
    Event,
    TimeseriesByEvent,
)
from .energy import (
    EnergySource,
    EnergyEndUse,
    EnergyConsumptionTimeseriesBySite,
    EnergyConsumptionTimeseriesByBuilding,
)

__all__ = [
    "User",
    "UserGroup",
    "UserByUserGroup",
    "Campaign",
    "CampaignScope",
    "UserGroupByCampaign",
    "UserGroupByCampaignScope",
    "StructuralElementProperty",
    "SiteProperty",
    "BuildingProperty",
    "StoreyProperty",
    "SpaceProperty",
    "ZoneProperty",
    "Site",
    "Building",
    "Storey",
    "Space",
    "Zone",
    "SitePropertyData",
    "BuildingPropertyData",
    "StoreyPropertyData",
    "SpacePropertyData",
    "ZonePropertyData",
    "TimeseriesDataState",
    "TimeseriesProperty",
    "Timeseries",
    "TimeseriesPropertyData",
    "TimeseriesByDataState",
    "TimeseriesData",
    "TimeseriesBySite",
    "TimeseriesByBuilding",
    "TimeseriesByStorey",
    "TimeseriesBySpace",
    "TimeseriesByZone",
    "EventCategory",
    "EventLevel",
    "Event",
    "TimeseriesByEvent",
    "EnergySource",
    "EnergyEndUse",
    "EnergyConsumptionTimeseriesBySite",
    "EnergyConsumptionTimeseriesByBuilding",
]


AUTH_MODEL_CLASSES = [
    User,
    UserGroup,
    UserByUserGroup,
    Campaign,
    CampaignScope,
    UserGroupByCampaign,
    UserGroupByCampaignScope,
    StructuralElementProperty,
    SiteProperty,
    BuildingProperty,
    StoreyProperty,
    SpaceProperty,
    ZoneProperty,
    Site,
    Building,
    Storey,
    Space,
    Zone,
    SitePropertyData,
    BuildingPropertyData,
    StoreyPropertyData,
    SpacePropertyData,
    ZonePropertyData,
    TimeseriesDataState,
    TimeseriesProperty,
    Timeseries,
    TimeseriesPropertyData,
    TimeseriesByDataState,
    TimeseriesBySite,
    TimeseriesByBuilding,
    TimeseriesByStorey,
    TimeseriesBySpace,
    TimeseriesByZone,
    EventCategory,
    EventLevel,
    Event,
    TimeseriesByEvent,
    EnergySource,
    EnergyEndUse,
    EnergyConsumptionTimeseriesBySite,
    EnergyConsumptionTimeseriesByBuilding,
]


AUTH_POLAR_FILE = Path(__file__).parent / "authorization.polar"
