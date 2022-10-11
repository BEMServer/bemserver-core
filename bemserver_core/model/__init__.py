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
    StructuralElement,
    Site,
    Building,
    Storey,
    Space,
    Zone,
    StructuralElementPropertyData,
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
    EventState,
    EventLevel,
    Event,
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
    "StructuralElement",
    "Site",
    "Building",
    "Storey",
    "Space",
    "Zone",
    "StructuralElementPropertyData",
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
    "EventState",
    "EventLevel",
    "Event",
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
    StructuralElement,
    Site,
    Building,
    Storey,
    Space,
    Zone,
    StructuralElementPropertyData,
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
    EventState,
    EventLevel,
    Event,
]


AUTH_POLAR_FILE = Path(__file__).parent / "authorization.polar"
