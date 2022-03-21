"""Model"""
from bemserver_core.authorization import init_authorization

from .users import User, UserGroup, UserByUserGroup
from .campaigns import (
    Campaign,
    CampaignScope,
    UserGroupByCampaign,
    UserGroupByCampaignScope,
)
from .sites import (
    Site,
    Building,
    Storey,
    Space,
    Zone,
)
from .timeseries import (
    TimeseriesDataState,
    TimeseriesProperty,
    Timeseries,
    TimeseriesPropertyData,
    TimeseriesByDataState,
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
    "Site",
    "Building",
    "Storey",
    "Space",
    "Zone",
    "TimeseriesDataState",
    "TimeseriesProperty",
    "Timeseries",
    "TimeseriesPropertyData",
    "TimeseriesByDataState",
    "TimeseriesData",
    "EventCategory",
    "EventState",
    "EventLevel",
    "Event",
]


# Register classes for authorization
auth_model_classes = [
    User,
    UserGroup,
    UserByUserGroup,
    Campaign,
    CampaignScope,
    UserGroupByCampaign,
    UserGroupByCampaignScope,
    Site,
    Building,
    Storey,
    Space,
    Zone,
    TimeseriesDataState,
    TimeseriesProperty,
    Timeseries,
    TimeseriesPropertyData,
    TimeseriesByDataState,
    EventCategory,
    EventState,
    EventLevel,
    Event,
]

init_authorization(auth_model_classes)
