"""Model"""
from bemserver_core.authorization import init_authorization

from .users import User
from .campaigns import Campaign, UserByCampaign, TimeseriesClusterGroupByCampaign
from .timeseries import (
    TimeseriesDataState,
    TimeseriesProperty,
    TimeseriesClusterGroup,
    TimeseriesClusterGroupByUser,
    TimeseriesCluster,
    TimeseriesClusterPropertyData,
    Timeseries,
)
from .timeseries_data import TimeseriesData  # noqa
from .events import (  # noqa
    EventCategory,
    EventState,
    EventLevel,
    EventChannel,
    EventChannelByCampaign,
    EventChannelByUser,
    Event,
)


__all__ = [
    "User",
    "Campaign",
    "UserByCampaign",
    "TimeseriesClusterGroupByCampaign",
    "TimeseriesDataState",
    "TimeseriesProperty",
    "TimeseriesClusterGroup",
    "TimeseriesClusterGroupByUser",
    "TimeseriesCluster",
    "TimeseriesClusterPropertyData",
    "Timeseries",
    "TimeseriesData",
    "EventCategory",
    "EventState",
    "EventLevel",
    "EventChannel",
    "EventChannelByCampaign",
    "EventChannelByUser",
    "Event",
]


# Register classes for authorization
auth_model_classes = [
    User,
    Campaign,
    UserByCampaign,
    TimeseriesClusterGroupByCampaign,
    TimeseriesDataState,
    TimeseriesProperty,
    TimeseriesClusterGroup,
    TimeseriesClusterGroupByUser,
    TimeseriesCluster,
    TimeseriesClusterPropertyData,
    Timeseries,
    EventCategory,
    EventState,
    EventLevel,
    EventChannel,
    EventChannelByCampaign,
    EventChannelByUser,
    Event,
]

init_authorization(auth_model_classes)
