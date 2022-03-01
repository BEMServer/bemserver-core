"""Model"""
from bemserver_core.authorization import init_authorization

from .users import User
from .campaigns import Campaign, UserByCampaign, TimeseriesGroupByCampaign
from .timeseries import (
    TimeseriesDataState,
    TimeseriesProperty,
    TimeseriesGroup,
    TimeseriesGroupByUser,
    Timeseries,
    TimeseriesPropertyData,
    TimeseriesByDataState,
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
    "TimeseriesGroupByCampaign",
    "TimeseriesDataState",
    "TimeseriesProperty",
    "TimeseriesGroup",
    "TimeseriesGroupByUser",
    "Timeseries",
    "TimeseriesPropertyData",
    "TimeseriesByDataState",
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
    TimeseriesGroupByCampaign,
    TimeseriesDataState,
    TimeseriesProperty,
    TimeseriesGroup,
    TimeseriesGroupByUser,
    Timeseries,
    TimeseriesPropertyData,
    TimeseriesByDataState,
    EventCategory,
    EventState,
    EventLevel,
    EventChannel,
    EventChannelByCampaign,
    EventChannelByUser,
    Event,
]

init_authorization(auth_model_classes)
