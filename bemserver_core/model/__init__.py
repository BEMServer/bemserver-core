"""Model"""
from bemserver_core.authorization import init_authorization

from .users import User
from .campaigns import Campaign, UserByCampaign, TimeseriesGroupByCampaign
from .timeseries import Timeseries, TimeseriesGroup, TimeseriesGroupByUser
from .timeseries_data import TimeseriesData  # noqa
from .events import (  # noqa
    EventCategory,
    EventState,
    EventLevel,
    EventChannel,
    EventChannelByCampaign,
    TimeseriesEvent,
)


__all__ = [
    "User",
    "Campaign",
    "UserByCampaign",
    "TimeseriesGroupByCampaign",
    "Timeseries",
    "TimeseriesGroup",
    "TimeseriesGroupByUser",
    "TimeseriesData",
    "EventCategory",
    "EventState",
    "EventLevel",
    "EventChannel",
    "EventChannelByCampaign",
    "TimeseriesEvent",
]


# Register classes for authorization
auth_model_classes = [
    User,
    Campaign,
    UserByCampaign,
    TimeseriesGroupByCampaign,
    Timeseries,
    TimeseriesGroup,
    TimeseriesGroupByUser,
    EventCategory,
    EventState,
    EventLevel,
    EventChannel,
    EventChannelByCampaign,
    TimeseriesEvent,
]

init_authorization(auth_model_classes)
