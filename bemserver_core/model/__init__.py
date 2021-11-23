"""Model"""
from bemserver_core.authorization import init_authorization

from .users import User
from .campaigns import (
    Campaign, UserByCampaign, TimeseriesByCampaign, TimeseriesByCampaignByUser
)
from .timeseries import Timeseries
from .timeseries_data import TimeseriesData
from .events import (
    Event, EventCategory, EventState, EventLevel, EventTarget
)


__all__ = [
    "User",
    "Campaign",
    "UserByCampaign",
    "TimeseriesByCampaign",
    "TimeseriesByCampaignByUser",
    "Timeseries",
    "TimeseriesData",
    "Event",
    "EventCategory",
    "EventState",
    "EventLevel",
    "EventTarget",
]


# Register classes for authorization
auth_model_classes = [
    User,
    Campaign,
    UserByCampaign,
    TimeseriesByCampaign,
    TimeseriesByCampaignByUser,
    Timeseries,
    TimeseriesData,
]

init_authorization(auth_model_classes)
