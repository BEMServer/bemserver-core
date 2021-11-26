"""Model"""
from bemserver_core.authorization import init_authorization

from .users import User
from .campaigns import Campaign, UserByCampaign, TimeseriesByCampaign
from .timeseries import Timeseries
from .timeseries_data import TimeseriesData
from .events import (  # noqa
    EventChannel, EventCategory, EventState, EventLevel,
    TimeseriesEvent, TimeseriesEventByTimeseries,
)


__all__ = [
    "User",
    "Campaign",
    "UserByCampaign",
    "TimeseriesByCampaign",
    "Timeseries",
    "TimeseriesData",
    "EventCategory",
    "EventState",
    "EventLevel",
    "TimeseriesEvent",
    "TimeseriesEventByTimeseries",
]


# Register classes for authorization
auth_model_classes = [
    User,
    Campaign,
    UserByCampaign,
    TimeseriesByCampaign,
    Timeseries,
    TimeseriesData,
]

init_authorization(auth_model_classes)
