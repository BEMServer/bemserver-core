"""Model"""
from bemserver_core.authorization import init_authorization

from .users import User  # noqa
from .timeseries import Timeseries  # noqa
from .timeseries_data import TimeseriesData  # noqa
from .events import (  # noqa
    Event, EventCategory, EventState, EventLevel, EventTarget
)
from .campaigns import (  # noqa
    Campaign, UserByCampaign, TimeseriesByCampaign, TimeseriesByCampaignByUser
)


model_classes = [
    User,
    Timeseries,
    TimeseriesData,
    Campaign,
    UserByCampaign,
    TimeseriesByCampaign,
    TimeseriesByCampaignByUser,
]

init_authorization(model_classes)
