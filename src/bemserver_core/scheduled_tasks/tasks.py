"""Scheduled task"""

import datetime as dt
from zoneinfo import ZoneInfo

import sqlalchemy as sqla
from sqlalchemy.dialects.postgresql import JSONB

from bemserver_core.authorization import AuthMixin, Relation, auth
from bemserver_core.celery import logger
from bemserver_core.database import Base
from bemserver_core.exceptions import (
    BEMServerCorePeriodError,
    BEMServerCoreScheduledTaskParametersError,
)
from bemserver_core.time_utils import floor, make_date_range_around_datetime


class TaskByCampaign(AuthMixin, Base):
    __tablename__ = "tasks_by_campaigns"

    id = sqla.Column(sqla.Integer, primary_key=True)
    # There is no validation on task name
    task_name = sqla.Column(sqla.String, nullable=False)
    is_enabled = sqla.Column(sqla.Boolean, default=True, nullable=False)
    campaign_id = sqla.Column(
        sqla.ForeignKey("campaigns.id"), unique=True, nullable=False
    )
    campaign = sqla.orm.relationship(
        "Campaign",
        backref=sqla.orm.backref(
            "st_cleanups_by_campaigns", cascade="all, delete-orphan"
        ),
    )
    parameters = sqla.Column(JSONB)

    @classmethod
    def register_class(cls):
        auth.register_class(
            cls,
            fields={
                "campaign": Relation(
                    kind="one",
                    other_type="Campaign",
                    my_field="campaign_id",
                    other_field="id",
                ),
            },
        )

    @staticmethod
    def make_interval(campaign, params):
        datetime = dt.datetime.now(tz=ZoneInfo(campaign.timezone))
        logger.debug("datetime: %s", datetime)

        try:
            round_dt = floor(datetime, params["period"], params["period_multiplier"])
            start_dt, end_dt = make_date_range_around_datetime(
                round_dt,
                params["period"],
                params["period_multiplier"],
                params.get("periods_before", 1),
                params.get("periods_after", 0),
            )
        except BEMServerCorePeriodError as exc:
            logger.critical(str(exc))
            raise BEMServerCoreScheduledTaskParametersError(str(exc)) from exc

        logger.debug("start_dt: %s, endt_dt: %s", start_dt, end_dt)

        return start_dt, end_dt
