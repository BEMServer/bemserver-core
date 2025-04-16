"""Scheduled task"""

import datetime as dt
from zoneinfo import ZoneInfo

import sqlalchemy as sqla
from sqlalchemy.dialects.postgresql import JSONB

from celery import Task

from bemserver_core import model
from bemserver_core.authorization import AuthMixin, CurrentUser, OpenBar, Relation, auth
from bemserver_core.celery import BEMServerCoreSystemTask, logger
from bemserver_core.database import Base
from bemserver_core.exceptions import BEMServerCoreTaskError
from bemserver_core.time_utils import PeriodEnum, make_date_offset


class BEMServerCoreClassBasedTaskMixin:
    @property
    def name(self):
        return self.__class__.__name__.removesuffix("Task")


class BEMServerCoreAsyncTask(BEMServerCoreClassBasedTaskMixin, Task):
    TASK_FUNCTION = None
    DEFAULT_PARAMETERS = {}

    def run(self, user_id, campaign_id, start_td, end_dt, **kwargs):
        logger.info("Start")

        with OpenBar():
            user = model.User.get_by_id(user_id)
        if user is None:
            raise BEMServerCoreTaskError(f"Unknown user ID {user_id}")

        with CurrentUser(user):
            campaign = model.Campaign.get_by_id(campaign_id)
            if campaign is None:
                raise BEMServerCoreTaskError(f"Unknown campaign ID {campaign_id}")

            # Function is bound at init. Use __func__ to avoid passing self
            self.TASK_FUNCTION.__func__(
                campaign,
                start_td,
                end_dt,
                **{**self.DEFAULT_PARAMETERS, **kwargs},
            )


class BEMServerCoreScheduledTask(
    BEMServerCoreClassBasedTaskMixin, BEMServerCoreSystemTask
):
    TASK_FUNCTION = None
    DEFAULT_PARAMETERS = {}

    def run(self):
        logger.info("Start")

        for tbc in TaskByCampaign.get(task_name=self.name, is_enabled=True):
            start_dt, end_dt = tbc.make_interval()

            # Function is bound at init. Use __func__ to avoid passing self
            self.TASK_FUNCTION.__func__(
                tbc.campaign,
                start_dt,
                end_dt,
                **{**self.DEFAULT_PARAMETERS, **tbc.parameters},
            )


class TaskByCampaign(AuthMixin, Base):
    __tablename__ = "tasks_by_campaigns"

    id = sqla.Column(sqla.Integer, primary_key=True)
    # There is no validation on task name
    task_name = sqla.Column(sqla.String, nullable=False)
    is_enabled = sqla.Column(sqla.Boolean, default=True, nullable=False)
    campaign_id = sqla.Column(sqla.ForeignKey("campaigns.id"), nullable=False)
    campaign = sqla.orm.relationship(
        "Campaign",
        backref=sqla.orm.backref("tasks_by_campaigns", cascade="all, delete-orphan"),
    )
    parameters = sqla.Column(JSONB, default=dict, nullable=False)
    offset_unit = sqla.Column(
        sqla.Enum(PeriodEnum, name="period_names"), nullable=False
    )
    start_offset = sqla.Column(sqla.Integer, default=-1, nullable=False)
    end_offset = sqla.Column(sqla.Integer, default=0, nullable=False)

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

    def make_interval(self):
        datetime = dt.datetime.now(tz=ZoneInfo(self.campaign.timezone))
        logger.debug("datetime: %s", datetime)

        start_dt = datetime + make_date_offset(
            self.offset_unit.value, self.start_offset
        )
        end_dt = datetime + make_date_offset(self.offset_unit.value, self.end_offset)
        logger.debug("start_dt: %s, endt_dt: %s", start_dt, end_dt)

        return start_dt, end_dt
