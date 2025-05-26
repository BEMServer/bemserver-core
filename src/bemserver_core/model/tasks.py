"""Scheduled task"""

import datetime as dt
from zoneinfo import ZoneInfo

import sqlalchemy as sqla
from sqlalchemy.dialects.postgresql import JSONB

from bemserver_core.authorization import AuthMixin, Relation, auth
from bemserver_core.celery import logger
from bemserver_core.database import Base
from bemserver_core.time_utils import PeriodEnum, make_date_offset


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
