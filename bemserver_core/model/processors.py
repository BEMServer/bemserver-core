"""Processor"""
import sqlalchemy as sqla

from bemserver_core.database import Base
from bemserver_core.authorization import AuthMixin


class Processor(AuthMixin, Base):
    __tablename__ = "processors"

    id = sqla.Column(sqla.String(40), primary_key=True)


class ProcessorByCampaign(AuthMixin, Base):
    """Processor x Campaign associations"""

    __tablename__ = "processors_by_campaigns"
    __table_args__ = (sqla.UniqueConstraint("campaign_id", "processor_id"),)

    id = sqla.Column(sqla.Integer, primary_key=True)
    campaign_id = sqla.Column(sqla.ForeignKey("campaigns.id"), nullable=False)
    processor_id = sqla.Column(sqla.ForeignKey("processors.id"), nullable=False)

    campaign = sqla.orm.relationship(
        "Campaign",
        backref=sqla.orm.backref(
            "processors_by_campaigns", cascade="all, delete-orphan"
        ),
    )
    processor = sqla.orm.relationship(
        Processor,
        backref=sqla.orm.backref(
            "processors_by_campaigns", cascade="all, delete-orphan"
        ),
    )
