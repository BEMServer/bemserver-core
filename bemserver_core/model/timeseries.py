"""Timeseries"""
import sqlalchemy as sqla

from bemserver_core.database import Base
from bemserver_core.authorization import AuthMixin, auth, Relation
from bemserver_core.model.campaigns import TimeseriesByCampaign


class Timeseries(AuthMixin, Base):
    __tablename__ = "timeseries"

    id = sqla.Column(sqla.Integer, primary_key=True)
    name = sqla.Column(sqla.String(80), unique=True, nullable=False)
    description = sqla.Column(sqla.String(500))
    unit = sqla.Column(sqla.String(20))
    min_value = sqla.Column(sqla.Float)
    max_value = sqla.Column(sqla.Float)

    @classmethod
    def register_class(cls):
        auth.register_class(
            cls,
            fields={
                "timeseries_by_campaigns": Relation(
                    kind="many",
                    other_type="TimeseriesByCampaign",
                    my_field="id",
                    other_field="timeseries_id",
                ),
            },
        )

    @classmethod
    def get(cls, *, campaign_id=None, **kwargs):
        query = super().get(**kwargs)
        if campaign_id:
            query = query.join(TimeseriesByCampaign).filter_by(campaign_id=campaign_id)
        return query
