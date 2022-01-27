"""Timeseries"""
import sqlalchemy as sqla

from bemserver_core.database import Base
from bemserver_core.authorization import AuthMixin, auth, Relation
from bemserver_core.model.campaigns import TimeseriesGroupByCampaign


class Timeseries(AuthMixin, Base):
    __tablename__ = "timeseries"

    id = sqla.Column(sqla.Integer, primary_key=True)
    name = sqla.Column(sqla.String(80), unique=True, nullable=False)
    description = sqla.Column(sqla.String(500))
    unit = sqla.Column(sqla.String(20))
    min_value = sqla.Column(sqla.Float)
    max_value = sqla.Column(sqla.Float)
    group_id = sqla.Column(sqla.ForeignKey("timeseries_groups.id"), nullable=False)
    group = sqla.orm.relationship("TimeseriesGroup")

    @classmethod
    def register_class(cls):
        auth.register_class(
            cls,
            fields={
                "group": Relation(
                    kind="one",
                    other_type="TimeseriesGroup",
                    my_field="group_id",
                    other_field="id",
                ),
            },
        )

    @classmethod
    def get(cls, *, campaign_id=None, **kwargs):
        query = super().get(**kwargs)
        if campaign_id:
            query = (
                query.join(Timeseries.group)
                .join(TimeseriesGroupByCampaign)
                .filter(TimeseriesGroupByCampaign.campaign_id == campaign_id)
            )
        return query


class TimeseriesGroup(AuthMixin, Base):
    __tablename__ = "timeseries_groups"

    id = sqla.Column(sqla.Integer, primary_key=True)
    name = sqla.Column(sqla.String(80), unique=True, nullable=False)

    @classmethod
    def register_class(cls):
        auth.register_class(
            cls,
            fields={
                "timeseries_groups_by_campaigns": Relation(
                    kind="many",
                    other_type="TimeseriesGroupByCampaign",
                    my_field="id",
                    other_field="timeseries_group_id",
                ),
            },
        )


class TimeseriesGroupByUser(AuthMixin, Base):
    """TimeseriesGroup x User associations"""

    __tablename__ = "timeseries_groups_by_users"
    __table_args__ = (sqla.UniqueConstraint("user_id", "timeseries_group_id"),)

    id = sqla.Column(sqla.Integer, primary_key=True)
    user_id = sqla.Column(sqla.ForeignKey("users.id"), nullable=False)
    timeseries_group_id = sqla.Column(
        sqla.ForeignKey("timeseries_groups.id"), nullable=False
    )

    @classmethod
    def register_class(cls):
        auth.register_class(
            cls,
            fields={
                "user": Relation(
                    kind="one",
                    other_type="User",
                    my_field="user_id",
                    other_field="id",
                ),
            },
        )
