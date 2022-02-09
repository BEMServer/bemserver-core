"""Timeseries"""
import sqlalchemy as sqla

from bemserver_core.database import Base
from bemserver_core.authorization import AuthMixin, auth, Relation
from bemserver_core.model.campaigns import TimeseriesClusterGroupByCampaign


class TimeseriesDataState(AuthMixin, Base):
    __tablename__ = "timeseries_data_states"

    id = sqla.Column(sqla.Integer, primary_key=True)
    name = sqla.Column(sqla.String(80), unique=True, nullable=False)


class TimeseriesCluster(AuthMixin, Base):
    __tablename__ = "timeseries_clusters"

    id = sqla.Column(sqla.Integer, primary_key=True)
    name = sqla.Column(sqla.String(80), unique=True, nullable=False)
    description = sqla.Column(sqla.String(500))
    unit_symbol = sqla.Column(sqla.String(20))
    group_id = sqla.Column(
        sqla.ForeignKey("timeseries_cluster_groups.id"), nullable=False
    )
    group = sqla.orm.relationship("TimeseriesClusterGroup")

    @classmethod
    def register_class(cls):
        auth.register_class(
            cls,
            fields={
                "group": Relation(
                    kind="one",
                    other_type="TimeseriesClusterGroup",
                    my_field="group_id",
                    other_field="id",
                ),
            },
        )

    @classmethod
    def get(cls, *, campaign_id=None, user_id=None, **kwargs):
        query = super().get(**kwargs)
        if campaign_id or user_id:
            query = query.join(cls.group)
            if campaign_id:
                query = query.join(TimeseriesClusterGroupByCampaign).filter(
                    TimeseriesClusterGroupByCampaign.campaign_id == campaign_id
                )
            if user_id:
                query = query.join(TimeseriesClusterGroupByUser).filter(
                    TimeseriesClusterGroupByUser.user_id == user_id
                )
        return query


class Timeseries(AuthMixin, Base):
    __tablename__ = "timeseries"
    __table_args__ = (sqla.UniqueConstraint("cluster_id", "data_state_id"),)

    id = sqla.Column(sqla.Integer, primary_key=True)
    cluster_id = sqla.Column(sqla.ForeignKey("timeseries_clusters.id"), nullable=False)
    cluster = sqla.orm.relationship("TimeseriesCluster")
    data_state_id = sqla.Column(
        sqla.ForeignKey("timeseries_data_states.id"), nullable=False
    )
    data_state = sqla.orm.relationship("TimeseriesDataState")

    @classmethod
    def register_class(cls):
        auth.register_class(
            cls,
            fields={
                "cluster": Relation(
                    kind="one",
                    other_type="TimeseriesCluster",
                    my_field="cluster_id",
                    other_field="id",
                ),
            },
        )


class TimeseriesClusterGroup(AuthMixin, Base):
    __tablename__ = "timeseries_cluster_groups"

    id = sqla.Column(sqla.Integer, primary_key=True)
    name = sqla.Column(sqla.String(80), unique=True, nullable=False)

    @classmethod
    def register_class(cls):
        auth.register_class(
            cls,
            fields={
                "timeseries_cluster_groups_by_campaigns": Relation(
                    kind="many",
                    other_type="TimeseriesClusterGroupByCampaign",
                    my_field="id",
                    other_field="timeseries_cluster_group_id",
                ),
                "timeseries_cluster_groups_by_users": Relation(
                    kind="many",
                    other_type="TimeseriesClusterGroupByUser",
                    my_field="id",
                    other_field="timeseries_cluster_group_id",
                ),
            },
        )


class TimeseriesClusterGroupByUser(AuthMixin, Base):
    """TimeseriesClusterGroup x User associations

    Users associated with a TimeseriesClusterGroup have R/W permissions on timeseries
    """

    __tablename__ = "timeseries_cluster_groups_by_users"
    __table_args__ = (sqla.UniqueConstraint("user_id", "timeseries_cluster_group_id"),)

    id = sqla.Column(sqla.Integer, primary_key=True)
    user_id = sqla.Column(sqla.ForeignKey("users.id"), nullable=False)
    timeseries_cluster_group_id = sqla.Column(
        sqla.ForeignKey("timeseries_cluster_groups.id"), nullable=False
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


@sqla.event.listens_for(TimeseriesDataState.__table__, "after_create")
def _insert_initial_timeseries_data_states(target, connection, **kwargs):
    connection.execute(target.insert(), {"name": "Raw"})
    connection.execute(target.insert(), {"name": "Clean"})
