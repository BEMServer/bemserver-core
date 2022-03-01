"""Timeseries"""
import sqlalchemy as sqla

from bemserver_core.database import Base
from bemserver_core.authorization import AuthMixin, auth, Relation
from bemserver_core.model.campaigns import TimeseriesGroupByCampaign


class TimeseriesProperty(AuthMixin, Base):
    __tablename__ = "timeseries_properties"

    id = sqla.Column(sqla.Integer, primary_key=True)
    name = sqla.Column(sqla.String(80), unique=True, nullable=False)
    description = sqla.Column(sqla.String(250))


class TimeseriesDataState(AuthMixin, Base):
    __tablename__ = "timeseries_data_states"

    id = sqla.Column(sqla.Integer, primary_key=True)
    name = sqla.Column(sqla.String(80), unique=True, nullable=False)


class Timeseries(AuthMixin, Base):
    __tablename__ = "timeseries"

    id = sqla.Column(sqla.Integer, primary_key=True)
    name = sqla.Column(sqla.String(80), unique=True, nullable=False)
    description = sqla.Column(sqla.String(500))
    unit_symbol = sqla.Column(sqla.String(20))
    group_id = sqla.Column(sqla.ForeignKey("timeseries_groups.id"), nullable=False)
    group = sqla.orm.relationship("TimeseriesGroup")
    timeseries_by_data_states = sqla.orm.relationship("TimeseriesByDataState")

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
    def get(cls, *, campaign_id=None, user_id=None, **kwargs):
        query = super().get(**kwargs)
        if campaign_id or user_id:
            query = query.join(cls.group)
            if campaign_id:
                query = query.join(TimeseriesGroupByCampaign).filter(
                    TimeseriesGroupByCampaign.campaign_id == campaign_id
                )
            if user_id:
                query = query.join(TimeseriesGroupByUser).filter(
                    TimeseriesGroupByUser.user_id == user_id
                )
        return query

    def get_timeseries_by_data_states(self, data_state):
        return next(
            (
                ts
                for ts in self.timeseries_by_data_states
                if ts.data_state == data_state
            ),
            None,
        )


class TimeseriesPropertyData(AuthMixin, Base):
    """Timeseries property data"""

    __tablename__ = "timeseries_property_data"
    __table_args__ = (sqla.UniqueConstraint("timeseries_id", "property_id"),)

    id = sqla.Column(sqla.Integer, primary_key=True)
    timeseries_id = sqla.Column(sqla.ForeignKey("timeseries.id"), nullable=False)
    property_id = sqla.Column(
        sqla.ForeignKey("timeseries_properties.id"), nullable=False
    )
    value = sqla.Column(sqla.Float)

    @classmethod
    def register_class(cls):
        auth.register_class(
            cls,
            fields={
                "timeseries": Relation(
                    kind="one",
                    other_type="Timeseries",
                    my_field="timeseries_id",
                    other_field="id",
                ),
            },
        )


class TimeseriesByDataState(AuthMixin, Base):
    __tablename__ = "timeseries_by_data_states"
    __table_args__ = (sqla.UniqueConstraint("timeseries_id", "data_state_id"),)

    id = sqla.Column(sqla.Integer, primary_key=True)
    timeseries_id = sqla.Column(sqla.ForeignKey("timeseries.id"), nullable=False)
    timeseries = sqla.orm.relationship(
        "Timeseries", back_populates="timeseries_by_data_states"
    )
    data_state_id = sqla.Column(
        sqla.ForeignKey("timeseries_data_states.id"), nullable=False
    )
    data_state = sqla.orm.relationship("TimeseriesDataState")

    @classmethod
    def register_class(cls):
        auth.register_class(
            cls,
            fields={
                "timeseries": Relation(
                    kind="one",
                    other_type="Timeseries",
                    my_field="timeseries_id",
                    other_field="id",
                ),
            },
        )


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
                "timeseries_groups_by_users": Relation(
                    kind="many",
                    other_type="TimeseriesGroupByUser",
                    my_field="id",
                    other_field="timeseries_group_id",
                ),
            },
        )


class TimeseriesGroupByUser(AuthMixin, Base):
    """TimeseriesGroup x User associations

    Users associated with a TimeseriesGroup have R/W permissions on timeseries
    """

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


@sqla.event.listens_for(TimeseriesDataState.__table__, "after_create")
def _insert_initial_timeseries_data_states(target, connection, **kwargs):
    connection.execute(target.insert(), {"name": "Raw"})
    connection.execute(target.insert(), {"name": "Clean"})
