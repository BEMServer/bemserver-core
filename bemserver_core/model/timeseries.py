"""Timeseries"""
import sqlalchemy as sqla
import sqlalchemy.orm as sqlaorm
from sqlalchemy.ext.hybrid import hybrid_property

from bemserver_core.database import Base
from bemserver_core.authorization import AuthMixin, auth, Relation
from bemserver_core.model.users import UserGroup, UserByUserGroup
from bemserver_core.model.campaigns import UserGroupByCampaign


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
    __table_args__ = (sqla.UniqueConstraint("name", "_campaign_id"),)

    id = sqla.Column(sqla.Integer, primary_key=True)
    name = sqla.Column(sqla.String(80), nullable=False)
    description = sqla.Column(sqla.String(500))
    unit_symbol = sqla.Column(sqla.String(20))
    campaign = sqla.orm.relationship("Campaign")
    campaign_scope = sqla.orm.relationship("CampaignScope")
    timeseries_by_data_states = sqla.orm.relationship("TimeseriesByDataState")

    # Use getter/setter to prevent modifying campaign / campaign_scope after commit
    @sqlaorm.declared_attr
    def _campaign_id(cls):
        return sqla.Column(
            sqla.Integer, sqla.ForeignKey("campaigns.id"), nullable=False
        )

    @hybrid_property
    def campaign_id(self):
        return self._campaign_id

    @campaign_id.setter
    def campaign_id(self, campaign_id):
        if self.id is not None:
            raise AttributeError("campaign_id cannot be modified")
        self._campaign_id = campaign_id

    @sqlaorm.declared_attr
    def _campaign_scope_id(cls):
        return sqla.Column(
            sqla.Integer, sqla.ForeignKey("campaign_scopes.id"), nullable=False
        )

    @hybrid_property
    def campaign_scope_id(self):
        return self._campaign_scope_id

    @campaign_scope_id.setter
    def campaign_scope_id(self, campaign_scope_id):
        if self.id is not None:
            raise AttributeError("campaign_scope_id cannot be modified")
        self._campaign_scope_id = campaign_scope_id

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
                "campaign_scope": Relation(
                    kind="one",
                    other_type="CampaignScope",
                    my_field="campaign_scope_id",
                    other_field="id",
                ),
            },
        )

    @classmethod
    def get(cls, *, user_id=None, **kwargs):
        query = super().get(**kwargs)
        if user_id:
            query = (
                query.join(cls.campaign)
                .join(UserGroupByCampaign)
                .join(UserGroup)
                .join(UserByUserGroup)
                .filter(UserByUserGroup.user_id == user_id)
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


@sqla.event.listens_for(TimeseriesDataState.__table__, "after_create")
def _insert_initial_timeseries_data_states(target, connection, **kwargs):
    connection.execute(target.insert(), {"name": "Raw"})
    connection.execute(target.insert(), {"name": "Clean"})
