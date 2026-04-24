"""Expressions"""

import sqlalchemy as sqla

from bemserver_core import expression_eval
from bemserver_core.authorization import AuthMgrMixin
from bemserver_core.common import AggregationFunctionsEnum
from bemserver_core.database import Base, db
from bemserver_core.exceptions import BEMServerCoreCampaignScopeError

from .campaigns import CampaignScope
from .timeseries import Timeseries


class Expression(AuthMgrMixin, Base):
    __tablename__ = "expressions"

    id = sqla.Column(sqla.Integer, primary_key=True)
    campaign_scope_id = sqla.Column(sqla.ForeignKey("c_scopes.id"), nullable=False)
    expr = sqla.Column(sqla.String, nullable=False)
    timeseries_id = sqla.Column(sqla.ForeignKey("timeseries.id"), nullable=False)
    # TODO: validate?
    unit_symbol = sqla.Column(sqla.String(20))

    campaign_scope = sqla.orm.relationship(
        "CampaignScope",
        backref=sqla.orm.backref("expressions", cascade="all, delete-orphan"),
    )
    timeseries = sqla.orm.relationship(
        "Timeseries",
        backref=sqla.orm.backref("expressions", cascade="all, delete-orphan"),
    )

    def validate(self):
        expression_eval.validate(self.expr, [v.name for v in self.variables])

    def _before_flush(self):
        # Ensure TS is in Campaign scope
        if self.timeseries_id and self.campaign_scope_id:
            timeseries = Timeseries.get_by_id(self.timeseries_id)
            if timeseries.campaign_scope_id != self.campaign_scope_id:
                raise BEMServerCoreCampaignScopeError(
                    "Expression and timeseries must be in same campaign scope"
                )

    @classmethod
    def authorize_query(cls, actor, query):
        return CampaignScope.authorize_query(actor, query.join(CampaignScope))

    def authorize_read(self, actor):
        campaign_scope = (
            db.session.query(CampaignScope)
            .filter(CampaignScope.id == self.campaign_scope_id)
            .one()
        )
        return campaign_scope.is_member(actor)


class ExpressionVariable(AuthMgrMixin, Base):
    __tablename__ = "expr_vars"
    __table_args__ = (sqla.UniqueConstraint("expression_id", "name"),)

    id = sqla.Column(sqla.Integer, primary_key=True)
    campaign_scope_id = sqla.Column(sqla.ForeignKey("c_scopes.id"), nullable=False)
    expression_id = sqla.Column(sqla.ForeignKey("expressions.id"), nullable=False)

    name = sqla.Column(sqla.String, nullable=False)
    timeseries_id = sqla.Column(sqla.ForeignKey("timeseries.id"), nullable=False)
    aggregation = sqla.Column(
        sqla.Enum(AggregationFunctionsEnum, name="aggfuncsenum"),
        nullable=False,
        default="avg",
    )
    # TODO: validate?
    unit_symbol = sqla.Column(sqla.String(20))

    campaign_scope = sqla.orm.relationship(
        "CampaignScope",
        backref=sqla.orm.backref("expressions_variables", cascade="all, delete-orphan"),
    )
    # TODO: cascade delete? invalidate expression?
    expression = sqla.orm.relationship(
        Expression,
        backref=sqla.orm.backref("variables", cascade="all, delete-orphan"),
    )
    timeseries = sqla.orm.relationship(
        "Timeseries",
        backref=sqla.orm.backref("expression_variables", cascade="all, delete-orphan"),
    )

    def _before_flush(self):
        # Ensure Expression and Timeseries are in Campaign scope
        if self.timeseries_id and self.expression_id and self.campaign_scope_id:
            timeseries = Timeseries.get_by_id(self.timeseries_id)
            expression = Expression.get_by_id(self.expression_id)
            if timeseries.campaign_scope_id != self.campaign_scope_id:
                raise BEMServerCoreCampaignScopeError(
                    "Expression variable and timeseries must be in same campaign scope"
                )
            if expression.campaign_scope_id != self.campaign_scope_id:
                raise BEMServerCoreCampaignScopeError(
                    "Expression variable and expression must be in same campaign scope"
                )

    @classmethod
    def authorize_query(cls, actor, query):
        return CampaignScope.authorize_query(actor, query.join(CampaignScope))

    def authorize_read(self, actor):
        campaign_scope = (
            db.session.query(CampaignScope)
            .filter(CampaignScope.id == self.campaign_scope_id)
            .one()
        )
        return campaign_scope.is_member(actor)
