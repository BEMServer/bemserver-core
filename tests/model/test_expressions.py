"""Expressions tests"""

import pytest

from bemserver_core.authorization import CurrentUser, OpenBar
from bemserver_core.database import db
from bemserver_core.exceptions import (
    BEMServerAuthorizationError,
    BEMServerCoreExpressionValidationError,
)
from bemserver_core.model import (
    Expression,
    ExpressionVariable,
)

DUMMY_ID = 69
DUMMY_NAME = "Dummy name"


class TestExpressionModel:
    @pytest.mark.parametrize("campaigns", (2,), indirect=True)
    @pytest.mark.parametrize("timeseries", (3,), indirect=True)
    def test_expressions_delete_cascade(self, users, timeseries, campaign_scopes):
        admin_user = users[0]
        cs_1 = campaign_scopes[0]
        ts_1 = timeseries[0]
        ts_3 = timeseries[2]

        with OpenBar():
            expr_1 = Expression.new(
                campaign_scope_id=cs_1.id,
                expr="2*a",
                timeseries_id=ts_1.id,
            )
            db.session.flush()
            ExpressionVariable.new(
                campaign_scope_id=cs_1.id,
                expression_id=expr_1.id,
                name="a",
                timeseries_id=ts_3.id,
                aggregation="avg",
            )
            db.session.flush()

        with CurrentUser(admin_user):
            assert len(list(ExpressionVariable.get())) == 1

            expr_1.delete()
            db.session.flush()
            assert len(list(ExpressionVariable.get())) == 0

    @pytest.mark.usefixtures("as_admin")
    def test_expression_validate(self, timeseries):
        ts_1 = timeseries[0]

        expr_1 = Expression.new(
            expr="2*a",
            timeseries_id=ts_1.id,
        )
        db.session.flush()
        ExpressionVariable.new(
            expression_id=expr_1.id,
            name="a",
            timeseries_id=timeseries[1].id,
            aggregation="avg",
        )
        db.session.flush()
        expr_1.validate()

        # Missing variable
        expr_2 = Expression.new(
            expr="2*a",
            timeseries_id=ts_1.id,
        )
        db.session.flush()
        with pytest.raises(BEMServerCoreExpressionValidationError):
            expr_2.validate()

        # Invalid expression
        expr_3 = Expression.new(
            expr="2a",
            timeseries_id=ts_1.id,
        )
        db.session.flush()
        ExpressionVariable.new(
            expression_id=expr_3.id,
            name="a",
            timeseries_id=timeseries[1].id,
            aggregation="avg",
        )
        db.session.flush()
        with pytest.raises(BEMServerCoreExpressionValidationError):
            expr_3.validate()

    def test_expression_authorizations_as_admin(
        self, users, campaign_scopes, timeseries
    ):
        admin_user = users[0]
        assert admin_user.is_admin
        cs_1 = campaign_scopes[0]
        ts_1 = timeseries[0]

        with CurrentUser(admin_user):
            expr_1 = Expression.new(
                campaign_scope_id=cs_1.id,
                expr="2*a",
                timeseries_id=ts_1.id,
            )
            db.session.flush()

            expr = Expression.get_by_id(expr_1.id)
            assert expr.id == expr_1.id
            assert expr.expr == expr_1.expr
            expr_l = list(Expression.get())
            assert len(expr_l) == 1
            assert expr_l[0].id == expr_1.id
            expr.update(expr="2+a")
            expr.delete()
            db.session.flush()

    @pytest.mark.usefixtures("users_by_user_groups")
    @pytest.mark.usefixtures("user_groups_by_campaign_scopes")
    def test_expression_authorizations_as_user(
        self,
        users,
        campaign_scopes,
        timeseries,
    ):
        user_1 = users[1]
        assert not user_1.is_admin
        cs_1 = campaign_scopes[0]
        cs_2 = campaign_scopes[1]
        ts_1 = timeseries[0]
        ts_2 = timeseries[1]

        with OpenBar():
            expr_1 = Expression.new(
                campaign_scope_id=cs_1.id,
                expr="2*a",
                timeseries_id=ts_1.id,
            )
            expr_2 = Expression.new(
                campaign_scope_id=cs_2.id,
                expr="2*a",
                timeseries_id=ts_2.id,
            )
            db.session.flush()

        with CurrentUser(user_1):
            with pytest.raises(BEMServerAuthorizationError):
                Expression.new(
                    campaign_scope_id=cs_2.id,
                    expr="2*a",
                    timeseries_id=ts_2.id,
                )

            expr = Expression.get_by_id(expr_2.id)
            expr_list = list(Expression.get())
            assert len(expr_list) == 1
            assert expr_list[0].id == expr_2.id
            with pytest.raises(BEMServerAuthorizationError):
                Expression.get_by_id(expr_1.id)
            with pytest.raises(BEMServerAuthorizationError):
                expr.update(expr="2+a")
            with pytest.raises(BEMServerAuthorizationError):
                expr.delete()


class TestExpressionVariableModel:
    def test_expression_variable_authorizations_as_admin(
        self, users, campaign_scopes, timeseries
    ):
        admin_user = users[0]
        assert admin_user.is_admin
        cs_1 = campaign_scopes[0]
        ts_1 = timeseries[0]

        with CurrentUser(admin_user):
            expr_1 = Expression.new(
                campaign_scope_id=cs_1.id,
                expr="2*a",
                timeseries_id=ts_1.id,
            )
            db.session.flush()
            expr_v_1 = ExpressionVariable.new(
                campaign_scope_id=cs_1.id,
                expression_id=expr_1.id,
                name="a",
                timeseries_id=ts_1.id,
                aggregation="avg",
            )
            db.session.flush()

            expr_v = ExpressionVariable.get_by_id(expr_v_1.id)
            assert expr_v.id == expr_v_1.id
            assert expr_v.name == expr_v_1.name
            expr_v_l = list(ExpressionVariable.get())
            assert len(expr_v_l) == 1
            assert expr_v_l[0].id == expr_v_1.id
            expr_v.update(expr_v="2+a")
            expr_v.delete()
            db.session.flush()

    @pytest.mark.usefixtures("users_by_user_groups")
    @pytest.mark.usefixtures("user_groups_by_campaign_scopes")
    def test_expression_variable_authorizations_as_user(
        self,
        users,
        campaign_scopes,
        timeseries,
    ):
        user_1 = users[1]
        assert not user_1.is_admin
        cs_1 = campaign_scopes[0]
        cs_2 = campaign_scopes[1]
        ts_1 = timeseries[0]
        ts_2 = timeseries[1]

        with OpenBar():
            expr_1 = Expression.new(
                campaign_scope_id=cs_1.id,
                expr="2*a",
                timeseries_id=ts_1.id,
            )
            expr_2 = Expression.new(
                campaign_scope_id=cs_2.id,
                expr="2*a",
                timeseries_id=ts_2.id,
            )
            expr_3 = Expression.new(
                campaign_scope_id=cs_2.id,
                expr="2*a",
                timeseries_id=ts_2.id,
            )
            db.session.flush()
            ExpressionVariable.new(
                campaign_scope_id=cs_1.id,
                expression_id=expr_1.id,
                name="a",
                timeseries_id=ts_1.id,
                aggregation="avg",
            )
            expr_var_2 = ExpressionVariable.new(
                campaign_scope_id=cs_2.id,
                expression_id=expr_2.id,
                name="a",
                timeseries_id=ts_2.id,
                aggregation="avg",
            )
            db.session.flush()

        with CurrentUser(user_1):
            with pytest.raises(BEMServerAuthorizationError):
                ExpressionVariable.new(
                    expression_id=expr_3.id,
                    name="a",
                    timeseries_id=ts_2.id,
                    aggregation="avg",
                )

            ExpressionVariable.get_by_id(expr_var_2.id)
            expr_var_list = list(ExpressionVariable.get())
            assert len(expr_var_list) == 1
            assert expr_var_list[0].id == expr_var_2.id
            with pytest.raises(BEMServerAuthorizationError):
                ExpressionVariable.get_by_id(expr_1.id)
            with pytest.raises(BEMServerAuthorizationError):
                expr_var_2.update(name="b")
            with pytest.raises(BEMServerAuthorizationError):
                expr_var_2.delete()
