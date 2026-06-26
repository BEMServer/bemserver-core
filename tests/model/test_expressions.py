"""Expressions tests"""

import pytest

import sqlalchemy as sqla

from bemserver_core.authorization import CurrentUser, OpenBar
from bemserver_core.database import db
from bemserver_core.exceptions import (
    BEMServerAuthorizationError,
    BEMServerCoreExpressionValidationError,
    BEMServerCoreIntegrityError,
)
from bemserver_core.model import (
    Expression,
    ExpressionVariable,
    TimeseriesExpression,
)

DUMMY_ID = 69
DUMMY_NAME = "Dummy name"


class TestExpressionModel:
    def test_expressions_delete_cascade(self, users, timeseries, campaign_scopes):
        admin_user = users[0]
        cs_1 = campaign_scopes[0]
        ts_1 = timeseries[0]

        with OpenBar():
            expr_1 = Expression.new(
                campaign_scope_id=cs_1.id,
                name="2 times a",
                expr="2*a",
            )
            db.session.flush()
            ExpressionVariable.new(
                campaign_scope_id=cs_1.id,
                expression_id=expr_1.id,
                name="a",
                timeseries_id=ts_1.id,
                aggregation="avg",
            )
            TimeseriesExpression.new(
                campaign_scope_id=cs_1.id,
                expression_id=expr_1.id,
                timeseries_id=ts_1.id,
                src_data_state_id=1,
                dest_data_state_id=1,
                bucket_width_value=1,
                bucket_width_unit="day",
            )
            db.session.flush()

        with CurrentUser(admin_user):
            assert len(list(ExpressionVariable.get())) == 1
            assert len(list(TimeseriesExpression.get())) == 1

            expr_1.delete()
            db.session.flush()
            assert len(list(ExpressionVariable.get())) == 0
            assert len(list(TimeseriesExpression.get())) == 0

    @pytest.mark.usefixtures("as_admin")
    def test_expression_validate(self, campaign_scopes, timeseries):
        cs_1 = campaign_scopes[0]
        ts_1 = timeseries[0]

        expr_1 = Expression.new(
            campaign_scope_id=cs_1.id,
            name="2 times a",
            expr="2*a",
        )
        db.session.flush()
        ExpressionVariable.new(
            campaign_scope_id=cs_1.id,
            expression_id=expr_1.id,
            name="a",
            timeseries_id=ts_1.id,
            aggregation="avg",
        )
        db.session.flush()
        expr_1.validate()

        # Missing variable
        expr_2 = Expression.new(
            campaign_scope_id=cs_1.id,
            name="2 times a",
            expr="2*a",
        )
        db.session.flush()
        with pytest.raises(BEMServerCoreExpressionValidationError):
            expr_2.validate()

        # Invalid expression
        expr_3 = Expression.new(
            campaign_scope_id=cs_1.id,
            name="2 times a",
            expr="2a",
        )
        db.session.flush()
        ExpressionVariable.new(
            campaign_scope_id=cs_1.id,
            expression_id=expr_3.id,
            name="a",
            timeseries_id=ts_1.id,
            aggregation="avg",
        )
        db.session.flush()
        with pytest.raises(BEMServerCoreExpressionValidationError):
            expr_3.validate()

    @pytest.mark.usefixtures("as_admin")
    def test_expression_read_only_fields(self, campaign_scopes, timeseries):
        cs_1 = campaign_scopes[0]
        cs_2 = campaign_scopes[1]

        expr_1 = Expression.new(
            campaign_scope_id=cs_2.id,
            name="2 times a",
            expr="2*a",
        )
        db.session.commit()

        expr_1.update(campaign_scope_id=cs_1.id)
        with pytest.raises(
            sqla.exc.IntegrityError,
            match="campaign_scope_id cannot be modified",
        ):
            db.session.flush()
        db.session.rollback()

    def test_expression_authorizations_as_admin(
        self, users, campaign_scopes, timeseries
    ):
        admin_user = users[0]
        assert admin_user.is_admin
        cs_1 = campaign_scopes[0]

        with CurrentUser(admin_user):
            expr_1 = Expression.new(
                campaign_scope_id=cs_1.id,
                name="2 times a",
                expr="2*a",
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

        with OpenBar():
            expr_1 = Expression.new(
                campaign_scope_id=cs_1.id,
                name="2 times a",
                expr="2*a",
            )
            expr_2 = Expression.new(
                campaign_scope_id=cs_2.id,
                name="2 times a",
                expr="2*a",
            )
            db.session.flush()

        with CurrentUser(user_1):
            with pytest.raises(BEMServerAuthorizationError):
                Expression.new(
                    campaign_scope_id=cs_2.id,
                    name="2 times a",
                    expr="2*a",
                )

            with pytest.raises(BEMServerAuthorizationError):
                Expression.from_dict(
                    {
                        "name": "2 times a",
                        "expr": "2*a",
                        "unit_symbol": "kW",
                        "campaign_scope_id": cs_2.id,
                        "variables": [],
                    }
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

    @pytest.mark.usefixtures("as_admin")
    def test_expression_from_dict_to_dict(self, campaign_scopes, timeseries):
        cs_1 = campaign_scopes[0]
        ts_1 = timeseries[0]

        expr_dict = {
            "name": "2 times a",
            "expr": "2*a",
            "unit_symbol": "kW",
            "campaign_scope_id": cs_1.id,
            "variables": [
                {
                    "name": "a",
                    "unit_symbol": "kW",
                    "timeseries_id": ts_1.id,
                    "aggregation": "avg",
                }
            ],
        }

        expr = Expression.from_dict(expr_dict)

        assert expr.name == "2 times a"
        assert expr.expr == "2*a"
        assert expr.unit_symbol == "kW"
        assert expr.campaign_scope_id == cs_1.id
        assert len(expr.variables) == 1
        expr_var = expr.variables[0]
        assert expr_var.name == "a"
        assert expr_var.unit_symbol == "kW"
        assert expr_var.timeseries_id == ts_1.id
        assert expr_var.aggregation == "avg"

        assert expr.to_dict() == expr_dict


class TestExpressionVariableModel:
    @pytest.mark.usefixtures("as_admin")
    def test_expression_variable_read_only_fields(self, campaign_scopes, timeseries):
        cs_1 = campaign_scopes[0]
        cs_2 = campaign_scopes[1]
        ts_1 = timeseries[0]
        ts_2 = timeseries[1]

        expr_1 = Expression.new(
            campaign_scope_id=cs_1.id,
            name="2 times a",
            expr="2*a",
        )
        # Same scope as expr_v_1: used to test expression_id read-only
        expr_2 = Expression.new(
            campaign_scope_id=cs_1.id,
            name="3 times a",
            expr="3*a",
        )
        # Different scope: used to test campaign_scope_id read-only
        expr_3 = Expression.new(
            campaign_scope_id=cs_2.id,
            name="4 times a",
            expr="4*a",
        )
        expr_v_1 = ExpressionVariable.new(
            campaign_scope_id=cs_1.id,
            expression_id=expr_1.id,
            name="a",
            timeseries_id=ts_1.id,
            aggregation="avg",
        )
        db.session.commit()

        # Update expression_id alongside so _before_flush doesn't raise
        expr_v_1.update(
            campaign_scope_id=cs_2.id,
            expression_id=expr_3.id,
            timeseries_id=ts_2.id,
        )
        with pytest.raises(
            sqla.exc.IntegrityError,
            match="campaign_scope_id cannot be modified",
        ):
            db.session.flush()
        db.session.rollback()

        # expression_id update alone: expr_2 is in cs_1, _before_flush passes
        expr_v_1.update(expression_id=expr_2.id)
        with pytest.raises(
            sqla.exc.IntegrityError,
            match="expression_id cannot be modified",
        ):
            db.session.flush()
        db.session.rollback()

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
                name="2 times a",
                expr="2*a",
            )
            db.session.commit()
            with pytest.raises(BEMServerCoreIntegrityError):
                ExpressionVariable.new(
                    campaign_scope_id=cs_1.id,
                    expression_id=expr_1.id,
                    name="a",
                    timeseries_id=DUMMY_ID,
                    aggregation="avg",
                )
                db.session.flush()
            db.session.rollback()
            with pytest.raises(BEMServerCoreIntegrityError):
                ExpressionVariable.new(
                    campaign_scope_id=cs_1.id,
                    expression_id=DUMMY_ID,
                    name="a",
                    timeseries_id=ts_1.id,
                    aggregation="avg",
                )
                db.session.flush()
            db.session.rollback()
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
                name="2 times a",
                expr="2*a",
            )
            expr_2 = Expression.new(
                campaign_scope_id=cs_2.id,
                name="2 times a",
                expr="2*a",
            )
            expr_3 = Expression.new(
                campaign_scope_id=cs_2.id,
                name="2 times a",
                expr="2*a",
            )
            db.session.flush()
            ExpressionVariable.new(
                campaign_scope_id=cs_1.id,
                expression_id=expr_1.id,
                name="a",
                timeseries_id=ts_1.id,
                aggregation="avg",
            )
            expr_v_2 = ExpressionVariable.new(
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

            ExpressionVariable.get_by_id(expr_v_2.id)
            expr_v_list = list(ExpressionVariable.get())
            assert len(expr_v_list) == 1
            assert expr_v_list[0].id == expr_v_2.id
            with pytest.raises(BEMServerAuthorizationError):
                ExpressionVariable.get_by_id(expr_1.id)
            with pytest.raises(BEMServerAuthorizationError):
                expr_v_2.update(name="b")
            with pytest.raises(BEMServerAuthorizationError):
                expr_v_2.delete()


class TestTimeseriesExpressionModel:
    @pytest.mark.usefixtures("as_admin")
    def test_timeseries_expression_read_only_fields(self, campaign_scopes, timeseries):
        cs_1 = campaign_scopes[0]
        cs_2 = campaign_scopes[1]
        ts_1 = timeseries[0]
        ts_2 = timeseries[1]

        expr_1 = Expression.new(
            campaign_scope_id=cs_1.id,
            name="2 times a",
            expr="2*a",
        )
        # Same scope as expr_v_1: used to test expression_id read-only
        expr_2 = Expression.new(
            campaign_scope_id=cs_1.id,
            name="3 times a",
            expr="3*a",
        )
        # Different scope: used to test campaign_scope_id read-only
        expr_3 = Expression.new(
            campaign_scope_id=cs_2.id,
            name="4 times a",
            expr="4*a",
        )
        ts_expr_1 = TimeseriesExpression.new(
            campaign_scope_id=cs_1.id,
            expression_id=expr_1.id,
            timeseries_id=ts_1.id,
            src_data_state_id=1,
            dest_data_state_id=1,
            bucket_width_value=1,
            bucket_width_unit="day",
        )
        db.session.commit()

        # Update expression_id alongside so _before_flush doesn't raise
        ts_expr_1.update(
            campaign_scope_id=cs_2.id,
            expression_id=expr_3.id,
            timeseries_id=ts_2.id,
        )
        with pytest.raises(
            sqla.exc.IntegrityError,
            match="campaign_scope_id cannot be modified",
        ):
            db.session.flush()
        db.session.rollback()

        # expression_id update alone: expr_2 is in cs_1, _before_flush passes
        ts_expr_1.update(expression_id=expr_2.id)
        with pytest.raises(
            sqla.exc.IntegrityError,
            match="expression_id cannot be modified",
        ):
            db.session.flush()
        db.session.rollback()

    def test_timeseries_expression_authorizations_as_admin(
        self, users, campaign_scopes, timeseries
    ):
        admin_user = users[0]
        assert admin_user.is_admin
        cs_1 = campaign_scopes[0]
        ts_1 = timeseries[0]

        with CurrentUser(admin_user):
            expr_1 = Expression.new(
                campaign_scope_id=cs_1.id,
                name="2 times a",
                expr="2*a",
            )
            db.session.commit()
            with pytest.raises(BEMServerCoreIntegrityError):
                TimeseriesExpression.new(
                    campaign_scope_id=cs_1.id,
                    expression_id=expr_1.id,
                    timeseries_id=DUMMY_ID,
                    src_data_state_id=1,
                    dest_data_state_id=1,
                    bucket_width_value=1,
                    bucket_width_unit="day",
                )
                db.session.flush()
            db.session.rollback()
            with pytest.raises(BEMServerCoreIntegrityError):
                TimeseriesExpression.new(
                    campaign_scope_id=cs_1.id,
                    expression_id=DUMMY_ID,
                    timeseries_id=ts_1.id,
                    src_data_state_id=1,
                    dest_data_state_id=1,
                    bucket_width_value=1,
                    bucket_width_unit="day",
                )
                db.session.flush()
            db.session.rollback()
            ts_expr_1 = TimeseriesExpression.new(
                campaign_scope_id=cs_1.id,
                expression_id=expr_1.id,
                timeseries_id=ts_1.id,
                src_data_state_id=1,
                dest_data_state_id=1,
                bucket_width_value=1,
                bucket_width_unit="day",
            )
            db.session.flush()

            ts_expr = TimeseriesExpression.get_by_id(ts_expr_1.id)
            assert ts_expr.id == ts_expr_1.id
            assert ts_expr.timeseries_id == ts_expr_1.timeseries_id
            ts_expr_l = list(TimeseriesExpression.get())
            assert len(ts_expr_l) == 1
            assert ts_expr_l[0].id == ts_expr_1.id
            ts_expr.update(ts_expr="2+a")
            ts_expr.delete()
            db.session.flush()

    @pytest.mark.usefixtures("users_by_user_groups")
    @pytest.mark.usefixtures("user_groups_by_campaign_scopes")
    def test_timeseries_expression_authorizations_as_user(
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
                name="2 times a",
                expr="2*a",
            )
            expr_2 = Expression.new(
                campaign_scope_id=cs_2.id,
                name="2 times a",
                expr="2*a",
            )
            expr_3 = Expression.new(
                campaign_scope_id=cs_2.id,
                name="2 times a",
                expr="2*a",
            )
            db.session.flush()
            TimeseriesExpression.new(
                campaign_scope_id=cs_1.id,
                expression_id=expr_1.id,
                timeseries_id=ts_1.id,
                src_data_state_id=1,
                dest_data_state_id=1,
                bucket_width_value=1,
                bucket_width_unit="day",
            )
            ts_expr_2 = TimeseriesExpression.new(
                campaign_scope_id=cs_2.id,
                expression_id=expr_2.id,
                timeseries_id=ts_2.id,
                src_data_state_id=2,
                dest_data_state_id=2,
                bucket_width_value=1,
                bucket_width_unit="day",
            )
            db.session.flush()

        with CurrentUser(user_1):
            with pytest.raises(BEMServerAuthorizationError):
                TimeseriesExpression.new(
                    expression_id=expr_3.id,
                    timeseries_id=ts_2.id,
                    bucket_width_value=1,
                    bucket_width_unit="day",
                    src_data_state_id=1,
                    dest_data_state_id=1,
                )

            TimeseriesExpression.get_by_id(ts_expr_2.id)
            ts_expr_list = list(TimeseriesExpression.get())
            assert len(ts_expr_list) == 1
            assert ts_expr_list[0].id == ts_expr_2.id
            with pytest.raises(BEMServerAuthorizationError):
                TimeseriesExpression.get_by_id(expr_1.id)
            with pytest.raises(BEMServerAuthorizationError):
                # Same value so update is noop but the point is to test auth failure
                ts_expr_2.update(timeserie_id=ts_2.id)
            with pytest.raises(BEMServerAuthorizationError):
                ts_expr_2.delete()
