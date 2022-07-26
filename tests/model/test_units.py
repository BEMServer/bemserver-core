"""Unit tests"""
# import sqlalchemy as sqla

import pytest

from bemserver_core.model import Unit
from bemserver_core.database import db
from bemserver_core.authorization import CurrentUser
from bemserver_core.exceptions import BEMServerAuthorizationError


class TestUnitModel:
    def test_unit_authorizations_as_admin(self, users):
        admin_user = users[0]
        assert admin_user.is_admin

        with CurrentUser(admin_user):
            assert Unit.get_by_symbol("°C").symbol == "°C"
            assert Unit.get_by_symbol("DUMMY") is None

            units = list(Unit.get())
            unit_1 = Unit.new(name="liter", symbol="gloops")
            db.session.add(unit_1)
            db.session.commit()
            assert Unit.get_by_id(unit_1.id) == unit_1
            assert len(list(Unit.get())) == len(units) + 1
            unit_1.update(symbol="L")
            unit_1.delete()
            db.session.commit()

            assert unit_1.label == "liter [L]"

    def test_unit_authorizations_as_user(self, users):
        user_1 = users[1]
        assert not user_1.is_admin

        with CurrentUser(user_1):
            assert Unit.get_by_symbol("°C").symbol == "°C"
            assert Unit.get_by_symbol("DUMMY") is None

            with pytest.raises(BEMServerAuthorizationError):
                Unit.new(name="liter", symbol="gloops")
            units = list(Unit.get())
            unit_1 = Unit.get_by_id(units[0].id)
            with pytest.raises(BEMServerAuthorizationError):
                unit_1.update(name="kelvin")
            with pytest.raises(BEMServerAuthorizationError):
                unit_1.delete()
