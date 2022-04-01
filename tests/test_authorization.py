"""Authentication framework related tests"""
import sqlalchemy as sqla

import pytest

from bemserver_core.database import Base, db
from bemserver_core.authorization import AuthMixin


class TestAuthMixin:
    @pytest.mark.usefixtures("database")
    def test_auth_mixin_sort(self):
        """Check AuthMixin doesn't break database sort feature"""

        class Test(Base, AuthMixin):
            __tablename__ = "test_auth_mixin_sort"

            id = sqla.Column(sqla.Integer, primary_key=True)
            title = sqla.Column(sqla.String())
            severity = sqla.Column(
                sqla.Enum("Low", "High", "Critical", name="test_auth_mixin_severity")
            )

        Test.__table__.create(bind=db.engine)

        mes_1 = Test(title="3 Hello", severity="Low")
        mes_2 = Test(title="2 Hello", severity="Critical")
        mes_3 = Test(title="1 Hello", severity="High")
        db.session.add(mes_1)
        db.session.add(mes_2)
        db.session.add(mes_3)
        db.session.commit()

        ret = Test.get(sort=["severity"]).all()
        assert ret == [mes_1, mes_3, mes_2]
        ret = Test.get(sort=["+severity"]).all()
        assert ret == [mes_1, mes_3, mes_2]
        ret = Test.get(sort=["-severity"]).all()
        assert ret == [mes_2, mes_3, mes_1]

        ret = Test.get(sort=["title"]).all()
        assert ret == [mes_3, mes_2, mes_1]

        mes_4 = Test(title="2 Hello", severity="Low")
        mes_5 = Test(title="1 Hello", severity="Critical")
        mes_6 = Test(title="3 Hello", severity="High")
        db.session.add(mes_4)
        db.session.add(mes_5)
        db.session.add(mes_6)
        db.session.commit()

        ret = Test.get(sort=["-severity", "title"]).all()
        assert ret == [mes_5, mes_2, mes_3, mes_6, mes_4, mes_1]
