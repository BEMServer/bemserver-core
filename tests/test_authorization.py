"""Authentication framework related tests"""

import datetime as dt

import pytest

import sqlalchemy as sqla

from bemserver_core.authorization import AuthMixin
from bemserver_core.database import Base, db


class TestAuthMixin:
    @pytest.mark.usefixtures("database")
    def test_auth_mixin_sort(self):
        """Check AuthMixin doesn't break database sort feature"""

        class TestAuthMixinSort(Base, AuthMixin):
            __tablename__ = "test_auth_mixin_sort"

            id = sqla.Column(sqla.Integer, primary_key=True)
            title = sqla.Column(sqla.String())
            severity = sqla.Column(
                sqla.Enum("Low", "High", "Critical", name="test_auth_mixin_severity")
            )

        Test = TestAuthMixinSort

        Test.__table__.create(bind=db.engine)

        mes_1 = Test(title="3 Hello", severity="Low")
        mes_2 = Test(title="2 Hello", severity="Critical")
        mes_3 = Test(title="1 Hello", severity="High")
        db.session.add(mes_1)
        db.session.add(mes_2)
        db.session.add(mes_3)

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

        ret = Test.get(sort=["-severity", "title"]).all()
        assert ret == [mes_5, mes_2, mes_3, mes_6, mes_4, mes_1]

    @pytest.mark.usefixtures("database")
    def test_auth_mixin_min_max(self):
        """Check AuthMixin doesn't break database min/max feature"""

        class TestAuthMixinMinMax(Base, AuthMixin):
            __tablename__ = "test_auth_mixin_min_max"

            id = sqla.Column(sqla.Integer, primary_key=True)
            note = sqla.Column(sqla.Float())
            date = sqla.Column(sqla.DateTime(timezone=True))

        Test = TestAuthMixinMinMax

        Test.__table__.create(bind=db.engine)

        mes_1 = Test(note=10, date=dt.datetime(2020, 1, 1, tzinfo=dt.timezone.utc))
        mes_2 = Test(note=15, date=dt.datetime(2025, 1, 1, tzinfo=dt.timezone.utc))
        mes_3 = Test(note=18, date=dt.datetime(2015, 1, 1, tzinfo=dt.timezone.utc))
        db.session.add(mes_1)
        db.session.add(mes_2)
        db.session.add(mes_3)

        ret = Test.get(note_min=12).all()
        assert ret == [mes_2, mes_3]

        ret = Test.get(date_min=dt.datetime(2019, 1, 1, tzinfo=dt.timezone.utc)).all()
        assert ret == [mes_1, mes_2]

        ret = Test.get(
            note_max=12,
            date_min=dt.datetime(2019, 1, 1, tzinfo=dt.timezone.utc),
        ).all()
        assert ret == [
            mes_1,
        ]
