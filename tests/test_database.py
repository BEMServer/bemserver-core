"""DB framework related tests"""
import sqlalchemy as sqla

import pytest

from bemserver_core.database import Base


class TestDatabase:
    @pytest.mark.usefixtures("database")
    def test_database_base_update(self):
        """Test update method of custom Base class"""

        class Test(Base):
            __tablename__ = "test"

            id = sqla.Column(sqla.Integer, primary_key=True)
            test_1 = sqla.Column(sqla.String(80), nullable=True)
            test_2 = sqla.Column(sqla.String(80), nullable=False)

        test = Test(test_1="test_1", test_2="test_2")
        assert test.test_1 == "test_1"
        assert test.test_2 == "test_2"

        test.update(test_1="test_11")
        assert test.test_1 == "test_11"
        test.update(test_1=None)
        assert test.test_1 is None

        # The nullable argument makes no difference here
        # This will only fail on commit
        test.update(test_2="test_21")
        assert test.test_2 == "test_21"
        test.update(test_2=None)
        assert test.test_2 is None
