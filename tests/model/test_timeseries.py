"""Timeseries tests"""
import pytest

from bemserver_core.model import Timeseries
from bemserver_core.database import db
from bemserver_core.authorization import CurrentUser
from bemserver_core.exceptions import BEMServerAuthorizationError


class TestTimeseriesModel:

    def test_timeseries_authorizations_as_admin(self, users):
        admin_user = users[0]
        assert admin_user.is_admin

        with CurrentUser(admin_user):
            ts_1 = Timeseries.new(
                name="Timeseries 1",
            )
            db.session.add(ts_1)
            db.session.commit()

            timeseries = Timeseries.get_by_id(ts_1.id)
            assert timeseries.id == ts_1.id
            assert timeseries.name == ts_1.name
            timeseriess = list(Timeseries.get())
            assert len(timeseriess) == 1
            assert timeseriess[0].id == ts_1.id
            timeseries.update(name="Super timeseries 1")
            timeseries.delete()
            db.session.commit()

    @pytest.mark.usefixtures("users_by_campaigns")
    @pytest.mark.usefixtures("timeseries_by_campaigns")
    def test_timeseries_authorizations_as_user(self, users, timeseries):
        user_1 = users[1]
        assert not user_1.is_admin
        ts_1 = timeseries[0]
        ts_2 = timeseries[1]

        with CurrentUser(user_1):
            with pytest.raises(BEMServerAuthorizationError):
                Timeseries.new(
                    name="Timeseries 1",
                )

            timeseries = Timeseries.get_by_id(ts_2.id)
            timeseries_list = list(Timeseries.get())
            assert len(timeseries_list) == 1
            assert timeseries_list[0].id == ts_2.id
            with pytest.raises(BEMServerAuthorizationError):
                Timeseries.get_by_id(ts_1.id)
            with pytest.raises(BEMServerAuthorizationError):
                timeseries.update(name="Super timeseries 1")
            with pytest.raises(BEMServerAuthorizationError):
                timeseries.delete()
