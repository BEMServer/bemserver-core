"""Energy tests"""

import pytest

from bemserver_core.authorization import CurrentUser
from bemserver_core.database import db
from bemserver_core.exceptions import BEMServerAuthorizationError
from bemserver_core.model import (
    WeatherParameterEnum,
    WeatherTimeseriesBySite,
)


class TestWeatherTimeseriesBySiteModel:
    @pytest.mark.usefixtures("weather_timeseries_by_sites")
    def test_weather_timeseries_by_site_delete_cascade(self, users, sites, timeseries):
        admin_user = users[0]

        site_1 = sites[0]
        ts_2 = timeseries[1]

        with CurrentUser(admin_user):
            assert len(list(WeatherTimeseriesBySite.get())) == 2
            site_1.delete()
            db.session.commit()
            assert len(list(WeatherTimeseriesBySite.get())) == 1
            ts_2.delete()
            db.session.commit()
            assert len(list(WeatherTimeseriesBySite.get())) == 0

    def test_weather_timeseries_by_site_authorizations_as_admin(
        self, users, timeseries, sites
    ):
        admin_user = users[0]
        assert admin_user.is_admin

        ts_1 = timeseries[0]
        ts_2 = timeseries[1]
        site_1 = sites[0]

        with CurrentUser(admin_user):
            assert not list(WeatherTimeseriesBySite.get())
            ectbs_1 = WeatherTimeseriesBySite.new(
                site_id=site_1.id,
                parameter=WeatherParameterEnum.AIR_TEMPERATURE,
                timeseries_id=ts_1.id,
                forecast=False,
            )
            db.session.commit()
            WeatherTimeseriesBySite.get_by_id(ectbs_1.id)
            ectbs_l = list(WeatherTimeseriesBySite.get())
            assert len(ectbs_l) == 1
            ectbs_1.update(timeseies_id=ts_2.id)
            ectbs_1.delete()
            db.session.commit()

    @pytest.mark.usefixtures("users_by_user_groups")
    @pytest.mark.usefixtures("user_groups_by_campaign_scopes")
    @pytest.mark.usefixtures("weather_timeseries_by_sites")
    def test_weather_timeseries_by_site_authorizations_as_user(
        self, users, timeseries, sites
    ):
        user_1 = users[1]
        assert not user_1.is_admin

        ts_2 = timeseries[1]
        site_2 = sites[1]

        with CurrentUser(user_1):
            ectbs_l = list(WeatherTimeseriesBySite.get())
            ectbs_2 = WeatherTimeseriesBySite.get_by_id(ectbs_l[0].id)
            with pytest.raises(BEMServerAuthorizationError):
                WeatherTimeseriesBySite.new(
                    site_id=site_2.id,
                    parameter=WeatherParameterEnum.AIR_TEMPERATURE,
                    timeseries_id=ts_2.id,
                    forecast=True,
                )
            with pytest.raises(BEMServerAuthorizationError):
                ectbs_2.update(parameter=WeatherParameterEnum.RELATIVE_HUMIDITY)
            with pytest.raises(BEMServerAuthorizationError):
                ectbs_2.delete()
