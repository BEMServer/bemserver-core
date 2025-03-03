"""Energy consumption tests"""

import datetime as dt
from copy import deepcopy

import pytest

import pandas as pd

from bemserver_core.authorization import CurrentUser, OpenBar
from bemserver_core.database import db
from bemserver_core.exceptions import (
    BEMServerCoreDimensionalityError,
    BEMServerCoreEnergyBreakdownProcessZeroDivisionError,
)
from bemserver_core.model import (
    Energy,
    EnergyConsumptionTimeseriesByBuilding,
    EnergyConsumptionTimeseriesBySite,
    EnergyEndUse,
    Timeseries,
    TimeseriesDataState,
)
from bemserver_core.process.energy_consumption import (
    compute_energy_consumption_breakdown_for_building,
    compute_energy_consumption_breakdown_for_site,
)
from tests.utils import create_timeseries_data


class TestEnergyConsumption:
    def _create_data(self, campaign, campaign_scope):
        start_dt = dt.datetime(2020, 1, 1, 0, 0, tzinfo=dt.timezone.utc)
        end_dt = dt.datetime(2020, 1, 1, 2, 0, tzinfo=dt.timezone.utc)

        timestamps = pd.date_range(start_dt, end_dt, inclusive="left", freq="h")

        # Create timeseries and timeseries data
        ds_clean = TimeseriesDataState.get(name="Clean").first()

        timeseries = []
        for i in range(9):
            ts_i = Timeseries.new(
                name=f"Timeseries {i + 1}",
                campaign=campaign,
                campaign_scope=campaign_scope,
                unit_symbol=("Wh" if i < 6 else "kWh"),
            )
            timeseries.append(ts_i)
        db.session.flush()

        create_timeseries_data(timeseries[0], ds_clean, timestamps, [71, 71])
        create_timeseries_data(timeseries[1], ds_clean, timestamps, [46, 46])
        create_timeseries_data(timeseries[2], ds_clean, timestamps, [25, 25])
        create_timeseries_data(timeseries[3], ds_clean, timestamps, [50, 50])
        create_timeseries_data(timeseries[4], ds_clean, timestamps, [25, 25])
        create_timeseries_data(timeseries[5], ds_clean, timestamps, [25, 25])
        create_timeseries_data(timeseries[6], ds_clean, timestamps, [0.021, 0.021])
        create_timeseries_data(timeseries[7], ds_clean, timestamps, [0.021, 0.021])

        expected_consumptions_wh = {
            "all": {
                "all": [71.0, 71.0],
                "heating": [46.0, 46.0],
                "cooling": [25.0, 25.0],
            },
            "electricity": {
                "all": [50.0, 50.0],
                "heating": [25.0, 25.0],
                "cooling": [25.0, 25.0],
            },
            "natural gas": {
                "all": [21.0, 21.0],
                "heating": [21.0, 21.0],
                "cooling": [0.0, 0.0],
            },
        }

        expected_consumptions_mwh = deepcopy(expected_consumptions_wh)
        for energy in expected_consumptions_mwh.values():
            for usage in energy.keys():
                energy[usage] = [val * 1000 for val in energy[usage]]

        expected = {
            "Wh": {
                "timestamps": timestamps.to_list(),
                "energy": expected_consumptions_wh,
            },
            "mWh": {
                "timestamps": timestamps.to_list(),
                "energy": expected_consumptions_mwh,
            },
        }

        return start_dt, end_dt, timeseries, expected

    def test_compute_energy_consumption_breakdown_for_site(
        self, users, sites, campaigns, campaign_scopes
    ):
        admin_user = users[0]
        assert admin_user.is_admin
        campaign_1 = campaigns[0]
        cs_1 = campaign_scopes[0]

        site_1 = sites[0]

        with OpenBar():
            start_dt, end_dt, timeseries, expected = self._create_data(campaign_1, cs_1)

            source_all = Energy.get(name="all").first()
            source_elec = Energy.get(name="electricity").first()
            source_gas = Energy.get(name="natural gas").first()
            end_use_all = EnergyEndUse.get(name="all").first()
            end_use_heating = EnergyEndUse.get(name="heating").first()
            end_use_cooling = EnergyEndUse.get(name="cooling").first()

            EnergyConsumptionTimeseriesBySite.new(
                site_id=sites[0].id,
                energy_id=source_all.id,
                end_use_id=end_use_all.id,
                timeseries_id=timeseries[0].id,
            )
            EnergyConsumptionTimeseriesBySite.new(
                site_id=sites[0].id,
                energy_id=source_all.id,
                end_use_id=end_use_heating.id,
                timeseries_id=timeseries[1].id,
            )
            EnergyConsumptionTimeseriesBySite.new(
                site_id=sites[0].id,
                energy_id=source_all.id,
                end_use_id=end_use_cooling.id,
                timeseries_id=timeseries[2].id,
            )
            EnergyConsumptionTimeseriesBySite.new(
                site_id=sites[0].id,
                energy_id=source_elec.id,
                end_use_id=end_use_all.id,
                timeseries_id=timeseries[3].id,
            )
            EnergyConsumptionTimeseriesBySite.new(
                site_id=sites[0].id,
                energy_id=source_elec.id,
                end_use_id=end_use_heating.id,
                timeseries_id=timeseries[4].id,
            )
            # Non-reg test: use the same timeseries for two end uses
            # It makes no sense but it shouldn't crash
            # Result is the same in this case since values are the same in both TS
            EnergyConsumptionTimeseriesBySite.new(
                site_id=sites[0].id,
                energy_id=source_elec.id,
                end_use_id=end_use_cooling.id,
                timeseries_id=timeseries[4].id,
            )
            EnergyConsumptionTimeseriesBySite.new(
                site_id=sites[0].id,
                energy_id=source_gas.id,
                end_use_id=end_use_all.id,
                timeseries_id=timeseries[6].id,
            )
            EnergyConsumptionTimeseriesBySite.new(
                site_id=sites[0].id,
                energy_id=source_gas.id,
                end_use_id=end_use_heating.id,
                timeseries_id=timeseries[7].id,
            )
            EnergyConsumptionTimeseriesBySite.new(
                site_id=sites[0].id,
                energy_id=source_gas.id,
                end_use_id=end_use_cooling.id,
                timeseries_id=timeseries[8].id,
            )

        with CurrentUser(admin_user):
            ret = compute_energy_consumption_breakdown_for_site(
                site_1, start_dt, end_dt, 1, "hour"
            )
            assert ret == expected["Wh"]
            ret = compute_energy_consumption_breakdown_for_site(
                site_1, start_dt, end_dt, 1, "hour", unit="mWh"
            )
            assert ret == expected["mWh"]
            ret = compute_energy_consumption_breakdown_for_site(
                site_1, start_dt, end_dt, 1, "hour", ratio=0.001
            )
            assert ret == expected["mWh"]

            # Check values are aggregated with a sum
            ret = compute_energy_consumption_breakdown_for_site(
                site_1, start_dt, end_dt, 2, "hour"
            )
            assert ret["energy"]["all"]["all"] == [142.0]

            # Check zero ratio
            with pytest.raises(BEMServerCoreEnergyBreakdownProcessZeroDivisionError):
                ret = compute_energy_consumption_breakdown_for_site(
                    site_1, start_dt, end_dt, 1, "hour", ratio=0
                )

            # Check wrong unit
            timeseries[0].unit_symbol = ""
            with pytest.raises(BEMServerCoreDimensionalityError):
                ret = compute_energy_consumption_breakdown_for_site(
                    site_1, start_dt, end_dt, 1, "hour"
                )

    def test_compute_energy_consumption_breakdown_for_building(
        self, users, buildings, campaigns, campaign_scopes
    ):
        admin_user = users[0]
        assert admin_user.is_admin
        campaign_1 = campaigns[0]
        cs_1 = campaign_scopes[0]

        building_1 = buildings[0]

        with OpenBar():
            start_dt, end_dt, timeseries, expected = self._create_data(campaign_1, cs_1)

            source_all = Energy.get(name="all").first()
            source_elec = Energy.get(name="electricity").first()
            source_gas = Energy.get(name="natural gas").first()
            end_use_all = EnergyEndUse.get(name="all").first()
            end_use_heating = EnergyEndUse.get(name="heating").first()
            end_use_cooling = EnergyEndUse.get(name="cooling").first()

            EnergyConsumptionTimeseriesByBuilding.new(
                building_id=buildings[0].id,
                energy_id=source_all.id,
                end_use_id=end_use_all.id,
                timeseries_id=timeseries[0].id,
            )
            EnergyConsumptionTimeseriesByBuilding.new(
                building_id=buildings[0].id,
                energy_id=source_all.id,
                end_use_id=end_use_heating.id,
                timeseries_id=timeseries[1].id,
            )
            EnergyConsumptionTimeseriesByBuilding.new(
                building_id=buildings[0].id,
                energy_id=source_all.id,
                end_use_id=end_use_cooling.id,
                timeseries_id=timeseries[2].id,
            )
            EnergyConsumptionTimeseriesByBuilding.new(
                building_id=buildings[0].id,
                energy_id=source_elec.id,
                end_use_id=end_use_all.id,
                timeseries_id=timeseries[3].id,
            )
            EnergyConsumptionTimeseriesByBuilding.new(
                building_id=buildings[0].id,
                energy_id=source_elec.id,
                end_use_id=end_use_heating.id,
                timeseries_id=timeseries[4].id,
            )
            EnergyConsumptionTimeseriesByBuilding.new(
                building_id=buildings[0].id,
                energy_id=source_elec.id,
                end_use_id=end_use_cooling.id,
                timeseries_id=timeseries[5].id,
            )
            EnergyConsumptionTimeseriesByBuilding.new(
                building_id=buildings[0].id,
                energy_id=source_gas.id,
                end_use_id=end_use_all.id,
                timeseries_id=timeseries[6].id,
            )
            EnergyConsumptionTimeseriesByBuilding.new(
                building_id=buildings[0].id,
                energy_id=source_gas.id,
                end_use_id=end_use_heating.id,
                timeseries_id=timeseries[7].id,
            )
            EnergyConsumptionTimeseriesByBuilding.new(
                building_id=buildings[0].id,
                energy_id=source_gas.id,
                end_use_id=end_use_cooling.id,
                timeseries_id=timeseries[8].id,
            )

        with CurrentUser(admin_user):
            ret = compute_energy_consumption_breakdown_for_building(
                building_1, start_dt, end_dt, 1, "hour"
            )
            assert ret == expected["Wh"]
            ret = compute_energy_consumption_breakdown_for_building(
                building_1, start_dt, end_dt, 1, "hour", unit="mWh"
            )
            assert ret == expected["mWh"]
            ret = compute_energy_consumption_breakdown_for_building(
                building_1, start_dt, end_dt, 1, "hour", ratio=0.001
            )
            assert ret == expected["mWh"]

            # Check values are aggregated with a sum
            ret = compute_energy_consumption_breakdown_for_building(
                building_1, start_dt, end_dt, 2, "hour"
            )
            assert ret["energy"]["all"]["all"] == [142.0]

            # Check zero ratio
            with pytest.raises(BEMServerCoreEnergyBreakdownProcessZeroDivisionError):
                ret = compute_energy_consumption_breakdown_for_building(
                    building_1, start_dt, end_dt, 1, "hour", ratio=0
                )

            # Check wrong unit
            timeseries[0].unit_symbol = ""
            with pytest.raises(BEMServerCoreDimensionalityError):
                ret = compute_energy_consumption_breakdown_for_building(
                    building_1, start_dt, end_dt, 1, "hour"
                )
