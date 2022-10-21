"""Energy consumption

Compute energy consumption breakdowns
"""
from collections import defaultdict

from bemserver_core.model import (
    TimeseriesDataState,
    EnergyConsumptionTimeseriesBySite,
    EnergyConsumptionTimeseriesByBuilding,
)
from bemserver_core.input_output import tsdio


DATA_STATE = "Clean"


def compute_energy_consumption_breakdown_for_site(
    site,
    start_dt,
    end_dt,
    bucket_width_value,
    bucket_width_unit,
    timezone="UTC",
):
    """Compute energy consumption breakdown for a Site"""
    return compute_energy_consumption_breakdown(
        list(EnergyConsumptionTimeseriesBySite.get(site_id=site.id)),
        start_dt,
        end_dt,
        bucket_width_value,
        bucket_width_unit,
        timezone=timezone,
    )


def compute_energy_consumption_breakdown_for_building(
    building,
    start_dt,
    end_dt,
    bucket_width_value,
    bucket_width_unit,
    timezone="UTC",
):
    """Compute energy consumption breakdown for a Building"""
    return compute_energy_consumption_breakdown(
        list(EnergyConsumptionTimeseriesByBuilding.get(building_id=building.id)),
        start_dt,
        end_dt,
        bucket_width_value,
        bucket_width_unit,
        timezone=timezone,
    )


def compute_energy_consumption_breakdown(
    ectbl_l,
    start_dt,
    end_dt,
    bucket_width_value,
    bucket_width_unit,
    timezone="UTC",
):

    timeseries = [ectbl.timeseries for ectbl in ectbl_l]

    data_state = TimeseriesDataState.get(name=DATA_STATE).first()

    data_df = tsdio.get_timeseries_buckets_data(
        start_dt,
        end_dt,
        timeseries,
        data_state,
        bucket_width_value,
        bucket_width_unit,
        "sum",
        timezone=timezone,
    ).fillna(0)

    brkdwn = {
        "timestamps": data_df.index.to_list(),
        "energy": defaultdict(dict),
    }
    for ectbl, ts_name in zip(ectbl_l, data_df.columns):
        brkdwn["energy"][ectbl.source.name][ectbl.end_use.name] = (
            ectbl.wh_conversion_factor * data_df[ts_name]
        ).to_list()

    return brkdwn
