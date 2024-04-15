"""Energy consumption

Compute energy consumption breakdowns
"""

from collections import defaultdict

import numpy as np

from bemserver_core.exceptions import (
    BEMServerCoreEnergyBreakdownProcessZeroDivisionError,
)
from bemserver_core.input_output import tsdio
from bemserver_core.model import (
    EnergyConsumptionTimeseriesByBuilding,
    EnergyConsumptionTimeseriesBySite,
    TimeseriesDataState,
)

DATA_STATE = "Clean"


def compute_energy_consumption_breakdown_for_site(
    site,
    start_dt,
    end_dt,
    bucket_width_value,
    bucket_width_unit,
    unit="Wh",
    ratio=1,
    timezone="UTC",
):
    """Compute energy consumption breakdown for a Site"""
    return compute_energy_consumption_breakdown(
        list(EnergyConsumptionTimeseriesBySite.get(site_id=site.id)),
        start_dt,
        end_dt,
        bucket_width_value,
        bucket_width_unit,
        unit=unit,
        ratio=ratio,
        timezone=timezone,
    )


def compute_energy_consumption_breakdown_for_building(
    building,
    start_dt,
    end_dt,
    bucket_width_value,
    bucket_width_unit,
    *,
    unit="Wh",
    ratio=1,
    timezone="UTC",
):
    """Compute energy consumption breakdown for a Building"""
    return compute_energy_consumption_breakdown(
        list(EnergyConsumptionTimeseriesByBuilding.get(building_id=building.id)),
        start_dt,
        end_dt,
        bucket_width_value,
        bucket_width_unit,
        unit=unit,
        ratio=ratio,
        timezone=timezone,
    )


def compute_energy_consumption_breakdown(
    ectbl_l,
    start_dt,
    end_dt,
    bucket_width_value,
    bucket_width_unit,
    *,
    unit="Wh",
    ratio=1,
    timezone="UTC",
):
    # Use a set to remove potential duplicates (although it shouldn't happen)
    timeseries = {ectbl.timeseries for ectbl in ectbl_l}

    data_state = TimeseriesDataState.get(name=DATA_STATE).first()

    data_df = (
        tsdio.get_timeseries_buckets_data(
            start_dt,
            end_dt,
            timeseries,
            data_state,
            bucket_width_value,
            bucket_width_unit,
            "sum",
            convert_to={ts.id: unit for ts in timeseries},
            timezone=timezone,
        ).fillna(0)
        / ratio
    )

    if np.isinf(data_df).any().any():
        raise BEMServerCoreEnergyBreakdownProcessZeroDivisionError(
            "Dividing by ratio with value 0"
        )

    brkdwn = {
        "timestamps": data_df.index.to_list(),
        "energy": defaultdict(dict),
    }
    for ectbl in ectbl_l:
        brkdwn["energy"][ectbl.energy.name][ectbl.end_use.name] = data_df[
            ectbl.timeseries_id
        ].to_list()

    return brkdwn
