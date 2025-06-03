"""Energy <=> Power conversions"""

import numpy as np
import pandas as pd

from bemserver_core.common import ureg
from bemserver_core.exceptions import (
    BEMServerCoreEnergyPowerProcessMissingIntervalError,
)
from bemserver_core.input_output import tsdio
from bemserver_core.process.forward_fill import ffill
from bemserver_core.time_utils import ceil, make_pandas_freq


def power2energy(
    start_dt,
    end_dt,
    power_ts,
    data_state,
    interval,
    convert_to,
):
    """Convert power to energy"""

    bucket_width_value = interval
    bucket_width_unit = "second"

    # Get power values, with forward fill interpolation to ensure no empty bucket
    power_s = ffill(
        start_dt,
        end_dt,
        (power_ts,),
        data_state,
        bucket_width_value,
        bucket_width_unit,
    )[power_ts.id]

    # Resample to align to freq
    pd_freq = make_pandas_freq(bucket_width_unit, bucket_width_value)
    power_s = power_s.resample(pd_freq, closed="left", label="left").agg("mean")

    # Energy = Power * Time
    energy_s = power_s * interval
    convert_from = ureg.validate_unit(power_ts.unit_symbol) * ureg.validate_unit("s")
    energy_s = pd.Series(
        ureg.convert(energy_s.values, convert_from, convert_to),
        index=energy_s.index,
    )

    return energy_s


def energy2power(
    start_dt,
    end_dt,
    energy_ts,
    data_state,
    convert_to,
):
    """Convert energy to power"""
    timezone = start_dt.tzinfo
    end_dt = end_dt.astimezone(timezone)

    interval = energy_ts.get_property_value("Interval")
    if interval is None:
        raise BEMServerCoreEnergyPowerProcessMissingIntervalError(
            f"Missing interval for timeseries {energy_ts.name}"
        )

    # Get energy values
    energy_s = tsdio.get_timeseries_data(
        start_dt,
        end_dt,
        (energy_ts,),
        data_state,
        timezone=str(timezone),
    )[energy_ts.id]

    # Power = Energy / Time
    power_s = energy_s / interval
    convert_from = ureg.validate_unit(energy_ts.unit_symbol) / ureg.validate_unit("s")
    energy_s = pd.Series(
        ureg.convert(power_s.values, convert_from, convert_to),
        index=power_s.index,
    )

    return energy_s


def energyindex2power(
    start_dt,
    end_dt,
    index_ts,
    data_state,
    interval,
    convert_to,
):
    """Convert energy index to power"""
    timezone = start_dt.tzinfo
    end_dt = end_dt.astimezone(timezone)

    # Get energy index values
    index_s = tsdio.get_timeseries_data(
        start_dt,
        end_dt,
        (index_ts,),
        data_state,
        timezone=str(timezone),
    )[index_ts.id]

    # Compute energy as diff, with a 0 min for index rollover or meter change
    energy = np.maximum(0, index_s.diff().shift(-1)[:-1])
    # Also compute time intervals. The cast to int64 produces nanoseconds
    intervals = index_s.index.to_series().diff().shift(-1)[:-1].astype("int64")

    # Power = Energy / Time
    power_s = energy / intervals

    # Interpolate using forward fill
    start_dt = ceil(start_dt, "second", interval)
    pd_freq = make_pandas_freq("second", interval)
    complete_idx = pd.date_range(
        start_dt,
        end_dt,
        freq=pd_freq,
        name="timestamp",
        inclusive="left",
    )
    power_s = power_s.reindex(power_s.index.union(complete_idx))

    # Forward fill up to last known value only
    if not index_s.empty:
        fill = power_s[power_s.index < index_s.index[-1]].ffill()
        power_s.update(fill)

    # Resample to expected interval
    power_s = power_s.resample(pd_freq, closed="left", label="left").agg("mean")

    # Convert to desired unit
    convert_from = ureg.validate_unit(index_ts.unit_symbol) / ureg.validate_unit("ns")
    power_s = pd.Series(
        ureg.convert(power_s.values, convert_from, convert_to),
        index=power_s.index,
    )

    return power_s


def energyindex2energy(
    start_dt,
    end_dt,
    index_ts,
    data_state,
    interval,
    convert_to,
):
    """Convert energy index to energy"""

    power_s = energyindex2power(
        start_dt,
        end_dt,
        index_ts,
        data_state,
        interval,
        "W",
    )

    # Energy = Power * Time
    energy_s = power_s * interval / 3600

    # Convert to desired unit
    energy_s = pd.Series(
        ureg.convert(energy_s.values, "Wh", convert_to),
        index=energy_s.index,
    )

    return energy_s
