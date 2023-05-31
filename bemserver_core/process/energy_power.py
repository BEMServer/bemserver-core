"""Energy <=> Power conversions"""

import pandas as pd

from bemserver_core.common import ureg
from bemserver_core.time_utils import make_pandas_freq
from bemserver_core.process.forward_fill import ffill


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
