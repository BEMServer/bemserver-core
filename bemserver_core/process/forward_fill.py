"""Forward fill

Forward fill from timeseries data
"""

import pandas as pd

from bemserver_core.input_output import tsdio
from bemserver_core.time_utils import ceil, make_pandas_freq


def ffill(
    start_dt,
    end_dt,
    timeseries,
    data_state,
    bucket_width_value,
    bucket_width_unit,
):
    """Forward fill process"""

    # Define expected index
    start_dt = ceil(start_dt, bucket_width_unit, bucket_width_value)
    pd_freq = make_pandas_freq(bucket_width_unit, bucket_width_value)
    complete_idx = pd.date_range(
        start_dt,
        end_dt,
        freq=pd_freq,
        name="timestamp",
        inclusive="left",
    )

    # Get last value before time interval, including start of interval
    last_values = tsdio.get_last(
        None, start_dt, timeseries, data_state, timezone="UTC", inclusive="right"
    )

    # Get data for time interval
    data_df = tsdio.get_timeseries_data(
        start_dt,
        end_dt,
        timeseries,
        data_state,
    )

    # For each TS, set last value before time interval as first value for interval
    data_df.loc[start_dt] = last_values["value"]

    # For each column, complete index with complete_idx, then interpolate
    # Interpolate each column independently and drop NaN, otherwise NaN in a
    # column due to data in another column generate unwanted extrapolated data
    columns = [
        col.reindex(col.dropna().index.union(complete_idx)).ffill()
        for _, col in data_df.items()
    ]

    # Not sorted by default since pandas 2.2
    # https://github.com/pandas-dev/pandas/issues/57006
    ret_df = pd.concat(columns, axis="columns", sort=True)

    return ret_df
