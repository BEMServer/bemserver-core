"""Cleanup

Remove outliers from timeseries data
"""

import numpy as np

from bemserver_core.input_output import tsdio
from bemserver_core.model import Timeseries


def cleanup(
    start_dt,
    end_dt,
    timeseries,
    data_state,
    *,
    inclusive="left",
):
    """Cleanup process

    Remove outliers from a list of timeseries.
    The bounds are the "Min" and "Max" timeseries properties.
    """
    timeseries_ids = [ts.id for ts in timeseries]

    # Get source data
    data_df = tsdio.get_timeseries_data(
        start_dt,
        end_dt,
        timeseries,
        data_state,
        inclusive=inclusive,
    )

    # Get min/max properties values for each TS
    ts_mins = Timeseries.get_property_for_many_timeseries(timeseries_ids, "Min")
    ts_maxs = Timeseries.get_property_for_many_timeseries(timeseries_ids, "Max")

    for ts_id in timeseries_ids:
        if (ts_min := ts_mins[ts_id]) is not None:
            data_df.loc[data_df[ts_id] < float(ts_min), ts_id] = np.nan
        if (ts_max := ts_maxs[ts_id]) is not None:
            data_df.loc[data_df[ts_id] > float(ts_max), ts_id] = np.nan

    return data_df
