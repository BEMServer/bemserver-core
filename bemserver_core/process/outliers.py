"""Check outliers

Check outliers in timeseries data
"""
import pandas as pd
import numpy as np

from bemserver_core.model import Timeseries
from bemserver_core.input_output import tsdio


def get_outliers(
    start_dt,
    end_dt,
    timeseries,
    data_state,
    *,
    inclusive="left",
):
    """Check outliers process

    Get outliers from a list of timeseries.
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

    for ts_id, (_, col) in zip(timeseries_ids, data_df.items()):
        conditions = pd.Series(True, index=col.index)
        if (ts_min := ts_mins[ts_id]) is not None:
            conditions &= col >= float(ts_min)
        if (ts_max := ts_maxs[ts_id]) is not None:
            conditions &= col <= float(ts_max)
        col.loc[conditions] = np.nan

    return data_df
