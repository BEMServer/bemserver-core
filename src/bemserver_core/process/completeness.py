"""Completeness

Compute indicators/stats about data completness
"""

from zoneinfo import ZoneInfo

import numpy as np
import pandas as pd

from bemserver_core.input_output import tsdio
from bemserver_core.model import Timeseries
from bemserver_core.time_utils import ceil, floor


def compute_completeness(
    start_dt,
    end_dt,
    timeseries,
    data_state,
    bucket_width_value,
    bucket_width_unit,
    timezone="UTC",
):
    """Compute data completeness for a given list of timeseries

    The expected number of values in each bucket is computed from the sample
    interval which is read in database or inferred if possible from existing
    data.
    """
    timeseries_ids = [ts.id for ts in timeseries]

    tz = ZoneInfo(timezone)
    start_dt = floor(start_dt.astimezone(tz), bucket_width_unit, bucket_width_value)
    end_dt = ceil(end_dt.astimezone(tz), bucket_width_unit, bucket_width_value)

    # Get data count per bucket (aggregation)
    counts_df = tsdio.get_timeseries_buckets_data(
        start_dt,
        end_dt,
        timeseries,
        data_state,
        bucket_width_value,
        bucket_width_unit,
        "count",
        timezone=timezone,
    )
    avg_counts_df = counts_df.mean()
    total_counts_df = counts_df.sum()

    # Compute number of seconds per bucket
    idx = pd.Series(counts_df.index)
    idx[len(idx)] = end_dt
    nb_s_per_bucket = [i.total_seconds() for i in idx.diff().iloc[1:]]

    # Compute data rate (count / second)
    rates_df = counts_df.div(nb_s_per_bucket, axis=0)

    # Get interval property value for each TS
    ts_intervals = Timeseries.get_property_for_many_timeseries(
        timeseries_ids, "Interval"
    )
    intervals = [ts_intervals[ts_id] for ts_id in timeseries_ids]
    undefined_intervals = [i is None for i in intervals]
    # Guess interval from max ratio if undefined
    intervals = [
        # Use interval, if defined
        (
            int(i)
            if i is not None
            # Otherwise, use max ratio
            else (
                1 / maxrate
                if (maxrate := rates_df[col].max()) != 0
                # Or nan if no value at all
                else np.nan
            )
        )
        for i, col in zip(intervals, counts_df.columns)
    ]

    # Add a special case for empty intervals to avoid a deprecation warning
    # in div() and mul(). See https://stackoverflow.com/questions/74448601/

    # Compute expected count (nb seconds per bucket / interval)
    if not intervals:
        expected_counts_df = pd.DataFrame({}, index=counts_df.index)
    else:
        expected_counts_df = pd.DataFrame(
            {col: nb_s_per_bucket for col in counts_df.columns}, index=counts_df.index
        ).div(intervals, axis=1)

    # Compute ratios (data rate x interval)
    if not intervals:
        ratios_df = pd.DataFrame({}, index=counts_df.index)
    else:
        ratios_df = rates_df.mul(intervals, axis=1)
    avg_ratios_df = ratios_df.mean()

    # Replace NaN with None
    ratios_df = ratios_df.astype(object)
    avg_ratios_df = avg_ratios_df.astype(object)
    expected_counts_df = expected_counts_df.astype(object)
    ratios_df = ratios_df.where(ratios_df.notnull(), None)
    avg_ratios_df = avg_ratios_df.where(avg_ratios_df.notnull(), None)
    expected_counts_df = expected_counts_df.where(expected_counts_df.notnull(), None)
    intervals = [None if pd.isna(i) else i for i in intervals]

    return {
        "timestamps": ratios_df.index.to_list(),
        "timeseries": {
            col: {
                "name": timeseries[idx].name,
                "count": counts_df[col].to_list(),
                "ratio": ratios_df[col].to_list(),
                "total_count": total_counts_df[col],
                "avg_count": avg_counts_df[col],
                "avg_ratio": avg_ratios_df[col],
                "interval": intervals[idx],
                "undefined_interval": undefined_intervals[idx],
                "expected_count": expected_counts_df[col].to_list(),
            }
            for idx, col in enumerate(ratios_df.columns)
        },
    }
