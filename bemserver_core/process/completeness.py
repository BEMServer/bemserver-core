"""Completeness

Compute indicators/stats about data completness
"""
import numpy as np
import pandas as pd

from bemserver_core.model import TimeseriesProperty, TimeseriesPropertyData
from bemserver_core.input_output import tsdio
from bemserver_core.input_output.timeseries_data_io import (
    gen_date_range,
    PANDAS_OFFSET_ALIASES,
)


def compute_completeness(
    start_dt, end_dt, timeseries, data_state, bucket_width, timezone="UTC"
):
    bw_val, bw_unit = bucket_width.split()
    pd_freq = f"{bw_val}{PANDAS_OFFSET_ALIASES[bw_unit]}"

    # Generate seconds per bucket (may not be constant due to variable size buckets)
    seconds = gen_date_range(start_dt, end_dt, "1 second", timezone)
    nb_s_per_bucket = (
        pd.DataFrame({"count": 1}, index=seconds)
        .resample(pd_freq, closed="left", label="left")
        .agg("count")
    )["count"]

    # Get data count per bucket (aggregation)
    counts_df = tsdio.get_timeseries_buckets_data(
        start_dt,
        end_dt,
        timeseries,
        data_state,
        bucket_width,
        "count",
        timezone=timezone,
        col_label="name",
    )
    avg_counts_df = counts_df.mean()
    total_counts_df = counts_df.sum()

    # Compute data rate (count / second)
    rates_df = counts_df.div(nb_s_per_bucket, axis=0)

    # Get interval for each TS, guess from max ratio if interval undefined
    intervals = [
        interval.value
        if (
            interval := (
                TimeseriesPropertyData.get(timeseries_id=ts.id)
                .join(TimeseriesProperty)
                .filter(TimeseriesProperty.name == "Interval")
                .first()
            )
        )
        is not None
        else None
        for ts in timeseries
    ]
    undefined_intervals = [i is None for i in intervals]
    intervals = [
        # Use interval, if defined
        i if i is not None
        # Otherwise, use max ratio
        else 1 / maxrate if (maxrate := rates_df[col].max()) != 0
        # Or nan if no value at all
        else np.nan
        for i, col in zip(intervals, counts_df.columns)
    ]

    # Compute expected count (nb seconds per bucket / interval)
    expected_counts_df = pd.DataFrame(
        {col: nb_s_per_bucket for col in counts_df.columns}, index=counts_df.index
    ).div(intervals, axis=1)

    # Compute ratios (data rate x interval)
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
