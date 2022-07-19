"""Completeness

Compute indicators/stats about data completness
"""
from zoneinfo import ZoneInfo

import sqlalchemy as sqla
import numpy as np
import pandas as pd

from bemserver_core.database import db
from bemserver_core.model import Timeseries, TimeseriesProperty, TimeseriesPropertyData
from bemserver_core.input_output import tsdio
from bemserver_core.input_output.timeseries_data_io import (
    gen_date_range,
    PANDAS_OFFSET_ALIASES,
)


def gen_seconds_per_bucket(
    start_dt,
    end_dt,
    bucket_width_value,
    bucket_width_unit,
    timezone,
):
    """Compute a Series with number of seconds per aggregation bucket

    The number of seconds per bucket may not be constant due to variable size buckets
    """
    pd_freq = f"{bucket_width_value}{PANDAS_OFFSET_ALIASES[bucket_width_unit]}"
    seconds = gen_date_range(start_dt, end_dt, 1, "second", timezone)
    nb_s_per_bucket = (
        pd.DataFrame({"count": 1}, index=seconds)
        # Pandas doc says origin TZ must match index TZ
        .resample(
            pd_freq,
            origin=start_dt.astimezone(ZoneInfo(timezone)),
            closed="left",
            label="left",
        ).agg("count")
    )["count"]
    return nb_s_per_bucket


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
    nb_s_per_bucket = gen_seconds_per_bucket(
        start_dt,
        end_dt,
        bucket_width_value,
        bucket_width_unit,
        timezone,
    )

    # Compute data rate (count / second)
    rates_df = counts_df.div(nb_s_per_bucket, axis=0)

    # Get interval property value for each TS
    subq = (
        sqla.select(TimeseriesPropertyData)
        .join(TimeseriesProperty)
        .filter(TimeseriesProperty.name == "Interval")
    ).subquery()
    stmt = (
        sqla.select(Timeseries.id, subq.c.value)
        .outerjoin(subq)
        .filter(Timeseries.id.in_(ts.id for ts in timeseries))
    )
    # Thanks to the outer join, the query produces a list of of (TS.id, interval) tuples
    # where interval is None if not defined
    # intervals list is of the form [(ts_1, 300), (ts_2, None), ..., (ts_N, 600)]
    ts_interval = dict(list(db.session.execute(stmt)))
    intervals = [ts_interval[ts.id] for ts in timeseries]
    undefined_intervals = [i is None for i in intervals]
    # Guess interval from max ratio if undefined
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
