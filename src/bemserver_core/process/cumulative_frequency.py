"""Cumulative frequency

Compute cumulative frequency of a timeseries
"""

import math

import pandas as pd


def compute_cumulative_frequency(
    data_s,
    precision,
):
    """Compute cumulative frequency for all columns of a dataframe"""

    #     tz = ZoneInfo(timezone)
    #     start_dt = floor(start_dt.astimezone(tz), bucket_width_unit, bucket_width_value)
    #     end_dt = ceil(end_dt.astimezone(tz), bucket_width_unit, bucket_width_value)
    #
    #     data_df.resample(make_date_offset(bucket_width_unit, bucket_width_value))

    print(data_s)

    lower = math.floor(data_s.min() * precision) / precision
    upper = math.ceil(data_s.max() * precision) / precision

    # Make range with given precision
    bins = pd.interval_range(lower, upper, freq=precision)

    hist = pd.cut(data_s, bins)

    print(hist)

    # cumsum

    # data_s


#     # Replace NaN with None
#     ratios_df = ratios_df.astype(object)
#     avg_ratios_df = avg_ratios_df.astype(object)
#     expected_counts_df = expected_counts_df.astype(object)
#     ratios_df = ratios_df.where(ratios_df.notnull(), None)
#     avg_ratios_df = avg_ratios_df.where(avg_ratios_df.notnull(), None)
#     expected_counts_df = expected_counts_df.where(expected_counts_df.notnull(), None)
#     intervals = [None if pd.isna(i) else i for i in intervals]
#
#     return {
#         "timestamps": ratios_df.index.to_list(),
#         "timeseries": {
#             col: {
#                 "name": timeseries[idx].name,
#                 "count": counts_df[col].to_list(),
#                 "ratio": ratios_df[col].to_list(),
#                 "total_count": total_counts_df[col],
#                 "avg_count": avg_counts_df[col],
#                 "avg_ratio": avg_ratios_df[col],
#                 "interval": intervals[idx],
#                 "undefined_interval": undefined_intervals[idx],
#                 "expected_count": expected_counts_df[col].to_list(),
#             }
#             for idx, col in enumerate(ratios_df.columns)
#         },
#     }
