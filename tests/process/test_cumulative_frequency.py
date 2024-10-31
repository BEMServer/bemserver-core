"""Cumulative frequency tests"""

import datetime as dt

import pandas as pd

from bemserver_core.process.cumulative_frequency import compute_cumulative_frequency


def test_cumulative_frequency():
    index = pd.date_range(
        "2020-01-01", "2020-01-01T10:00", freq="h", tz=dt.timezone.utc, inclusive="left"
    )
    data_df = pd.DataFrame(index=index)
    data_df["data_1"] = [1, 2, 2, 3, 3, 3, 4, 5, 6, 7]
    data_df["data_2"] = [7, 6, 5, 4, 3, 3, 3, 2, 2, 1]
    data_df["data_3"] = [None, None, None, None, None, None, None, None, None, None]

    compute_cumulative_frequency(data_df["data_1"], 2)
