"""Test utils"""

import sqlalchemy as sqla

import pandas as pd

from bemserver_core.authorization import OpenBar
from bemserver_core.database import db
from bemserver_core.model import (
    TimeseriesData,
)


def create_timeseries_data(timeseries, data_state, timestamps, values):
    """Create timeseries data

    :param Timeseries timeseries: Timeseries
    :param TimeseriesDataState data_state: Timeseries data state
    :param list timestamps: List of timestamps
    :param list values: List of values

    timestamps and values must be of same length
    """

    with OpenBar():
        tsbds = timeseries.get_timeseries_by_data_state(data_state)

        in_df = pd.DataFrame(
            {tsbds.id: values}, index=pd.DatetimeIndex(timestamps, name="timestamp")
        )
        in_df = in_df.melt(
            value_vars=in_df.columns,
            var_name="ts_by_data_state_id",
            ignore_index=False,
        )
        data_rows = [
            row
            for row in in_df.reset_index().to_dict(orient="records")
            if pd.notna(row["value"])
        ]
        # Empty data_rows would result in cryptic error later
        assert data_rows
        query = (
            sqla.dialects.postgresql.insert(TimeseriesData)
            .values(data_rows)
            .on_conflict_do_nothing()
        )
        db.session.execute(query)
        db.session.commit()
