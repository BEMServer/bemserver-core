"""Cleanup

Remove outliers from timeseries data
"""
import sqlalchemy as sqla
import numpy as np

from bemserver_core.database import db
from bemserver_core.model import Timeseries, TimeseriesProperty, TimeseriesPropertyData
from bemserver_core.input_output import tsdio


def cleanup(
    start_dt,
    end_dt,
    timeseries,
    data_state,
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
    )

    # Get min/max properties values for each TS
    # TODO: merge into a single query
    subq = (
        sqla.select(TimeseriesPropertyData)
        .join(TimeseriesProperty)
        .filter(TimeseriesProperty.name == "Min")
    ).subquery()
    stmt = (
        sqla.select(Timeseries.id, subq.c.value)
        .outerjoin(subq)
        .filter(Timeseries.id.in_(timeseries_ids))
    )
    ts_mins = dict(list(db.session.execute(stmt)))
    subq = (
        sqla.select(TimeseriesPropertyData)
        .join(TimeseriesProperty)
        .filter(TimeseriesProperty.name == "Max")
    ).subquery()
    stmt = (
        sqla.select(Timeseries.id, subq.c.value)
        .outerjoin(subq)
        .filter(Timeseries.id.in_(timeseries_ids))
    )
    ts_maxs = dict(list(db.session.execute(stmt)))

    for ts_id, (_, col) in zip(timeseries_ids, data_df.items()):
        if (ts_min := ts_mins[ts_id]) is not None:
            col.loc[col < float(ts_min)] = np.nan
        if (ts_max := ts_maxs[ts_id]) is not None:
            col.loc[col > float(ts_max)] = np.nan

    return data_df
