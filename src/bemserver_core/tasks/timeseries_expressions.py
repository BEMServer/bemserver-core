"""Compute timeseries expressions"""

import pandas as pd

from bemserver_core.celery import BEMServerCoreAsyncTask, celery, logger
from bemserver_core.database import db
from bemserver_core.input_output import tsdio
from bemserver_core.model import (
    CampaignScope,
    TimeseriesDataState,
    TimeseriesExpression,
)
from bemserver_core.processing.expressions import evaluate


def compute_timeseries_expressions(campaign, start_dt, end_dt):
    logger.info("Compute timeseries expressions for campaign %s", campaign.name)
    logger.info("Time interval: [%s - %s]", start_dt, end_dt)

    ds_clean = TimeseriesDataState.get(name="Clean").first()

    ts_expressions = (
        db.session.query(TimeseriesExpression)
        .join(CampaignScope)
        .where(CampaignScope.campaign_id == campaign.id)
    )

    for ts_expr in ts_expressions:
        logger.info("Computing expression for timeseries %s", ts_expr.timeseries.name)
        data_s = evaluate(
            ts_expr.expression,
            start_dt,
            end_dt,
            ts_expr.bucket_width_value,
            ts_expr.bucket_width_unit,
            ts_expr.timezone,
        )
        data_df = pd.DataFrame({ts_expr.timeseries_id: data_s}, data_s.index)
        tsdio.set_timeseries_data(
            data_df, ds_clean, convert_from=ts_expr.expression.unit_symbol
        )

    logger.debug("Committing")
    db.session.commit()


@celery.register_task
class ComputeTimeseriesExpressions(BEMServerCoreAsyncTask):
    TASK_FUNCTION = compute_timeseries_expressions
