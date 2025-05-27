"""Cleanup scheduled task"""

from bemserver_core.celery import BEMServerCoreAsyncTask, celery, logger
from bemserver_core.database import db
from bemserver_core.input_output import tsdio
from bemserver_core.model import TimeseriesDataState
from bemserver_core.process.cleanup import cleanup as cleanup_process


def cleanup_data(campaign, start_dt, end_dt):
    logger.info("Cleanup campaign %s", campaign.name)
    logger.info("Time interval: [%s - %s]", start_dt, end_dt)

    ds_raw = TimeseriesDataState.get(name="Raw").first()
    ds_clean = TimeseriesDataState.get(name="Clean").first()

    for ts in campaign.timeseries:
        logger.debug("Cleaning data for timeseries %s", ts.name)

        data_df = cleanup_process(
            start_dt,
            end_dt,
            [ts],
            ds_raw,
            inclusive="neither",
        )

        if data_df.empty:
            logger.debug("No data since last run")
            continue

        logger.debug("Writing clean data")
        tsdio.set_timeseries_data(data_df, ds_clean)

        logger.debug("Committing")
        db.session.commit()


@celery.register_task
class Cleanup(BEMServerCoreAsyncTask):
    TASK_FUNCTION = cleanup_data
    DEFAULT_PARAMETERS = {}
