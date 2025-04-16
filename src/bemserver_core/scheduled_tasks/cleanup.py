"""Cleanup scheduled task"""

from bemserver_core.celery import celery, logger
from bemserver_core.database import db
from bemserver_core.input_output import tsdio
from bemserver_core.model import TimeseriesDataState
from bemserver_core.process.cleanup import cleanup as cleanup_process
from bemserver_core.scheduled_tasks.tasks import TaskByCampaign

CLEANUP_TASK_NAME = "Cleanup"


def cleanup_data(campaign, start_dt, end_dt):
    logger.info("Cleanup campaign %s", campaign.name)

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


@celery.task(name=CLEANUP_TASK_NAME)
def cleanup_scheduled_task():
    logger.info("Start")

    for tbc in TaskByCampaign.get(name=CLEANUP_TASK_NAME, is_enabled=True):
        start_dt, end_dt = TaskByCampaign.make_interval(tbc.campaign, tbc.parameters)
        cleanup_data(tbc.campaign, start_dt, end_dt)
