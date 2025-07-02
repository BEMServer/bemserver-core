"""Cleanup scheduled task"""

from bemserver_core.celery import (
    BEMServerCoreAsyncTask,
    BEMServerCoreScheduledTask,
    BEMServerCoreTask,
    celery,
    logger,
)
from bemserver_core.database import db
from bemserver_core.input_output import tsdio
from bemserver_core.model import TimeseriesDataState
from bemserver_core.process.cleanup import cleanup as cleanup_process


class CleanupBase(BEMServerCoreTask):
    DEFAULT_PARAMETERS = {}

    def do_run(self, campaign, start_dt, end_dt):
        logger.info("Cleanup campaign %s", campaign.name)
        logger.info("Time interval: [%s - %s]", start_dt, end_dt)

        ds_raw = TimeseriesDataState.get(name="Raw").first()
        ds_clean = TimeseriesDataState.get(name="Clean").first()

        nb_ts = len(campaign.timeseries)

        for idx, ts in enumerate(campaign.timeseries):
            logger.debug(
                "Cleaning data for timeseries %s [%s/s%]", ts.name, idx + 1, nb_ts
            )

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

            self.set_progress(idx + 1, nb_ts)


@celery.register_task
class Cleanup(CleanupBase, BEMServerCoreAsyncTask):
    """Cleanup async task"""


@celery.register_task
class CleanupScheduled(CleanupBase, BEMServerCoreScheduledTask):
    """Cleanup scheduled task"""
