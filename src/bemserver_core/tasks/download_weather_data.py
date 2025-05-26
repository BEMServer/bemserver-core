"""Download weather data scheduled task"""

import sqlalchemy as sqla

from bemserver_core.celery import (
    BEMServerCoreAsyncTask,
    BEMServerCoreScheduledTask,
    celery,
    logger,
)
from bemserver_core.database import db
from bemserver_core.exceptions import BEMServerCoreScheduledTaskParametersError
from bemserver_core.model import Site
from bemserver_core.process.weather import wdp


def download_weather_data(campaign, start_dt, end_dt, sites, forecast=False):
    for site_name in sites:
        try:
            site = Site.get(name=site_name).one()
        except sqla.exc.NoResultFound as exc:
            error_message = f"Can't find site {site_name} in campaign {campaign.name}"
            logger.critical(error_message)
            raise BEMServerCoreScheduledTaskParametersError(error_message) from exc

        logger.info(
            "Getting weather data for site %s for period [%s, %s]",
            site.name,
            start_dt.isoformat(),
            end_dt.isoformat(),
        )
        wdp.get_weather_data_for_site(site, start_dt, end_dt, forecast=forecast)
    logger.debug("Committing")
    db.session.commit()


@celery.register_task
class DownloadWeatherDataTask(BEMServerCoreAsyncTask):
    TASK_FUNCTION = download_weather_data
    DEFAULT_PARAMETERS = {
        "sites": [],
        "forecast": False,
    }


@celery.register_task
class DownloadWeatherDataScheduledTask(BEMServerCoreScheduledTask):
    TASK_FUNCTION = download_weather_data
    DEFAULT_PARAMETERS = {
        "sites": [],
        "forecast": False,
    }
