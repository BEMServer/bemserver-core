"""Download weather data scheduled task"""

import sqlalchemy as sqla

from bemserver_core.celery import BEMServerCoreAsyncTask, celery, logger
from bemserver_core.database import db
from bemserver_core.exceptions import BEMServerCoreScheduledTaskParametersError
from bemserver_core.model import Site
from bemserver_core.process.weather import wdp


def download_weather_data(campaign, start_dt, end_dt, sites, forecast=False):
    frcst_str = " forecast" if forecast else ""
    logger.info(f"Download weather{frcst_str} data for campaign %s", campaign.name)
    logger.info("Time interval: [%s - %s]", start_dt, end_dt)
    logger.info("Sites: %s", sites)

    for site_name in sites:
        try:
            site = Site.get(name=site_name).one()
        except sqla.exc.NoResultFound as exc:
            error_message = f"Can't find site {site_name} in campaign {campaign.name}"
            logger.critical(error_message)
            raise BEMServerCoreScheduledTaskParametersError(error_message) from exc

        logger.info(f"Getting weather{frcst_str} data for site %s", site.name)
        wdp.get_weather_data_for_site(site, start_dt, end_dt, forecast=forecast)

    logger.debug("Committing")
    db.session.commit()


@celery.register_task
class DownloadWeatherData(BEMServerCoreAsyncTask):
    TASK_FUNCTION = download_weather_data
    DEFAULT_PARAMETERS = {
        "sites": [],
        "forecast": False,
    }
