"""Download weather data scheduled task"""

import sqlalchemy as sqla

from bemserver_core.celery import celery, logger
from bemserver_core.database import db
from bemserver_core.exceptions import BEMServerCoreScheduledTaskParametersError
from bemserver_core.model import Site
from bemserver_core.process.weather import wdp
from bemserver_core.scheduled_tasks.tasks import TaskByCampaign

DOWNLOAD_WEATHER_DATA_TASK_NAME = "DownloadWeatherData"
DOWNLOAD_WEATHER_FORECAST_DATA_TASK_NAME = "DownloadWeatherForecastData"


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


@celery.task(name=DOWNLOAD_WEATHER_DATA_TASK_NAME)
def download_weather_data_scheduled_task(
    period, period_multiplier, periods_before, periods_after, timezone="UTC"
):
    logger.info("Start")

    for tbc in TaskByCampaign.get(
        name=DOWNLOAD_WEATHER_DATA_TASK_NAME, is_enabled=True
    ):
        start_dt, end_dt = TaskByCampaign.make_interval(tbc.campaign, tbc.parameters)
        download_weather_data(
            tbc.campaign,
            start_dt,
            end_dt,
            tbc.params["sites"],
            forecast=False,
        )

    logger.debug("Committing")
    db.session.commit()


@celery.task(name=DOWNLOAD_WEATHER_FORECAST_DATA_TASK_NAME)
def download_weather_forecast_data_scheduled_task(
    period, period_multiplier, periods_before, periods_after, timezone="UTC"
):
    logger.info("Start")

    for tbc in TaskByCampaign.get(
        name=DOWNLOAD_WEATHER_FORECAST_DATA_TASK_NAME, is_enabled=True
    ):
        start_dt, end_dt = TaskByCampaign.make_interval(tbc.campaign, tbc.parameters)
        download_weather_data(
            tbc.campaign,
            start_dt,
            end_dt,
            tbc.params["sites"],
            forecast=True,
        )

    logger.debug("Committing")
    db.session.commit()
