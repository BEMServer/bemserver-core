"""Download weather data scheduled task"""

import datetime as dt
from zoneinfo import ZoneInfo

import sqlalchemy as sqla

from bemserver_core.authorization import AuthMixin, Relation, auth
from bemserver_core.celery import celery, logger
from bemserver_core.database import Base, db
from bemserver_core.exceptions import BEMServerCorePeriodError
from bemserver_core.model import Site
from bemserver_core.process.weather import wdp
from bemserver_core.time_utils import floor, make_date_range_around_datetime


class ST_DownloadWeatherDataBySiteBase(Base):
    __abstract__ = True

    id = sqla.Column(sqla.Integer, primary_key=True)
    site_id = sqla.Column(sqla.ForeignKey("sites.id"), unique=True, nullable=False)
    is_enabled = sqla.Column(sqla.Boolean, default=True, nullable=False)

    @classmethod
    def get_all(cls, *, is_enabled=None, **kwargs):
        """Get "download weather data" service state for all sites, even if
        site has no explicit relation with "service".
        """
        # Extract sort info to apply it at the end.
        sort = kwargs.pop("sort", None)

        # Extract and prepare kwargs for each sub-request.
        site_kwargs = {}
        if "in_site_name" in kwargs:
            site_kwargs["in_name"] = kwargs.pop("in_site_name")
        if "campaign_id" in kwargs:
            site_kwargs["campaign_id"] = kwargs.pop("campaign_id")
        if "site_id" in kwargs:
            site_kwargs["id"] = kwargs.pop("site_id")

        # Prepare sub-requests.
        site_subq = sqla.orm.aliased(
            Site,
            alias=Site.get(**site_kwargs).subquery(),
        )
        dwdbs_subq = sqla.orm.aliased(
            cls,
            alias=cls.get(**kwargs).subquery(),
        )

        # Main request.
        query = db.session.query(
            dwdbs_subq.id,
            site_subq.id.label("site_id"),
            site_subq.name.label("site_name"),
            dwdbs_subq.is_enabled,
        ).join(
            dwdbs_subq,
            dwdbs_subq.site_id == site_subq.id,
            isouter=True,
        )

        # Apply a special filter for is_enabled attribute (None is considered as False).
        if is_enabled is not None:
            query = cls._filter_bool_none_as_false(
                query, dwdbs_subq.is_enabled, is_enabled
            )

        # Apply sort on final result.
        if sort is not None:
            for field in sort:
                cls_field = dwdbs_subq
                if "site_" in field:
                    field = field.replace("site_", "")
                    cls_field = site_subq
                query = cls_field._apply_sort_query_filter(query, field)

        return query


class ST_DownloadWeatherDataBySite(AuthMixin, ST_DownloadWeatherDataBySiteBase):
    __tablename__ = "st_dl_weather_data_by_site"

    site = sqla.orm.relationship(
        "Site",
        backref=sqla.orm.backref(
            "st_dl_weather_data_by_site", cascade="all, delete-orphan"
        ),
    )

    @classmethod
    def register_class(cls):
        auth.register_class(
            cls,
            fields={
                "site": Relation(
                    kind="one",
                    other_type="Site",
                    my_field="site_id",
                    other_field="id",
                ),
            },
        )


class ST_DownloadWeatherForecastDataBySite(AuthMixin, ST_DownloadWeatherDataBySiteBase):
    __tablename__ = "st_dl_weather_fcast_data_by_site"

    site = sqla.orm.relationship(
        "Site",
        backref=sqla.orm.backref(
            "st_dl_weather_fcast_data_by_site", cascade="all, delete-orphan"
        ),
    )

    @classmethod
    def register_class(cls):
        auth.register_class(
            cls,
            fields={
                "site": Relation(
                    kind="one",
                    other_type="Site",
                    my_field="site_id",
                    other_field="id",
                ),
            },
        )


def download_weather_data(
    datetime, period, period_multiplier, periods_before, periods_after, forecast=False
):
    logger.debug("datetime: %s", datetime)

    st_dwdbs_cls = (
        ST_DownloadWeatherForecastDataBySite
        if forecast
        else ST_DownloadWeatherDataBySite
    )

    try:
        round_dt = floor(datetime, period, period_multiplier)
        start_dt, end_dt = make_date_range_around_datetime(
            round_dt, period, period_multiplier, periods_before, periods_after
        )
    except BEMServerCorePeriodError as exc:
        logger.critical(str(exc))
        raise

    for dwdbs in st_dwdbs_cls.get(is_enabled=True):
        site = dwdbs.site
        logger.info(
            "Getting weather data for site %s for period [%s, %s]",
            site.name,
            start_dt.isoformat(),
            end_dt.isoformat(),
        )
        wdp.get_weather_data_for_site(site, start_dt, end_dt, forecast=forecast)


@celery.task(name="DownloadWeatherData")
def download_weather_data_scheduled_task(
    period, period_multiplier, periods_before, periods_after, timezone="UTC"
):
    logger.info("Start")

    download_weather_data(
        dt.datetime.now(tz=ZoneInfo(timezone)),
        period,
        period_multiplier,
        periods_before,
        periods_after,
        forecast=False,
    )

    logger.debug("Committing")
    db.session.commit()


@celery.task(name="DownloadWeatherForecastData")
def download_weather_forecast_data_scheduled_task(
    period, period_multiplier, periods_before, periods_after, timezone="UTC"
):
    logger.info("Start")

    download_weather_data(
        dt.datetime.now(tz=ZoneInfo(timezone)),
        period,
        period_multiplier,
        periods_before,
        periods_after,
        forecast=True,
    )

    logger.debug("Committing")
    db.session.commit()
