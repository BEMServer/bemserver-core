"""Download weather data scheduled task"""
import datetime as dt
from zoneinfo import ZoneInfo

import sqlalchemy as sqla

from bemserver_core.model import Site
from bemserver_core.database import Base, db
from bemserver_core.authorization import AuthMixin, auth, Relation
from bemserver_core.time_utils import floor, make_date_range_around_datetime
from bemserver_core.process.weather import wdp
from bemserver_core.celery import celery, logger
from bemserver_core.exceptions import BEMServerCorePeriodError


class ST_DownloadWeatherDataBySite(AuthMixin, Base):
    __tablename__ = "st_dl_weather_data_by_site"

    id = sqla.Column(sqla.Integer, primary_key=True)
    site_id = sqla.Column(sqla.ForeignKey("sites.id"), unique=True, nullable=False)
    is_enabled = sqla.Column(sqla.Boolean, default=True, nullable=False)
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
            ST_DownloadWeatherDataBySite,
            alias=ST_DownloadWeatherDataBySite.get(**kwargs).subquery(),
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


def download_weather_data(
    datetime, period, period_multiplier, periods_before, periods_after
):
    logger.debug("datetime: %s", datetime)

    try:
        round_dt = floor(datetime, period, period_multiplier)
        start_dt, end_dt = make_date_range_around_datetime(
            round_dt, period, period_multiplier, periods_before, periods_after
        )
    except BEMServerCorePeriodError as exc:
        logger.critical(str(exc))
        raise

    for dwdbs in ST_DownloadWeatherDataBySite.get(is_enabled=True):
        site = dwdbs.site
        logger.debug(
            "Getting weather data for site %s for period [%s, %s]",
            site.name,
            start_dt.isoformat(),
            end_dt.isoformat(),
        )
        wdp.get_weather_data_for_site(site, start_dt, end_dt)


@celery.task(name="DownloadWeatherDataScheduledTask")
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
    )

    logger.debug("Committing")
    db.session.commit()
