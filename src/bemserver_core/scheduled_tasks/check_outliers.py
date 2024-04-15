"""Check outliers scheduled task"""

import datetime as dt
from zoneinfo import ZoneInfo

import sqlalchemy as sqla

from bemserver_core.authorization import AuthMixin, Relation, auth
from bemserver_core.celery import celery, logger
from bemserver_core.database import Base, db
from bemserver_core.exceptions import BEMServerCorePeriodError
from bemserver_core.model import (
    Campaign,
    Event,
    EventCategory,
    EventLevelEnum,
    Timeseries,
    TimeseriesByDataState,
    TimeseriesByEvent,
    TimeseriesData,
    TimeseriesDataState,
)
from bemserver_core.process.cleanup import cleanup
from bemserver_core.time_utils import floor, make_date_range_around_datetime

SERVICE_NAME = "BEMServer - Check outliers"


class ST_CheckOutliersByCampaign(AuthMixin, Base):
    __tablename__ = "st_check_outliers_by_campaigns"

    id = sqla.Column(sqla.Integer, primary_key=True)
    campaign_id = sqla.Column(
        sqla.ForeignKey("campaigns.id"), unique=True, nullable=False
    )
    is_enabled = sqla.Column(sqla.Boolean, default=True, nullable=False)
    campaign = sqla.orm.relationship(
        "Campaign",
        backref=sqla.orm.backref(
            "st_check_outliers_by_campaigns", cascade="all, delete-orphan"
        ),
    )

    @classmethod
    def get_all(cls, *, is_enabled=None, **kwargs):
        """Get "check outliers" service state for all campaigns, even if campaign has no
        explicit relation with "check outliers" (campaign has never been checked yet).
        """
        # Extract sort info to apply it at the end.
        sort = kwargs.pop("sort", None)

        # Extract and prepare kwargs for each sub-request.
        camp_kwargs = {}
        if "in_campaign_name" in kwargs:
            camp_kwargs["in_name"] = kwargs.pop("in_campaign_name")
        if "campaign_id" in kwargs:
            camp_kwargs["id"] = kwargs.pop("campaign_id")

        # Prepare sub-requests.
        camp_subq = sqla.orm.aliased(
            Campaign,
            alias=Campaign.get(**camp_kwargs).subquery(),
        )
        check_outliers_subq = sqla.orm.aliased(
            ST_CheckOutliersByCampaign,
            alias=ST_CheckOutliersByCampaign.get(**kwargs).subquery(),
        )

        # Main request.
        query = db.session.query(
            check_outliers_subq.id,
            camp_subq.id.label("campaign_id"),
            camp_subq.name.label("campaign_name"),
            check_outliers_subq.is_enabled,
        ).join(
            check_outliers_subq,
            check_outliers_subq.campaign_id == camp_subq.id,
            isouter=True,
        )

        # Apply a special filter for is_enabled attribute (None is considered as False).
        if is_enabled is not None:
            query = cls._filter_bool_none_as_false(
                query, check_outliers_subq.is_enabled, is_enabled
            )

        # Apply sort on final result.
        if sort is not None:
            for field in sort:
                cls_field = check_outliers_subq
                if "campaign_" in field:
                    field = field.replace("campaign_", "")
                    cls_field = camp_subq
                query = cls_field._apply_sort_query_filter(query, field)

        return query

    @classmethod
    def register_class(cls):
        auth.register_class(
            cls,
            fields={
                "campaign": Relation(
                    kind="one",
                    other_type="Campaign",
                    my_field="campaign_id",
                    other_field="id",
                ),
            },
        )


def check_outliers_ts_data(
    datetime,
    period,
    period_multiplier,
    periods_before=1,
    periods_after=0,
    min_correctness_ratio=0.9,
):
    logger.debug("datetime: %s", datetime)

    try:
        round_dt = floor(datetime, period, period_multiplier)
        start_dt, end_dt = make_date_range_around_datetime(
            round_dt,
            period,
            period_multiplier,
            periods_before,
            periods_after,
        )
    except BEMServerCorePeriodError as exc:
        logger.critical(str(exc))
        raise

    logger.debug("Check interval: [%s - %s]", start_dt, end_dt)

    ds_raw = TimeseriesDataState.get(name="Raw").first()
    ec_data_outliers = EventCategory.get(name="Data outliers").first()
    ec_data_no_outliers = EventCategory.get(name="No data outliers").first()

    for cbc in ST_CheckOutliersByCampaign.get(is_enabled=True):
        campaign = cbc.campaign

        logger.info("Checking outliers data for campaign %s", campaign.name)

        for c_scope in campaign.campaign_scopes:
            logger.debug("Checking outliers data for campaign scope %s", c_scope.name)

            if not c_scope.timeseries:
                continue

            logger.debug("Querying for timeseries with outliers already")

            # Check current status: which timeseries have outliers in last period
            ts_status_outliers = {}
            for ts in c_scope.timeseries:
                query = (
                    db.session.query(Event)
                    .join(TimeseriesByEvent)
                    .filter(TimeseriesByEvent.timeseries_id == ts.id)
                    .filter(
                        sqla.or_(
                            Event.category_id == ec_data_outliers.id,
                            Event.category_id == ec_data_no_outliers.id,
                        )
                    )
                    .order_by(sqla.desc(Event.timestamp))
                    .limit(1)
                )
                tbes = list(query)
                ts_status_outliers[(ts.id, ts.name)] = (
                    bool(tbes) and tbes[0].category_id == ec_data_outliers.id
                )

            logger.debug("Timeseries outliers status: %s", ts_status_outliers)

            # Get count for each TS
            stmt = (
                sqla.select(
                    Timeseries.id,
                    Timeseries.name,
                    sqla.func.count(TimeseriesData.value),
                )
                .filter(
                    TimeseriesData.timeseries_by_data_state_id
                    == TimeseriesByDataState.id
                )
                .filter(TimeseriesByDataState.timeseries_id == Timeseries.id)
                .filter(TimeseriesByDataState.data_state_id == ds_raw.id)
                .filter(
                    TimeseriesByDataState.timeseries_id.in_(
                        ts.id for ts in c_scope.timeseries
                    )
                )
                .filter(start_dt <= TimeseriesData.timestamp)
                .filter(TimeseriesData.timestamp < end_dt)
                .group_by(Timeseries.id)
            )
            ts_counts = {
                ts_id: {"name": name, "count": count}
                for ts_id, name, count in db.session.execute(stmt).all()
            }

            logger.debug("Timeseries counts: %s", ts_counts)

            # Get outliers
            outliers_df = cleanup(
                start_dt, end_dt, c_scope.timeseries, ds_raw, inclusive="left"
            )
            outliers_count = outliers_df.count()

            outliers_ts = [
                (ts_id, ts_info["name"])
                for ts_id, ts_info in ts_counts.items()
                if outliers_count[ts_id] / ts_info["count"] < min_correctness_ratio
            ]

            logger.debug("Timeseries with outliers: %s", outliers_ts)

            new_outliers_ts = [ts for ts in outliers_ts if not ts_status_outliers[ts]]
            already_outliers_ts = [ts for ts in outliers_ts if ts_status_outliers[ts]]
            new_no_outlier_ts = [
                ts
                for ts, outliers in ts_status_outliers.items()
                if outliers and ts not in outliers_ts
            ]

            # Create Event for newly timeseries with outliers
            if new_outliers_ts:
                logger.debug("Creating new timeseries with outliers event")

                event = Event.new(
                    campaign_scope_id=c_scope.id,
                    category_id=ec_data_outliers.id,
                    level=EventLevelEnum.WARNING,
                    timestamp=datetime,
                    source=SERVICE_NAME,
                    description=(
                        "The following timeseries have outliers: "
                        f"{','.join(ts[1] for ts in new_outliers_ts)}"
                    ),
                )
                db.session.flush()

                logger.debug("Creating timeseries x event associations")

                for ts_id, _ts_name in new_outliers_ts:
                    TimeseriesByEvent.new(event_id=event.id, timeseries_id=ts_id)

            # Create Event for timeseries already having outliers
            if already_outliers_ts:
                logger.debug("Creating timeseries already having outliers event")

                event = Event.new(
                    campaign_scope_id=c_scope.id,
                    category_id=ec_data_outliers.id,
                    level=EventLevelEnum.INFO,
                    timestamp=datetime,
                    source=SERVICE_NAME,
                    description=(
                        "The following timeseries still have outliers: "
                        f"{','.join(ts[1] for ts in already_outliers_ts)}"
                    ),
                )
                db.session.flush()

                logger.debug("Creating timeseries x event associations")

                for ts_id, _ts_name in already_outliers_ts:
                    TimeseriesByEvent.new(event_id=event.id, timeseries_id=ts_id)

            # Create Event for (formerly outliers) present data
            if new_no_outlier_ts:
                logger.debug("Creating timeseries without no outliers event")

                event = Event.new(
                    campaign_scope_id=c_scope.id,
                    category_id=ec_data_no_outliers.id,
                    level=EventLevelEnum.INFO,
                    timestamp=datetime,
                    source=SERVICE_NAME,
                    description=(
                        "The following timeseries don't have outliers anymore: "
                        f"{','.join(ts[1] for ts in new_no_outlier_ts)}"
                    ),
                )
                db.session.flush()

                logger.debug("Creating timeseries x event associations")

                for ts_id, _ts_name in new_no_outlier_ts:
                    TimeseriesByEvent.new(event_id=event.id, timeseries_id=ts_id)


@celery.task(name="CheckOutliers")
def check_outliers_scheduled_task(
    period, period_multiplier, timezone="UTC", min_correctness_ratio=0.9
):
    logger.info("Start")

    check_outliers_ts_data(
        dt.datetime.now(tz=ZoneInfo(timezone)),
        period,
        period_multiplier=period_multiplier,
        min_correctness_ratio=min_correctness_ratio,
    )
    logger.debug("Committing")
    db.session.commit()
