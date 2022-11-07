"""Check missing data scheduled task"""
import datetime as dt
from zoneinfo import ZoneInfo

import sqlalchemy as sqla

from bemserver_core.model import TimeseriesDataState, Campaign, Event, TimeseriesByEvent
from bemserver_core.database import Base, db
from bemserver_core.authorization import AuthMixin, auth, Relation
from bemserver_core.process.completeness import compute_completeness
from bemserver_core.celery import celery, logger
from bemserver_core.time_utils import floor, make_date_offset
from bemserver_core.exceptions import BEMServerCorePeriodError


SERVICE_NAME = "BEMServer - Check missing data"


class ST_CheckMissingByCampaign(AuthMixin, Base):
    __tablename__ = "st_check_missing_by_campaigns"

    id = sqla.Column(sqla.Integer, primary_key=True)
    campaign_id = sqla.Column(
        sqla.ForeignKey("campaigns.id"), unique=True, nullable=False
    )
    is_enabled = sqla.Column(sqla.Boolean, default=True, nullable=False)
    campaign = sqla.orm.relationship(
        "Campaign",
        backref=sqla.orm.backref(
            "st_check_missing_by_campaigns", cascade="all, delete-orphan"
        ),
    )

    @classmethod
    def get_all(cls, *, is_enabled=None, **kwargs):
        """Get "check missing" service state for all campaigns, even if campaign has
        no explicit relation with "check missing" (campaign has never been checked yet).
        """
        # Extract sort info to apply it at the end.
        sort = kwargs.pop("sort", None)

        # Extract and prepare kwargs for each sub-request.
        camp_alias_name = "campaign"
        camp_kwargs = {}
        if f"in_{camp_alias_name}_name" in kwargs:
            camp_kwargs["in_name"] = kwargs.pop(f"in_{camp_alias_name}_name")
        if f"{camp_alias_name}_id" in kwargs:
            camp_kwargs["id"] = kwargs.pop(f"{camp_alias_name}_id")

        # Prepare sub-requests.
        camp_subq = sqla.orm.aliased(
            Campaign,
            alias=Campaign.get(**camp_kwargs).subquery(),
        )
        check_missing_subq = sqla.orm.aliased(
            ST_CheckMissingByCampaign,
            alias=ST_CheckMissingByCampaign.get(**kwargs).subquery(),
        )

        # Main request.
        query = db.session.query(
            check_missing_subq.id,
            camp_subq.id.label(f"{camp_alias_name}_id"),
            camp_subq.name.label(f"{camp_alias_name}_name"),
            check_missing_subq.is_enabled,
        ).join(
            check_missing_subq,
            check_missing_subq.campaign_id == camp_subq.id,
            isouter=True,
        )

        # Apply a special filter for is_enabled attribute (None is considered as False).
        if is_enabled is not None:
            if is_enabled:
                query = query.filter(check_missing_subq.is_enabled == is_enabled)
            else:
                query = query.filter(
                    sqla.or_(
                        check_missing_subq.is_enabled == is_enabled,
                        check_missing_subq.is_enabled.is_(None),
                    )
                )

        # Apply sort on final result.
        if sort is not None:
            for field in sort:
                cls_field = check_missing_subq
                if camp_alias_name in field:
                    field = field.replace(f"{camp_alias_name}_", "")
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


def check_missing_ts_data(
    datetime, period, period_multiplier, timezone="UTC", min_completeness_ratio=0.9
):
    logger.debug("datetime: %s", datetime)

    # Check last period before datetime
    offset = make_date_offset(period, period_multiplier)
    try:
        start_dt = floor(datetime - offset, period, period_multiplier)
    except BEMServerCorePeriodError as exc:
        logger.critical(str(exc))
    end_dt = start_dt + offset

    logger.debug("Check interval: [%s - %s]", start_dt, end_dt)

    ds_raw = TimeseriesDataState.get(name="Raw").first()

    for cbc in ST_CheckMissingByCampaign.get():
        campaign = cbc.campaign

        logger.info("Checking missing data for campaign %s", campaign.name)

        for c_scope in campaign.campaign_scopes:

            logger.debug("Checking missing data for campaign scope %s", c_scope.name)

            if not c_scope.timeseries:
                continue

            completeness = compute_completeness(
                start_dt,
                end_dt,
                c_scope.timeseries,
                ds_raw,
                period_multiplier,
                period,
                timezone=timezone,
            )

            # If interval is unknown, compute_completeness tries to guess
            # Since it works on a single bucket, either there is data and
            # ratio is 1, either there is no data and ratio is 0.
            # Consider missing only if 0. If even a single sample is present,
            # we can't conclude without knowing the expected interval.
            missing_ts = [
                (ts_id, ts_info["name"])
                for ts_id, ts_info in completeness["timeseries"].items()
                # Timeseries missing if ratio < threshold
                if (
                    (ratio := ts_info["avg_ratio"]) is not None
                    and ratio < min_completeness_ratio
                )
                # or no data in check period, whatever the expected interval
                or ts_info["avg_count"] == 0
            ]
            logger.debug("Missing timeseries: %s", missing_ts)

            if missing_ts:

                logger.debug("Creating event")

                event = Event.new(
                    campaign_scope_id=c_scope.id,
                    category="Data missing",
                    level="WARNING",
                    timestamp=datetime,
                    source=SERVICE_NAME,
                    description=f"Missing timeseries: {[m[1] for m in missing_ts]}",
                )
                db.session.flush()

                logger.debug("Creating timeseries x event associations")

                for ts_id, _ts_name in missing_ts:
                    TimeseriesByEvent.new(event_id=event.id, timeseries_id=ts_id)

                # TODO: add Event for (formerly missing) present data?

                logger.debug("Committing")
                db.session.commit()


@celery.task(name="CheckMissing")
def check_missing_scheduled_task(
    period, period_multiplier, timezone="UTC", min_completeness_ratio=0.9
):
    logger.info("Start")

    check_missing_ts_data(
        dt.datetime.now(tz=ZoneInfo(timezone)),
        period,
        period_multiplier=period_multiplier,
        timezone=timezone,
        min_completeness_ratio=min_completeness_ratio,
    )
