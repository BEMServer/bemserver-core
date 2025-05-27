"""Check outliers scheduled task"""

import sqlalchemy as sqla

from bemserver_core.celery import BEMServerCoreAsyncTask, celery, logger
from bemserver_core.database import db
from bemserver_core.input_output import tsdio
from bemserver_core.model import (
    Event,
    EventCategory,
    EventLevelEnum,
    TimeseriesByEvent,
    TimeseriesDataState,
)
from bemserver_core.process.cleanup import cleanup

SERVICE_NAME = "BEMServer - Check outliers"


def check_outliers_ts_data(
    campaign,
    start_dt,
    end_dt,
    min_correctness_ratio=0.9,
):
    logger.info("Check outliers for campaign %s", campaign.name)
    logger.info("Time interval: [%s - %s]", start_dt, end_dt)
    logger.info("min_correctness_ratio: %s", min_correctness_ratio)

    ds_raw = TimeseriesDataState.get(name="Raw").first()
    ec_data_outliers = EventCategory.get(name="Data outliers").first()
    ec_data_no_outliers = EventCategory.get(name="No data outliers").first()

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
        data_df = tsdio.get_timeseries_aggregate_data(
            start_dt,
            end_dt,
            c_scope.timeseries,
            ds_raw,
            agg="count",
        )
        ts_counts = {
            ts.id: {"name": ts.name, "count": data_df.loc[ts.id, "count"]}
            for ts in c_scope.timeseries
            # Only keep TS with count > 0 to avoid zero division later
            # 0 count TS don't have outliers anyway
            if data_df.loc[ts.id, "count"]
        }

        logger.debug("Timeseries counts: %s", ts_counts)

        # Get correct data count for each TS
        correct_df = cleanup(
            start_dt, end_dt, c_scope.timeseries, ds_raw, inclusive="left"
        )
        correct_count = correct_df.count()

        # Compare correct/total to min correctness ratio
        outliers_ts = [
            (ts_id, ts_info["name"])
            for ts_id, ts_info in ts_counts.items()
            if correct_count[ts_id] / ts_info["count"] < min_correctness_ratio
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
                timestamp=start_dt,
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
                timestamp=start_dt,
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
                timestamp=start_dt,
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

    logger.debug("Committing")
    db.session.commit()


@celery.register_task
class CheckOutliers(BEMServerCoreAsyncTask):
    TASK_FUNCTION = check_outliers_ts_data
    DEFAULT_PARAMETERS = {
        "min_correctness_ratio": 0.9,
    }
