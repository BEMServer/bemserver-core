"""Check missing data scheduled task"""

import sqlalchemy as sqla

from bemserver_core.celery import BEMServerCoreAsyncTask, celery, logger
from bemserver_core.database import db
from bemserver_core.input_output import tsdio
from bemserver_core.model import (
    Event,
    EventCategory,
    EventLevelEnum,
    Timeseries,
    TimeseriesByEvent,
    TimeseriesDataState,
)

SERVICE_NAME = "BEMServer - Check missing data"


def check_missing_ts_data(campaign, start_dt, end_dt, min_completeness_ratio=0.9):
    logger.info("Check missing data for campaign %s", campaign.name)
    logger.info("Time interval: [%s - %s]", start_dt, end_dt)
    logger.info("min_completeness_ratio: %s", min_completeness_ratio)

    ds_raw = TimeseriesDataState.get(name="Raw").first()
    ec_data_missing = EventCategory.get(name="Data missing").first()
    ec_data_present = EventCategory.get(name="Data present").first()

    for c_scope in campaign.campaign_scopes:
        logger.debug("Checking missing data for campaign scope %s", c_scope.name)

        if not c_scope.timeseries:
            continue

        logger.debug("Querying for already missing timeseries")

        # Check current status: which timeseries are already missing
        ts_status_missing = {}
        for ts in c_scope.timeseries:
            query = (
                db.session.query(Event)
                .join(TimeseriesByEvent)
                .filter(TimeseriesByEvent.timeseries_id == ts.id)
                .filter(
                    sqla.or_(
                        Event.category_id == ec_data_missing.id,
                        Event.category_id == ec_data_present.id,
                    )
                )
                .order_by(sqla.desc(Event.timestamp))
                .limit(1)
            )
            tbes = list(query)
            ts_status_missing[(ts.id, ts.name)] = (
                bool(tbes) and tbes[0].category_id == ec_data_missing.id
            )

            logger.debug("Timeseries missing status: %s", ts_status_missing)

            # Get count for each TS
            counts_df = tsdio.get_timeseries_aggregate_data(
                start_dt,
                end_dt,
                c_scope.timeseries,
                ds_raw,
                agg="count",
            )

            # TS is missing if either count/expected < min or no expectation and count=0
            nb_s = (end_dt - start_dt).total_seconds()
            ts_intervals = Timeseries.get_property_for_many_timeseries(
                [ts.id for ts in c_scope.timeseries], "Interval"
            )
            missing_ts = []
            for timeseries in c_scope.timeseries:
                if ts_intervals[timeseries.id] is not None:
                    if (
                        counts_df.loc[timeseries.id, "count"]
                        * float(ts_intervals[timeseries.id])
                        / nb_s
                        < min_completeness_ratio
                    ):
                        missing_ts.append((timeseries.id, timeseries.name))
                elif counts_df.loc[timeseries.id, "count"] == 0:
                    missing_ts.append((timeseries.id, timeseries.name))

        logger.debug("Missing timeseries: %s", missing_ts)

        new_missing_ts = [ts for ts in missing_ts if not ts_status_missing[ts]]
        already_missing_ts = [ts for ts in missing_ts if ts_status_missing[ts]]
        new_present_ts = [
            ts
            for ts, missing in ts_status_missing.items()
            if missing and ts not in missing_ts
        ]

        # Create Event for newly missing data
        if new_missing_ts:
            logger.debug("Creating new missing timeseries event")

            event = Event.new(
                campaign_scope_id=c_scope.id,
                category_id=ec_data_missing.id,
                level=EventLevelEnum.WARNING,
                timestamp=start_dt,
                source=SERVICE_NAME,
                description=(
                    "The following timeseries are missing: "
                    f"{','.join(ts[1] for ts in new_missing_ts)}"
                ),
            )
            db.session.flush()

            logger.debug("Creating timeseries x event associations")

            for ts_id, _ts_name in new_missing_ts:
                TimeseriesByEvent.new(event_id=event.id, timeseries_id=ts_id)

        # Create Event for already missing data
        if already_missing_ts:
            logger.debug("Creating already missing timeseries event")

            event = Event.new(
                campaign_scope_id=c_scope.id,
                category_id=ec_data_missing.id,
                level=EventLevelEnum.INFO,
                timestamp=start_dt,
                source=SERVICE_NAME,
                description=(
                    "The following timeseries are still missing: "
                    f"{','.join(ts[1] for ts in already_missing_ts)}"
                ),
            )
            db.session.flush()

            logger.debug("Creating timeseries x event associations")

            for ts_id, _ts_name in already_missing_ts:
                TimeseriesByEvent.new(event_id=event.id, timeseries_id=ts_id)

        # Create Event for (formerly missing) present data
        if new_present_ts:
            logger.debug("Creating present timeseries event")

            event = Event.new(
                campaign_scope_id=c_scope.id,
                category_id=ec_data_present.id,
                level=EventLevelEnum.INFO,
                timestamp=start_dt,
                source=SERVICE_NAME,
                description=(
                    "The following timeseries are not missing anymore: "
                    f"{','.join(ts[1] for ts in new_present_ts)}"
                ),
            )
            db.session.flush()

            logger.debug("Creating timeseries x event associations")

            for ts_id, _ts_name in new_present_ts:
                TimeseriesByEvent.new(event_id=event.id, timeseries_id=ts_id)

    logger.debug("Committing")
    db.session.commit()


@celery.register_task
class CheckMissingData(BEMServerCoreAsyncTask):
    TASK_FUNCTION = check_missing_ts_data
    DEFAULT_PARAMETERS = {
        "min_completeness_ratio": 0.9,
    }
