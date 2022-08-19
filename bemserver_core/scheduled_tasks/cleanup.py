"""Cleanup scheduled task"""
import sqlalchemy as sqla

from bemserver_core.model import TimeseriesDataState
from bemserver_core.input_output import tsdio
from bemserver_core.database import Base, db
from bemserver_core.authorization import AuthMixin
from bemserver_core.process.cleanup import cleanup as cleanup_process
from bemserver_core.celery import celery, logger


class ST_CleanupByCampaign(AuthMixin, Base):
    __tablename__ = "st_cleanups_by_campaigns"

    id = sqla.Column(sqla.Integer, primary_key=True)
    campaign_id = sqla.Column(
        sqla.ForeignKey("campaigns.id"), unique=True, nullable=False
    )
    enabled = sqla.Column(sqla.Boolean(), nullable=False)
    campaign = sqla.orm.relationship(
        "Campaign",
        backref=sqla.orm.backref(
            "processors_by_campaigns", cascade="all, delete-orphan"
        ),
    )


class ST_CleanupByTimeseries(AuthMixin, Base):
    __tablename__ = "st_cleanups_by_timeseries"

    id = sqla.Column(sqla.Integer, primary_key=True)
    st_cleanup_by_campaign_id = sqla.Column(
        sqla.ForeignKey("st_cleanups_by_campaigns.id"), nullable=False
    )
    timeseries_id = sqla.Column(
        sqla.ForeignKey("timeseries.id"), unique=True, nullable=False
    )
    last_timestamp = sqla.Column(sqla.DateTime(timezone=True))

    st_cleanup_by_campaign = sqla.orm.relationship(
        "ST_CleanupByCampaign",
        backref=sqla.orm.backref(
            "st_cleanups_by_timeseries", cascade="all, delete-orphan"
        ),
    )
    timeseries = sqla.orm.relationship(
        "Timeseries",
        backref=sqla.orm.backref(
            "st_cleanups_by_timeseries", cascade="all, delete-orphan"
        ),
    )


@celery.task(name="Cleanup")
def cleanup_scheduled_task():
    logger.info("Start")

    for cbc in ST_CleanupByCampaign.get():
        campaign = cbc.campaign

        logger.debug(
            "Campaign %s: %s",
            campaign.name,
            "enabled" if cbc.enabled else "disabled",
        )

        if cbc.enabled:

            logger.info("Cleanup campaign %s", campaign.name)

            ds_raw = TimeseriesDataState.get(name="Raw").first()
            ds_clean = TimeseriesDataState.get(name="Clean").first()

            logger.debug("Cleaning data")

            for ts in campaign.timeseries:

                logger.debug("Cleaning data for timeseries %s", ts.name)

                cbt = ST_CleanupByTimeseries.get(
                    st_cleanup_by_campaign_id=cbc.id
                ).first()
                if cbt is None:
                    cbt = ST_CleanupByTimeseries.new(
                        st_cleanup_by_campaign_id=cbc.id,
                        timeseries_id=ts.id,
                    )
                last_timestamp = cbt.last_timestamp if cbt else None

                data_df = cleanup_process(
                    last_timestamp,
                    None,
                    [ts],
                    ds_raw,
                    inclusive="neither",
                )

                if data_df.empty:
                    logger.debug("No data since last run")
                    continue

                logger.debug("Writing clean data")
                tsdio.set_timeseries_data(data_df, ds_clean)

                last_timestamp = data_df.index[-1]
                logger.debug("Updating last timestamp: %s", last_timestamp)
                cbt.last_timestamp = last_timestamp

                logger.debug("Committing")
                db.session.commit()


celery.add_periodic_task(5, cleanup_scheduled_task)
