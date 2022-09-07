"""Cleanup scheduled task"""
import sqlalchemy as sqla

from bemserver_core.model import Timeseries, TimeseriesDataState
from bemserver_core.input_output import tsdio
from bemserver_core.database import Base, db
from bemserver_core.authorization import AuthMixin, auth, Relation
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


class ST_CleanupByTimeseries(AuthMixin, Base):
    __tablename__ = "st_cleanups_by_timeseries"

    id = sqla.Column(sqla.Integer, primary_key=True)
    timeseries_id = sqla.Column(
        sqla.ForeignKey("timeseries.id"), unique=True, nullable=False
    )
    last_timestamp = sqla.Column(sqla.DateTime(timezone=True))

    timeseries = sqla.orm.relationship(
        "Timeseries",
        backref=sqla.orm.backref(
            "st_cleanups_by_timeseries", cascade="all, delete-orphan"
        ),
    )

    @classmethod
    def register_class(cls):
        auth.register_class(
            cls,
            fields={
                "timeseries": Relation(
                    kind="one",
                    other_type="Timeseries",
                    my_field="timeseries_id",
                    other_field="id",
                ),
            },
        )

    @classmethod
    def get(cls, *, campaign_id=None, **kwargs):
        query = super().get(**kwargs)
        # Filter by campaign
        if campaign_id is not None:
            timeseries = sqla.orm.aliased(Timeseries)
            query = query.join(timeseries, cls.timeseries_id == timeseries.id).filter(
                timeseries.campaign_id == campaign_id
            )
        return query


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

                cbt = ST_CleanupByTimeseries.get(timeseries_id=ts.id).first()
                if cbt is None:
                    cbt = ST_CleanupByTimeseries.new(timeseries_id=ts.id)
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


celery.add_periodic_task(300, cleanup_scheduled_task)
