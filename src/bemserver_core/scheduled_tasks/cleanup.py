"""Cleanup scheduled task"""

import sqlalchemy as sqla

from bemserver_core.authorization import AuthMixin, Relation, auth
from bemserver_core.celery import celery, logger
from bemserver_core.database import Base, db
from bemserver_core.input_output import tsdio
from bemserver_core.model import Campaign, Timeseries, TimeseriesDataState
from bemserver_core.process.cleanup import cleanup as cleanup_process


class ST_CleanupByCampaign(AuthMixin, Base):
    __tablename__ = "st_cleanups_by_campaigns"

    id = sqla.Column(sqla.Integer, primary_key=True)
    campaign_id = sqla.Column(
        sqla.ForeignKey("campaigns.id"), unique=True, nullable=False
    )
    is_enabled = sqla.Column(sqla.Boolean, default=True, nullable=False)
    campaign = sqla.orm.relationship(
        "Campaign",
        backref=sqla.orm.backref(
            "st_cleanups_by_campaigns", cascade="all, delete-orphan"
        ),
    )

    @classmethod
    def get_all(cls, *, is_enabled=None, **kwargs):
        """Get the cleanup service state for all campaigns, even if campaign has
        not explicitly a relation with cleanup (campaign has never been cleaned yet).
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
        cleanup_subq = sqla.orm.aliased(
            ST_CleanupByCampaign,
            alias=ST_CleanupByCampaign.get(**kwargs).subquery(),
        )

        # Main request.
        query = db.session.query(
            cleanup_subq.id,
            camp_subq.id.label("campaign_id"),
            camp_subq.name.label("campaign_name"),
            cleanup_subq.is_enabled,
        ).join(cleanup_subq, cleanup_subq.campaign_id == camp_subq.id, isouter=True)

        # Apply a special filter for is_enabled attribute (None is considered as False).
        if is_enabled is not None:
            query = cls._filter_bool_none_as_false(
                query, cleanup_subq.is_enabled, is_enabled
            )

        # Apply sort on final result.
        if sort is not None:
            for field in sort:
                cls_field = cleanup_subq
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


class ST_CleanupByTimeseries(AuthMixin, Base):
    __tablename__ = "st_cleanups_by_ts"

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

    @classmethod
    def get_all(cls, **kwargs):
        """Get the last cleanup timestamp for all timeseries, even if timeseries
        has never been cleaned yet (because just new...).
        """
        # Extract sort info to apply it at the end.
        sort = kwargs.pop("sort", None)

        # Extract and prepare kwargs for each sub-request.
        ts_kwargs = {}
        if "in_timeseries_name" in kwargs:
            ts_kwargs["in_name"] = kwargs.pop("in_timeseries_name")
        if "campaign_id" in kwargs:
            ts_kwargs["campaign_id"] = kwargs["campaign_id"]

        # Prepare sub-requests.
        ts_subq = sqla.orm.aliased(
            Timeseries,
            alias=Timeseries.get(**ts_kwargs).subquery(),
        )
        cleanup_subq = sqla.orm.aliased(
            ST_CleanupByTimeseries,
            alias=ST_CleanupByTimeseries.get(**kwargs).subquery(),
        )

        # Main request.
        query = db.session.query(
            cleanup_subq.id,
            ts_subq.id.label("timeseries_id"),
            ts_subq.name.label("timeseries_name"),
            ts_subq.unit_symbol.label("timeseries_unit_symbol"),
            cleanup_subq.last_timestamp,
        ).join(cleanup_subq, cleanup_subq.timeseries_id == ts_subq.id, isouter=True)

        # Apply sort on final result.
        if sort is not None:
            for sort_field in sort:
                cls_field = cleanup_subq
                if "timeseries_" in sort_field:
                    sort_field = sort_field.replace("timeseries_", "")
                    cls_field = ts_subq
                # nulls_last ensures that null timestamps stay at the end of results,
                #  whatever the sort direction.
                query = cls_field._apply_sort_query_filter(
                    query,
                    sort_field,
                    nulls_last=sort_field.endswith("last_timestamp"),
                )

        return query


def cleanup_data():
    ds_raw = TimeseriesDataState.get(name="Raw").first()
    ds_clean = TimeseriesDataState.get(name="Clean").first()

    for cbc in ST_CleanupByCampaign.get(is_enabled=True):
        campaign = cbc.campaign

        logger.info("Cleanup campaign %s", campaign.name)

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


@celery.task(name="Cleanup")
def cleanup_scheduled_task():
    logger.info("Start")

    cleanup_data()
