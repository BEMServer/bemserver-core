"""Timeseries data I/O"""
import csv

import sqlalchemy as sqla
import numpy as np
import pandas as pd
import dateutil

from bemserver_core.database import db
from bemserver_core.model import (
    Timeseries,
    TimeseriesData,
    TimeseriesByDataState,
)
from bemserver_core.authorization import auth, get_current_user
from bemserver_core.exceptions import (
    TimeseriesDataIOInvalidAggregationError,
    TimeseriesDataCSVIOError,
)

from .base import BaseCSVIO


AGGREGATION_FUNCTIONS = ("avg", "sum", "min", "max", "count")


class TimeseriesDataIO:
    """Base class for TimeseriesData IO classes"""

    @classmethod
    def set_timeseries_data(cls, data_df, data_state, campaign):
        """Insert timeseries data

        :param DataFrame data_df: Input timeseries data
        :param TimeseriesDataState data_state: Timeseries data state
        :param Campaign campaign: Campaign
        """
        timeseries = data_df.columns
        if campaign is None:
            timeseries = Timeseries.get_many_by_id(timeseries)
        else:
            timeseries = Timeseries.get_many_by_name(campaign, timeseries)

        # Check permissions
        for ts in timeseries:
            auth.authorize(get_current_user(), "write_data", ts)

        # Get timeseries x data states ids
        tsbds_ids = [
            ts.get_timeseries_by_data_state(data_state).id for ts in timeseries
        ]

        data_df.columns = tsbds_ids

        data_df = data_df.melt(
            value_vars=data_df.columns,
            var_name="timeseries_by_data_state_id",
            ignore_index=False,
        )
        data_dict = data_df.reset_index().to_dict(orient="records")

        query = (
            sqla.dialects.postgresql.insert(TimeseriesData)
            .values(data_dict)
            .on_conflict_do_nothing()
        )
        db.session.execute(query)
        db.session.commit()

    @staticmethod
    def _fill_missing_columns(data_df, ts_l, attr, fill_value=np.nan):
        """Add missing columns, in query order"""
        for idx, ts in enumerate(ts_l):
            val = getattr(ts, attr)
            if val not in data_df:
                data_df.insert(idx, val, fill_value)

    @classmethod
    def get_timeseries_data(
        cls, start_dt, end_dt, timeseries, data_state, col_label="id"
    ):
        """Export timeseries data

        :param datetime start_dt: Time interval lower bound (tz-aware)
        :param datetime end_dt: Time interval exclusive upper bound (tz-aware)
        :param list timeseries: List of timeseries
        :param TimeseriesDataState data_state: Timeseries data state
        :param string col_label: Timeseries attribute to use for column header.
            Should be "id" or "name". Default: "id".

        Returns a dataframe.
        """
        # Check permissions
        for ts in timeseries:
            auth.authorize(get_current_user(), "read_data", ts)

        # Get timeseries data
        data = db.session.execute(
            sqla.select(
                TimeseriesData.timestamp,
                Timeseries.id,
                Timeseries.name,
                TimeseriesData.value,
            )
            .filter(
                TimeseriesData.timeseries_by_data_state_id == TimeseriesByDataState.id
            )
            .filter(TimeseriesByDataState.data_state_id == data_state.id)
            .filter(TimeseriesByDataState.timeseries_id == Timeseries.id)
            .filter(Timeseries.id.in_(ts.id for ts in timeseries))
            .filter(start_dt <= TimeseriesData.timestamp)
            .filter(TimeseriesData.timestamp < end_dt)
        ).all()

        data_df = pd.DataFrame(
            data, columns=("timestamp", "id", "name", "value")
        ).set_index("timestamp")
        data_df["value"] = data_df["value"].astype(float)
        data_df.index = pd.DatetimeIndex(data_df.index)

        data_df = data_df.pivot(columns=col_label, values="value")

        cls._fill_missing_columns(data_df, timeseries, col_label)

        return data_df

    @classmethod
    def get_timeseries_buckets_data(
        cls,
        start_dt,
        end_dt,
        timeseries,
        data_state,
        bucket_width,
        timezone="UTC",
        aggregation="avg",
        col_label="id",
    ):
        """Bucket timeseries data and export

        :param datetime start_dt: Time interval lower bound (tz-aware)
        :param datetime end_dt: Time interval exclusive upper bound (tz-aware)
        :param list timeseries: List of timeseries
        :param TimeseriesDataState data_state: Timeseries data state
        :param str bucket_width: Bucket width of the form "value unit" where
            value is an int and unit a string in
            {"second", "minute", "hour", "day", "week", "month", "year"}
            E.g.: "1 day", "3 year"
        :param str timezone: IANA timezone
        :param str aggreagation: Aggregation function. Must be one of
            "avg", "sum", "min", "max" and "count".
        :param string col_label: Timeseries attribute to use for column header.
            Should be "id" or "name". Default: "id".

        Returns a dataframe.
        """
        if aggregation not in AGGREGATION_FUNCTIONS:
            raise TimeseriesDataIOInvalidAggregationError("Invalid aggregation method")

        # Check permissions
        for ts in timeseries:
            auth.authorize(get_current_user(), "read_data", ts)

        # Get timeseries data
        query = sqla.text(
            "SELECT time_bucket("
            " :bucket_width, timestamp AT TIME ZONE :timezone)"
            f"  AS bucket, timeseries.id, timeseries.name, {aggregation}(value) "
            "FROM timeseries_data, timeseries, timeseries_by_data_states "
            "WHERE timeseries_data.timeseries_by_data_state_id = "
            "      timeseries_by_data_states.id "
            "  AND timeseries_by_data_states.data_state_id = :data_state_id "
            "  AND timeseries_by_data_states.timeseries_id = timeseries.id "
            "  AND timeseries_id IN :timeseries_ids "
            "  AND timestamp >= :start_dt AND timestamp < :end_dt "
            "GROUP BY bucket, timeseries.id "
            "ORDER BY bucket;"
        )
        params = {
            "bucket_width": bucket_width,
            "timezone": timezone,
            "timeseries_ids": tuple(ts.id for ts in timeseries),
            "data_state_id": data_state.id,
            "start_dt": start_dt,
            "end_dt": end_dt,
        }
        data = db.session.execute(query, params)

        data_df = pd.DataFrame(
            data, columns=("timestamp", "id", "name", "value")
        ).set_index("timestamp")
        data_df.index = pd.DatetimeIndex(data_df.index).tz_localize(timezone)
        data_df = data_df.pivot(columns=col_label, values="value")

        cls._fill_missing_columns(data_df, timeseries, col_label)

        return data_df

    @classmethod
    def delete(cls, start_dt, end_dt, timeseries, data_state):
        """Delete timeseries data

        :param datetime start_dt: Time interval lower bound (tz-aware)
        :param datetime end_dt: Time interval exclusive upper bound (tz-aware)
        :param list timeseries: List of timeseries IDs or names
        :param TimeseriesDataState data_state: Timeseries data state
        """
        # Check permissions
        for ts in timeseries:
            auth.authorize(get_current_user(), "write_data", ts)

        # Delete timeseries data
        (
            db.session.query(TimeseriesData)
            .filter(
                TimeseriesData.timeseries_by_data_state_id == TimeseriesByDataState.id
            )
            .filter(TimeseriesByDataState.data_state_id == data_state.id)
            .filter(TimeseriesByDataState.timeseries_id == Timeseries.id)
            .filter(Timeseries.id.in_(ts.id for ts in timeseries))
            .filter(start_dt <= TimeseriesData.timestamp)
            .filter(TimeseriesData.timestamp < end_dt)
            .delete(synchronize_session=False)
        )
        db.session.commit()


class TimeseriesDataCSVIO(TimeseriesDataIO, BaseCSVIO):
    @classmethod
    def import_csv(cls, csv_file, data_state, campaign=None):
        """Import CSV file

        :param srt|TextIOBase csv_file: CSV as string or text stream
        :param TimeseriesDataState data_state: Timeseries data state
        :param Campaign campaign: Campaign

        If campaign is None, the CSV header is expected to contain timeseries IDs.
        Otherwise, timeseries names are expected.
        """
        csv_file = cls._enforce_iterator(csv_file)

        # Check header first. Some errors are hard to catch in or after pandas read_csv
        reader = csv.reader(csv_file)
        try:
            header = next(reader)
        except StopIteration as exc:
            raise TimeseriesDataCSVIOError("Missing headers line") from exc
        if "" in header:
            raise TimeseriesDataCSVIOError("Empty timeseries name or trailing comma")

        # Rewind cursor, otherwise header is alreay consumed and not passed to read_csv
        csv_file.seek(0)
        try:
            data_df = pd.read_csv(csv_file, index_col=0)
        except pd.errors.EmptyDataError as exc:
            raise TimeseriesDataCSVIOError("Empty file") from exc

        # Index
        try:
            data_df.index = pd.DatetimeIndex(data_df.index, name="timestamp")
        except dateutil.parser._parser.ParserError as exc:
            raise TimeseriesDataCSVIOError("Invalid timestamp") from exc

        # Values
        try:
            data_df = data_df.astype(float)
        except ValueError as exc:
            raise TimeseriesDataCSVIOError("Invalid values") from exc

        # Cast timeseries ID to int if needed
        if campaign is None:
            invalid_timeseries = [ts for ts in data_df.columns if not ts.isdecimal()]
            if invalid_timeseries:
                raise TimeseriesDataCSVIOError(
                    f"Invalid timeseries IDs: {invalid_timeseries}"
                )
            data_df.columns = [int(col_name) for col_name in data_df.columns]

        # Insert data
        cls.set_timeseries_data(data_df, data_state=data_state, campaign=campaign)

    @classmethod
    def export_csv(cls, start_dt, end_dt, timeseries, data_state, col_label="id"):
        """Export timeseries data as CSV file

        :param datetime start_dt: Time interval lower bound (tz-aware)
        :param datetime end_dt: Time interval exclusive upper bound (tz-aware)
        :param list timeseries: List of timeseries
        :param TimeseriesDataState data_state: Timeseries data state
        :param string col_label: Timeseries attribute to use for column header.
            Should be "id" or "name". Default: "id".

        Returns csv as a string.
        """
        data_df = cls.get_timeseries_data(
            start_dt,
            end_dt,
            timeseries,
            data_state,
            col_label=col_label,
        )
        data_df.index.name = "Datetime"

        # Specify ISO 8601 manually
        # https://github.com/pandas-dev/pandas/issues/27328
        return data_df.to_csv(date_format="%Y-%m-%dT%H:%M:%S%z")

    @classmethod
    def export_csv_bucket(
        cls,
        start_dt,
        end_dt,
        timeseries,
        data_state,
        bucket_width,
        timezone="UTC",
        aggregation="avg",
        col_label="id",
    ):
        """Bucket timeseries data and export as CSV file

        :param datetime start_dt: Time interval lower bound (tz-aware)
        :param datetime end_dt: Time interval exclusive upper bound (tz-aware)
        :param list timeseries: List of timeseries
        :param TimeseriesDataState data_state: Timeseries data state
        :param str bucket_width: Bucket width of the form "value unit" where
            value is an int and unit a string in
            {"second", "minute", "hour", "day", "week", "month", "year"}
            E.g.: "1 day", "3 year"
        :param str timezone: IANA timezone
        :param str aggreagation: Aggregation function. Must be one of
            "avg", "sum", "min" and "max".
        :param string col_label: Timeseries attribute to use for column header.
            Should be "id" or "name". Default: "id".

        Returns csv as a string.
        """
        data_df = cls.get_timeseries_buckets_data(
            start_dt,
            end_dt,
            timeseries,
            data_state,
            bucket_width,
            timezone=timezone,
            aggregation=aggregation,
            col_label=col_label,
        )
        data_df.index.name = "Datetime"

        # Specify ISO 8601 manually
        # https://github.com/pandas-dev/pandas/issues/27328
        return data_df.to_csv(date_format="%Y-%m-%dT%H:%M:%S%z")


tsdio = TimeseriesDataIO()
tsdcsvio = TimeseriesDataCSVIO()
