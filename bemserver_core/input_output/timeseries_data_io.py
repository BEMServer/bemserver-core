"""Timeseries data I/O"""
import re

import sqlalchemy as sqla
import pandas as pd

from bemserver_core.database import db
from bemserver_core.model import (
    Timeseries,
    TimeseriesData,
    TimeseriesByDataState,
)
from bemserver_core.authorization import auth, get_current_user
from bemserver_core.exceptions import (
    TimeseriesDataIOInvalidAggregationError,
    TimeseriesDataIOWriteError,
    TimeseriesDataCSVIOError,
)

from .base import BaseCSVIO


AGGREGATION_FUNCTIONS = ("avg", "sum", "min", "max")

# Copied from Django
ISO8601_DATETIME_RE = re.compile(
    r"(?P<year>\d{4})-(?P<month>\d{1,2})-(?P<day>\d{1,2})"
    r"[T ](?P<hour>\d{1,2}):(?P<minute>\d{1,2})"
    r"(?::(?P<second>\d{1,2})(?:\.(?P<microsecond>\d{1,6})\d{0,6})?)?"
    r"(?P<tzinfo>Z|[+-]\d{2}(?::?\d{2})?)?$"
)


class TimeseriesDataIO:
    """Base class for TimeseriesData IO classes"""

    @classmethod
    def _set_timeseries_data(cls, data):
        """Insert timeseries data

        :param list data: List of dicts, each dict containing a timestamp, a
        timeseries x data state association ID and a value.
        """
        query = (
            sqla.dialects.postgresql.insert(TimeseriesData)
            .values(data)
            .on_conflict_do_nothing()
        )

        try:
            db.session.execute(query)
            db.session.commit()
        except sqla.exc.DBAPIError as exc:
            raise TimeseriesDataIOWriteError("Error writing to DB") from exc

    @staticmethod
    def _fill_missing_columns(data_df, ts_l, attr):
        """Add missing columns, in query order"""
        for idx, ts in enumerate(ts_l):
            val = getattr(ts, attr)
            if val not in data_df:
                data_df.insert(idx, val, None)

    @classmethod
    def _get_timeseries_data(
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

        # Get timeseries x data states ids
        # TODO: integrate to query below
        tsbds_ids = [
            ts.get_timeseries_by_data_state(data_state).id for ts in timeseries
        ]

        # Get timeseries data
        data = db.session.execute(
            sqla.select(
                TimeseriesData.timestamp,
                Timeseries.id,
                Timeseries.name,
                TimeseriesData.value,
            )
            .where(Timeseries.id == TimeseriesByDataState.timeseries_id)
            .where(
                TimeseriesData.timeseries_by_data_state_id == TimeseriesByDataState.id
            )
            .filter(TimeseriesData.timeseries_by_data_state_id.in_(tsbds_ids))
            .filter(start_dt <= TimeseriesData.timestamp)
            .filter(TimeseriesData.timestamp < end_dt)
        ).all()

        data_df = pd.DataFrame(
            data, columns=("Datetime", "id", "name", "value")
        ).set_index("Datetime")
        data_df.index = pd.DatetimeIndex(data_df.index)

        data_df = data_df.pivot(columns=col_label, values="value")

        cls._fill_missing_columns(data_df, timeseries, col_label)

        return data_df

    @classmethod
    def _get_timeseries_buckets_data(
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
        :param str bucket_width: Bucket width (ISO 8601 or PostgreSQL interval)
        :param str timezone: IANA timezone
        :param str aggreagation: Aggregation function. Must be one of
            "avg", "sum", "min" and "max".
        :param string col_label: Timeseries attribute to use for column header.
            Should be "id" or "name". Default: "id".

        Returns csv as a string.
        """
        if aggregation not in AGGREGATION_FUNCTIONS:
            raise TimeseriesDataIOInvalidAggregationError("Invalid aggregation method")

        # Check permissions
        for ts in timeseries:
            auth.authorize(get_current_user(), "read_data", ts)

        # Get timeseries x data states ids
        # TODO: integrate to query below
        tsbds_ids = [
            ts.get_timeseries_by_data_state(data_state).id for ts in timeseries
        ]

        # Get timeseries data
        query = sqla.text(
            "SELECT time_bucket("
            " :bucket_width, timestamp AT TIME ZONE :timezone)"
            f"  AS bucket, timeseries.id, timeseries.name, {aggregation}(value) "
            "FROM timeseries_data, timeseries, timeseries_by_data_states "
            "WHERE timeseries.id = timeseries_by_data_states.timeseries_id "
            "  AND timeseries_data.timeseries_by_data_state_id = "
            "      timeseries_by_data_states.id "
            "  AND timeseries_by_data_state_id IN :timeseries_by_data_state_ids "
            "  AND timestamp >= :start_dt AND timestamp < :end_dt "
            "GROUP BY bucket, timeseries.id "
            "ORDER BY bucket;"
        )
        params = {
            "bucket_width": bucket_width,
            "timezone": timezone,
            "timeseries_by_data_state_ids": tuple(tsbds_ids),
            "start_dt": start_dt,
            "end_dt": end_dt,
        }
        data = db.session.execute(query, params)

        data_df = pd.DataFrame(
            data, columns=("Datetime", "id", "name", "value")
        ).set_index("Datetime")
        data_df.index = (
            pd.DatetimeIndex(data_df.index).tz_localize(timezone).tz_convert("UTC")
        )
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

        # Get timeseries x data states ids
        # TODO: integrate to query below
        tsbds_ids = [
            ts.get_timeseries_by_data_state(data_state).id for ts in timeseries
        ]

        # Delete timeseries data
        db.session.query(TimeseriesData).where(
            Timeseries.id == TimeseriesByDataState.timeseries_id
        ).where(
            TimeseriesData.timeseries_by_data_state_id == TimeseriesByDataState.id
        ).filter(
            TimeseriesData.timeseries_by_data_state_id.in_(tsbds_ids)
        ).filter(
            start_dt <= TimeseriesData.timestamp
        ).filter(
            TimeseriesData.timestamp < end_dt
        ).delete(
            synchronize_session=False
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
        reader = cls.csv_reader(csv_file)

        # Read headers
        try:
            header = next(reader)
        except StopIteration as exc:
            raise TimeseriesDataCSVIOError("Missing headers line") from exc
        if header[0] != "Datetime":
            raise TimeseriesDataCSVIOError('First column must be "Datetime"')
        timeseries = header[1:]
        if campaign is None:
            # Check all timeseries IDs are integers to prevent crash in _get_timeseries
            invalid_timeseries = [ts for ts in timeseries if not ts.isdecimal()]
            if invalid_timeseries:
                raise TimeseriesDataCSVIOError(
                    "Invalid timeseries IDs: {invalid_timeseries}"
                )
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

        data = []
        for irow, row in enumerate(reader):
            irow += 1
            timestamp = row[0]
            if not ISO8601_DATETIME_RE.match(timestamp):
                raise TimeseriesDataCSVIOError(f"Invalid timestamp row {irow}")
            for icol, tsbds_id in enumerate(tsbds_ids):
                icol += 1
                try:
                    value = float(row[icol])
                except IndexError as exc:
                    raise TimeseriesDataCSVIOError(
                        f"Missing column row {irow}"
                    ) from exc
                except ValueError as exc:
                    raise TimeseriesDataCSVIOError(
                        f"Invalid value row {irow} col {icol}"
                    ) from exc
                data.append(
                    {
                        "timestamp": timestamp,
                        "timeseries_by_data_state_id": tsbds_id,
                        "value": value,
                    }
                )

        # Insert data
        cls._set_timeseries_data(data)

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
        data_df = cls._get_timeseries_data(
            start_dt,
            end_dt,
            timeseries,
            data_state,
            col_label=col_label,
        )

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
        :param str bucket_width: Bucket width (ISO 8601 or PostgreSQL interval)
        :param str timezone: IANA timezone
        :param str aggreagation: Aggregation function. Must be one of
            "avg", "sum", "min" and "max".
        :param string col_label: Timeseries attribute to use for column header.
            Should be "id" or "name". Default: "id".

        Returns csv as a string.
        """
        data_df = cls._get_timeseries_buckets_data(
            start_dt,
            end_dt,
            timeseries,
            data_state,
            bucket_width,
            timezone=timezone,
            aggregation=aggregation,
            col_label=col_label,
        )

        # Specify ISO 8601 manually
        # https://github.com/pandas-dev/pandas/issues/27328
        return data_df.to_csv(date_format="%Y-%m-%dT%H:%M:%S%z")


tsdcsvio = TimeseriesDataCSVIO()
