"""Timeseries data I/O"""
import re

import sqlalchemy as sqla
import pandas as pd

from bemserver_core.database import db
from bemserver_core.model import (
    Timeseries,
    TimeseriesData,
    TimeseriesDataState,
    TimeseriesByDataState,
)
from bemserver_core.authorization import auth, get_current_user
from bemserver_core.exceptions import (
    TimeseriesDataIOUnknownDataStateError,
    TimeseriesDataIOUnknownTimeseriesError,
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
    def _get_timeseries_data_state(cls, data_state_id):
        data_state = TimeseriesDataState.get_by_id(data_state_id)
        if data_state is None:
            raise TimeseriesDataIOUnknownDataStateError("Unknown data state ID")
        return data_state

    @classmethod
    def _get_timeseries(cls, timeseries, campaign=None):
        # If campaign is None, expect TS IDs
        if campaign is None:
            ts_l = [Timeseries.get_by_id(col) for col in timeseries]
        # Otherwise, expect TS names
        else:
            ts_l = [Timeseries.get_by_name(campaign, col) for col in timeseries]
        if None in ts_l:
            raise TimeseriesDataIOUnknownTimeseriesError(
                f'Unknown timeseries {"name" if campaign else "ID"}'
            )
        return ts_l

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
        cls, start_dt, end_dt, timeseries, data_state_id, campaign=None
    ):
        """Export timeseries data

        :param datetime start_dt: Time interval lower bound (tz-aware)
        :param datetime end_dt: Time interval exclusive upper bound (tz-aware)
        :param list timeseries: List of timeseries IDs or names
        :param Campaign campaign: Campaign

        If campaign is None, timeseries list is expected to contain timeseries IDs.
        Otherwise, timeseries names are expected.

        Returns a dataframe.
        """
        ts_l = cls._get_timeseries(timeseries, campaign)
        data_state = cls._get_timeseries_data_state(data_state_id)

        # Check permissions
        for ts in ts_l:
            auth.authorize(get_current_user(), "read_data", ts)

        # Get timeseries x data states ids
        tsbds_ids = [ts.get_timeseries_by_data_state(data_state).id for ts in ts_l]

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
            data, columns=("Datetime", "tsid", "tsname", "value")
        ).set_index("Datetime")
        data_df.index = pd.DatetimeIndex(data_df.index)
        data_df = data_df.pivot(
            columns="tsid" if campaign is None else "tsname",
            values="value",
        )

        cls._fill_missing_columns(data_df, ts_l, "id" if campaign is None else "name")

        return data_df

    @classmethod
    def _get_timeseries_buckets_data(
        cls,
        start_dt,
        end_dt,
        timeseries,
        data_state_id,
        bucket_width,
        timezone="UTC",
        aggregation="avg",
        campaign=None,
    ):
        """Bucket timeseries data and export

        :param datetime start_dt: Time interval lower bound (tz-aware)
        :param datetime end_dt: Time interval exclusive upper bound (tz-aware)
        :param list timeseries: List of timeseries IDs or names
        :param str bucket_width: Bucket width (ISO 8601 or PostgreSQL interval)
        :param str timezone: IANA timezone
        :param str aggreagation: Aggregation function. Must be one of
            "avg", "sum", "min" and "max".
        :param Campaign campaign: Campaign

        If campaign is None, timeseries list is expected to contain timeseries IDs.
        Otherwise, timeseries names are expected.

        Returns csv as a string.
        """
        ts_l = cls._get_timeseries(timeseries, campaign)
        data_state = cls._get_timeseries_data_state(data_state_id)

        # Check permissions
        for ts in ts_l:
            auth.authorize(get_current_user(), "read_data", ts)

        if aggregation not in AGGREGATION_FUNCTIONS:
            raise TimeseriesDataIOInvalidAggregationError("Invalid aggregation method")

        # Get timeseries x data states ids
        tsbds_ids = [ts.get_timeseries_by_data_state(data_state).id for ts in ts_l]

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
            data, columns=("Datetime", "tsid", "tsname", "value")
        ).set_index("Datetime")
        data_df.index = (
            pd.DatetimeIndex(data_df.index).tz_localize(timezone).tz_convert("UTC")
        )
        data_df = data_df.pivot(
            columns="tsid" if campaign is None else "tsname",
            values="value",
        )

        cls._fill_missing_columns(data_df, ts_l, "id" if campaign is None else "name")

        return data_df


class TimeseriesDataCSVIO(TimeseriesDataIO, BaseCSVIO):
    @classmethod
    def import_csv(cls, csv_file, data_state_id, campaign=None):
        """Import CSV file

        :param srt|TextIOBase csv_file: CSV as string or text stream
        :param int data_state_id: Data state ID
        :param Campaign campaign: Campaign

        If campaign is None, the CSV header is expected to contain timeseries IDs.
        Otherwise, timeseries names are expected.
        """
        data_state = cls._get_timeseries_data_state(data_state_id)

        reader = cls.csv_reader(csv_file)

        # Read headers
        try:
            header = next(reader)
        except StopIteration as exc:
            raise TimeseriesDataCSVIOError("Missing headers line") from exc
        if header[0] != "Datetime":
            raise TimeseriesDataCSVIOError('First column must be "Datetime"')
        ts_l = cls._get_timeseries(header[1:], campaign=campaign)

        # Check permissions
        for ts in ts_l:
            auth.authorize(get_current_user(), "write_data", ts)

        # Get timeseries x data states ids
        tsbds_ids = [ts.get_timeseries_by_data_state(data_state).id for ts in ts_l]

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
    def export_csv(cls, start_dt, end_dt, timeseries, data_state_id, campaign=None):
        """Export timeseries data as CSV file

        :param datetime start_dt: Time interval lower bound (tz-aware)
        :param datetime end_dt: Time interval exclusive upper bound (tz-aware)
        :param list timeseries: List of timeseries IDs or names
        :param Campaign campaign: Campaign

        If campaign is None, timeseries list is expected to contain timeseries IDs.
        Otherwise, timeseries names are expected.

        Returns csv as a string.
        """
        data_df = cls._get_timeseries_data(
            start_dt,
            end_dt,
            timeseries,
            data_state_id,
            campaign=campaign,
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
        data_state_id,
        bucket_width,
        timezone="UTC",
        aggregation="avg",
        campaign=None,
    ):
        """Bucket timeseries data and export as CSV file

        :param datetime start_dt: Time interval lower bound (tz-aware)
        :param datetime end_dt: Time interval exclusive upper bound (tz-aware)
        :param list timeseries: List of timeseries
        :param str bucket_width: Bucket width (ISO 8601 or PostgreSQL interval)
        :param str timezone: IANA timezone
        :param str aggreagation: Aggregation function. Must be one of
            "avg", "sum", "min" and "max".
        :param Campaign campaign: Campaign

        If campaign is None, timeseries list is expected to contain timeseries IDs.
        Otherwise, timeseries names are expected.

        Returns csv as a string.
        """
        data_df = cls._get_timeseries_buckets_data(
            start_dt,
            end_dt,
            timeseries,
            data_state_id,
            bucket_width,
            timezone=timezone,
            aggregation=aggregation,
            campaign=campaign,
        )

        # Specify ISO 8601 manually
        # https://github.com/pandas-dev/pandas/issues/27328
        return data_df.to_csv(date_format="%Y-%m-%dT%H:%M:%S%z")


tsdcsvio = TimeseriesDataCSVIO()
