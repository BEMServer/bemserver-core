"""Timeseries CSV I/O"""
import io
import csv

import sqlalchemy as sqla
import pandas as pd

from bemserver_core.database import db
from bemserver_core.model import Timeseries, TimeseriesData
from bemserver_core.authorization import auth, get_current_user
from bemserver_core.exceptions import TimeseriesCSVIOError


AGGREGATION_FUNCTIONS = ("avg", "sum", "min", "max")


class TimeseriesCSVIO:
    @staticmethod
    def import_csv(csv_file):
        """Import CSV file

        :param srt|TextIOBase csv_file: CSV as string or text stream
        """
        # If input is not a text stream, then it is a plain string
        # Make it an iterator
        if not isinstance(csv_file, io.TextIOBase):
            csv_file = csv_file.splitlines()

        reader = csv.reader(csv_file)

        try:
            header = next(reader)
        except StopIteration as exc:
            raise TimeseriesCSVIOError("Missing headers line") from exc
        if header[0] != "Datetime":
            raise TimeseriesCSVIOError('First column must be "Datetime"')
        timeseries_l = [db.session.get(Timeseries, col) for col in header[1:]]
        if None in timeseries_l:
            raise TimeseriesCSVIOError("Unknown timeseries ID")

        for timeseries in timeseries_l:
            auth.authorize(get_current_user(), "write_data", timeseries)

        datas = []
        for row in reader:
            try:
                datas.extend(
                    [
                        {
                            "timestamp": row[0],
                            "timeseries_id": timeseries.id,
                            "value": row[col + 1],
                        }
                        for col, timeseries in enumerate(timeseries_l)
                    ]
                )
            except IndexError as exc:
                raise TimeseriesCSVIOError("Missing column") from exc

        query = (
            sqla.dialects.postgresql.insert(TimeseriesData)
            .values(datas)
            .on_conflict_do_nothing()
        )

        try:
            db.session.execute(query)
            db.session.commit()
        # TODO: filter server and client errors (constraint violation)
        except sqla.exc.DBAPIError as exc:
            raise TimeseriesCSVIOError("Error writing to DB") from exc

    @staticmethod
    def export_csv(start_dt, end_dt, timeseries_ids):
        """Export timeseries data as CSV file

        :param datetime start_dt: Time interval lower bound (tz-aware)
        :param datetime end_dt: Time interval exclusive upper bound (tz-aware)
        :param list timeseries_ids: List of timeseries IDs

        Returns csv as a string.
        """
        timeseries_l = [db.session.get(Timeseries, ts_id) for ts_id in timeseries_ids]
        if None in timeseries_l:
            raise TimeseriesCSVIOError("Unknown timeseries ID")

        for timeseries in timeseries_l:
            auth.authorize(get_current_user(), "read_data", timeseries)

        data = db.session.execute(
            sqla.select(
                TimeseriesData.timestamp,
                TimeseriesData.timeseries_id,
                TimeseriesData.value,
            )
            .filter(TimeseriesData.timeseries_id.in_(timeseries_ids))
            .filter(start_dt <= TimeseriesData.timestamp)
            .filter(TimeseriesData.timestamp < end_dt)
        ).all()

        data_df = pd.DataFrame(data, columns=("Datetime", "tsid", "value")).set_index(
            "Datetime"
        )
        data_df.index = pd.DatetimeIndex(data_df.index)
        data_df = data_df.pivot(columns="tsid", values="value")

        # Add missing columns, in query order
        for idx, ts_id in enumerate(timeseries_ids):
            if ts_id not in data_df:
                data_df.insert(idx, ts_id, None)

        # Specify ISO 8601 manually
        # https://github.com/pandas-dev/pandas/issues/27328
        return data_df.to_csv(date_format="%Y-%m-%dT%H:%M:%S%z")

    @staticmethod
    def export_csv_bucket(
        start_dt,
        end_dt,
        timeseries_ids,
        bucket_width,
        timezone="UTC",
        aggregation="avg",
    ):
        """Bucket timeseries data and export as CSV file

        :param datetime start_dt: Time interval lower bound (tz-aware)
        :param datetime end_dt: Time interval exclusive upper bound (tz-aware)
        :param list timeseries_ids: List of timeseries IDs
        :param str bucket_width: Bucket width (ISO 8601 or PostgreSQL interval)
        :param str timezone: IANA timezone
        :param str aggreagation: Aggregation function. Must be one of
            "avg", "sum", "min" and "max".

        Returns csv as a string.
        """
        timeseries_l = [db.session.get(Timeseries, ts_id) for ts_id in timeseries_ids]
        if None in timeseries_l:
            raise TimeseriesCSVIOError("Unknown timeseries ID")

        for timeseries in timeseries_l:
            auth.authorize(get_current_user(), "read_data", timeseries)

        if aggregation not in AGGREGATION_FUNCTIONS:
            raise ValueError(f'Invalid aggregation method "{aggregation}"')

        query = sqla.text(
            "SELECT time_bucket("
            " :bucket_width, timestamp AT TIME ZONE :timezone)"
            f"  AS bucket, timeseries_id, {aggregation}(value) "
            "FROM timeseries_data "
            "WHERE timeseries_id IN :timeseries_ids "
            "  AND timestamp >= :start_dt AND timestamp < :end_dt "
            "GROUP BY bucket, timeseries_id "
            "ORDER BY bucket;"
        )
        params = {
            "bucket_width": bucket_width,
            "timezone": timezone,
            "timeseries_ids": tuple(timeseries_ids),
            "start_dt": start_dt,
            "end_dt": end_dt,
        }
        data = db.session.execute(query, params)

        data_df = pd.DataFrame(data, columns=("Datetime", "tsid", "value")).set_index(
            "Datetime"
        )
        data_df.index = (
            pd.DatetimeIndex(data_df.index).tz_localize(timezone).tz_convert("UTC")
        )
        data_df = data_df.pivot(columns="tsid", values="value")

        # Add missing columns, in query order
        for idx, ts_id in enumerate(timeseries_ids):
            if ts_id not in data_df:
                data_df.insert(idx, ts_id, None)

        # Specify ISO 8601 manually
        # https://github.com/pandas-dev/pandas/issues/27328
        return data_df.to_csv(date_format="%Y-%m-%dT%H:%M:%S%z")


tscsvio = TimeseriesCSVIO()
