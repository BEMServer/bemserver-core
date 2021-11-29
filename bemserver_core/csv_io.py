"""Timeseries CSV I/O"""
import io
import csv
import datetime as dt

import sqlalchemy as sqla
import pandas as pd

from .database import db
from .exceptions import TimeseriesCSVIOError
from .model import Timeseries, TimeseriesData


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
            raise TimeseriesCSVIOError('Missing headers line') from exc
        if header[0] != "Datetime":
            raise TimeseriesCSVIOError('First column must be "Datetime"')
        try:
            ts_ids = [
                db.session.get(Timeseries, col).id
                for col in header[1:]
            ]
        except AttributeError as exc:
            raise TimeseriesCSVIOError('Unknown timeseries ID') from exc

        datas = []
        for row in reader:
            try:
                datas.extend([
                    {
                        "timestamp": row[0],
                        "timeseries_id": ts_id,
                        "value": row[col+1]
                    }
                    for col, ts_id in enumerate(ts_ids)
                ])
            except IndexError as exc:
                raise TimeseriesCSVIOError('Missing column') from exc

        # TODO: manage all ISO formats
        timestamps = [dt.datetime.fromisoformat(r["timestamp"]) for r in datas]
        start_dt, end_dt = min(timestamps), max(timestamps)

        TimeseriesData.check_can_import(start_dt, end_dt, ts_ids)

        query = (
            sqla.dialects.postgresql
            .insert(TimeseriesData).values(datas)
            .on_conflict_do_nothing()
        )

        try:
            with db.session() as session:
                session.execute(query)
                session.commit()
        # TODO: filter server and client errors (constraint violation)
        except sqla.exc.DBAPIError as exc:
            raise TimeseriesCSVIOError('Error writing to DB') from exc

    @staticmethod
    def export_csv(start_dt, end_dt, timeseries):
        """Export timeseries data as CSV file

        :param datetime start_dt: Time interval lower bound (tz-aware)
        :param datetime end_dt: Time interval exclusive upper bound (tz-aware)
        :param list timeseries: List of timeseries IDs

        Returns csv as a string.
        """
        TimeseriesData.check_can_export(start_dt, end_dt, timeseries)

        data = db.session.execute(
            sqla.select(
                TimeseriesData.timestamp,
                TimeseriesData.timeseries_id,
                TimeseriesData.value,
            ).filter(
                TimeseriesData.timeseries_id.in_(timeseries)
            ).filter(
                start_dt <= TimeseriesData.timestamp
            ).filter(
                TimeseriesData.timestamp < end_dt
            )
        ).all()

        data_df = (
            pd.DataFrame(data, columns=('Datetime', 'tsid', 'value'))
            .set_index("Datetime")
        )
        data_df.index = pd.DatetimeIndex(data_df.index)
        data_df = data_df.pivot(columns='tsid', values='value')

        # Add missing columns, in query order
        for idx, ts_id in enumerate(timeseries):
            if ts_id not in data_df:
                data_df.insert(idx, ts_id, None)

        # Specify ISO 8601 manually
        # https://github.com/pandas-dev/pandas/issues/27328
        return data_df.to_csv(date_format='%Y-%m-%dT%H:%M:%S%z')

    @staticmethod
    def export_csv_bucket(
        start_dt,
        end_dt,
        timeseries,
        bucket_width,
        timezone="UTC",
        aggregation="avg",
    ):
        """Bucket timeseries data and export as CSV file

        :param datetime start_dt: Time interval lower bound (tz-aware)
        :param datetime end_dt: Time interval exclusive upper bound (tz-aware)
        :param list timeseries: List of timeseries IDs
        :param str bucket_width: Bucket width (ISO 8601 or PostgreSQL interval)
        :param str timezone: IANA timezone
        :param str aggreagation: Aggregation function. Must be one of
            "avg", "sum", "min" and "max".

        Returns csv as a string.
        """
        TimeseriesData.check_can_export(start_dt, end_dt, timeseries)

        if aggregation not in AGGREGATION_FUNCTIONS:
            raise ValueError(f'Invalid aggregation method "{aggregation}"')

        query = sqla.text(
            "SELECT time_bucket("
            " :bucket_width, timestamp AT TIME ZONE :timezone)"
            f"  AS bucket, timeseries_id, {aggregation}(value) "
            "FROM timeseries_data "
            "WHERE timeseries_id IN :timeseries "
            "  AND timestamp >= :start_dt AND timestamp < :end_dt "
            "GROUP BY bucket, timeseries_id "
            "ORDER BY bucket;"
        )
        params = {
            "bucket_width": bucket_width,
            "timezone": timezone,
            "timeseries": tuple(timeseries),
            "start_dt": start_dt,
            "end_dt": end_dt,
        }
        with db.session() as session:
            data = session.execute(query, params)

        data_df = (
            pd.DataFrame(data, columns=('Datetime', 'tsid', 'value'))
            .set_index("Datetime")
        )
        data_df.index = (
            pd.DatetimeIndex(data_df.index)
            .tz_localize(timezone)
            .tz_convert('UTC')
        )
        data_df = data_df.pivot(columns='tsid', values='value')

        # Add missing columns, in query order
        for idx, ts_id in enumerate(timeseries):
            if ts_id not in data_df:
                data_df.insert(idx, ts_id, None)

        # Specify ISO 8601 manually
        # https://github.com/pandas-dev/pandas/issues/27328
        return data_df.to_csv(date_format='%Y-%m-%dT%H:%M:%S%z')


tscsvio = TimeseriesCSVIO()
