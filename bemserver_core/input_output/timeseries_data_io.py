"""Timeseries data I/O"""
import io
import datetime as dt
from zoneinfo import ZoneInfo
import json
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
from bemserver_core.time_utils import floor, ceil, PERIODS, make_pandas_freq
from bemserver_core.exceptions import (
    TimeseriesDataIODatetimeError,
    TimeseriesDataIOInvalidTimeseriesIDTypeError,
    TimeseriesDataIOInvalidBucketWidthError,
    TimeseriesDataIOInvalidAggregationError,
    TimeseriesDataCSVIOError,
    TimeseriesDataJSONIOError,
)

from .base import BaseCSVIO, BaseJSONIO


AGGREGATION_FUNCTIONS = ("avg", "sum", "min", "max", "count")

# Function to use to re-aggregate in pandas after SQL aggregation
PANDAS_RE_AGGREG_FUNC_MAPPING = {
    "avg": "mean",
    "min": "min",
    "max": "max",
    "sum": "sum",
    "count": "sum",
}


class TimeseriesDataIO:
    """Base class for TimeseriesData IO classes"""

    @classmethod
    def set_timeseries_data(cls, data_df, data_state, campaign=None):
        """Insert timeseries data

        :param DataFrame data_df: Input timeseries data
        :param TimeseriesDataState data_state: Timeseries data state
        :param Campaign campaign: Campaign
        """
        # Ensure columns labels are of right type
        try:
            data_df.columns = data_df.columns.astype(str if campaign else int)
        except TypeError as exc:
            raise TimeseriesDataIOInvalidTimeseriesIDTypeError(
                "Wrong timeseries ID type"
            ) from exc

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
            var_name="ts_by_data_state_id",
            ignore_index=False,
        )
        data_rows = [
            row
            for row in data_df.reset_index().to_dict(orient="records")
            if pd.notna(row["value"])
        ]
        # Ensure values array is not empty (otherwise the query crashes)
        if not data_rows:
            return

        query = (
            sqla.dialects.postgresql.insert(TimeseriesData)
            .values(data_rows)
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
        cls,
        start_dt,
        end_dt,
        timeseries,
        data_state,
        *,
        timezone="UTC",
        inclusive="left",
        col_label="id",
    ):
        """Export timeseries data

        :param datetime start_dt: Time interval lower bound (tz-aware)
        :param datetime end_dt: Time interval exclusive upper bound (tz-aware)
        :param list timeseries: List of timeseries
        :param TimeseriesDataState data_state: Timeseries data state
        :param str timezone: IANA timezone
        :param str inclusive: Whether to set each bound as closed or open.
            Must be "both", "neither", "left" or "right". Default: "left".
        :param string col_label: Timeseries attribute to use for column header.
            Should be "id" or "name". Default: "id".

        Returns a dataframe.
        """
        # Check permissions
        for ts in timeseries:
            auth.authorize(get_current_user(), "read_data", ts)

        # Get timeseries data
        stmt = (
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
        )
        if start_dt:
            if inclusive in {"both", "left"}:
                stmt = stmt.filter(start_dt <= TimeseriesData.timestamp)
            else:
                stmt = stmt.filter(start_dt < TimeseriesData.timestamp)
        if end_dt:
            if inclusive in {"both", "right"}:
                stmt = stmt.filter(TimeseriesData.timestamp <= end_dt)
            else:
                stmt = stmt.filter(TimeseriesData.timestamp < end_dt)
        data = db.session.execute(stmt).all()

        data_df = pd.DataFrame(
            data, columns=("timestamp", "id", "name", "value")
        ).set_index("timestamp")
        data_df["value"] = data_df["value"].astype(float)
        data_df.index = pd.DatetimeIndex(data_df.index, tz="UTC").tz_convert(
            ZoneInfo(timezone)
        )

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
        bucket_width_value,
        bucket_width_unit,
        aggregation="avg",
        *,
        timezone="UTC",
        col_label="id",
    ):
        """Bucket timeseries data and export

        :param datetime start_dt: Time interval lower bound (tz-aware)
        :param datetime end_dt: Time interval exclusive upper bound (tz-aware)
        :param list timeseries: List of timeseries
        :param TimeseriesDataState data_state: Timeseries data state
        :param int bucket_witdh_value: Value of the bucket width.
            Must be at least 1.
        :param str bucket_witdh_unit: Unit of the bucket width
            One of "second", "minute", "hour", "day", "week", "month", "year".
        :param str aggregation: Aggregation function.
            One of "avg", "sum", "min", "max" and "count".
        :param str timezone: IANA timezone
        :param string col_label: Timeseries attribute to use for column header.
            Should be "id" or "name". Default: "id".

        The time alignment of the buckets respects the timezone.

        Note: ``start_dt`` and ``end_dt`` may have timezones that don't match
        ``timezone`` parameter. The conversion is done internally. In practice,
        though, it might not be the most intuitive way to use this function.

        Returns a dataframe.
        """
        if bucket_width_value < 1:
            raise TimeseriesDataIOInvalidBucketWidthError(
                "bucket_width_value must be greater than or equal to 1"
            )
        if bucket_width_unit not in PERIODS:
            raise TimeseriesDataIOInvalidBucketWidthError(
                f"bucket_width_unit not in {PERIODS}"
            )
        if aggregation not in AGGREGATION_FUNCTIONS:
            raise TimeseriesDataIOInvalidAggregationError("Invalid aggregation method")

        # Check permissions
        for ts in timeseries:
            auth.authorize(get_current_user(), "read_data", ts)

        fill_value = 0 if aggregation == "count" else np.nan
        dtype = int if aggregation == "count" else float

        # Ensure start/end dates are in target timezone
        tz_info = ZoneInfo(timezone)
        start_dt = start_dt.astimezone(tz_info)
        end_dt = end_dt.astimezone(tz_info)

        # Floor/ceil start/end dates to return complete buckets
        start_dt = floor(start_dt, bucket_width_unit, bucket_width_value)
        end_dt = ceil(end_dt, bucket_width_unit, bucket_width_value)

        pd_freq = make_pandas_freq(bucket_width_unit, bucket_width_value)

        # Create expected complete index
        complete_idx = pd.date_range(
            start_dt,
            end_dt,
            freq=pd_freq,
            tz=tz_info,
            name="timestamp",
            inclusive="left",
        )

        if not timeseries:
            return pd.DataFrame({}, index=complete_idx)

        # At this stage, date_trunc can only aggregate by 1 x unit.
        # For a N x width bucket size, the remaining aggregation is
        # done in Pandas below.
        params = {
            "timezone": timezone,
            "timeseries_ids": tuple(ts.id for ts in timeseries),
            "data_state_id": data_state.id,
            "start_dt": start_dt,
            "end_dt": end_dt,
            "bucket_width_unit": bucket_width_unit,
        }
        query = (
            "SELECT date_trunc(:bucket_width_unit, timestamp, :timezone) AS bucket,"
            f"  timeseries.id, timeseries.name, {aggregation}(value) "
            "FROM ts_data, timeseries, ts_by_data_states "
            "WHERE ts_data.ts_by_data_state_id = ts_by_data_states.id "
            "  AND ts_by_data_states.data_state_id = :data_state_id "
            "  AND ts_by_data_states.timeseries_id = timeseries.id "
            "  AND timeseries_id IN :timeseries_ids "
            "  AND timestamp >= :start_dt AND timestamp < :end_dt "
            "GROUP BY bucket, timeseries.id "
            "ORDER BY bucket;"
        )
        data = db.session.execute(sqla.text(query), params)

        data_df = pd.DataFrame(
            data, columns=("timestamp", "id", "name", "value")
        ).set_index("timestamp")

        data_df.index = pd.DatetimeIndex(data_df.index, tz="UTC").tz_convert(tz_info)

        # Pivot table to get timeseries in columns
        data_df = data_df.pivot(values="value", columns=col_label).fillna(fill_value)

        # Variable size intervals are aggregated to 1 x unit due to date_trunc
        # Further aggregation is achieved here in pandas
        if bucket_width_value != 1:
            func = PANDAS_RE_AGGREG_FUNC_MAPPING[aggregation]
            data_df = data_df.resample(pd_freq, closed="left", label="left").agg(func)

        # Fill gaps: reindex with complete index
        data_df = data_df.reindex(complete_idx, fill_value=fill_value)

        # Fill missing columns
        cls._fill_missing_columns(
            data_df,
            timeseries,
            col_label,
            fill_value=fill_value,
        )

        data_df = data_df.astype(dtype)

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


def to_utc_index(series):
    """Create UTC datetime index from timezone aware datetime list"""

    def to_utc_datetime(timestamp_dt):
        try:
            return timestamp_dt.astimezone(dt.timezone.utc)
        except TypeError as exc:
            if timestamp_dt.tzinfo is None:
                raise TimeseriesDataIODatetimeError(
                    "Invalid or TZ-naive timestamp"
                ) from exc
            raise

    try:
        index = pd.to_datetime(pd.Series(series))
    except dateutil.parser._parser.ParserError as exc:
        raise TimeseriesDataIODatetimeError("Invalid timestamp") from exc

    # We can't just use tz_convert because it would silently swallow naive datetimes
    index = index.apply(to_utc_datetime)

    return pd.DatetimeIndex(index, name="timestamp")


class TimeseriesDataCSVIO(TimeseriesDataIO, BaseCSVIO):
    @classmethod
    def import_csv(cls, csv_data, data_state, campaign=None):
        """Import CSV file

        :param srt csv_data: CSV as string
        :param TimeseriesDataState data_state: Timeseries data state
        :param Campaign campaign: Campaign

        If campaign is None, the CSV header is expected to contain timeseries IDs.
        Otherwise, timeseries names are expected.
        """
        # Check header first. Some errors are hard to catch in or after pandas read_csv
        reader = csv.reader(io.StringIO(csv_data))
        try:
            header = next(reader)
        except StopIteration as exc:
            raise TimeseriesDataCSVIOError("Missing headers") from exc
        if not header:
            raise TimeseriesDataCSVIOError("Missing headers")
        if "" in header:
            raise TimeseriesDataCSVIOError("Empty timeseries name or trailing comma")
        if header[0] != "Datetime":
            raise TimeseriesDataCSVIOError("Invalid file")

        # Load CSV into DataFrame
        try:
            data_df = pd.read_csv(io.StringIO(csv_data), index_col=0)
        except pd.errors.EmptyDataError as exc:
            raise TimeseriesDataCSVIOError("Empty file") from exc

        # Index
        data_df.index = to_utc_index(data_df.index)

        # Values
        try:
            data_df = data_df.astype(float)
        except ValueError as exc:
            raise TimeseriesDataCSVIOError("Invalid values") from exc

        # Insert data
        cls.set_timeseries_data(data_df, data_state=data_state, campaign=campaign)

    @classmethod
    def export_csv(
        cls,
        start_dt,
        end_dt,
        timeseries,
        data_state,
        *,
        timezone="UTC",
        col_label="id",
    ):
        """Export timeseries data as CSV string

        See ``TimeseriesDataIO.get_timeseries_data``.
        """
        data_df = cls.get_timeseries_data(
            start_dt,
            end_dt,
            timeseries,
            data_state,
            timezone=timezone,
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
        bucket_width_value,
        bucket_width_unit,
        aggregation="avg",
        *,
        timezone="UTC",
        col_label="id",
    ):
        """Bucket timeseries data and export as CSV string

        See ``TimeseriesDataIO.get_timeseries_buckets_data``.
        """
        data_df = cls.get_timeseries_buckets_data(
            start_dt,
            end_dt,
            timeseries,
            data_state,
            bucket_width_value,
            bucket_width_unit,
            aggregation,
            timezone=timezone,
            col_label=col_label,
        )
        data_df.index.name = "Datetime"

        # Specify ISO 8601 manually
        # https://github.com/pandas-dev/pandas/issues/27328
        return data_df.to_csv(date_format="%Y-%m-%dT%H:%M:%S%z")


class TimeseriesDataJSONIO(TimeseriesDataIO, BaseJSONIO):
    @classmethod
    def import_json(cls, json_data, data_state, campaign=None):
        """Import JSON file

        :param srt json_data: JSON as string or text stream
        :param TimeseriesDataState data_state: Timeseries data state
        :param Campaign campaign: Campaign

        If campaign is None, the JSON header is expected to contain timeseries IDs.
        Otherwise, timeseries names are expected.
        """
        # Load JSON into DataFrame
        try:
            data_df = pd.read_json(json_data, orient="columns", dtype=float)
        except ValueError as exc:
            raise TimeseriesDataJSONIOError("Wrong JSON file") from exc

        # Check empty column name
        if pd.NaT in data_df.columns:
            raise TimeseriesDataJSONIOError("Wrong timeseries ID")

        # Index
        data_df.index = to_utc_index(data_df.index)

        # Insert data
        cls.set_timeseries_data(data_df, data_state=data_state, campaign=campaign)

    @staticmethod
    def _df_to_json(data_df, dropna=False):
        """Serialize dataframe to json

        pandas to_json has a few shortcomings
        - can't drop NaN values
        - will convert datetimes to UTC
        """
        data_df.index = pd.Series(data_df.index).apply(lambda x: x.isoformat())

        ret = {}
        for col in data_df.columns:
            val = data_df[col]
            if dropna:
                val = val.dropna()
            else:
                val = val.replace([np.nan], [None])
            if not val.empty:
                ret[str(col)] = val.to_dict()

        return json.dumps(ret)

    @classmethod
    def export_json(
        cls,
        start_dt,
        end_dt,
        timeseries,
        data_state,
        *,
        timezone="UTC",
        col_label="id",
    ):
        """Export timeseries data as JSON string

        See ``TimeseriesDataIO.get_timeseries_data``.
        """
        data_df = cls.get_timeseries_data(
            start_dt,
            end_dt,
            timeseries,
            data_state,
            timezone=timezone,
            col_label=col_label,
        )
        return cls._df_to_json(data_df, dropna=True)

    @classmethod
    def export_json_bucket(
        cls,
        start_dt,
        end_dt,
        timeseries,
        data_state,
        bucket_width_value,
        bucket_width_unit,
        aggregation="avg",
        *,
        timezone="UTC",
        col_label="id",
    ):
        """Bucket timeseries data and export as JSON string

        See ``TimeseriesDataIO.get_timeseries_buckets_data``.
        """
        data_df = cls.get_timeseries_buckets_data(
            start_dt,
            end_dt,
            timeseries,
            data_state,
            bucket_width_value,
            bucket_width_unit,
            aggregation,
            timezone=timezone,
            col_label=col_label,
        )
        return cls._df_to_json(data_df)


tsdio = TimeseriesDataIO()
tsdcsvio = TimeseriesDataCSVIO()
tsdjsonio = TimeseriesDataJSONIO()
