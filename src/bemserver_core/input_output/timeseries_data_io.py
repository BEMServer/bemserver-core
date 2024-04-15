"""Timeseries data I/O"""

import csv
import datetime as dt
import io
import json
from zoneinfo import ZoneInfo

import sqlalchemy as sqla

import numpy as np
import pandas as pd

from bemserver_core.authorization import auth, get_current_user
from bemserver_core.common import ureg
from bemserver_core.database import db
from bemserver_core.exceptions import (
    TimeseriesDataCSVIOError,
    TimeseriesDataIODatetimeError,
    TimeseriesDataIOInvalidAggregationError,
    TimeseriesDataIOInvalidBucketWidthError,
    TimeseriesDataIOInvalidTimeseriesIDTypeError,
    TimeseriesDataJSONIOError,
)
from bemserver_core.model import (
    Timeseries,
    TimeseriesByDataState,
    TimeseriesData,
)
from bemserver_core.time_utils import PERIODS, ceil, floor, make_pandas_freq

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
    def get_last(
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
        """Get timeseries last values

        :param datetime start_dt: Time interval lower bound (tz-aware)
        :param datetime end_dt: Time interval exclusive upper bound (tz-aware)
        :param list timeseries: List of timeseries
        :param TimeseriesDataState data_state: Timeseries data state
        :param str timezone: IANA timezone to use for first/last timestamp
        :param str inclusive: Whether to set each bound as closed or open.
            Must be "both", "neither", "left" or "right". Default: "left".
        :param string col_label: Timeseries attribute to use for column header.
            Should be "id" or "name". Default: "id".

        Returns a dataframe.
        """
        # Check permissions
        for ts in timeseries:
            auth.authorize(get_current_user(), "read_data", ts)

        params = {
            "timeseries_ids": [ts.id for ts in timeseries],
            "data_state_id": data_state.id,
            "start_dt": start_dt,
            "end_dt": end_dt,
        }

        time_interval_filter = ""
        if start_dt:
            compar = ">=" if inclusive in {"both", "left"} else ">"
            time_interval_filter += f"AND timestamp {compar} :start_dt "
            params["start_dt"] = start_dt
        if end_dt:
            compar = "<=" if inclusive in {"both", "right"} else "<"
            time_interval_filter += f"AND timestamp {compar} :end_dt "
            params["end_dt"] = end_dt

        query = (
            # https://stackoverflow.com/a/7630564
            f"SELECT DISTINCT ON (timeseries.{col_label}) timeseries.{col_label}, "
            "timestamp, value "
            "FROM ts_data, timeseries, ts_by_data_states "
            "WHERE ts_data.ts_by_data_state_id = ts_by_data_states.id "
            "  AND ts_by_data_states.data_state_id = :data_state_id "
            "  AND ts_by_data_states.timeseries_id = timeseries.id "
            "  AND timeseries_id = ANY(:timeseries_ids) "
            f"{time_interval_filter}"
            f"ORDER BY timeseries.{col_label}, timestamp DESC"
        )
        data = db.session.execute(sqla.text(query), params)

        data_df = (
            pd.DataFrame(
                data,
                columns=(
                    col_label,
                    "timestamp",
                    "value",
                ),
            )
            .set_index(col_label)
            .reindex(getattr(ts, col_label) for ts in timeseries)
            .astype({"timestamp": "datetime64[ns, UTC]", "value": float})
        )

        data_df["timestamp"] = data_df["timestamp"].dt.tz_convert(timezone)

        return data_df

    @classmethod
    def get_timeseries_stats(
        cls,
        timeseries,
        data_state,
        *,
        timezone="UTC",
        col_label="id",
    ):
        """Get timeseries stats

        :param list timeseries: List of timeseries
        :param TimeseriesDataState data_state: Timeseries data state
        :param str timezone: IANA timezone to use for first/last timestamp
        :param string col_label: Timeseries attribute to use for column header.
            Should be "id" or "name". Default: "id".

        Returns a dataframe.
        """
        # Check permissions
        for ts in timeseries:
            auth.authorize(get_current_user(), "read_data", ts)

        params = {
            "timeseries_ids": [ts.id for ts in timeseries],
            "data_state_id": data_state.id,
        }
        query = (
            f"SELECT timeseries.{col_label}, "
            "min(timestamp), max(timestamp), "
            "count(value), min(value), max(value), avg(value), stddev_samp(value)"
            "FROM ts_data, timeseries, ts_by_data_states "
            "WHERE ts_data.ts_by_data_state_id = ts_by_data_states.id "
            "  AND ts_by_data_states.data_state_id = :data_state_id "
            "  AND ts_by_data_states.timeseries_id = timeseries.id "
            "  AND timeseries_id = ANY(:timeseries_ids) "
            "GROUP BY timeseries.id "
        )
        data = db.session.execute(sqla.text(query), params)

        data_df = (
            pd.DataFrame(
                data,
                columns=(
                    col_label,
                    "first_timestamp",
                    "last_timestamp",
                    "count",
                    "min",
                    "max",
                    "avg",
                    "stddev",
                ),
            )
            .set_index(col_label)
            .reindex(getattr(ts, col_label) for ts in timeseries)
        )
        data_df["count"] = data_df["count"].fillna(0)
        data_df = data_df.astype(
            {
                "first_timestamp": "datetime64[ns, UTC]",
                "last_timestamp": "datetime64[ns, UTC]",
                "count": int,
                "min": float,
                "max": float,
                "avg": float,
                "stddev": float,
            }
        )

        for col in ("first_timestamp", "last_timestamp"):
            data_df[col] = data_df[col].dt.tz_convert(timezone)

        return data_df

    @staticmethod
    def _convert_from(data_df, ts_l, col_label, convert_from):
        """Convert data to given units

        :param DataFrame data_df: DataFrame to convert
        :param list ts_l: List of Timeseries objects
        :param str col_label: DataFrame column labels: IDs or names
        :param dict convert_from: Mapping of timeseries ID/name -> unit

        Converts column for each item in convert_from dict.
        """
        ureg.convert_df(
            data_df,
            convert_from,
            {
                label: ts.unit_symbol
                for ts in ts_l
                if (label := getattr(ts, col_label)) in convert_from
            },
        )

    @staticmethod
    def _convert_to(data_df, ts_l, col_label, convert_to, *, src_unit=None):
        """Convert data to given units

        :param DataFrame data_df: DataFrame to convert
        :param list ts_l: List of Timeseries objects
        :param str col_label: DataFrame column labels: IDs or names
        :param dict convert_to: Mapping of timeseries ID/name -> unit
        :param string src_unit: Unit to use as source unit for all timeseries

        Converts column for each item in convert_to dict.

        If src_unit is provided, it is used as source unit for all timeseries
        in place of their respective units. This is useful for aggregated data
        where the result of the aggregation may not have the same unit as the
        original data (e.g. count).
        """
        ureg.convert_df(
            data_df,
            {getattr(ts, col_label): src_unit or ts.unit_symbol for ts in ts_l},
            convert_to,
        )

    @classmethod
    def set_timeseries_data(
        cls, data_df, data_state, campaign=None, *, convert_from=None
    ):
        """Insert timeseries data

        :param DataFrame data_df: Input timeseries data
        :param TimeseriesDataState data_state: Timeseries data state
        :param Campaign campaign: Campaign
        :param dict convert_from: Mapping of timeseries ID/name -> unit to convert
            timeseries data from
        """
        # Copy so that modifications here don't affect input dataframe
        # Only a shallow copy is needed
        data_df = data_df.copy(deep=False)

        # Ensure columns labels are of right type
        try:
            data_df.columns = data_df.columns.astype(str if campaign else int)
        except ValueError as exc:
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

        if convert_from:
            cls._convert_from(
                data_df, timeseries, "name" if campaign else "id", convert_from
            )

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
        data_rows = [
            row
            for row in data_df.reset_index().to_dict(orient="records")
            if pd.notna(row["value"])
        ]
        # Ensure values array is not empty (otherwise the query crashes)
        if not data_rows:
            return

        db.session.execute(
            sqla.dialects.postgresql.insert(TimeseriesData).on_conflict_do_nothing(),
            data_rows,
        )

    @staticmethod
    def _fill_missing_and_reorder_columns(data_df, ts_l, col_label, fill_value=np.nan):
        """Add missing columns and reorder colums

        - Add missing columns (columns with no values in DB)
        - Ensure columns are in the order of the timeseries list parameter.
            The SQL query may return them in any order.
        """
        timeseries_labels = [ts.id if col_label == "id" else ts.name for ts in ts_l]
        # Fill missing
        for col in set(timeseries_labels) - set(data_df.columns):
            data_df[col] = fill_value
        # Ensure order
        return data_df[timeseries_labels]

    @classmethod
    def get_timeseries_data(
        cls,
        start_dt,
        end_dt,
        timeseries,
        data_state,
        *,
        convert_to=None,
        timezone="UTC",
        inclusive="left",
        col_label="id",
    ):
        """Export timeseries data

        :param datetime start_dt: Time interval lower bound (tz-aware)
        :param datetime end_dt: Time interval exclusive upper bound (tz-aware)
        :param list timeseries: List of timeseries
        :param TimeseriesDataState data_state: Timeseries data state
        :param dict convert_to: Mapping of timeseries ID/name -> unit to convert
            timeseries data to
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

        data_df = cls._fill_missing_and_reorder_columns(data_df, timeseries, col_label)

        if convert_to:
            cls._convert_to(data_df, timeseries, col_label, convert_to)

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
        convert_to=None,
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
        :param dict convert_to: Mapping of timeseries ID/name -> unit to convert
            timeseries data to
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
            "timeseries_ids": [ts.id for ts in timeseries],
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
            "  AND timeseries_id = ANY(:timeseries_ids) "
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
        data_df = cls._fill_missing_and_reorder_columns(
            data_df,
            timeseries,
            col_label,
            fill_value=fill_value,
        )

        data_df = data_df.astype(dtype)

        if convert_to:
            # If aggregation is count, data is not in original TS unit but dimensionless
            src_unit = "count" if aggregation == "count" else None
            cls._convert_to(
                data_df, timeseries, col_label, convert_to, src_unit=src_unit
            )

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


def to_utc_index(index):
    """Create UTC datetime index from timezone aware datetime list"""

    try:
        # Cast to series so that output is series, with an apply method
        index = pd.Series(index).apply(lambda x: pd.to_datetime(x, format="ISO8601"))
    except (
        ValueError,
        pd.errors.OutOfBoundsDatetime,
        pd._libs.tslibs.parsing.DateParseError,
    ) as exc:
        raise TimeseriesDataIODatetimeError("Invalid timestamp") from exc

    # We can't just use tz_convert because it would silently swallow naive datetimes
    try:
        index = index.apply(lambda x: x.astimezone(dt.timezone.utc))
    except TypeError as exc:
        raise TimeseriesDataIODatetimeError("Invalid or TZ-naive timestamp") from exc

    return pd.DatetimeIndex(index, name="timestamp")


class TimeseriesDataCSVIO(TimeseriesDataIO, BaseCSVIO):
    @classmethod
    def import_csv(cls, csv_data, data_state, campaign=None, convert_from=None):
        """Import CSV file

        :param srt csv_data: CSV as string
        :param TimeseriesDataState data_state: Timeseries data state
        :param Campaign campaign: Campaign
        :param dict convert_from: Mapping of timeseries ID/name -> unit to convert
            timeseries data from

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
        except pd.errors.ParserError as exc:
            raise TimeseriesDataCSVIOError("Bad CSV file") from exc

        # Index
        data_df.index = to_utc_index(data_df.index)

        # Values
        try:
            data_df = data_df.astype(float)
        except ValueError as exc:
            raise TimeseriesDataCSVIOError("Invalid values") from exc

        # Insert data
        cls.set_timeseries_data(
            data_df, data_state=data_state, campaign=campaign, convert_from=convert_from
        )

    @classmethod
    def export_csv(
        cls,
        start_dt,
        end_dt,
        timeseries,
        data_state,
        *,
        convert_to=None,
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
            convert_to=convert_to,
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
        convert_to=None,
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
            convert_to=convert_to,
            timezone=timezone,
            col_label=col_label,
        )
        data_df.index.name = "Datetime"

        # Specify ISO 8601 manually
        # https://github.com/pandas-dev/pandas/issues/27328
        return data_df.to_csv(date_format="%Y-%m-%dT%H:%M:%S%z")


class TimeseriesDataJSONIO(TimeseriesDataIO, BaseJSONIO):
    @classmethod
    def import_json(cls, json_data, data_state, campaign=None, convert_from=None):
        """Import JSON file

        :param srt json_data: JSON as string or text stream
        :param TimeseriesDataState data_state: Timeseries data state
        :param Campaign campaign: Campaign
        :param dict convert_from: Mapping of timeseries ID/name -> unit to convert
            timeseries data from

        If campaign is None, the JSON header is expected to contain timeseries IDs.
        Otherwise, timeseries names are expected.
        """
        # Load JSON into DataFrame
        try:
            data_df = pd.read_json(
                io.StringIO(json_data), orient="columns", dtype=False
            )
        except ValueError as exc:
            raise TimeseriesDataJSONIOError("Wrong JSON file") from exc

        # Check empty column name
        if pd.NaT in data_df.columns:
            raise TimeseriesDataJSONIOError("Wrong timeseries ID")

        # Index
        data_df.index = to_utc_index(data_df.index)

        # Values
        try:
            data_df = data_df.astype(float)
        except ValueError as exc:
            raise TimeseriesDataJSONIOError("Invalid values") from exc

        # Insert data
        cls.set_timeseries_data(
            data_df, data_state=data_state, campaign=campaign, convert_from=convert_from
        )

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
        convert_to=None,
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
            convert_to=convert_to,
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
        convert_to=None,
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
            convert_to=convert_to,
            timezone=timezone,
            col_label=col_label,
        )
        return cls._df_to_json(data_df)


tsdio = TimeseriesDataIO()
tsdcsvio = TimeseriesDataCSVIO()
tsdjsonio = TimeseriesDataJSONIO()
