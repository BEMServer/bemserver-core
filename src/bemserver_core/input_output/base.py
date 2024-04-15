"""Base I/O classes"""

import csv
import io

from bemserver_core import model
from bemserver_core.database import db
from bemserver_core.exceptions import BEMServerCoreCSVIOError, BEMServerCoreIOError


class BaseIO:
    """Base class for IO classes"""

    @classmethod
    def _get_campaign_scope_by_name(cls, campaign, campaign_scope_name):
        try:
            return db.session.query(model.CampaignScope).filter_by(
                campaign_id=campaign.id, name=campaign_scope_name
            )[0]
        except IndexError as exc:
            raise BEMServerCoreIOError(
                f'Unknown campaign scope: "{campaign_scope_name}"'
            ) from exc

    @classmethod
    def _get_site_by_name(cls, campaign, site_name):
        try:
            return db.session.query(model.Site).filter_by(
                campaign_id=campaign.id, name=site_name
            )[0]
        except IndexError as exc:
            raise BEMServerCoreIOError(f'Unknown site: "{site_name}"') from exc

    @classmethod
    def _get_building_by_name(cls, site, building_name):
        try:
            return db.session.query(model.Building).filter_by(
                site_id=site.id, name=building_name
            )[0]
        except IndexError as exc:
            raise BEMServerCoreIOError(
                f'Unknown building: "{site.name}/{building_name}"'
            ) from exc

    @classmethod
    def _get_storey_by_name(cls, site, building, storey_name):
        try:
            return db.session.query(model.Storey).filter_by(
                building_id=building.id, name=storey_name
            )[0]
        except IndexError as exc:
            raise BEMServerCoreIOError(
                f'Unknown storey: "{site.name}/{building.name}/{storey_name}"'
            ) from exc

    @classmethod
    def _get_space_by_name(cls, site, building, storey, space_name):
        try:
            return db.session.query(model.Space).filter_by(
                storey_id=storey.id, name=space_name
            )[0]
        except IndexError as exc:
            raise BEMServerCoreIOError(
                "Unknown space: "
                f'"{site.name}/{building.name}/{storey.name}/{space_name}"'
            ) from exc

    @classmethod
    def _get_zone_by_name(cls, campaign, zone_name):
        try:
            return db.session.query(model.Zone).filter_by(
                campaign_id=campaign.id, name=zone_name
            )[0]
        except IndexError as exc:
            raise BEMServerCoreIOError(f'Unknown zone: "{zone_name}"') from exc


class BaseFileIOMixin:
    """Mixin for file IO classes"""

    @staticmethod
    def _enforce_iterator(in_file):
        # If input is not a text stream, then it is a plain string
        if not isinstance(in_file, io.TextIOBase):
            in_file = io.StringIO(in_file)
        return in_file


class BaseCSVIO(BaseIO):
    """Base class for CSV IO classes"""

    @classmethod
    def csv_dict_reader(cls, csv_file):
        csv_file = cls._enforce_iterator(csv_file)
        return csv.DictReader(csv_file)

    @classmethod
    def _check_headers(cls, reader, required_field_names):
        """Check missing columns in headers

        Only applies to DictReader
        """
        if reader.fieldnames is None:
            raise BEMServerCoreCSVIOError("Empty CSV file")
        missing_fields = required_field_names - set(reader.fieldnames)
        if missing_fields:
            raise BEMServerCoreCSVIOError(f"Missing columns: {list(missing_fields)}")


class BaseCSVFileIO(BaseCSVIO, BaseFileIOMixin):
    """Base class for CSV file IO classes"""


class BaseJSONIO(BaseIO):
    """Base class for JSON IO classes"""
