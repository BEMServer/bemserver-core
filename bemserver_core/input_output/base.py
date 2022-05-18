import io
import csv

from bemserver_core.database import db
from bemserver_core import model


def enforce_iterator(csv_file):
    # If input is not a text stream, then it is a plain string
    # Make it an iterator
    if not isinstance(csv_file, io.TextIOBase):
        csv_file = csv_file.splitlines()
    return csv_file


class BaseIO:
    """Base class for IO classes"""

    #: Exception to raise on I/O errors
    ERROR = None

    @classmethod
    def _get_campaign_scope_by_name(cls, campaign, campaign_scope_name):
        try:
            return db.session.query(model.CampaignScope).filter_by(
                campaign_id=campaign.id, name=campaign_scope_name
            )[0]
        except IndexError as exc:
            raise cls.ERROR(f'Unknown campaign scope: "{campaign_scope_name}"') from exc

    @classmethod
    def _get_site_by_name(cls, campaign, site_name):
        try:
            return db.session.query(model.Site).filter_by(
                campaign_id=campaign.id, name=site_name
            )[0]
        except IndexError as exc:
            raise cls.ERROR(f'Unknown site: "{site_name}"') from exc

    @classmethod
    def _get_building_by_name(cls, site, building_name):
        try:
            return db.session.query(model.Building).filter_by(
                site_id=site.id, name=building_name
            )[0]
        except IndexError as exc:
            raise cls.ERROR(f'Unknown building: "{site.name}/{building_name}"') from exc

    @classmethod
    def _get_storey_by_name(cls, site, building, storey_name):
        try:
            return db.session.query(model.Storey).filter_by(
                building_id=building.id, name=storey_name
            )[0]
        except IndexError as exc:
            raise cls.ERROR(
                f'Unknown storey: "{site.name}/{building.name}/{storey_name}"'
            ) from exc

    @classmethod
    def _get_space_by_name(cls, site, building, storey, space_name):
        try:
            return db.session.query(model.Space).filter_by(
                storey_id=storey.id, name=space_name
            )[0]
        except IndexError as exc:
            raise cls.ERROR(
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
            raise cls.ERROR(f'Unknown zone: "{zone_name}"') from exc


class BaseCSVIO(BaseIO):
    """Base class for CSV IO classes"""

    @staticmethod
    def csv_reader(csv_file):
        csv_file = enforce_iterator(csv_file)
        return csv.reader(csv_file)

    @staticmethod
    def csv_dict_reader(csv_file):
        csv_file = enforce_iterator(csv_file)
        return csv.DictReader(csv_file)

    @classmethod
    def _check_headers(cls, reader, required_field_names):
        """Check missing columns in headers

        Only applies to DictReader
        """
        missing_fields = required_field_names - set(reader.fieldnames)
        if missing_fields:
            raise cls.ERROR(f"Missing columns: {list(missing_fields)}")
