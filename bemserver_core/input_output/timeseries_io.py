"""Timeseries I/O"""
import io
import csv

import sqlalchemy as sqla

from bemserver_core.database import db
from bemserver_core import model
from bemserver_core.exceptions import TimeseriesCSVIOError


def _get_property_by_name(model_cls, name):
    try:
        return db.session.query(model_cls).filter_by(name=name)[0]
    except IndexError as exc:
        raise TimeseriesCSVIOError(f'Unknown property: "{name}"') from exc


def _get_campaign_scope_by_name(campaign, campaign_scope_name):
    try:
        return db.session.query(model.CampaignScope).filter_by(
            campaign_id=campaign.id, name=campaign_scope_name
        )[0]
    except IndexError as exc:
        raise TimeseriesCSVIOError(
            f'Unknown campaign scope: "{campaign_scope_name}"'
        ) from exc


def _get_site_by_name(campaign, site_name):
    try:
        return db.session.query(model.Site).filter_by(
            campaign_id=campaign.id, name=site_name
        )[0]
    except IndexError as exc:
        raise TimeseriesCSVIOError(f'Unknown site: "{site_name}"') from exc


def _get_building_by_name(site, building_name):
    try:
        return db.session.query(model.Building).filter_by(
            site_id=site.id, name=building_name
        )[0]
    except IndexError as exc:
        raise TimeseriesCSVIOError(
            f'Unknown building: "{site.name}/{building_name}"'
        ) from exc


def _get_storey_by_name(site, building, storey_name):
    try:
        return db.session.query(model.Storey).filter_by(
            building_id=building.id, name=storey_name
        )[0]
    except IndexError as exc:
        raise TimeseriesCSVIOError(
            f'Unknown storey: "{site.name}/{building.name}/{storey_name}"'
        ) from exc


def _get_space_by_name(site, building, storey, space_name):
    try:
        return db.session.query(model.Space).filter_by(
            storey_id=storey.id, name=space_name
        )[0]
    except IndexError as exc:
        raise TimeseriesCSVIOError(
            f'Unknown space: "{site.name}/{building.name}/{storey.name}/{space_name}"'
        ) from exc


def _get_zone_by_name(campaign, zone_name):
    try:
        return db.session.query(model.Zone).filter_by(
            campaign_id=campaign.id, name=zone_name
        )[0]
    except IndexError as exc:
        raise TimeseriesCSVIOError(f'Unknown zone: "{zone_name}"') from exc


class TimeseriesCSVIO:
    @staticmethod
    def _pop_row_value_for_col(row, col):
        try:
            return row.pop(col)
        except KeyError as exc:
            raise TimeseriesCSVIOError(f'Missing column: "{exc.args[0]}"') from exc

    @staticmethod
    def csv_reader(csv_file):
        # If input is not a text stream, then it is a plain string
        # Make it an iterator
        if not isinstance(csv_file, io.TextIOBase):
            csv_file = csv_file.splitlines()
        return csv.DictReader(csv_file)

    @classmethod
    def import_csv(
        cls,
        campaign,
        timeseries_csv,
    ):
        """Import timeseries from CSV file"""
        reader = cls.csv_reader(timeseries_csv)

        for row in reader:

            cs_name = cls._pop_row_value_for_col(row, "Campaign scope")
            timeseries = model.Timeseries.new(
                campaign_id=campaign.id,
                name=cls._pop_row_value_for_col(row, "Name"),
                description=cls._pop_row_value_for_col(row, "Description"),
                unit_symbol=cls._pop_row_value_for_col(row, "Unit"),
                campaign_scope=_get_campaign_scope_by_name(campaign, cs_name),
            )
            try:
                db.session.flush()
            except sqla.exc.DataError as exc:
                raise TimeseriesCSVIOError(
                    f'Timeseries "{timeseries.name}" can\'t be created.'
                ) from exc
            except sqla.exc.IntegrityError as exc:
                db.session.rollback()
                raise TimeseriesCSVIOError(
                    f'Timeseries "{campaign.name}/{timeseries.name}" already exists.'
                ) from exc

            site_name = cls._pop_row_value_for_col(row, "Site")
            if site_name:
                site = _get_site_by_name(campaign, site_name)
                model.TimeseriesBySite.new(
                    timeseries_id=timeseries.id,
                    site_id=site.id,
                )
            building_name = cls._pop_row_value_for_col(row, "Building")
            if building_name:
                if not site_name:
                    raise TimeseriesCSVIOError(
                        f'Missing site for building "{building_name}"'
                    )
                building = _get_building_by_name(site, building_name)
                model.TimeseriesByBuilding.new(
                    timeseries_id=timeseries.id,
                    building_id=building.id,
                )
            storey_name = cls._pop_row_value_for_col(row, "Storey")
            if storey_name:
                if not building_name:
                    raise TimeseriesCSVIOError(
                        f'Missing building for storey "{storey_name}"'
                    )
                storey = _get_storey_by_name(site, building, storey_name)
                model.TimeseriesByStorey.new(
                    timeseries_id=timeseries.id,
                    storey_id=storey.id,
                )
            space_name = cls._pop_row_value_for_col(row, "Space")
            if space_name:
                if not storey_name:
                    raise TimeseriesCSVIOError(
                        f'Missing storey for space "{space_name}"'
                    )
                space = _get_space_by_name(site, building, storey, space_name)
                model.TimeseriesBySpace.new(
                    timeseries_id=timeseries.id,
                    space_id=space.id,
                )
            zone_name = cls._pop_row_value_for_col(row, "Zone")
            if zone_name:
                zone = _get_zone_by_name(campaign, zone_name)
                model.TimeseriesByZone.new(
                    timeseries_id=timeseries.id,
                    zone_id=zone.id,
                )

            for key, value in ((k, v) for k, v in row.items() if k is not None):
                if value:
                    prop = _get_property_by_name(model.TimeseriesProperty, key)
                    model.TimeseriesPropertyData.new(
                        property_id=prop.id,
                        timeseries=timeseries,
                        value=value,
                    )
                    try:
                        db.session.flush()
                    except sqla.exc.DataError as exc:
                        raise TimeseriesCSVIOError(
                            f'Timeseries "{timeseries.name}" property "{key}"'
                            " can't be created."
                        ) from exc
        db.session.commit()


timeseries_csv_io = TimeseriesCSVIO()
