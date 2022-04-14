"""Sites I/O"""
import io
import csv

import sqlalchemy as sqla

from bemserver_core.database import db
from bemserver_core import model
from bemserver_core.exceptions import SitesCSVIOError


def _get_property_by_name(model_cls, name):
    try:
        return (
            db.session.query(model_cls)
            .join(model.StructuralElementProperty)
            .filter_by(name=name)[0]
        )
    except IndexError as exc:
        raise SitesCSVIOError(f'Unknown property: "{name}"') from exc


def _get_site_by_name(campaign, site_name):
    try:
        return db.session.query(model.Site).filter_by(
            campaign_id=campaign.id, name=site_name
        )[0]
    except IndexError as exc:
        raise SitesCSVIOError(f'Unknown site: "{site_name}"') from exc


def _get_building_by_name(site, building_name):
    try:
        return db.session.query(model.Building).filter_by(
            site_id=site.id, name=building_name
        )[0]
    except IndexError as exc:
        raise SitesCSVIOError(
            f'Unknown building: "{site.name}/{building_name}"'
        ) from exc


def _get_storey_by_name(site, building, storey_name):
    try:
        return db.session.query(model.Storey).filter_by(
            building_id=building.id, name=storey_name
        )[0]
    except IndexError as exc:
        raise SitesCSVIOError(
            f'Unknown storey: "{site.name}/{building.name}/{storey_name}"'
        ) from exc


class SiteDataCSVIO:
    @staticmethod
    def _pop_row_value_for_col(row, col):
        try:
            return row.pop(col)
        except KeyError as exc:
            raise SitesCSVIOError(f'Missing column: "{exc.args[0]}"') from exc

    @staticmethod
    def csv_reader(csv_file):
        # If input is not a text stream, then it is a plain string
        # Make it an iterator
        if not isinstance(csv_file, io.TextIOBase):
            csv_file = csv_file.splitlines()
        return csv.DictReader(csv_file)

    @classmethod
    def _import_csv_sites(cls, campaign, sites_csv):
        reader = cls.csv_reader(sites_csv)

        for row in reader:
            site = model.Site.new(
                campaign_id=campaign.id,
                name=cls._pop_row_value_for_col(row, "Name"),
                description=cls._pop_row_value_for_col(row, "Description"),
            )
            try:
                db.session.flush()
            except sqla.exc.DataError as exc:
                raise SitesCSVIOError(f'Site "{site.name}" can\'t be created.') from exc
            except sqla.exc.IntegrityError as exc:
                raise SitesCSVIOError(f'Site "{site.name}" already exists.') from exc
            for key, value in ((k, v) for k, v in row.items() if k is not None):
                if value:
                    prop = _get_property_by_name(model.SiteProperty, key)
                    model.SitePropertyData.new(
                        site_property_id=prop.id,
                        site=site,
                        value=value,
                    )
                    try:
                        db.session.flush()
                    except sqla.exc.DataError as exc:
                        raise SitesCSVIOError(
                            f'Site "{site.name}" property "{key}" can\'t be created.'
                        ) from exc

    @classmethod
    def _import_csv_buildings(cls, campaign, buildings_csv):
        reader = cls.csv_reader(buildings_csv)

        for row in reader:
            site_name = cls._pop_row_value_for_col(row, "Site")
            site = _get_site_by_name(campaign, site_name)
            building = model.Building.new(
                site_id=site.id,
                name=cls._pop_row_value_for_col(row, "Name"),
                description=cls._pop_row_value_for_col(row, "Description"),
            )
            try:
                db.session.flush()
            except sqla.exc.DataError as exc:
                raise SitesCSVIOError(
                    f'Building "{building.name}" can\'t be created.'
                ) from exc
            except sqla.exc.IntegrityError as exc:
                raise SitesCSVIOError(
                    f'Building "{site_name}/{building.name}" already exists.'
                ) from exc

            for key, value in ((k, v) for k, v in row.items() if k is not None):
                if value:
                    prop = _get_property_by_name(model.BuildingProperty, key)
                    model.BuildingPropertyData.new(
                        building_property_id=prop.id,
                        building=building,
                        value=value,
                    )
                    try:
                        db.session.flush()
                    except sqla.exc.DataError as exc:
                        raise SitesCSVIOError(
                            f'Building "{building.name}" property "{key}" '
                            "can't be created."
                        ) from exc

    @classmethod
    def _import_csv_storeys(cls, campaign, storeys_csv):
        reader = cls.csv_reader(storeys_csv)

        for row in reader:
            site_name = cls._pop_row_value_for_col(row, "Site")
            site = _get_site_by_name(campaign, site_name)
            building_name = cls._pop_row_value_for_col(row, "Building")
            building = _get_building_by_name(site, building_name)
            storey = model.Storey.new(
                building_id=building.id,
                name=cls._pop_row_value_for_col(row, "Name"),
                description=cls._pop_row_value_for_col(row, "Description"),
            )
            try:
                db.session.flush()
            except sqla.exc.DataError as exc:
                raise SitesCSVIOError(
                    f'Storey "{storey.name}" can\'t be created.'
                ) from exc
            except sqla.exc.IntegrityError as exc:
                raise SitesCSVIOError(
                    f'Storey "{site_name}/{building_name}/{storey.name}" '
                    "already exists."
                ) from exc

            for key, value in ((k, v) for k, v in row.items() if k is not None):
                if value:
                    prop = _get_property_by_name(model.StoreyProperty, key)
                    model.StoreyPropertyData.new(
                        storey_property_id=prop.id,
                        storey=storey,
                        value=value,
                    )
                    try:
                        db.session.flush()
                    except sqla.exc.DataError as exc:
                        raise SitesCSVIOError(
                            f'Storey "{storey.name}" property "{key}" '
                            "can't be created."
                        ) from exc

    @classmethod
    def _import_csv_spaces(cls, campaign, storeys_csv):
        reader = cls.csv_reader(storeys_csv)

        for row in reader:
            site_name = cls._pop_row_value_for_col(row, "Site")
            site = _get_site_by_name(campaign, site_name)
            building_name = cls._pop_row_value_for_col(row, "Building")
            building = _get_building_by_name(site, building_name)
            storey_name = cls._pop_row_value_for_col(row, "Storey")
            storey = _get_storey_by_name(site, building, storey_name)
            space = model.Space.new(
                storey_id=storey.id,
                name=cls._pop_row_value_for_col(row, "Name"),
                description=cls._pop_row_value_for_col(row, "Description"),
            )
            try:
                db.session.flush()
            except sqla.exc.DataError as exc:
                raise SitesCSVIOError(
                    f'Space "{space.name}" can\'t be created.'
                ) from exc
            except sqla.exc.IntegrityError as exc:
                raise SitesCSVIOError(
                    f'Space "{site_name}/{building_name}/{storey_name}/{space.name}" '
                    "already exists."
                ) from exc

            for key, value in ((k, v) for k, v in row.items() if k is not None):
                if value:
                    prop = _get_property_by_name(model.SpaceProperty, key)
                    model.SpacePropertyData.new(
                        space_property_id=prop.id,
                        space=space,
                        value=value,
                    )
                    try:
                        db.session.flush()
                    except sqla.exc.DataError as exc:
                        raise SitesCSVIOError(
                            f'Space "{space.name}" property "{key}" can\'t be created.'
                        ) from exc

    @classmethod
    def _import_csv_zones(cls, campaign, zones_csv):
        reader = cls.csv_reader(zones_csv)

        for row in reader:
            zone = model.Zone.new(
                campaign_id=campaign.id,
                name=cls._pop_row_value_for_col(row, "Name"),
                description=cls._pop_row_value_for_col(row, "Description"),
            )
            try:
                db.session.flush()
            except sqla.exc.DataError as exc:
                raise SitesCSVIOError(f'Zone "{zone.name}" can\'t be created.') from exc
            except sqla.exc.IntegrityError as exc:
                raise SitesCSVIOError(f'Zone "{zone.name}" already exists.') from exc

            for key, value in ((k, v) for k, v in row.items() if k is not None):
                if value:
                    prop = _get_property_by_name(model.ZoneProperty, key)
                    model.ZonePropertyData.new(
                        zone_property_id=prop.id,
                        zone=zone,
                        value=value,
                    )
                    try:
                        db.session.flush()
                    except sqla.exc.DataError as exc:
                        raise SitesCSVIOError(
                            f'Zone "{zone.name}" property "{key}" can\'t be created.'
                        ) from exc

    @classmethod
    def import_csv(
        cls,
        campaign,
        sites_csv=None,
        buildings_csv=None,
        storeys_csv=None,
        spaces_csv=None,
        zones_csv=None,
    ):
        """Import site description tree from CSV files"""

        if sites_csv is not None:
            cls._import_csv_sites(campaign, sites_csv)
        if buildings_csv is not None:
            cls._import_csv_buildings(campaign, buildings_csv)
        if storeys_csv is not None:
            cls._import_csv_storeys(campaign, storeys_csv)
        if spaces_csv is not None:
            cls._import_csv_spaces(campaign, spaces_csv)
        if zones_csv is not None:
            cls._import_csv_zones(campaign, zones_csv)
        db.session.commit()


sdio = SiteDataCSVIO()
