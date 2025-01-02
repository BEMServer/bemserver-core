"""Sites I/O"""

import sqlalchemy as sqla

from bemserver_core import model
from bemserver_core.database import db
from bemserver_core.exceptions import PropertyTypeInvalidError, SitesCSVIOError

from .base import BaseCSVFileIO


def _get_se_property_by_name(model_cls, name):
    try:
        return (
            db.session.query(model_cls)
            .join(model.StructuralElementProperty)
            .filter_by(name=name)[0]
        )
    except IndexError as exc:
        raise SitesCSVIOError(f'Unknown property: "{name}"') from exc


def _get_se_properties(model_cls, reader, known_fields):
    property_names = set(reader.fieldnames) - known_fields
    return {pn: _get_se_property_by_name(model_cls, pn) for pn in property_names}


class SitesCSVIO(BaseCSVFileIO):
    SITE_FIELDS = {"Name", "Description", "IFC_ID"}
    BUILDING_FIELDS = {"Site", "Name", "Description", "IFC_ID"}
    STOREY_FIELDS = {"Site", "Building", "Name", "Description", "IFC_ID"}
    SPACE_FIELDS = {"Site", "Building", "Storey", "Name", "Description", "IFC_ID"}
    ZONE_FIELDS = {"Name", "Description", "IFC_ID"}

    @classmethod
    def _import_csv_sites(cls, campaign, sites_csv):
        reader = cls.csv_dict_reader(sites_csv)
        cls._check_headers(reader, cls.SITE_FIELDS)
        properties = _get_se_properties(model.SiteProperty, reader, cls.SITE_FIELDS)

        for row in reader:
            name = row.pop("Name")
            kwargs = {
                "campaign_id": campaign.id,
                "name": name,
                "description": row.pop("Description"),
                "ifc_id": row.pop("IFC_ID"),
            }
            site = model.Site.get(campaign_id=campaign.id, name=name).first()
            if site is None:
                site = model.Site.new(**kwargs)
            else:
                site.update(**kwargs)
            try:
                db.session.flush()
            except sqla.exc.DataError as exc:
                raise SitesCSVIOError(f'Site "{site.name}" can\'t be created.') from exc

            for key, value in ((k, v) for k, v in row.items() if k is not None):
                if value:
                    prop = properties[key]
                    kwargs = {
                        "site_property_id": prop.id,
                        "site": site,
                    }
                    spd = model.SitePropertyData.get(**kwargs).first()
                    if spd is None:
                        spd = model.SitePropertyData.new(**kwargs)
                    spd.value = value
                    try:
                        db.session.flush()
                    except (sqla.exc.DataError, PropertyTypeInvalidError) as exc:
                        raise SitesCSVIOError(
                            f'Site "{site.name}" property "{key}" can\'t be created.'
                        ) from exc

    @classmethod
    def _import_csv_buildings(cls, campaign, buildings_csv):
        reader = cls.csv_dict_reader(buildings_csv)
        cls._check_headers(reader, cls.BUILDING_FIELDS)
        properties = _get_se_properties(
            model.BuildingProperty, reader, cls.BUILDING_FIELDS
        )

        for row in reader:
            site_name = row.pop("Site")
            site = cls._get_site_by_name(campaign, site_name)
            name = row.pop("Name")
            kwargs = {
                "site_id": site.id,
                "name": name,
                "description": row.pop("Description"),
                "ifc_id": row.pop("IFC_ID"),
            }
            building = model.Building.get(site_id=site.id, name=name).first()
            if building is None:
                building = model.Building.new(**kwargs)
            else:
                building.update(**kwargs)
            try:
                db.session.flush()
            except sqla.exc.DataError as exc:
                raise SitesCSVIOError(
                    f'Building "{building.name}" can\'t be created.'
                ) from exc

            for key, value in ((k, v) for k, v in row.items() if k is not None):
                if value:
                    prop = properties[key]
                    kwargs = {
                        "building_property_id": prop.id,
                        "building": building,
                    }
                    bpd = model.BuildingPropertyData.get(**kwargs).first()
                    if bpd is None:
                        bpd = model.BuildingPropertyData.new(**kwargs)
                    bpd.value = value
                    try:
                        db.session.flush()
                    except (sqla.exc.DataError, PropertyTypeInvalidError) as exc:
                        raise SitesCSVIOError(
                            f'Building "{building.name}" property "{key}" '
                            "can't be created."
                        ) from exc

    @classmethod
    def _import_csv_storeys(cls, campaign, storeys_csv):
        reader = cls.csv_dict_reader(storeys_csv)
        cls._check_headers(reader, cls.STOREY_FIELDS)
        properties = _get_se_properties(model.StoreyProperty, reader, cls.STOREY_FIELDS)

        for row in reader:
            site_name = row.pop("Site")
            site = cls._get_site_by_name(campaign, site_name)
            building_name = row.pop("Building")
            building = cls._get_building_by_name(site, building_name)
            name = row.pop("Name")
            kwargs = {
                "building_id": building.id,
                "name": name,
                "description": row.pop("Description"),
                "ifc_id": row.pop("IFC_ID"),
            }
            storey = model.Storey.get(building_id=building.id, name=name).first()
            if storey is None:
                storey = model.Storey.new(**kwargs)
            else:
                storey.update(**kwargs)
            try:
                db.session.flush()
            except sqla.exc.DataError as exc:
                raise SitesCSVIOError(
                    f'Storey "{storey.name}" can\'t be created.'
                ) from exc

            for key, value in ((k, v) for k, v in row.items() if k is not None):
                if value:
                    prop = properties[key]
                    kwargs = {
                        "storey_property_id": prop.id,
                        "storey": storey,
                    }
                    spd = model.StoreyPropertyData.get(**kwargs).first()
                    if spd is None:
                        spd = model.StoreyPropertyData.new(**kwargs)
                    spd.value = value
                    try:
                        db.session.flush()
                    except (sqla.exc.DataError, PropertyTypeInvalidError) as exc:
                        raise SitesCSVIOError(
                            f'Storey "{storey.name}" property "{key}" '
                            "can't be created."
                        ) from exc

    @classmethod
    def _import_csv_spaces(cls, campaign, storeys_csv):
        reader = cls.csv_dict_reader(storeys_csv)
        cls._check_headers(reader, cls.SPACE_FIELDS)
        properties = _get_se_properties(model.SpaceProperty, reader, cls.SPACE_FIELDS)

        for row in reader:
            site_name = row.pop("Site")
            site = cls._get_site_by_name(campaign, site_name)
            building_name = row.pop("Building")
            building = cls._get_building_by_name(site, building_name)
            storey_name = row.pop("Storey")
            storey = cls._get_storey_by_name(site, building, storey_name)
            name = row.pop("Name")
            kwargs = {
                "storey_id": storey.id,
                "name": name,
                "description": row.pop("Description"),
                "ifc_id": row.pop("IFC_ID"),
            }
            space = model.Space.get(storey_id=storey.id, name=name).first()
            if space is None:
                space = model.Space.new(**kwargs)
            else:
                space.update(**kwargs)
            try:
                db.session.flush()
            except sqla.exc.DataError as exc:
                raise SitesCSVIOError(
                    f'Space "{space.name}" can\'t be created.'
                ) from exc

            for key, value in ((k, v) for k, v in row.items() if k is not None):
                if value:
                    prop = properties[key]
                    kwargs = {
                        "space_property_id": prop.id,
                        "space": space,
                    }
                    spd = model.SpacePropertyData.get(**kwargs).first()
                    if spd is None:
                        spd = model.SpacePropertyData.new(**kwargs)
                    spd.value = value
                    try:
                        db.session.flush()
                    except (sqla.exc.DataError, PropertyTypeInvalidError) as exc:
                        raise SitesCSVIOError(
                            f'Space "{space.name}" property "{key}" can\'t be created.'
                        ) from exc

    @classmethod
    def _import_csv_zones(cls, campaign, zones_csv):
        reader = cls.csv_dict_reader(zones_csv)
        cls._check_headers(reader, cls.ZONE_FIELDS)
        properties = _get_se_properties(model.ZoneProperty, reader, cls.ZONE_FIELDS)

        for row in reader:
            name = row.pop("Name")
            kwargs = {
                "campaign_id": campaign.id,
                "name": name,
                "description": row.pop("Description"),
                "ifc_id": row.pop("IFC_ID"),
            }
            zone = model.Zone.get(campaign_id=campaign.id, name=name).first()
            if zone is None:
                zone = model.Zone.new(**kwargs)
            else:
                zone.update(**kwargs)
            try:
                db.session.flush()
            except sqla.exc.DataError as exc:
                raise SitesCSVIOError(f'Zone "{zone.name}" can\'t be created.') from exc

            for key, value in ((k, v) for k, v in row.items() if k is not None):
                if value:
                    prop = properties[key]
                    kwargs = {
                        "zone_property_id": prop.id,
                        "zone": zone,
                    }
                    zpd = model.ZonePropertyData.get(**kwargs).first()
                    if zpd is None:
                        zpd = model.ZonePropertyData.new(**kwargs)
                    zpd.value = value
                    try:
                        db.session.flush()
                    except (sqla.exc.DataError, PropertyTypeInvalidError) as exc:
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


sites_csv_io = SitesCSVIO()
