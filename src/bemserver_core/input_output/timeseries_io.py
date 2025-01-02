"""Timeseries I/O"""

import sqlalchemy as sqla

from bemserver_core import model
from bemserver_core.database import db
from bemserver_core.exceptions import (
    BEMServerCoreUndefinedUnitError,
    PropertyTypeInvalidError,
    TimeseriesCSVIOError,
)

from .base import BaseCSVFileIO


def _get_ts_property_by_name(model_cls, name):
    try:
        return db.session.query(model_cls).filter_by(name=name)[0]
    except IndexError as exc:
        raise TimeseriesCSVIOError(f'Unknown property: "{name}"') from exc


def _get_ts_properties(model_cls, reader, known_fields):
    property_names = set(reader.fieldnames) - known_fields
    return {pn: _get_ts_property_by_name(model_cls, pn) for pn in property_names}


class TimeseriesCSVIO(BaseCSVFileIO):
    TS_FIELDS = {
        "Campaign scope",
        "Name",
        "Description",
        "Unit",
        "Site",
        "Building",
        "Storey",
        "Space",
        "Zone",
    }
    ERROR = TimeseriesCSVIOError

    @classmethod
    def import_csv(
        cls,
        campaign,
        timeseries_csv,
    ):
        """Import timeseries from CSV file"""
        reader = cls.csv_dict_reader(timeseries_csv)
        cls._check_headers(reader, cls.TS_FIELDS)
        properties = _get_ts_properties(model.TimeseriesProperty, reader, cls.TS_FIELDS)

        for row in reader:
            cs_name = row.pop("Campaign scope")
            name = row.pop("Name")
            kwargs = {
                "campaign_id": campaign.id,
                "name": name,
                "description": row.pop("Description"),
                "unit_symbol": row.pop("Unit"),
                "campaign_scope": cls._get_campaign_scope_by_name(campaign, cs_name),
            }
            timeseries = model.Timeseries.get(
                campaign_id=campaign.id, name=name
            ).first()
            if timeseries is None:
                timeseries = model.Timeseries.new(**kwargs)
            else:
                timeseries.update(**kwargs)
                for ts_relation_table in (
                    model.TimeseriesBySite,
                    model.TimeseriesByBuilding,
                    model.TimeseriesByStorey,
                    model.TimeseriesBySpace,
                    model.TimeseriesByZone,
                ):
                    relation = ts_relation_table.get(
                        timeseries_id=timeseries.id
                    ).first()
                    if relation is not None:
                        relation.delete()
            try:
                db.session.flush()
            except (sqla.exc.DataError, BEMServerCoreUndefinedUnitError) as exc:
                raise TimeseriesCSVIOError(
                    f'Timeseries "{timeseries.name}" can\'t be created.'
                ) from exc

            site_name = row.pop("Site")
            building_name = row.pop("Building")
            storey_name = row.pop("Storey")
            space_name = row.pop("Space")
            zone_name = row.pop("Zone")
            if site_name:
                site = cls._get_site_by_name(campaign, site_name)
                if not building_name:
                    model.TimeseriesBySite.new(
                        timeseries_id=timeseries.id,
                        site_id=site.id,
                    )
            if building_name:
                if not site_name:
                    raise TimeseriesCSVIOError(
                        f'Missing site for building "{building_name}"'
                    )
                building = cls._get_building_by_name(site, building_name)
                if not storey_name:
                    model.TimeseriesByBuilding.new(
                        timeseries_id=timeseries.id,
                        building_id=building.id,
                    )
            if storey_name:
                if not building_name:
                    raise TimeseriesCSVIOError(
                        f'Missing building for storey "{storey_name}"'
                    )
                storey = cls._get_storey_by_name(site, building, storey_name)
                if not space_name:
                    model.TimeseriesByStorey.new(
                        timeseries_id=timeseries.id,
                        storey_id=storey.id,
                    )
            if space_name:
                if not storey_name:
                    raise TimeseriesCSVIOError(
                        f'Missing storey for space "{space_name}"'
                    )
                space = cls._get_space_by_name(site, building, storey, space_name)
                model.TimeseriesBySpace.new(
                    timeseries_id=timeseries.id,
                    space_id=space.id,
                )
            if zone_name:
                zone = cls._get_zone_by_name(campaign, zone_name)
                model.TimeseriesByZone.new(
                    timeseries_id=timeseries.id,
                    zone_id=zone.id,
                )

            for key, value in ((k, v) for k, v in row.items() if k is not None):
                if value:
                    prop = properties[key]
                    kwargs = {
                        "property_id": prop.id,
                        "timeseries": timeseries,
                    }
                    tpd = model.TimeseriesPropertyData.get(**kwargs).first()
                    if tpd is None:
                        tpd = model.TimeseriesPropertyData.new(**kwargs)
                    tpd.value = value
                    try:
                        db.session.flush()
                    except (sqla.exc.DataError, PropertyTypeInvalidError) as exc:
                        raise TimeseriesCSVIOError(
                            f'Timeseries "{timeseries.name}" property "{key}"'
                            " can't be created."
                        ) from exc


timeseries_csv_io = TimeseriesCSVIO()
