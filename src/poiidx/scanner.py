import osmium
import sys
import logging
from shapely.geometry import shape
import shapely
from .poi import Poi
from .administrativeBoundary import AdministrativeBoundary
from .baseModel import database_proxy as db
from .osm import calculate_rank, MAX_RANK
from .projection import LocalProjection

logger = logging.getLogger(__name__)

def administrative_scan(pbf_path, region_key):
    processor = osmium.FileProcessor(pbf_path)
    processor.with_filter(osmium.filter.TagFilter(("boundary", "administrative")))
    processor.with_filter(osmium.filter.KeyFilter("name"))
    processor.with_filter(osmium.filter.KeyFilter("admin_level"))
    processor.with_areas()
    processor.with_filter(osmium.filter.GeoInterfaceFilter())

    with db.atomic():
        for obj in processor:
            if not hasattr(obj, "__geo_interface__"):
                continue  # No geometry available

            geom = shape(obj.__geo_interface__["geometry"])  # type: ignore
            admin_level = obj.tags.get("admin_level")
            name = obj.tags.get("name")
            if admin_level is None or name is None:
                continue

            # Store administrative boundary in the database
            # Assuming an AdministrativeBoundary model exists
            AdministrativeBoundary.create(
                osm_id=obj.id,
                name=name,
                region=region_key,
                admin_level=admin_level,
                coordinates=geom,
                wikidata_id=obj.tags.get("wikidata")
            )

def poi_scan(filter_config, pbf_path, region_key):
    all_filters_keys = set()
    for filter_item in filter_config:
        for filter_expression in filter_item["filters"]:
            all_filters_keys.update(filter_expression.keys())

    processor = osmium.FileProcessor(pbf_path)
    processor.with_filter(osmium.filter.KeyFilter("name"))
    processor.with_filter(osmium.filter.KeyFilter(*all_filters_keys))
    processor.with_locations()
    processor.with_areas()
    processor.with_filter(osmium.filter.GeoInterfaceFilter())

    with db.atomic():
        for obj in processor:
            filter_item_id = None
            filter_expression_id = None

            # Process each POI as needed
            if obj.id is None:
                continue
            poi_name = obj.tags.get("name")
            if poi_name is None:
                continue

            found = False
            for filter_item_id, filter_item in enumerate(filter_config):  # noqa: B007
                for filter_expression_id, filter_expression in enumerate(  # noqa: B007
                    filter_item["filters"],
                ):
                    poi_tags = obj.tags
                    matches = [
                        (poi_tags.get(k)) if v is True else (poi_tags.get(k) == v)
                        for k, v in filter_expression.items()
                    ]
                    if all(matches):
                        found = True
                        break
                if found:
                    break

            if not found:
                continue

            type_str = obj.type_str()
            poi_id = obj.id

            if type_str == "n":
                geom = shapely.Point(obj.lon, obj.lat)
                rank = calculate_rank(place=obj.tags.get("place"))
                radius = None

            else:
                if not hasattr(obj, "__geo_interface__"):
                    continue  # No geometry available

                geom = shape(obj.__geo_interface__["geometry"])  # type: ignore
                proj = LocalProjection(geom)
                local_geom = proj.to_local(geom)
                centroid = proj.to_wgs(local_geom.centroid)
                lat = centroid.y
                lon = centroid.x
                radius = shapely.minimum_bounding_radius(local_geom)
                rank = calculate_rank(radius=radius, place=obj.tags.get("place"))

            if rank is None:
                rank = MAX_RANK

            assert filter_item_id is not None, "Filter item ID should not be None"
            assert filter_expression_id is not None, (
                "Filter expression ID should not be None"
            )

            Poi.create(
                osm_id=poi_id,
                name=poi_name,
                region=region_key,
                filter_item=filter_item_id,
                filter_expression=filter_expression_id,
                rank=rank,
                coordinates=geom,
                symbol=filter_item["symbol"],
            )