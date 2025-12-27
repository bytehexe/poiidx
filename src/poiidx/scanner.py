import logging
from typing import Any

import osmium
import shapely
from shapely.geometry import shape

from .administrativeBoundary import AdministrativeBoundary
from .baseModel import database as db
from .osm import MAX_RANK, calculate_rank
from .poi import Poi
from .projection import LocalProjection

logger = logging.getLogger(__name__)


def encode_osm_id(obj_id: int, type_str: str) -> str:
    """Get the original OSM ID from an osmium object with type prefix.

    When using .with_areas(), osmium converts way/relation IDs to area IDs:
    - Ways: area_id = way_id * 2
    - Relations: area_id = relation_id * 2 + 1

    Returns a string with type prefix (e.g., "r62422", "n123", "w456").
    """

    if type_str == "n":
        # Nodes keep their original ID
        return f"n{obj_id}"
    elif type_str == "a":
        # Area: decode to get original way or relation ID
        if obj_id % 2 == 0:
            # Even = way
            return f"w{obj_id // 2}"
        else:
            # Odd = relation
            return f"r{(obj_id - 1) // 2}"
    elif type_str == "w":
        return f"w{obj_id}"
    elif type_str == "r":
        return f"r{obj_id}"
    else:
        return f"{type_str}{obj_id}"


def administrative_scan(pbf_path: str, region_key: str) -> None:
    logger.debug("Starting administrative boundary scan")

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

            # Extract localized names
            localized_names = extract_localized_names(obj)

            # Store administrative boundary in the database
            # Assuming an AdministrativeBoundary model exists
            AdministrativeBoundary.create(
                osm_id=encode_osm_id(obj.id, obj.type_str()),
                name=name,
                region=region_key,
                admin_level=admin_level,
                coordinates=geom,
                wikidata_id=obj.tags.get("wikidata"),
                localized_names=localized_names,
            )


def process_admin_centre_relations(pbf_path: str) -> None:
    logger.debug("Processing admin_centre relations")

    processor = osmium.FileProcessor(pbf_path)
    processor.with_filter(osmium.filter.TagFilter(("boundary", "administrative")))
    processor.with_filter(osmium.filter.KeyFilter("name"))
    processor.with_filter(osmium.filter.KeyFilter("admin_level"))
    processor.with_filter(osmium.filter.EntityFilter(osmium.osm.RELATION))

    with db.atomic():
        for obj in processor:
            for member in obj.members:  # type: ignore
                if member.role == "admin_centre":
                    admin_level = obj.tags.get("admin_level")
                    member_ref_id = encode_osm_id(member.ref, member.type)
                    obj_id = encode_osm_id(obj.id, obj.type_str())

                    # Update the corresponding POI entry
                    for poi_id in [member_ref_id, obj_id]:
                        poi = Poi.get_or_none(Poi.osm_id == poi_id)
                        if poi:
                            try:
                                admin_level_int = int(admin_level)  # type: ignore
                            except ValueError:
                                admin_level_int = None

                            if (
                                admin_level_int is not None
                                and (
                                    poi.admin_level is None
                                    or admin_level_int < poi.admin_level
                                )
                                and (
                                    poi.capital_level is None
                                    or admin_level_int > poi.capital_level
                                )
                            ):
                                poi.admin_level = admin_level_int
                                poi.save()


def poi_scan(
    filter_config: list[dict[str, Any]], pbf_path: str, region_key: str
) -> None:
    logger.debug("Starting POI scan")

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
            poi_id = encode_osm_id(obj.id, type_str)

            if type_str == "n":
                geom = shapely.Point(obj.lon, obj.lat)  # type: ignore[union-attr]
                rank = calculate_rank(place=obj.tags.get("place"))
                radius = None

            else:
                if not hasattr(obj, "__geo_interface__"):
                    continue  # No geometry available

                geom = shape(obj.__geo_interface__["geometry"])  # type: ignore
                proj = LocalProjection(geom)
                local_geom = proj.to_local(geom)
                radius = shapely.minimum_bounding_radius(local_geom)
                rank = calculate_rank(radius=radius, place=obj.tags.get("place"))

            if rank is None:
                rank = MAX_RANK

            assert filter_item_id is not None, "Filter item ID should not be None"
            assert filter_expression_id is not None, (
                "Filter expression ID should not be None"
            )

            # Prepare localization data if available
            localized_names = extract_localized_names(obj)

            admin_level = obj.tags.get("admin_level")
            admin_level_int = None
            if admin_level is not None:
                try:
                    admin_level_int = int(admin_level)
                except ValueError:
                    pass

            capital_level_int = None
            capital_level = obj.tags.get("capital")
            if capital_level is not None:
                try:
                    capital_level_int = int(capital_level)
                except ValueError:
                    pass

            Poi.create(
                osm_id=poi_id,
                name=poi_name,
                region=region_key,
                filter_item=filter_item_id,
                filter_expression=filter_expression_id,
                rank=rank,
                coordinates=geom,
                symbol=filter_item["symbol"],
                localized_names=localized_names,
                admin_level=admin_level_int,
                capital_level=capital_level_int,
            )


def extract_localized_names(obj: Any) -> dict[str, str]:
    localizations = [x for x in list(obj.tags) if x.k.startswith("name:")]
    localized_names = {}
    for loc_tag in localizations:
        loc_lang = loc_tag.k.split("name:")[1]

        if len(loc_lang) != 2 or not loc_lang.isalpha() or not loc_lang.islower():
            continue  # Skip invalid language codes:

        loc_name = loc_tag.v
        if loc_name:
            localized_names[loc_lang] = loc_name

    return localized_names
