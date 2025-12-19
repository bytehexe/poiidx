import json
import logging
from copy import deepcopy
from typing import Any, NamedTuple

import shapely
from shapely.geometry.base import BaseGeometry

class Region(NamedTuple):
    id: str
    name: str
    url: str

logger = logging.getLogger(__name__)


class RegionFinder:
    def __init__(self, geofabrik_data: dict[str, Any]) -> None:
        self.geofabrik_data = geofabrik_data

        # Pre-compute region geometries and areas once for performance
        self._region_cache = {}
        for region in geofabrik_data["features"]:
            region_id = region["properties"]["id"]
            # Convert geometry directly without JSON round-trip
            shape = shapely.from_geojson(json.dumps(region["geometry"]))
            area = shapely.area(shape)
            self._region_cache[region_id] = {
                "shape": shape,
                "area": area,
                "region": region,
            }

    def find_regions(self, geo_data: BaseGeometry) -> list[Region]:
        geo_data = deepcopy(geo_data)

        regions: list[Region] = []
        logger.info("Finding best matching Geofabrik regions...", extra={"icon": "🗺️"})
        while geo_data.is_empty is False:
            best_region, remaining_geo_data = self._findBestRegion(
                geo_data,
                regions,
            )
            if best_region is None:
                break
            geo_data = remaining_geo_data

            regions.append(best_region)
        logger.info("Selected Geofabrik regions for POI extraction:")
        for region in regions:
            logger.info(f" - {region.name}")
        return regions

    def _findBestRegion(
        self, geo_data: Any, used_regions: list[Region]
    ) -> tuple[Region | None, Any]:
        best = None
        remaining_geo_data = geo_data
        return_geo_data = geo_data
        best_size = float("inf")

        # Create set of used region IDs for O(1) lookup instead of O(n) search
        used_region_ids = {r.id for r in used_regions}

        for region in self.geofabrik_data["features"]:
            #if "iso3166-1:alpha2" not in region["properties"]:
            #    continue  # Skip regions without country code

            region_id = region["properties"]["id"]
            if region_id in used_region_ids:
                continue  # Skip already used regions

            # Use cached geometry and area
            cache_entry = self._region_cache[region_id]
            shape = cache_entry["shape"]

            # Fast intersection test before expensive difference operation
            if not geo_data.intersects(shape):
                continue  # No intersection

            # Only compute difference if there's actually an intersection
            remaining_geo_data = shapely.difference(geo_data, shape)
            if remaining_geo_data.equals(geo_data):
                continue  # No actual overlap (edge case)

            # Use pre-computed area
            size = cache_entry["area"]

            if best is None or size < best_size:
                best = Region(
                    id=region["properties"]["id"],
                    name=region["properties"]["name"],
                    url=region["properties"]["urls"]["pbf"],
                )
                best_size = size
                return_geo_data = remaining_geo_data

        return best, return_geo_data
