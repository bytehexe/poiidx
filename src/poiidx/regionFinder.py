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

    @staticmethod
    def _region_from_feature(feature: dict[str, Any]) -> Region | None:
        """Return the region, or None if it has no PBF download (nothing to fetch)."""
        properties = feature["properties"]
        url = properties.get("urls", {}).get("pbf")
        if url is None:
            return None
        return Region(id=properties["id"], name=properties["name"], url=url)

    def all_regions(self) -> list[Region]:
        regions = (
            self._region_from_feature(feature)
            for feature in self.geofabrik_data["features"]
        )
        return [region for region in regions if region is not None]

    def find_candidates(self, geo_data: BaseGeometry) -> list[Region]:
        """All regions intersecting the shape, smallest first (not just the used ones)."""
        candidates: list[tuple[float, Region]] = []
        for region in self.all_regions():
            entry = self._region_cache[region.id]
            if geo_data.intersects(entry["shape"]):
                candidates.append((entry["area"], region))
        candidates.sort(key=lambda candidate: candidate[0])
        return [region for _, region in candidates]

    def lookup(self, name_or_id: str) -> Region:
        key = name_or_id.strip()
        if not key:
            raise ValueError("Region name or id must not be empty.")
        regions = self.all_regions()
        for region in regions:
            if region.id == key:
                return region
        matches = [
            region for region in regions if region.name.casefold() == key.casefold()
        ]
        if not matches:
            raise ValueError(f"No Geofabrik region with id or name {name_or_id!r}.")
        if len(matches) > 1:
            ids = ", ".join(sorted(region.id for region in matches))
            raise ValueError(
                f"Region name {name_or_id!r} is ambiguous; use one of these ids: {ids}."
            )
        return matches[0]

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
            candidate = self._region_from_feature(region)
            if candidate is None:
                continue  # No PBF to download

            region_id = candidate.id
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
                best = candidate
                best_size = size
                return_geo_data = remaining_geo_data

        return best, return_geo_data
