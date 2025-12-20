import json
import logging
import pathlib
import tempfile
from contextlib import nullcontext
from typing import Any

import platformdirs
import shapely
import yaml
from peewee import SQL

from .administrativeBoundary import AdministrativeBoundary
from .baseModel import database
from .country import Country
from .countryQuery import country_query
from .ext import knn
from .geofabrik import download_region_data
from .pbf import Pbf
from .poi import Poi
from .projection import LocalProjection
from .regionFinder import RegionFinder
from .scanner import administrative_scan, poi_scan
from .system import System

logger = logging.getLogger(__name__)


class PoiIdx:
    TABLES = [Poi, System, AdministrativeBoundary, Country]

    @classmethod
    def connect(cls, pbf_cache: bool = True, **kwargs: Any) -> None:
        database.init(**kwargs)
        cls.__finder = None
        cls.__pbf_cache = pbf_cache
        cls._initialized = True

    @classmethod
    def assert_initialized(cls) -> None:
        if not hasattr(cls, "_initialized"):
            raise RuntimeError("PoiIdx not initialized. Call PoiIdx.connect() first.")

    @classmethod
    def close(cls) -> None:
        database.close()

    @classmethod
    def init_schema(cls) -> None:
        database.create_tables(cls.TABLES)

    @classmethod
    def drop_schema(cls) -> None:
        database.drop_tables(cls.TABLES)

    @classmethod
    def recreate_schema(cls) -> None:
        cls.drop_schema()
        cls.init_schema()

    @classmethod
    def init_if_new(cls) -> None:
        existing_tables = database.get_tables()
        tables_to_create = [
            table
            for table in cls.TABLES
            if table._meta.table_name not in existing_tables
        ]
        if tables_to_create:
            cls.recreate_schema()
            cls.init_region_data()

    @staticmethod
    def init_region_data() -> None:
        # Initialize the Region table with a default system region
        System.create(system=True)

        # Download region data
        download_region_data()

    @classmethod
    def get_finder(cls) -> RegionFinder:
        if cls.__finder is None:
            system = System.get_or_none(System.system)

            if system is None or system.region_index is None:
                raise RuntimeError(
                    "Region data is not initialized. Please run init_region_data() first."
                )

            cls.__finder = RegionFinder(json.loads(system.region_index))
        return cls.__finder

    @classmethod
    def find_regions_by_shape(
        cls, shape: shapely.geometry.base.BaseGeometry
    ) -> list[Any]:
        regions = cls.get_finder().find_regions(shape)
        return regions

    @classmethod
    def has_region_data(cls, region_key: str) -> bool:
        """Return true if there is at least one POI for the given region key."""
        return Poi.select().where(Poi.region == region_key).exists()

    @classmethod
    def initialize_pois_for_region(cls, region_key: str) -> None:
        """Initialize POIs for a given region."""
        # Use the finder to get the URL for the region
        region = next(
            (
                r
                for r in cls.get_finder().geofabrik_data["features"]
                if r["properties"]["id"] == region_key
            ),
            None,
        )
        if region is None:
            raise ValueError(
                f"Region with key {region_key} not found in Geofabrik data."
            )

        region_url = region["properties"]["urls"]["pbf"]
        region_id = region["properties"]["id"]

        if cls.__pbf_cache:
            cachedir = (
                pathlib.Path(platformdirs.user_cache_dir("mkmapdiary", "bytehexe"))
                / "pbf"
            )
            cachedir.mkdir(parents=True, exist_ok=True)
            tempfile_context = nullcontext()
        else:
            tempfile_context = tempfile.TemporaryDirectory()
            cachedir = pathlib.Path(tempfile_context.name)

        with tempfile_context:
            pbf_handler = Pbf(cachedir)
            pbf_file = pbf_handler.get_pbf_filename(region_id, region_url)

            # Here you would add the logic to parse the PBF file and populate the POI table.
            # This is a placeholder for demonstration purposes.
            logger.info(
                f"Initialized POIs for region {region_key} from PBF file {pbf_file}"
            )

            # For now, hardcode the filter configuration
            with open(pathlib.Path(__file__).parent / "poi_filter_config.yaml") as f:
                filter_config = yaml.safe_load(f)

            poi_scan(filter_config, pbf_file, region_id)
            administrative_scan(pbf_file, region_id)

    @classmethod
    def init_regions_by_shape(
        cls, shape: shapely.geometry.base.BaseGeometry, buffer: float | None
    ) -> list[Any]:
        """Initialize POIs and administrative boundaries for a given region key."""

        if buffer is not None:
            lp = LocalProjection(shape)
            local_shape = lp.to_local(shape)
            local_shape = local_shape.convex_hull().buffer(buffer)
            shape = lp.to_wgs(local_shape)

        regions = cls.find_regions_by_shape(shape)
        if not regions:
            return []
        for region in regions:
            if not cls.has_region_data(region.id):
                cls.initialize_pois_for_region(region.id)

        return [region.id for region in regions]

    @classmethod
    def get_nearest_pois(
        cls,
        shape: shapely.geometry.base.BaseGeometry,
        max_distance: float | None = None,
        limit: int = 1,
        regions: list[str] | None = None,
        rank_range: tuple[int, int] | None = None,
    ) -> list[Poi]:
        """Get nearest POIs to the given shape using KNN index.

        Args:
            shape: Shapely geometry to search from
            max_distance: Optional maximum distance in meters. If None, returns k nearest regardless of distance.
            limit: Number of nearest POIs to return (k in KNN)
            regions: Optional list of region keys to filter by. If None, searches all regions.
            rank_range: Optional tuple of (min_rank, max_rank) to filter by rank. If None, no rank filtering.
        """

        # Build query - use <-> operator for KNN index search
        query = Poi.select()

        # Optionally filter by regions
        if regions is not None:
            query = query.where(Poi.region.in_(regions))

        # Optionally filter by rank range
        if rank_range is not None:
            min_rank, max_rank = rank_range
            query = query.where((Poi.rank >= min_rank) & (Poi.rank <= max_rank))

        # Optionally filter by max distance first
        if max_distance is not None:
            query = query.where(
                SQL(
                    "ST_DWithin(coordinates, ST_GeogFromText(%s), %s)",
                    (shape.wkt, max_distance),
                )
            )

        # Use KNN operator (<->) for efficient nearest neighbor search with index
        query = query.order_by(
            knn(Poi.coordinates, SQL("ST_GeogFromText(%s)", (shape.wkt,)))
        ).limit(limit)

        return list(query)

    @classmethod
    def get_administrative_hierarchy(
        cls, shape: shapely.geometry.base.BaseGeometry
    ) -> list[AdministrativeBoundary]:
        """Get administrative boundaries containing the given shape.

        Args:
            shape: Shapely geometry to search from
        """

        query = (
            AdministrativeBoundary.select()
            .where(SQL("ST_Covers(coordinates, ST_GeogFromText(%s))", (shape.wkt,)))
            .order_by(AdministrativeBoundary.admin_level.desc())
        )

        hierarchy = list(query)

        if [x for x in hierarchy if x.admin_level == 2]:
            return hierarchy

        # Try to add country level if missing

        admin_with_wikidata = None
        for admin in reversed(hierarchy):
            if admin.wikidata_id is not None:
                admin_with_wikidata = admin
                break

        if admin_with_wikidata is not None and admin_with_wikidata.admin_level <= 6:
            result = country_query(admin_with_wikidata)
            if result is not None:
                name, localized_names = result
                country_admin = AdministrativeBoundary(
                    osm_id="N/A",
                    name=name,
                    region="global",
                    admin_level=2,
                    coordinates=None,
                    wikidata_id=None,
                    localized_names=localized_names,
                )
                hierarchy.append(country_admin)

        return hierarchy

    @classmethod
    def get_administrative_hierarchy_string(
        cls, shape: shapely.geometry.base.BaseGeometry, lang: str | None = None
    ) -> str:
        """Get administrative boundaries containing the given shape as a formatted string.

        Args:
            shape: Shapely geometry to search from
        """
        admin_boundaries = cls.get_administrative_hierarchy(shape)
        items = []
        last_name = None
        for admin in admin_boundaries:
            if lang is None or admin.localized_names is None:
                name = admin.name
            else:
                name = admin.localized_names.get(lang, admin.name)

            if name != last_name:
                items.append(name)
                last_name = name
        return ", ".join(items)
