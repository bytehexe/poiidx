import hashlib
import json
import logging
import pathlib
import tempfile
from contextlib import nullcontext
from typing import Any

import platformdirs
import shapely
from peewee import SQL, ProgrammingError

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
from .schemaHash import SchemaHash
from .system import System

logger = logging.getLogger(__name__)


class PoiIdx:
    TABLES = [SchemaHash, Poi, System, AdministrativeBoundary, Country]

    @classmethod
    def connect(cls, pbf_cache: bool = True, **kwargs: Any) -> None:
        database.init(**kwargs)
        cls.__finder = None  # type: ignore[attr-defined]
        cls.__pbf_cache = pbf_cache  # type: ignore[attr-defined]
        cls._initialized = True  # type: ignore[attr-defined]

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
    def get_full_schema_sql(cls, model: Any) -> list[str]:
        stmts = []

        # Get CREATE TABLE SQL
        ctx = database.get_sql_context()
        queries = model._schema._create_table(safe=False).query()
        for query in queries:
            sql, params = ctx.sql(query).query()
            if params:
                # Format SQL with parameters
                sql = sql % tuple(repr(p) for p in params)
            stmts.append(sql)

        # Get index creation SQL
        for index in model._meta.indexes:
            sql, params = index.create_sql(model._meta.table_name)
            if sql is not None:
                if params:
                    sql = sql % tuple(repr(p) for p in params)
                stmts.append(sql)

        return stmts

    @classmethod
    def get_schema_hash(cls) -> str:
        schema_sqls = []
        for table in cls.TABLES:
            schema_sqls.extend(cls.get_full_schema_sql(table))

        schema_sqls.sort()

        hasher = hashlib.sha256()
        for sql in schema_sqls:
            hasher.update(sql.encode("utf-8"))
        schema_hash = hasher.hexdigest()

        return schema_hash

    @classmethod
    def init_if_new(cls, filter_config: list[dict[str, Any]]) -> None:
        existing_tables = database.get_tables()
        tables_to_create = [
            table
            for table in cls.TABLES
            if table._meta.table_name not in existing_tables  # type: ignore[attr-defined]
        ]
        do_recreate = False
        do_recreate_index = False
        if tables_to_create:
            do_recreate = True
        else:
            schema_hash = cls.get_schema_hash()
            try:
                existing_schema_hash = SchemaHash.get_or_none(SchemaHash.instance)
            except ProgrammingError:
                existing_schema_hash = None
            if (
                existing_schema_hash is None
                or existing_schema_hash.schema_hash != schema_hash
            ):
                do_recreate = True
            else:
                system = System.get_or_none(System.system)
                if system is None:
                    do_recreate = True
                else:
                    if system.filter_config != json.dumps(filter_config):
                        do_recreate = True
                    if system.region_index is None:
                        do_recreate_index = True

        if do_recreate:
            cls.recreate_schema()
            cls.init_region_data(filter_config=filter_config)
        elif do_recreate_index:
            cls.init_region_data(filter_config=filter_config)

    @classmethod
    def init_region_data(cls, filter_config: list[dict[str, Any]]) -> None:
        # Initialize the Region table with a default system region
        s, _ = System.get_or_create(system=True)
        s.filter_config = json.dumps(filter_config)
        s.save()

        h, _ = SchemaHash.get_or_create(instance=True)
        h.schema_hash = cls.get_schema_hash()
        h.save()

        # Download region data
        download_region_data()

    @classmethod
    def get_finder(cls) -> RegionFinder:
        if cls.__finder is None:  # type: ignore[attr-defined]
            system = System.get_or_none(System.system)

            if system is None or system.region_index is None:
                raise RuntimeError(
                    "Region data is not initialized. Please run init_region_data() first."
                )

            cls.__finder = RegionFinder(json.loads(system.region_index))  # type: ignore[attr-defined]
        return cls.__finder  # type: ignore[attr-defined]

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

        if cls.__pbf_cache:  # type: ignore[attr-defined]
            cachedir = (
                pathlib.Path(platformdirs.user_cache_dir("poiidx", "bytehexe")) / "pbf"
            )
            cachedir.mkdir(parents=True, exist_ok=True)
            tempfile_context: Any = nullcontext()
        else:
            tempfile_context = tempfile.TemporaryDirectory()
            cachedir = pathlib.Path(tempfile_context.name)  # type: ignore[attr-defined]

        # Get the filter config from the System table
        system = System.get_or_none(System.system)
        if system is None:
            raise RuntimeError(
                "System configuration not found. Please run init_region_data() first."
            )
        filter_config = json.loads(system.filter_config)

        with tempfile_context:
            pbf_handler = Pbf(cachedir)
            pbf_file = pbf_handler.get_pbf_filename(region_id, region_url)

            # Here you would add the logic to parse the PBF file and populate the POI table.
            # This is a placeholder for demonstration purposes.
            logger.info(
                f"Initialized POIs for region {region_key} from PBF file {pbf_file}"
            )

            poi_scan(filter_config, str(pbf_file), region_id)
            administrative_scan(str(pbf_file), region_id)

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
        limit: int | None = None,
        regions: list[str] | None = None,
        rank_range: tuple[int, int] | None = None,
    ) -> list[Poi]:
        """Get nearest POIs to the given shape using KNN index.

        Args:
            shape: Shapely geometry to search from
            max_distance: Optional maximum distance in meters. If None, returns k nearest regardless of distance.
            limit: Optional number of nearest POIs to return (k in KNN). At least one of limit or max_distance must be provided.
            regions: Optional list of region keys to filter by. If None, searches all regions.
            rank_range: Optional tuple of (min_rank, max_rank) to filter by rank. If None, no rank filtering.

        Raises:
            ValueError: If neither limit nor max_distance is provided.
        """
        if limit is None and max_distance is None:
            raise ValueError(
                "At least one of 'limit' or 'max_distance' must be provided"
            )

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
        )

        # Apply limit if specified
        if limit is not None:
            query = query.limit(limit)

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
                name = admin.localized_names.get(lang, admin.name)  # type: ignore[attr-defined]

            if name != last_name:
                items.append(name)
                last_name = name
        return ", ".join(items)  # type: ignore[arg-type]
