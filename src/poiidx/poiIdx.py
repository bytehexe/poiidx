from peewee import PostgresqlDatabase, SQL
from .baseModel import database_proxy
from .system import System
from .geofabrik import download_region_data
from .regionFinder import RegionFinder
from .poi import Poi
from .country import Country
from .administrativeBoundary import AdministrativeBoundary
import shapely
import json
import yaml
import logging
import requests
import pathlib
import platformdirs
import tempfile
from contextlib import nullcontext
from .pbf import Pbf
from .scanner import poi_scan, administrative_scan
from .ext import knn
from .countryQuery import country_query

logger = logging.getLogger(__name__)

class PoiIdx:

    TABLES = [Poi, System, AdministrativeBoundary, Country]

    def __init__(self, pbf_cache=True, **kwargs):
        self.db = PostgresqlDatabase(**kwargs)
        self.db.connect()
        database_proxy.initialize(self.db)
        self.__finder = None

        self.__pbf_cache = pbf_cache

    def close(self):
        self.db.close()

    def init_schema(self):
        self.db.create_tables(self.TABLES)

    def drop_schema(self):
        self.db.drop_tables(self.TABLES)

    def recreate_schema(self):
        self.drop_schema()
        self.init_schema()

    def init_if_new(self):
        existing_tables = self.db.get_tables()
        tables_to_create = [table for table in self.TABLES if table._meta.table_name not in existing_tables]
        if tables_to_create:
            self.recreate_schema()
            self.init_region_data()

    def init_region_data(self):
        # Initialize the Region table with a default system region
        System.create(system=True)
        
        # Download region data
        download_region_data()

    @property
    def finder(self):
        if self.__finder is None:
            system = System.get_or_none(System.system == True)

            if system is None or system.region_index is None:
                raise RuntimeError("Region data is not initialized. Please run init_region_data() first.")

            self.__finder = RegionFinder(json.loads(system.region_index))
        return self.__finder

    def find_regions_by_shape(self, shape: shapely.geometry.base.BaseGeometry):
        regions = self.finder.find_regions(shape)
        return regions
    
    def has_region_data(self, region_key: str) -> bool:
        """Return true if there is at least one POI for the given region key."""
        return Poi.select().where(Poi.region == region_key).exists()
    
    def initialize_pois_for_region(self, region_key: str):
        """Initialize POIs for a given region."""
        # Use the finder to get the URL for the region
        region = next((r for r in self.finder.geofabrik_data["features"] if r["properties"]["id"] == region_key), None)
        if region is None:
            raise ValueError(f"Region with key {region_key} not found in Geofabrik data.")
        
        region_url = region["properties"]["urls"]["pbf"]
        region_id = region["properties"]["id"]

        if self.__pbf_cache:
            cachedir = pathlib.Path(platformdirs.user_cache_dir("mkmapdiary", "bytehexe")) / "pbf"
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
            logger.info(f"Initialized POIs for region {region_key} from PBF file {pbf_file}")

            # For now, hardcode the filter configuration
            with open(pathlib.Path(__file__).parent / "poi_filter_config.yaml", "r") as f:
                filter_config = yaml.safe_load(f)

            poi_scan(filter_config, pbf_file, region_id)
            administrative_scan(pbf_file, region_id)

    def get_nearest_pois(self, shape: shapely.geometry.base.BaseGeometry, max_distance: float | None = None, limit: int = 1, regions: list[str] | None = None, rank_range: tuple[int, int] | None = None):
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
                SQL("ST_DWithin(coordinates, ST_GeogFromText(%s), %s)", (shape.wkt, max_distance))
            )
        
        # Use KNN operator (<->) for efficient nearest neighbor search with index
        query = (
            query
            .order_by(knn(Poi.coordinates, SQL("ST_GeogFromText(%s)", (shape.wkt,))))
            .limit(limit)
        )
        
        return list(query)
    
    def get_administrative_hierarchy(self, shape: shapely.geometry.base.BaseGeometry):
        """Get administrative boundaries containing the given shape.
        
        Args:
            shape: Shapely geometry to search from
        """
        from peewee import SQL
        
        query = AdministrativeBoundary.select().where(
            SQL("ST_Covers(coordinates, ST_GeogFromText(%s))", (shape.wkt,))
        ).order_by(AdministrativeBoundary.admin_level.desc())
        
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
            name = country_query(admin_with_wikidata)
            if name is not None:
                country_admin = AdministrativeBoundary(
                    osm_id=0,
                    name=name,
                    region="global",
                    admin_level=2,
                    coordinates=None,
                    wikidata_id=None,
                )
                hierarchy.append(country_admin)

        return hierarchy
    
    def get_administrative_hierarchy_string(self, shape: shapely.geometry.base.BaseGeometry):
        """Get administrative boundaries containing the given shape as a formatted string.
        
        Args:
            shape: Shapely geometry to search from
        """
        admin_boundaries = self.get_administrative_hierarchy(shape)
        items = []
        last_name = None
        for admin in admin_boundaries:
            if admin.name != last_name:
                items.append(admin.name)
                last_name = admin.name
        return ", ".join(items)