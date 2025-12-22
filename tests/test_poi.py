from collections.abc import Generator

import pytest  # type: ignore[import-not-found]
from peewee import PostgresqlDatabase
from shapely.geometry import Point
from testcontainers.postgres import PostgresContainer  # type: ignore[import-not-found]

from poiidx.administrativeBoundary import AdministrativeBoundary
from poiidx.baseModel import database
from poiidx.country import Country
from poiidx.poi import Poi


@pytest.fixture(scope="module")
def postgres_container() -> Generator[PostgresContainer, None, None]:
    """Fixture to provide a PostgreSQL container with PostGIS extension."""
    with PostgresContainer("postgis/postgis:latest") as postgres:
        yield postgres


@pytest.fixture(scope="module")
def test_database(
    postgres_container: PostgresContainer,
) -> Generator[PostgresqlDatabase, None, None]:
    """Fixture to provide a database connection and create tables."""
    PostgresqlDatabase(
        postgres_container.dbname,
        user=postgres_container.username,
        password=postgres_container.password,
        host=postgres_container.get_container_host_ip(),
        port=postgres_container.get_exposed_port(5432),
    )

    # Initialize the database proxy
    database.init(
        postgres_container.dbname,
        user=postgres_container.username,
        password=postgres_container.password,
        host=postgres_container.get_container_host_ip(),
        port=postgres_container.get_exposed_port(5432),
    )

    # Enable PostGIS extension
    with database.connection_context():
        database.execute_sql("CREATE EXTENSION IF NOT EXISTS postgis;")

    # Create tables (Country must be created first due to foreign key in AdministrativeBoundary)
    database.create_tables([Country, Poi, AdministrativeBoundary])

    yield database

    # Cleanup
    database.drop_tables([Poi, AdministrativeBoundary, Country])
    database.close()


def test_poi_insert_and_retrieve(test_database: PostgresqlDatabase) -> None:
    """Test that a POI object can be inserted and retrieved with identical values."""
    # Create a test point geometry
    test_point = Point(13.4050, 52.5200)  # Berlin coordinates

    # Create and insert a POI object
    original_poi = Poi.create(
        osm_id="node/12345",
        name="Test Restaurant",
        region="europe/germany",
        coordinates=test_point,
        filter_item="amenity",
        filter_expression="restaurant",
        rank=1,
        symbol="restaurant",
    )

    # Retrieve the POI from the database
    retrieved_poi = Poi.get(Poi.osm_id == "node/12345")

    # Verify all values are identical
    assert retrieved_poi.osm_id == original_poi.osm_id
    assert retrieved_poi.name == original_poi.name
    assert retrieved_poi.region == original_poi.region
    assert retrieved_poi.filter_item == original_poi.filter_item
    assert retrieved_poi.filter_expression == original_poi.filter_expression
    assert retrieved_poi.rank == original_poi.rank
    assert retrieved_poi.symbol == original_poi.symbol

    # Verify geometry is identical (comparing coordinates)
    assert retrieved_poi.coordinates.wkt == test_point.wkt

    # Verify the POI can be queried
    all_pois = list(Poi.select())
    assert len(all_pois) == 1
    assert all_pois[0].osm_id == "node/12345"


def test_poi_multiple_inserts(test_database: PostgresqlDatabase) -> None:
    """Test inserting multiple POIs and retrieving them."""
    # Clear any existing data
    Poi.delete().execute()

    # Create multiple test POIs
    pois_data = [
        {
            "osm_id": "node/001",
            "name": "Restaurant One",
            "region": "europe/germany",
            "coordinates": Point(13.4050, 52.5200),
            "filter_item": "amenity",
            "filter_expression": "restaurant",
            "rank": 1,
        },
        {
            "osm_id": "node/002",
            "name": "Cafe Two",
            "region": "europe/germany",
            "coordinates": Point(13.4100, 52.5250),
            "filter_item": "amenity",
            "filter_expression": "cafe",
            "rank": 2,
        },
        {
            "osm_id": "node/003",
            "name": "Bar Three",
            "region": "europe/france",
            "coordinates": Point(2.3522, 48.8566),
            "filter_item": "amenity",
            "filter_expression": "bar",
            "rank": 3,
        },
    ]

    # Insert all POIs
    for poi_data in pois_data:
        Poi.create(**poi_data)

    # Verify all POIs were inserted
    all_pois = list(Poi.select().order_by(Poi.osm_id))
    assert len(all_pois) == 3

    # Verify each POI's data
    for idx, poi in enumerate(all_pois):
        expected = pois_data[idx]
        assert poi.osm_id == expected["osm_id"]
        assert poi.name == expected["name"]
        assert poi.region == expected["region"]
        assert poi.filter_item == expected["filter_item"]
        assert poi.filter_expression == expected["filter_expression"]
        assert poi.rank == expected["rank"]
        assert poi.coordinates.wkt == expected["coordinates"].wkt  # type: ignore[attr-defined]


def test_poi_unique_osm_id_constraint(test_database: PostgresqlDatabase) -> None:
    """Test that duplicate OSM IDs can be inserted (no unique constraint on osm_id)."""
    # Clear any existing data
    Poi.delete().execute()

    test_point = Point(13.4050, 52.5200)

    # Insert first POI
    Poi.create(
        osm_id="node/duplicate",
        name="First POI",
        region="europe/germany",
        coordinates=test_point,
        filter_item="amenity",
        filter_expression="restaurant",
        rank=1,
    )

    # Insert another POI with the same osm_id (should succeed as there's no unique constraint)
    Poi.create(
        osm_id="node/duplicate",
        name="Second POI",
        region="europe/france",
        coordinates=test_point,
        filter_item="amenity",
        filter_expression="cafe",
        rank=2,
    )

    # Verify both POIs were inserted
    all_pois = list(Poi.select().where(Poi.osm_id == "node/duplicate"))
    assert len(all_pois) == 2


def test_spatial_index_created(test_database: PostgresqlDatabase) -> None:
    """Test that a spatial index has been created on the coordinates field."""
    # Query PostgreSQL system tables to check for the index
    query = """
        SELECT
            i.relname as index_name,
            am.amname as index_type
        FROM pg_class t
        JOIN pg_index ix ON t.oid = ix.indrelid
        JOIN pg_class i ON i.oid = ix.indexrelid
        JOIN pg_am am ON i.relam = am.oid
        WHERE t.relname = %s
        AND i.relname LIKE %s
    """

    cursor = test_database.execute_sql(query, ("poi", "%coordinates%"))
    results = cursor.fetchall()

    # Verify that at least one spatial index exists on the coordinates column
    assert len(results) > 0, "No index found on coordinates field"

    # Check that the index type is SPGIST (as specified in the model)
    index_name, index_type = results[0]
    assert index_type == "spgist", f"Expected SPGIST index but got {index_type}"
    assert "coordinates" in index_name, (
        f"Index name {index_name} doesn't reference coordinates field"
    )


def test_max_distance_parameter(test_database: PostgresqlDatabase) -> None:
    """Test that max_distance parameter correctly filters POIs by distance."""
    from poiidx.poiIdx import PoiIdx

    # Clear any existing data
    Poi.delete().execute()

    # Reference point: Brandenburg Gate in Berlin
    reference_point = Point(13.3777, 52.5163)

    # Create test POIs at known distances from the reference point
    # Using approximate offsets:
    # - ~0.01 degrees longitude at Berlin latitude ≈ 660 meters
    # - ~0.01 degrees latitude ≈ 1110 meters
    pois_data = [
        {
            "osm_id": "node/nearby_100m",
            "name": "Very Close POI",
            "region": "europe/germany",
            # Offset ~100m (0.0015 deg longitude)
            "coordinates": Point(13.3777 + 0.0015, 52.5163),
            "filter_item": "amenity",
            "filter_expression": "restaurant",
            "rank": 1,
        },
        {
            "osm_id": "node/nearby_500m",
            "name": "Close POI",
            "region": "europe/germany",
            # Offset ~500m (0.0075 deg longitude)
            "coordinates": Point(13.3777 + 0.0075, 52.5163),
            "filter_item": "amenity",
            "filter_expression": "cafe",
            "rank": 1,
        },
        {
            "osm_id": "node/medium_1500m",
            "name": "Medium Distance POI",
            "region": "europe/germany",
            # Offset ~1500m (0.0225 deg longitude)
            "coordinates": Point(13.3777 + 0.0225, 52.5163),
            "filter_item": "amenity",
            "filter_expression": "bar",
            "rank": 1,
        },
        {
            "osm_id": "node/far_3000m",
            "name": "Far POI",
            "region": "europe/germany",
            # Offset ~3000m (0.045 deg longitude)
            "coordinates": Point(13.3777 + 0.045, 52.5163),
            "filter_item": "amenity",
            "filter_expression": "pub",
            "rank": 1,
        },
        {
            "osm_id": "node/very_far_5000m",
            "name": "Very Far POI",
            "region": "europe/germany",
            # Offset ~5000m (0.075 deg longitude)
            "coordinates": Point(13.3777 + 0.075, 52.5163),
            "filter_item": "amenity",
            "filter_expression": "biergarten",
            "rank": 1,
        },
    ]

    # Insert all POIs
    for poi_data in pois_data:
        Poi.create(**poi_data)

    # Test 1: max_distance=1000m should return only the first two POIs
    results_1000m = PoiIdx.get_nearest_pois(
        reference_point, max_distance=1000, limit=10
    )
    assert len(results_1000m) == 2, (
        f"Expected 2 POIs within 1000m, got {len(results_1000m)}"
    )
    osm_ids_1000m = {poi.osm_id for poi in results_1000m}
    assert osm_ids_1000m == {
        "node/nearby_100m",
        "node/nearby_500m",
    }, f"Unexpected POIs within 1000m: {osm_ids_1000m}"

    # Test 2: max_distance=2000m should return the first three POIs
    results_2000m = PoiIdx.get_nearest_pois(
        reference_point, max_distance=2000, limit=10
    )
    assert len(results_2000m) == 3, (
        f"Expected 3 POIs within 2000m, got {len(results_2000m)}"
    )
    osm_ids_2000m = {poi.osm_id for poi in results_2000m}
    assert osm_ids_2000m == {
        "node/nearby_100m",
        "node/nearby_500m",
        "node/medium_1500m",
    }, f"Unexpected POIs within 2000m: {osm_ids_2000m}"

    # Test 3: max_distance=4000m should return the first four POIs
    results_4000m = PoiIdx.get_nearest_pois(
        reference_point, max_distance=4000, limit=10
    )
    assert len(results_4000m) == 4, (
        f"Expected 4 POIs within 4000m, got {len(results_4000m)}"
    )
    osm_ids_4000m = {poi.osm_id for poi in results_4000m}
    assert osm_ids_4000m == {
        "node/nearby_100m",
        "node/nearby_500m",
        "node/medium_1500m",
        "node/far_3000m",
    }, f"Unexpected POIs within 4000m: {osm_ids_4000m}"

    # Test 4: max_distance=None should return all POIs (up to limit)
    results_no_limit = PoiIdx.get_nearest_pois(
        reference_point, max_distance=None, limit=10
    )
    assert len(results_no_limit) == 5, (
        f"Expected 5 POIs with no distance limit, got {len(results_no_limit)}"
    )

    # Test 5: max_distance with limit smaller than available POIs
    results_limited = PoiIdx.get_nearest_pois(
        reference_point, max_distance=4000, limit=2
    )
    assert len(results_limited) == 2, (
        f"Expected 2 POIs (limited), got {len(results_limited)}"
    )
    # Should return the two closest POIs
    osm_ids_limited = {poi.osm_id for poi in results_limited}
    assert osm_ids_limited == {
        "node/nearby_100m",
        "node/nearby_500m",
    }, f"Expected closest 2 POIs, got: {osm_ids_limited}"

    # Test 6: Very small max_distance should return no or very few POIs
    results_small = PoiIdx.get_nearest_pois(reference_point, max_distance=50, limit=10)
    assert len(results_small) == 0, (
        f"Expected 0 POIs within 50m, got {len(results_small)}"
    )

    # Test 7: Verify results are ordered by distance (closest first)
    results_ordered = PoiIdx.get_nearest_pois(
        reference_point, max_distance=4000, limit=10
    )
    # Calculate actual distances to verify ordering

    import pyproj

    # Create a transformer for distance calculation (WGS84 to a suitable projection)
    geod = pyproj.Geod(ellps="WGS84")

    previous_distance = 0.0
    for poi in results_ordered:
        # Calculate geodesic distance
        _, _, distance = geod.inv(
            reference_point.x,
            reference_point.y,
            poi.coordinates.x,  # type: ignore[attr-defined]
            poi.coordinates.y,  # type: ignore[attr-defined]
        )
        assert distance >= previous_distance, (
            f"POIs not ordered by distance: {distance} < {previous_distance}"
        )
        previous_distance = distance
