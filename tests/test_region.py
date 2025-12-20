import pytest
from testcontainers.postgres import PostgresContainer
from peewee import PostgresqlDatabase
from shapely.geometry import Polygon
from datetime import datetime

from poiidx.baseModel import database
from poiidx.region import Region


@pytest.fixture(scope="module")
def postgres_container():
    """Fixture to provide a PostgreSQL container with PostGIS extension."""
    with PostgresContainer("postgis/postgis:latest") as postgres:
        yield postgres


@pytest.fixture(scope="module")
def database(postgres_container):
    """Fixture to provide a database connection and create tables."""
    db = PostgresqlDatabase(
        postgres_container.dbname,
        user=postgres_container.username,
        password=postgres_container.password,
        host=postgres_container.get_container_host_ip(),
        port=postgres_container.get_exposed_port(5432),
    )
    
    # Initialize the database proxy
    database.initialize(db)
    
    # Enable PostGIS extension
    with db.cursor() as cursor:
        cursor.execute("CREATE EXTENSION IF NOT EXISTS postgis;")
    
    # Create tables
    db.create_tables([Region])
    
    yield db
    
    # Cleanup
    db.drop_tables([Region])
    db.close()


def test_region_insert_and_retrieve(database):
    """Test that a region object can be inserted and retrieved with identical values."""
    # Create a test polygon geometry (a simple square)
    test_geom = Polygon([
        (0.0, 0.0),
        (1.0, 0.0),
        (1.0, 1.0),
        (0.0, 1.0),
        (0.0, 0.0)
    ])
    
    # Create and insert a region object
    original_region = Region.create(
        key="test_region_001",
        name="Test Region",
        geom=test_geom,
        size=1234.56
    )
    
    # Retrieve the region from the database
    retrieved_region = Region.get(Region.key == "test_region_001")
    
    # Verify all values are identical
    assert retrieved_region.key == original_region.key
    assert retrieved_region.name == original_region.name
    assert retrieved_region.size == original_region.size
    
    # Verify geometry is identical (using WKT comparison for precision)
    assert retrieved_region.geom.wkt == test_geom.wkt
    
    # Verify that last_update was set
    assert retrieved_region.last_update is not None
    assert isinstance(retrieved_region.last_update, datetime)
    
    # Verify the region can be queried
    all_regions = list(Region.select())
    assert len(all_regions) == 1
    assert all_regions[0].key == "test_region_001"


def test_region_multiple_inserts(database):
    """Test inserting multiple regions and retrieving them."""
    # Clear any existing data
    Region.delete().execute()
    
    # Create multiple test regions
    regions_data = [
        {
            "key": "region_001",
            "name": "Region One",
            "geom": Polygon([(0, 0), (1, 0), (1, 1), (0, 1), (0, 0)]),
            "size": 100.0
        },
        {
            "key": "region_002",
            "name": "Region Two",
            "geom": Polygon([(2, 2), (3, 2), (3, 3), (2, 3), (2, 2)]),
            "size": 200.0
        },
        {
            "key": "region_003",
            "name": "Region Three",
            "geom": Polygon([(4, 4), (5, 4), (5, 5), (4, 5), (4, 4)]),
            "size": 300.0
        }
    ]
    
    # Insert all regions
    for region_data in regions_data:
        Region.create(**region_data)
    
    # Verify all regions were inserted
    all_regions = list(Region.select().order_by(Region.key))
    assert len(all_regions) == 3
    
    # Verify each region's data
    for idx, region in enumerate(all_regions):
        expected = regions_data[idx]
        assert region.key == expected["key"]
        assert region.name == expected["name"]
        assert region.size == expected["size"]
        assert region.geom.wkt == expected["geom"].wkt


def test_region_unique_key_constraint(database):
    """Test that duplicate keys raise an error."""
    # Clear any existing data
    Region.delete().execute()
    
    test_geom = Polygon([(0, 0), (1, 0), (1, 1), (0, 1), (0, 0)])
    
    # Insert first region
    Region.create(
        key="duplicate_key",
        name="First Region",
        geom=test_geom,
        size=100.0
    )
    
    # Attempt to insert another region with the same key
    with pytest.raises(Exception):  # This will be an IntegrityError
        Region.create(
            key="duplicate_key",
            name="Second Region",
            geom=test_geom,
            size=200.0
        )


def test_spatial_index_created(database):
    """Test that a GIST spatial index has been created on the geom field."""
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
    
    cursor = database.execute_sql(query, ('region', '%geom%'))
    results = cursor.fetchall()
    
    # Verify that at least one GIST index exists on the geom column
    assert len(results) > 0, "No index found on geom field"
    
    # Check that the index type is GIST
    index_name, index_type = results[0]
    assert index_type == 'gist', f"Expected GIST index but got {index_type}"
    assert 'geom' in index_name, f"Index name {index_name} doesn't reference geom field"
