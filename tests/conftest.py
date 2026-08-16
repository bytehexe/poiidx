from collections.abc import Generator

import pytest  # type: ignore[import-not-found]
from peewee import PostgresqlDatabase
from testcontainers.postgres import PostgresContainer  # type: ignore[import-not-found]

from poiidx.administrativeBoundary import AdministrativeBoundary
from poiidx.baseModel import database
from poiidx.country import Country
from poiidx.poi import Poi


@pytest.fixture(scope="session")
def postgres_container() -> Generator[PostgresContainer, None, None]:
    """Fixture to provide a PostgreSQL container with PostGIS extension."""
    with PostgresContainer("postgis/postgis:latest") as postgres:
        yield postgres


@pytest.fixture(scope="session")
def test_database(
    postgres_container: PostgresContainer,
) -> Generator[PostgresqlDatabase, None, None]:
    """Fixture to provide a database connection and create tables."""
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
