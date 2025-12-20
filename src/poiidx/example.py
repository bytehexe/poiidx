import time

import click
from shapely.geometry import Point

import poiidx


@click.command()
@click.option('--password-file', type=click.Path(exists=True), help='Path to file containing the database password.', required=True)
@click.option('--re-init', is_flag=True, help='Re-initialize the database even if it already exists.')
def run_example(password_file, re_init) -> None:

    with open(password_file) as f:
        password = f.read().strip()

    poiidx.init(
        host='localhost',
        port=5432,
        user='poiidx_user',
        password=password,
        database='poiidx_db',
        recreate=re_init
    )

    click.echo("Database initialized.")

    # Point for Berlin, Germany
    berlin_point = Point(13.4050, 52.5200)

    # Find nearest POIs to a point in Berlin
    click.echo("\nFinding nearest POIs to a point in Berlin...")
    start_time = time.time()
    nearest_pois = poiidx.get_nearest_pois(berlin_point, max_distance=1000, limit=5)
    if nearest_pois:
        click.echo(f"Found {len(nearest_pois)} POI(s) within 1 km:")
        for poi in nearest_pois:
            click.echo(f"  - {poi['name']} (Region: {poi['region']}, Rank: {poi['rank']})")
    else:
        click.echo("No POIs found within 5 km.")
    elapsed_time = time.time() - start_time
    click.echo(f"Nearest POI query took {elapsed_time:.3f} seconds")

    # Get the administrative hierarchy for Berlin
    click.echo("\nRetrieving administrative hierarchy for Berlin...")
    admin_hierarchy = poiidx.get_administrative_hierarchy(berlin_point)
    if admin_hierarchy:
        click.echo("Administrative Hierarchy:")
        for admin in admin_hierarchy:
            click.echo(f"  - Level {admin['admin_level']}: {admin['name']} (OSM ID: {admin['osm_id']})")
    else:
        click.echo("No administrative boundaries found for the given point.")

    start_time = time.time()
    admin_hierarchy_str = poiidx.get_administrative_hierarchy_string(berlin_point)
    elapsed_time = time.time() - start_time
    click.echo(f"\nAdministrative hierarchy string retrieval took {elapsed_time:.3f} seconds")
    click.echo("\nAdministrative Hierarchy (String Representation):")
    click.echo(admin_hierarchy_str)

    # Get administrative hierarchy for Hannover
    hannover_point = Point(9.7320, 52.3759)
    click.echo("\nRetrieving administrative hierarchy for Hannover...")
    admin_hierarchy = poiidx.get_administrative_hierarchy(hannover_point)
    if admin_hierarchy:
        click.echo("Administrative Hierarchy:")
        for admin in admin_hierarchy:
            click.echo(f"  - Level {admin['admin_level']}: {admin['name']} (OSM ID: {admin['osm_id']})")
    else:
        click.echo("No administrative boundaries found for the given point.")

    # Get administrative hierarchy string for Hannover, but in French
    start_time = time.time()
    admin_hierarchy_str = poiidx.get_administrative_hierarchy_string(hannover_point, lang='fr')
    elapsed_time = time.time() - start_time
    click.echo(f"\nAdministrative hierarchy string retrieval took {elapsed_time:.3f} seconds")
    click.echo("\nAdministrative Hierarchy for Hannover (String Representation in French):")
    click.echo(admin_hierarchy_str)

if __name__ == '__main__':
    run_example()
