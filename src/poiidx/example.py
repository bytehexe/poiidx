from .poiIdx import PoiIdx
import click
from shapely.geometry import Point
import time


@click.command()
@click.option('--password-file', type=click.Path(exists=True), help='Path to file containing the database password.', required=True)
@click.option('--re-init', is_flag=True, help='Re-initialize the database even if it already exists.')
def run_example(password_file, re_init):

    with open(password_file, 'r') as f:
        password = f.read().strip()

    p = PoiIdx(
        host='localhost',
        port=5432,
        user='poiidx_user',
        password=password,
        database='poiidx_db'
    )

    if re_init:
        p.drop_schema()

    p.init_if_new()

    click.echo("Database initialized and region data downloaded.")

    # Point for Berlin, Germany
    berlin_point = Point(13.4050, 52.5200)

    start_time = time.time()
    regions = p.find_regions_by_shape(berlin_point)
    elapsed_time = time.time() - start_time

    if regions:
        click.echo(f"Found {len(regions)} region(s) for the given point:")
        for region in regions:
            click.echo(f"  - {region.name} (key: {region.id})")
    else:
        click.echo("No region found for the given point.")
    
    click.echo(f"\nQuery took {elapsed_time:.3f} seconds")

    # Try again to demonstrate caching
    start_time = time.time()
    regions = p.find_regions_by_shape(berlin_point)
    elapsed_time = time.time() - start_time
    click.echo(f"\nSecond query took {elapsed_time:.3f} seconds (should be faster due to caching)")

    # Initialize POIs for Berlin region if not already done
    berlin_region_key = 'berlin'
    #berlin_region_key = 'germany'
    if not p.has_region_data(berlin_region_key):
        click.echo(f"\nInitializing POIs for region '{berlin_region_key}'...")
        p.initialize_pois_for_region(berlin_region_key)
        click.echo("POIs initialized.")
    else:
        click.echo(f"\nPOIs for region '{berlin_region_key}' are already initialized.")

    # Find nearest POIs to a point in Berlin
    click.echo("\nFinding nearest POIs to a point in Berlin...")
    start_time = time.time()
    nearest_pois = p.get_nearest_pois(berlin_point, max_distance=1000, limit=5)
    if nearest_pois:
        click.echo(f"Found {len(nearest_pois)} POI(s) within 1 km:")
        for poi in nearest_pois:
            click.echo(f"  - {poi.name} (Region: {poi.region}, Rank: {poi.rank})")
    else:
        click.echo("No POIs found within 5 km.")
    elapsed_time = time.time() - start_time
    click.echo(f"Nearest POI query took {elapsed_time:.3f} seconds")

    # Get the administrative hierarchy for Berlin
    click.echo("\nRetrieving administrative hierarchy for Berlin...")
    admin_hierarchy = p.get_administrative_hierarchy(berlin_point)
    if admin_hierarchy:
        click.echo("Administrative Hierarchy:")
        for admin in admin_hierarchy:
            click.echo(f"  - Level {admin.admin_level}: {admin.name} (OSM ID: {admin.osm_id})")
    else:
        click.echo("No administrative boundaries found for the given point.")

    start_time = time.time()
    admin_hierarchy_str = p.get_administrative_hierarchy_string(berlin_point)
    elapsed_time = time.time() - start_time
    click.echo(f"\nAdministrative hierarchy string retrieval took {elapsed_time:.3f} seconds")
    click.echo("\nAdministrative Hierarchy (String Representation):")
    click.echo(admin_hierarchy_str)

    # Get administrative hierarchy for Hannover
    hannover_point = Point(9.7320, 52.3759)
    click.echo("\nRetrieving administrative hierarchy for Hannover...")
    admin_hierarchy = p.get_administrative_hierarchy(hannover_point)
    if admin_hierarchy:
        click.echo("Administrative Hierarchy:")
        for admin in admin_hierarchy:
            click.echo(f"  - Level {admin.admin_level}: {admin.name} (OSM ID: {admin.osm_id})")
    else:
        click.echo("No administrative boundaries found for the given point.")

if __name__ == '__main__':
    run_example()