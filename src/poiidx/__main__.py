import pathlib
from typing import Any

import click  # type: ignore
import platformdirs
import yaml
from coordinate_parser import parse_coordinate
from shapely.geometry import Point

import poiidx


def load_config(config_path: str) -> dict[str, Any]:
    """Load database configuration from YAML file."""
    with open(config_path) as f:
        db_config = yaml.safe_load(f)

    if "filter_config" not in db_config or db_config["filter_config"] is None:
        # Use example filter config from the poiidx package
        with open(pathlib.Path(poiidx.__file__).parent / "poi_filter_config.yaml") as f:
            db_config["filter_config"] = yaml.safe_load(f)

    return db_config


def get_default_config_path() -> str:
    """Get default configuration file path using platformdirs."""
    config_dir = platformdirs.user_config_dir("poiidx", "bytehexe")
    return str(pathlib.Path(config_dir) / "cli.yaml")


@click.group()
@click.option(
    "--config",
    type=click.Path(exists=True),
    default=get_default_config_path,
    help="Path to configuration file",
)
@click.option(
    "--re-init",
    is_flag=True,
    help="Re-initialize the database even if it already exists",
)
@click.pass_context
def cli(ctx: click.Context, config: str, re_init: bool) -> None:
    """poiidx command-line interface."""
    ctx.ensure_object(dict)
    ctx.obj["config"] = config
    ctx.obj["re_init"] = re_init


@cli.command()
@click.argument("lat")
@click.argument("lon")
@click.option(
    "--count",
    type=int,
    help="Number of nearest POIs to return",
)
@click.option(
    "--distance",
    type=float,
    help="Maximum distance in meters",
)
@click.pass_context
def poi(
    ctx: click.Context, lat: str, lon: str, count: int | None, distance: float | None
) -> None:
    """Query Points of Interest at the given location.

    Coordinates can be provided in various formats:
    - Decimal degrees: 52.5200 13.4050
    - DMS: 52°31'12\"N 13°24'18\"E
    """
    # Validate that at least one of count or distance is provided
    if count is None and distance is None:
        raise click.UsageError("At least one of --count or --distance must be set")

    # Load configuration
    db_config = load_config(ctx.obj["config"])

    # Initialize poiidx
    poiidx.init(**db_config, recreate=ctx.obj["re_init"])

    try:
        # Parse coordinates
        lat_decimal = parse_coordinate(lat, coord_type="latitude")
        lon_decimal = parse_coordinate(lon, coord_type="longitude")

        if lat_decimal is None or lon_decimal is None:
            raise click.BadParameter("Invalid coordinate format")

        # Create point
        point = Point(float(lon_decimal), float(lat_decimal))

        # Query POIs
        pois = poiidx.get_nearest_pois(
            point, max_distance=distance, limit=count if count is not None else None
        )

        # Clean up
        for poi in pois:
            if "coordinates" in poi:
                del poi["coordinates"]

        # Output results as YAML
        click.echo(yaml.dump(pois, default_flow_style=False))
    finally:
        poiidx.close()


@cli.command()
@click.argument("lat")
@click.argument("lon")
@click.option(
    "--short",
    is_flag=True,
    help="Return short string representation instead of full YAML",
)
@click.option(
    "--max-admin-level",
    type=int,
    help="Maximum administrative level to include (e.g., 6 for county level)",
)
@click.pass_context
def admin(
    ctx: click.Context, lat: str, lon: str, short: bool, max_admin_level: int | None
) -> None:
    """Query administrative boundaries at the given location.

    Coordinates can be provided in various formats:
    - Decimal degrees: 52.5200 13.4050
    - DMS: 52°31'12\"N 13°24'18\"E
    """
    # Load configuration
    db_config = load_config(ctx.obj["config"])

    # Initialize poiidx
    poiidx.init(**db_config, recreate=ctx.obj["re_init"])

    try:
        # Parse coordinates
        lat_decimal = parse_coordinate(lat, coord_type="latitude")
        lon_decimal = parse_coordinate(lon, coord_type="longitude")

        if lat_decimal is None or lon_decimal is None:
            raise click.BadParameter("Invalid coordinate format")

        # Create point
        point = Point(float(lon_decimal), float(lat_decimal))

        if short:
            # Use string representation
            result = poiidx.get_administrative_hierarchy_string(
                point, max_admin_level=max_admin_level
            )
            click.echo(result)
        else:
            # Return full YAML
            hierarchy = poiidx.get_administrative_hierarchy(
                point, max_admin_level=max_admin_level
            )

            # Clean up the output
            for level in hierarchy:
                if "coordinates" in level:
                    del level["coordinates"]

            click.echo(yaml.dump(hierarchy, default_flow_style=False))
    finally:
        poiidx.close()


if __name__ == "__main__":
    cli()
