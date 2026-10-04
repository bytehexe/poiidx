import threading
from typing import Any

from shapely.geometry import box, mapping


def feature(
    region_id: str,
    name: str,
    bounds: tuple[float, float, float, float],
    pbf: bool = True,
) -> dict[str, Any]:
    urls = {"pbf": f"https://example.invalid/{region_id}.osm.pbf"} if pbf else {}
    return {
        "type": "Feature",
        "properties": {"id": region_id, "name": name, "urls": urls},
        "geometry": mapping(box(*bounds)),
    }


def build_index() -> dict[str, Any]:
    """A small Geofabrik-like index: dach contains germany and austria; bremen is in germany."""
    return {
        "features": [
            feature("dach", "DACH", (5, 45, 18, 56)),
            feature("germany", "Germany", (5, 47, 15, 55)),
            feature("austria", "Austria", (9.5, 46.3, 17, 49)),
            feature("bremen", "Bremen", (8.4, 53, 9, 53.3)),
            feature("georgia-asia", "Georgia", (40, 41, 46, 43)),
            feature("georgia-us", "Georgia", (-85, 30, -80, 35)),
            feature("bremen-alias", "bremen", (100, 10, 101, 11)),
            feature("everywhere", "Everywhere", (-180, -90, 180, 90), pbf=False),
        ],
    }


def run_concurrently(target: Any, count: int) -> None:
    """Run target in `count` threads and re-raise the first failure."""
    errors: list[BaseException] = []

    def wrapper() -> None:
        try:
            target()
        except BaseException as error:
            errors.append(error)

    threads = [threading.Thread(target=wrapper) for _ in range(count)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join(timeout=10)
        assert not thread.is_alive(), "thread deadlocked"
    if errors:
        raise errors[0]
