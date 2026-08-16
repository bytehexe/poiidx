import pathlib
import threading
from typing import Any

import pytest  # type: ignore[import-not-found]
from peewee import PostgresqlDatabase
from shapely.geometry import Point

from poiidx import poiIdx as poi_idx_module
from poiidx.pbf import Pbf
from poiidx.poi import Poi
from poiidx.poiIdx import PoiIdx

SHAPE = Point(13.4050, 52.5200)


class _FakeRegion:
    def __init__(self, region_id: str) -> None:
        self.id = region_id


def _run_concurrently(target: Any, count: int) -> None:
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


def test_concurrent_init_of_the_same_region_ingests_it_only_once(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Two threads querying the same region must not both download and scan it."""
    ingested: set[str] = set()
    calls: list[str] = []
    calls_lock = threading.Lock()
    both_in_loop = threading.Barrier(2)
    both_checked = threading.Barrier(2)

    def fake_find(shape: Any) -> list[_FakeRegion]:
        both_in_loop.wait(timeout=5)  # force the threads to race
        return [_FakeRegion("bremen")]

    def fake_has_region_data(region_key: str) -> bool:
        result = region_key in ingested
        # Unserialised, both threads reach this together and both see "missing".
        # Serialised, the second thread arrives after the barrier has broken and
        # its check already reflects the first thread's ingestion.
        try:
            both_checked.wait(timeout=0.5)
        except threading.BrokenBarrierError:
            pass
        return result

    def fake_initialize(region_key: str) -> None:
        with calls_lock:
            calls.append(region_key)
        ingested.add(region_key)

    monkeypatch.setattr(PoiIdx, "find_regions_by_shape", staticmethod(fake_find))
    monkeypatch.setattr(PoiIdx, "has_region_data", staticmethod(fake_has_region_data))
    monkeypatch.setattr(
        PoiIdx, "initialize_pois_for_region", staticmethod(fake_initialize)
    )

    _run_concurrently(lambda: PoiIdx.init_regions_by_shape(SHAPE, None), 2)

    assert calls == ["bremen"]


def test_initialising_different_regions_is_not_serialised(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A slow download of one region must not block ingestion of another."""
    regions = iter([[_FakeRegion("bremen")], [_FakeRegion("berlin")]])
    regions_lock = threading.Lock()
    both_ingesting = threading.Barrier(2)

    def fake_find(shape: Any) -> list[_FakeRegion]:
        with regions_lock:
            return next(regions)

    def fake_initialize(region_key: str) -> None:
        # Times out into BrokenBarrierError if a global lock serialises regions.
        both_ingesting.wait(timeout=5)

    monkeypatch.setattr(PoiIdx, "find_regions_by_shape", staticmethod(fake_find))
    monkeypatch.setattr(PoiIdx, "has_region_data", staticmethod(lambda key: False))
    monkeypatch.setattr(
        PoiIdx, "initialize_pois_for_region", staticmethod(fake_initialize)
    )

    _run_concurrently(lambda: PoiIdx.init_regions_by_shape(SHAPE, None), 2)


class _FakeSystem:
    filter_config = "[]"


def test_failed_ingestion_leaves_no_partial_region_data(
    test_database: PostgresqlDatabase, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A region is ingested all-or-nothing.

    Otherwise a crash part-way through leaves rows behind, and has_region_data
    then reports the half-scanned region as complete forever.
    """
    region = {
        "properties": {
            "id": "testregion",
            "urls": {"pbf": "http://127.0.0.1:1/testregion-latest.osm.pbf"},
        }
    }

    class _FakeFinder:
        geofabrik_data = {"features": [region]}

    def fake_poi_scan(filter_config: Any, pbf_file: str, region_id: str) -> None:
        Poi.create(
            osm_id="n1",
            name="Half Scanned",
            region=region_id,
            coordinates=Point(8.8, 53.1),
            filter_item="amenity",
            filter_expression="restaurant",
            rank=1,
        )

    def fake_administrative_scan(pbf_file: str, region_id: str) -> None:
        raise RuntimeError("scan crashed halfway")

    monkeypatch.setattr(PoiIdx, "get_finder", staticmethod(_FakeFinder))
    monkeypatch.setattr(PoiIdx, "_PoiIdx__pbf_cache", False, raising=False)
    monkeypatch.setattr(
        poi_idx_module.System, "get_or_none", staticmethod(lambda *a: _FakeSystem())
    )
    monkeypatch.setattr(
        Pbf, "get_pbf_filename", lambda self, rid, url: pathlib.Path("unused.pbf")
    )
    monkeypatch.setattr(poi_idx_module, "poi_scan", fake_poi_scan)
    monkeypatch.setattr(poi_idx_module, "administrative_scan", fake_administrative_scan)

    try:
        with pytest.raises(RuntimeError, match="scan crashed halfway"):
            PoiIdx.initialize_pois_for_region("testregion")

        assert Poi.select().where(Poi.region == "testregion").count() == 0
    finally:
        Poi.delete().where(Poi.region == "testregion").execute()
