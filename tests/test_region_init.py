import pathlib
import threading
from typing import Any

import pytest  # type: ignore[import-not-found]
from peewee import PostgresqlDatabase
from shapely.geometry import Point

from poiidx import poiIdx as poi_idx_module
from poiidx.administrativeBoundary import AdministrativeBoundary
from poiidx.pbf import Pbf
from poiidx.poi import Poi
from poiidx.poiIdx import PoiIdx

from .helpers import run_concurrently

SHAPE = Point(13.4050, 52.5200)


class _FakeRegion:
    def __init__(self, region_id: str) -> None:
        self.id = region_id


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

    run_concurrently(lambda: PoiIdx.init_regions_by_shape(SHAPE, None), 2)

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

    run_concurrently(lambda: PoiIdx.init_regions_by_shape(SHAPE, None), 2)


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


def test_buffered_shape_without_buffer_returns_the_shape() -> None:
    assert PoiIdx.buffered_shape(SHAPE, None) is SHAPE


def test_buffered_shape_widens_the_shape() -> None:
    """Regression: `convex_hull()` was called as a method and raised TypeError."""
    result = PoiIdx.buffered_shape(SHAPE, 1000)
    min_x, _, max_x, _ = result.bounds
    assert result.contains(SHAPE)
    assert 0.02 < max_x - min_x < 0.04  # ~2 km at 52.5 degrees north


def test_region_with_only_boundaries_counts_as_imported(
    test_database: PostgresqlDatabase,
) -> None:
    """A region whose filter matched no POIs must not be re-ingested forever."""
    AdministrativeBoundary.create(
        osm_id="r1",
        name="Boundary Only",
        region="boundaryonly",
        admin_level=4,
        coordinates=Point(8.8, 53.1).buffer(0.1),
    )
    try:
        assert PoiIdx.has_region_data("boundaryonly")
        assert not PoiIdx.has_region_data("norows")
    finally:
        AdministrativeBoundary.delete().where(
            AdministrativeBoundary.region == "boundaryonly"
        ).execute()


def test_import_region_skips_a_region_that_is_already_imported(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls: list[str] = []
    monkeypatch.setattr(PoiIdx, "has_region_data", staticmethod(lambda key: True))
    monkeypatch.setattr(
        PoiIdx, "initialize_pois_for_region", staticmethod(calls.append)
    )

    PoiIdx.import_region("bremen")

    assert calls == []


def test_buffered_shape_near_the_antimeridian_stays_valid() -> None:
    """Fiji: the buffer crosses 180 degrees and must not wrap into a global sliver."""
    fiji = Point(179.9, -17)

    result = PoiIdx.buffered_shape(fiji, 20_000)

    assert result.is_valid
    min_x, _, max_x, _ = result.bounds
    assert min_x < -179.5 and max_x > 179.5  # both sides of the line...
    assert result.contains(fiji)
    assert result.contains(Point(-179.95, -17))  # ...and the far side is covered
    assert result.area < 1  # a ~0.4 x 0.4 degree patch, not the whole globe


def test_buffered_shape_with_zero_buffer_is_the_shape() -> None:
    """buffer=0 means no buffer; a zero-width buffer of a point is an empty polygon."""
    assert PoiIdx.buffered_shape(SHAPE, 0) is SHAPE


def test_buffered_shape_rejects_a_negative_buffer() -> None:
    with pytest.raises(ValueError, match="must not be negative"):
        PoiIdx.buffered_shape(SHAPE, -1)
