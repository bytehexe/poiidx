import pathlib
import threading
import time
from typing import Any

import pytest  # type: ignore[import-not-found]
from shapely.geometry import MultiPoint, Point

import poiidx
from poiidx import poiIdx as poi_idx_module
from poiidx.pbf import Pbf
from poiidx.poiIdx import PoiIdx
from poiidx.regionFinder import RegionFinder

from .helpers import build_index, run_concurrently

IN_BREMEN = Point(8.7, 53.1)


class _World:
    """Fake ingestion state shared by the stubs."""

    def __init__(self) -> None:
        self.ingested: set[str] = set()
        self.initialized: list[str] = []
        self.downloaded: list[str] = []
        self.lock = threading.Lock()


@pytest.fixture
def world(tmp_path: pathlib.Path, monkeypatch: pytest.MonkeyPatch) -> _World:
    state = _World()
    finder = RegionFinder(build_index())

    def fake_initialize(region_id: str) -> None:
        time.sleep(0.2)  # widen the race window
        with state.lock:
            state.initialized.append(region_id)
        state.ingested.add(region_id)

    def fake_get(self: Pbf, region_id: str, url: str) -> pathlib.Path:
        path = pathlib.Path(self.pbf_dir) / f"{region_id}.pbf"
        path.write_bytes(b"x")
        state.downloaded.append(region_id)
        return path

    monkeypatch.setattr(
        poi_idx_module.platformdirs, "user_cache_dir", lambda *a, **k: str(tmp_path)
    )
    monkeypatch.setattr(PoiIdx, "_initialized", True, raising=False)
    monkeypatch.setattr(PoiIdx, "_PoiIdx__pbf_cache", True, raising=False)
    monkeypatch.setattr(PoiIdx, "get_finder", staticmethod(lambda: finder))
    monkeypatch.setattr(
        PoiIdx, "has_region_data", staticmethod(lambda key: key in state.ingested)
    )
    monkeypatch.setattr(
        PoiIdx, "initialize_pois_for_region", staticmethod(fake_initialize)
    )
    monkeypatch.setattr(Pbf, "get_pbf_filename", fake_get)
    return state


def _ids(infos: list[dict[str, Any]]) -> list[str]:
    return [info["id"] for info in infos]


def test_import_region_by_name_imports_once(world: _World) -> None:
    result = poiidx.import_region("Austria")

    assert _ids(result) == ["austria"]
    assert result[0]["imported"] is True
    assert result[0]["used"] is False  # no shape, so nothing was "selected"
    assert result[0]["distinct"] is None
    poiidx.import_region("austria")
    assert world.initialized == ["austria"]


def test_import_region_with_ambiguous_name_raises(world: _World) -> None:
    with pytest.raises(ValueError, match="georgia-asia, georgia-us"):
        poiidx.import_region("Georgia")
    assert world.initialized == []


def test_download_region_does_not_import(world: _World, tmp_path: pathlib.Path) -> None:
    result = poiidx.download_region("bremen")

    assert result[0]["downloaded"] is True
    assert result[0]["imported"] is False
    assert (tmp_path / "pbf" / "bremen.pbf").exists()
    assert world.initialized == []


def test_download_without_persistent_cache_raises_before_lookup(
    world: _World, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(PoiIdx, "_PoiIdx__pbf_cache", False, raising=False)

    with pytest.raises(RuntimeError, match="pbf_cache=True"):
        poiidx.download_region("not-a-region")
    with pytest.raises(RuntimeError, match="pbf_cache=True"):
        poiidx.download_regions_at(IN_BREMEN)
    assert world.downloaded == []


def test_import_regions_at_imports_only_the_selected_region(world: _World) -> None:
    result = poiidx.import_regions_at(IN_BREMEN)

    assert _ids(result) == ["bremen"]
    assert result[0]["used"] is True
    assert result[0]["distinct"] is True
    assert world.initialized == ["bremen"]  # not germany, not dach


def test_download_regions_at_downloads_every_selected_region(world: _World) -> None:
    shape = MultiPoint([(8.7, 53.1), (13, 47.5)])

    result = poiidx.download_regions_at(shape)

    assert _ids(result) == ["bremen", "austria"]
    assert world.downloaded == ["bremen", "austria"]
    assert world.initialized == []


def test_regions_at_outside_every_region_is_empty(world: _World) -> None:
    ocean = Point(-150, 0)

    assert poiidx.import_regions_at(ocean) == []
    assert poiidx.download_regions_at(ocean) == []
    assert poiidx.list_regions(ocean) == []


def test_regions_at_accepts_a_buffer(world: _World) -> None:
    result = poiidx.import_regions_at(IN_BREMEN, buffer=1_000_000)

    assert "austria" in _ids(result)


def test_concurrent_import_of_the_same_region_ingests_once(world: _World) -> None:
    run_concurrently(lambda: poiidx.import_region("bremen"), 2)

    assert world.initialized == ["bremen"]


def test_functions_require_initialization(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delattr(PoiIdx, "_initialized", raising=False)

    for call in (
        lambda: poiidx.list_regions(),
        lambda: poiidx.download_region("bremen"),
        lambda: poiidx.import_region("bremen"),
        lambda: poiidx.download_regions_at(IN_BREMEN),
        lambda: poiidx.import_regions_at(IN_BREMEN),
    ):
        with pytest.raises(RuntimeError, match="not initialized"):
            call()
