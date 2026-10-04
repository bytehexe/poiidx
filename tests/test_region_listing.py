import pathlib
from typing import Any

import pytest  # type: ignore[import-not-found]
from shapely.geometry import Point

from poiidx import poiIdx as poi_idx_module
from poiidx.pbf import Pbf
from poiidx.poiIdx import PoiIdx
from poiidx.regionFinder import RegionFinder

from .helpers import build_index

IN_BREMEN = Point(8.7, 53.1)


@pytest.fixture
def cache_dir(tmp_path: pathlib.Path, monkeypatch: pytest.MonkeyPatch) -> pathlib.Path:
    """Persistent cache pointing at tmp_path/pbf; fake finder; nothing imported."""
    monkeypatch.setattr(
        poi_idx_module.platformdirs, "user_cache_dir", lambda *a, **k: str(tmp_path)
    )
    monkeypatch.setattr(PoiIdx, "_PoiIdx__pbf_cache", True, raising=False)
    finder = RegionFinder(build_index())
    monkeypatch.setattr(PoiIdx, "get_finder", staticmethod(lambda: finder))
    monkeypatch.setattr(PoiIdx, "has_region_data", staticmethod(lambda key: False))
    return tmp_path / "pbf"


def _by_id(infos: list[dict[str, Any]]) -> dict[str, dict[str, Any]]:
    return {info["id"]: info for info in infos}


def test_list_regions_without_shape_lists_everything_unused(
    cache_dir: pathlib.Path,
) -> None:
    infos = PoiIdx.list_regions(None, None)

    assert "everywhere" not in _by_id(infos)  # no pbf url
    assert len(infos) == 7
    assert not any(info["used"] for info in infos)


def test_list_regions_marks_the_region_poiidx_would_use(
    cache_dir: pathlib.Path,
) -> None:
    """DACH contains the point but is never used: bremen already covers it."""
    infos = PoiIdx.list_regions(IN_BREMEN, None)

    assert [(i["id"], i["used"]) for i in infos] == [
        ("bremen", True),
        ("germany", False),
        ("dach", False),
    ]
    assert set(infos[0]) == {"id", "name", "url", "used", "downloaded", "imported"}


def test_list_regions_outside_every_region_is_empty(cache_dir: pathlib.Path) -> None:
    assert PoiIdx.list_regions(Point(-150, 0), None) == []


def test_list_regions_buffer_widens_the_candidates(cache_dir: pathlib.Path) -> None:
    assert "austria" not in _by_id(PoiIdx.list_regions(IN_BREMEN, None))
    # Austria is ~600 km from Bremen.
    assert "austria" in _by_id(PoiIdx.list_regions(IN_BREMEN, 1_000_000))


def test_downloaded_reflects_the_cache_directory(cache_dir: pathlib.Path) -> None:
    cache_dir.mkdir(parents=True)
    (cache_dir / "bremen.pbf").write_bytes(b"x")

    infos = _by_id(PoiIdx.list_regions(IN_BREMEN, None))

    assert infos["bremen"]["downloaded"] is True
    assert infos["germany"]["downloaded"] is False


def test_imported_reflects_has_region_data(
    cache_dir: pathlib.Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(
        PoiIdx, "has_region_data", staticmethod(lambda key: key == "germany")
    )

    infos = _by_id(PoiIdx.list_regions(IN_BREMEN, None))

    assert infos["germany"]["imported"] is True
    assert infos["bremen"]["imported"] is False


def test_downloaded_is_false_without_a_persistent_cache(
    cache_dir: pathlib.Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    cache_dir.mkdir(parents=True)
    (cache_dir / "bremen.pbf").write_bytes(b"x")
    monkeypatch.setattr(PoiIdx, "_PoiIdx__pbf_cache", False, raising=False)

    assert _by_id(PoiIdx.list_regions(IN_BREMEN, None))["bremen"]["downloaded"] is False


def test_download_region_pbf_fetches_into_the_cache(
    cache_dir: pathlib.Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    def fake_get(self: Pbf, region_id: str, url: str) -> pathlib.Path:
        path = pathlib.Path(self.pbf_dir) / f"{region_id}.pbf"
        path.write_bytes(b"x")
        return path

    monkeypatch.setattr(Pbf, "get_pbf_filename", fake_get)
    region = RegionFinder(build_index()).lookup("bremen")

    PoiIdx.download_region_pbf(region)

    assert (cache_dir / "bremen.pbf").exists()


def test_download_region_pbf_needs_a_persistent_cache(
    cache_dir: pathlib.Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(PoiIdx, "_PoiIdx__pbf_cache", False, raising=False)
    monkeypatch.setattr(
        Pbf,
        "get_pbf_filename",
        lambda *a: pytest.fail("must not download without a persistent cache"),
    )
    region = RegionFinder(build_index()).lookup("bremen")

    with pytest.raises(RuntimeError, match="pbf_cache=True"):
        PoiIdx.download_region_pbf(region)
