# Region Management API Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add five public functions to download/import Geofabrik regions explicitly (by name/id or by point/shape) and to list which regions contain a shape and which of them poiidx would use.

**Architecture:** `RegionFinder` gains candidate listing and name/id lookup. `PoiIdx` gains shared helpers (buffering, cache dir, region state), `import_region`, `download_region_pbf`, `list_regions`, `fetch_named`, `fetch_at`; `init_regions_by_shape` is rebuilt on `import_region`. `poiidx/__init__.py` stays thin and delegates.

**Tech Stack:** Python, peewee, shapely, pytest, hatch (never call pytest/ruff/mypy/pip directly).

**Spec:** `docs/superpowers/specs/2026-10-04-region-management-api-design.md`

## Global Constraints

- All tooling via hatch: `hatch test <path>`, `hatch run ruff:ruff check`, `hatch run ruff:ruff format`, `hatch run types:check`.
- Ruff enforces `ANN` (full annotations incl. `-> None`, in tests too) and `COM` (trailing commas).
- Module filenames in `src/poiidx/` are lowerCamelCase; test files are snake_case.
- Commit messages follow conventional commits; end each with `Co-Authored-By: Claude Sonnet 5.5 <noreply@anthropic.com>`. Stage explicit paths only (never `git add -A` / `.`).
- No schema change: adding a field/index/`Meta` change wipes users' databases (CLAUDE.md).
- All five public functions return `list[dict[str, Any]]`; dict keys are exactly `id, name, url, used, downloaded, imported`.
- `download_*` raise `RuntimeError` when `pbf_cache=False`; this check runs before any lookup or network access.
- `__about__.py` is generated; never edit it.
- Tests that touch the database use the `test_database` fixture and need a Docker daemon; everything else in this plan is Docker-free.

## Review Focus

1. Shape outside every Geofabrik region (open ocean): `list_regions` and `*_at` return `[]`, no exception. (Task 1, Task 4)
2. Geofabrik feature without a `urls.pbf` entry: never listed, never selected, never raises. (Task 1)
3. Empty or whitespace `name_or_id`: `ValueError`, not "matches everything". (Task 1)
4. `buffer=` on any shape-based call: currently raises `TypeError` (`convex_hull()` is a property in shapely 2). Must work and widen the candidate set. (Task 2, Task 3)
5. Two threads importing the same region concurrently ingest it once. (Task 2, Task 4)

---

### Task 1: RegionFinder candidates, listing and lookup

**Files:**
- Create: `tests/helpers.py`, `tests/test_region_finder.py`
- Modify: `src/poiidx/regionFinder.py`

**Interfaces:**
- Consumes: existing `Region(id, name, url)`, `RegionFinder(geofabrik_data)`.
- Produces:
  - `RegionFinder.all_regions() -> list[Region]` (only features with a pbf url)
  - `RegionFinder.find_candidates(geo_data: BaseGeometry) -> list[Region]` (all intersecting, ascending area)
  - `RegionFinder.lookup(name_or_id: str) -> Region` (raises `ValueError`)
  - `tests/helpers.py::build_index() -> dict[str, Any]` (fake Geofabrik index, used by later tasks)

- [ ] **Step 1: Write the test helper**

Create `tests/helpers.py`:

```python
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
```

- [ ] **Step 2: Write the failing tests**

Create `tests/test_region_finder.py`:

```python
import pytest  # type: ignore[import-not-found]
from shapely.geometry import MultiPoint, Point

from poiidx.regionFinder import RegionFinder

from .helpers import build_index

IN_BREMEN = Point(8.7, 53.1)
IN_OCEAN = Point(-150, 0)


@pytest.fixture(scope="module")
def finder() -> RegionFinder:
    return RegionFinder(build_index())


def _ids(regions: list) -> list[str]:  # type: ignore[type-arg]
    return [region.id for region in regions]


def test_candidates_include_larger_parents_smallest_first(finder: RegionFinder) -> None:
    assert _ids(finder.find_candidates(IN_BREMEN)) == ["bremen", "germany", "dach"]


def test_find_regions_selects_only_the_smallest_cover(finder: RegionFinder) -> None:
    assert _ids(finder.find_regions(IN_BREMEN)) == ["bremen"]


def test_find_regions_for_two_countries(finder: RegionFinder) -> None:
    shape = MultiPoint([(8.7, 53.1), (13, 47.5)])
    assert _ids(finder.find_regions(shape)) == ["bremen", "austria"]
    assert set(_ids(finder.find_candidates(shape))) == {
        "bremen",
        "austria",
        "germany",
        "dach",
    }


def test_shape_outside_every_region_yields_nothing(finder: RegionFinder) -> None:
    # "everywhere" covers this point but has no pbf url, so it must be ignored.
    assert finder.find_candidates(IN_OCEAN) == []
    assert finder.find_regions(IN_OCEAN) == []


def test_region_without_pbf_url_is_not_listed(finder: RegionFinder) -> None:
    assert "everywhere" not in _ids(finder.all_regions())


def test_lookup_by_id(finder: RegionFinder) -> None:
    assert finder.lookup("austria").id == "austria"


def test_lookup_by_name_ignores_case(finder: RegionFinder) -> None:
    assert finder.lookup("AUSTRIA").id == "austria"
    assert finder.lookup("  Austria ").id == "austria"


def test_lookup_id_wins_over_name(finder: RegionFinder) -> None:
    # "bremen-alias" has the *name* "bremen"; the id "bremen" must win.
    assert finder.lookup("bremen").id == "bremen"


def test_lookup_ambiguous_name_lists_ids(finder: RegionFinder) -> None:
    with pytest.raises(ValueError, match="georgia-asia, georgia-us"):
        finder.lookup("Georgia")


@pytest.mark.parametrize("value", ["nowhere", "everywhere"])
def test_lookup_unknown_region(finder: RegionFinder, value: str) -> None:
    with pytest.raises(ValueError, match="No Geofabrik region"):
        finder.lookup(value)


@pytest.mark.parametrize("value", ["", "   "])
def test_lookup_empty_value(finder: RegionFinder, value: str) -> None:
    with pytest.raises(ValueError, match="must not be empty"):
        finder.lookup(value)
```

- [ ] **Step 3: Run tests to verify they fail**

Run: `hatch test tests/test_region_finder.py`
Expected: FAIL (`AttributeError: ... has no attribute 'find_candidates'` / `all_regions` / `lookup`).

- [ ] **Step 4: Implement**

In `src/poiidx/regionFinder.py`, add a helper and the three methods to `RegionFinder`, and make `_findBestRegion` use the helper so a feature without a pbf url is skipped (it currently would raise `KeyError`).

Add inside the class (after `__init__`):

```python
    @staticmethod
    def _region_from_feature(feature: dict[str, Any]) -> Region | None:
        """Return the region, or None if it has no PBF download (nothing to fetch)."""
        properties = feature["properties"]
        url = properties.get("urls", {}).get("pbf")
        if url is None:
            return None
        return Region(id=properties["id"], name=properties["name"], url=url)

    def all_regions(self) -> list[Region]:
        regions = (
            self._region_from_feature(feature)
            for feature in self.geofabrik_data["features"]
        )
        return [region for region in regions if region is not None]

    def find_candidates(self, geo_data: BaseGeometry) -> list[Region]:
        """All regions intersecting the shape, smallest first (not just the used ones)."""
        candidates: list[tuple[float, Region]] = []
        for region in self.all_regions():
            entry = self._region_cache[region.id]
            if geo_data.intersects(entry["shape"]):
                candidates.append((entry["area"], region))
        candidates.sort(key=lambda candidate: candidate[0])
        return [region for _, region in candidates]

    def lookup(self, name_or_id: str) -> Region:
        key = name_or_id.strip()
        if not key:
            raise ValueError("Region name or id must not be empty.")
        regions = self.all_regions()
        for region in regions:
            if region.id == key:
                return region
        matches = [region for region in regions if region.name.casefold() == key.casefold()]
        if not matches:
            raise ValueError(f"No Geofabrik region with id or name {name_or_id!r}.")
        if len(matches) > 1:
            ids = ", ".join(sorted(region.id for region in matches))
            raise ValueError(
                f"Region name {name_or_id!r} is ambiguous; use one of these ids: {ids}."
            )
        return matches[0]
```

In `_findBestRegion`, replace the loop head and the `Region(...)` construction:

```python
        for region in self.geofabrik_data["features"]:
            candidate = self._region_from_feature(region)
            if candidate is None:
                continue  # No PBF to download

            region_id = candidate.id
```
(delete the two commented-out `iso3166` lines and the old `region_id = region["properties"]["id"]`), and

```python
            if best is None or size < best_size:
                best = candidate
                best_size = size
                return_geo_data = remaining_geo_data
```

- [ ] **Step 5: Run tests, lint, types**

Run: `hatch test tests/test_region_finder.py` → PASS.
Run: `hatch run ruff:ruff format && hatch run ruff:ruff check && hatch run types:check` → clean. (If mypy rejects the bare `list` annotation on `_ids`, change it to `list[Region]` and import `Region`; remove the ignore comment.)

- [ ] **Step 6: Commit**

```bash
git add tests/helpers.py tests/test_region_finder.py src/poiidx/regionFinder.py
git commit -m "feat: Add region candidates, listing and name/id lookup to RegionFinder"
```

---

### Task 2: import_region, buffer fix and has_region_data

**Files:**
- Modify: `src/poiidx/poiIdx.py` (`has_region_data` ~198, `init_regions_by_shape` ~260), `tests/helpers.py`, `tests/test_region_init.py`

**Interfaces:**
- Consumes: `PoiIdx.find_regions_by_shape`, `has_region_data`, `initialize_pois_for_region`, `_region_lock`.
- Produces:
  - `tests/helpers.py::run_concurrently(target: Any, count: int) -> None`
  - `PoiIdx.buffered_shape(shape: BaseGeometry, buffer: float | None) -> BaseGeometry` (staticmethod)
  - `PoiIdx.import_region(region_id: str) -> None`
  - `has_region_data` now also true for boundary-only regions.
  - `init_regions_by_shape(shape, buffer) -> list[str]` unchanged signature and result.

- [ ] **Step 1: Move the concurrency helper**

Cut `_run_concurrently` from `tests/test_region_init.py` and paste it into `tests/helpers.py` as `run_concurrently` (add `import threading` there; same body). In `test_region_init.py` add `from .helpers import run_concurrently` and replace the three `_run_concurrently(` call sites with `run_concurrently(`; drop the now unused `threading`-only helper but keep `import threading` (still used by the tests).

Run: `hatch test tests/test_region_init.py` → PASS (unchanged behaviour).

- [ ] **Step 2: Write the failing tests**

Append to `tests/test_region_init.py`:

```python
from poiidx.administrativeBoundary import AdministrativeBoundary


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
```

- [ ] **Step 3: Run tests to verify they fail**

Run: `hatch test tests/test_region_init.py`
Expected: FAIL (`AttributeError: ... 'buffered_shape'` / `'import_region'`; the boundary test fails on `has_region_data`).

- [ ] **Step 4: Implement**

In `src/poiidx/poiIdx.py`:

Replace `has_region_data`:

```python
    @classmethod
    def has_region_data(cls, region_key: str) -> bool:
        """Return true if the region has at least one POI or administrative boundary.

        Boundaries count too: a region whose filter config matches no POIs would
        otherwise look un-imported and be re-ingested on every call.
        """
        return (
            Poi.select().where(Poi.region == region_key).exists()
            or AdministrativeBoundary.select()
            .where(AdministrativeBoundary.region == region_key)
            .exists()
        )
```

Replace `init_regions_by_shape` and add the two new methods above it:

```python
    @staticmethod
    def buffered_shape(
        shape: shapely.geometry.base.BaseGeometry, buffer: float | None
    ) -> shapely.geometry.base.BaseGeometry:
        """Return the convex hull of the shape widened by `buffer` meters (WGS84 in/out)."""
        if buffer is None:
            return shape
        lp = LocalProjection(shape)
        local_shape = lp.to_local(shape)
        return lp.to_wgs(local_shape.convex_hull.buffer(buffer))

    @classmethod
    def import_region(cls, region_id: str) -> None:
        """Import a region unless it already has data. Safe to call concurrently."""
        with _region_lock(region_id):
            # Checked inside the lock: another thread may have ingested the
            # region while we were waiting for it.
            if cls.has_region_data(region_id):
                logger.debug(f"Region {region_id} already initialized")
                return
            logger.debug(f"Initializing region {region_id}")
            cls.initialize_pois_for_region(region_id)

    @classmethod
    def init_regions_by_shape(
        cls, shape: shapely.geometry.base.BaseGeometry, buffer: float | None
    ) -> list[Any]:
        """Import every region the finder selects for the shape; return their ids."""
        logger.debug("Initializing regions by shape")
        regions = cls.find_regions_by_shape(cls.buffered_shape(shape, buffer))
        for region in regions:
            cls.import_region(region.id)
        return [region.id for region in regions]
```

- [ ] **Step 5: Run the full suite, lint, types**

Run: `hatch test` → PASS (the three existing concurrency/atomicity tests must still pass unchanged).
Run: `hatch run ruff:ruff format && hatch run ruff:ruff check && hatch run types:check` → clean.

- [ ] **Step 6: Commit**

```bash
git add src/poiidx/poiIdx.py tests/helpers.py tests/test_region_init.py
git commit -m "fix: Make buffer work, treat boundary-only regions as imported, add import_region"
```

---

### Task 3: Download, region state and listing internals

**Files:**
- Modify: `src/poiidx/poiIdx.py` (`initialize_pois_for_region` ~203), imports
- Create: `tests/test_region_listing.py`

**Interfaces:**
- Consumes: `RegionFinder.all_regions/find_candidates`, `PoiIdx.buffered_shape/find_regions_by_shape/has_region_data/import_region`, `Region`.
- Produces:
  - `PoiIdx._cache_dir() -> pathlib.Path | None` (None when `pbf_cache=False`; does not create the directory)
  - `PoiIdx._require_cache_dir() -> pathlib.Path` (raises `RuntimeError`)
  - `PoiIdx.download_region_pbf(region: Region) -> None`
  - `PoiIdx.region_info(region: Region, used: bool) -> dict[str, Any]`
  - `PoiIdx.list_regions(shape: BaseGeometry | None, buffer: float | None) -> list[dict[str, Any]]`

- [ ] **Step 1: Write the failing tests**

Create `tests/test_region_listing.py`:

```python
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
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `hatch test tests/test_region_listing.py`
Expected: FAIL (`AttributeError: ... 'list_regions'` / `'download_region_pbf'`).

- [ ] **Step 3: Implement**

In `src/poiidx/poiIdx.py`, add the import `from .regionFinder import Region, RegionFinder` (extend the existing line).

Add these methods (next to `has_region_data`):

```python
    @classmethod
    def _cache_dir(cls) -> pathlib.Path | None:
        """The persistent PBF cache directory, or None when `pbf_cache=False`."""
        if not cls.__pbf_cache:  # type: ignore[attr-defined]
            return None
        return pathlib.Path(platformdirs.user_cache_dir("poiidx", "bytehexe")) / "pbf"

    @classmethod
    def _require_cache_dir(cls) -> pathlib.Path:
        cachedir = cls._cache_dir()
        if cachedir is None:
            raise RuntimeError(
                "Downloading without importing needs a persistent PBF cache: "
                "connect with pbf_cache=True."
            )
        return cachedir

    @classmethod
    def download_region_pbf(cls, region: Region) -> None:
        """Ensure the region's PBF is in the persistent cache; do not import it."""
        cachedir = cls._require_cache_dir()
        cachedir.mkdir(parents=True, exist_ok=True)
        # Same lock as import, so a download and an import never fetch the file twice.
        with _region_lock(region.id):
            Pbf(cachedir).get_pbf_filename(region.id, region.url)

    @classmethod
    def region_info(cls, region: Region, used: bool) -> dict[str, Any]:
        cachedir = cls._cache_dir()
        return {
            "id": region.id,
            "name": region.name,
            "url": region.url,
            "used": used,
            "downloaded": cachedir is not None
            and (cachedir / f"{region.id}.pbf").exists(),
            "imported": cls.has_region_data(region.id),
        }

    @classmethod
    def list_regions(
        cls,
        shape: shapely.geometry.base.BaseGeometry | None,
        buffer: float | None,
    ) -> list[dict[str, Any]]:
        """All regions, or those intersecting the shape with the used ones flagged."""
        finder = cls.get_finder()
        if shape is None:
            return [cls.region_info(r, used=False) for r in finder.all_regions()]
        shape = cls.buffered_shape(shape, buffer)
        used_ids = {region.id for region in cls.find_regions_by_shape(shape)}
        return [
            cls.region_info(region, used=region.id in used_ids)
            for region in finder.find_candidates(shape)
        ]
```

Refactor the cache selection in `initialize_pois_for_region` to use `_cache_dir` (replace the `if cls.__pbf_cache:` block):

```python
        cachedir = cls._cache_dir()
        if cachedir is not None:
            cachedir.mkdir(parents=True, exist_ok=True)
            tempfile_context: Any = nullcontext()
            logger.debug(f"Using PBF cache directory: {cachedir}")
        else:
            tempfile_context = tempfile.TemporaryDirectory()
            cachedir = pathlib.Path(tempfile_context.name)  # type: ignore[attr-defined]
            logger.debug("Using temporary PBF cache directory")
```

- [ ] **Step 4: Run the full suite, lint, types**

Run: `hatch test` → PASS (including `test_failed_ingestion_leaves_no_partial_region_data`, which sets `_PoiIdx__pbf_cache` to False).
Run: `hatch run ruff:ruff format && hatch run ruff:ruff check && hatch run types:check` → clean.

- [ ] **Step 5: Commit**

```bash
git add src/poiidx/poiIdx.py tests/test_region_listing.py
git commit -m "feat: Add region listing and PBF-only download to PoiIdx"
```

---

### Task 4: Public API functions

**Files:**
- Modify: `src/poiidx/poiIdx.py`, `src/poiidx/__init__.py`
- Create: `tests/test_region_api.py`

**Interfaces:**
- Consumes: Task 3 `_require_cache_dir`, `download_region_pbf`, `region_info`, `list_regions`; Task 2 `import_region`, `buffered_shape`; `RegionFinder.lookup`.
- Produces:
  - `PoiIdx.fetch_named(name_or_id: str, import_: bool) -> list[dict[str, Any]]`
  - `PoiIdx.fetch_at(shape: BaseGeometry, buffer: float | None, import_: bool) -> list[dict[str, Any]]`
  - `poiidx.list_regions(shape=None, buffer=None)`, `poiidx.download_region(name_or_id)`, `poiidx.import_region(name_or_id)`, `poiidx.download_regions_at(shape, buffer=None)`, `poiidx.import_regions_at(shape, buffer=None)`, all `-> list[dict[str, Any]]`.

- [ ] **Step 1: Write the failing tests**

Create `tests/test_region_api.py`:

```python
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
    result = poiidx.import_region("Bremen")

    assert _ids(result) == ["bremen"]
    assert result[0]["imported"] is True
    assert result[0]["used"] is False  # no shape, so nothing was "selected"
    poiidx.import_region("bremen")
    assert world.initialized == ["bremen"]


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
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `hatch test tests/test_region_api.py`
Expected: FAIL (`AttributeError: module 'poiidx' has no attribute 'import_region'`).

- [ ] **Step 3: Implement PoiIdx orchestration**

Add to `PoiIdx` in `src/poiidx/poiIdx.py`:

```python
    @classmethod
    def _fetch(cls, region: Region, import_: bool) -> None:
        if import_:
            cls.import_region(region.id)
        else:
            cls.download_region_pbf(region)

    @classmethod
    def fetch_named(cls, name_or_id: str, import_: bool) -> list[dict[str, Any]]:
        """Download (or import) one region looked up by id or name."""
        if not import_:
            cls._require_cache_dir()  # fail before touching the network
        region = cls.get_finder().lookup(name_or_id)
        cls._fetch(region, import_)
        return [cls.region_info(region, used=False)]

    @classmethod
    def fetch_at(
        cls,
        shape: shapely.geometry.base.BaseGeometry,
        buffer: float | None,
        import_: bool,
    ) -> list[dict[str, Any]]:
        """Download (or import) every region the finder selects for the shape."""
        if not import_:
            cls._require_cache_dir()
        regions = cls.find_regions_by_shape(cls.buffered_shape(shape, buffer))
        for region in regions:
            cls._fetch(region, import_)
        return [cls.region_info(region, used=True) for region in regions]
```

- [ ] **Step 4: Implement the public functions**

Append to `src/poiidx/__init__.py`:

```python
def list_regions(
    shape: shapely.geometry.base.BaseGeometry | None = None,
    buffer: float | None = None,
) -> list[dict[str, Any]]:
    assert_initialized()
    return PoiIdx.list_regions(shape, buffer)


def download_region(name_or_id: str) -> list[dict[str, Any]]:
    assert_initialized()
    return PoiIdx.fetch_named(name_or_id, import_=False)


def import_region(name_or_id: str) -> list[dict[str, Any]]:
    assert_initialized()
    return PoiIdx.fetch_named(name_or_id, import_=True)


def download_regions_at(
    shape: shapely.geometry.base.BaseGeometry, buffer: float | None = None
) -> list[dict[str, Any]]:
    assert_initialized()
    return PoiIdx.fetch_at(shape, buffer, import_=False)


def import_regions_at(
    shape: shapely.geometry.base.BaseGeometry, buffer: float | None = None
) -> list[dict[str, Any]]:
    assert_initialized()
    return PoiIdx.fetch_at(shape, buffer, import_=True)
```

- [ ] **Step 5: Run the full suite, lint, types**

Run: `hatch test` → PASS.
Run: `hatch run ruff:ruff format && hatch run ruff:ruff check && hatch run types:check` → clean.

- [ ] **Step 6: Commit**

```bash
git add src/poiidx/poiIdx.py src/poiidx/__init__.py tests/test_region_api.py
git commit -m "feat: Add public region listing, download and import functions"
```

---

### Task 5: Documentation

**Files:**
- Modify: `docs/reference.md` (insert before `#### \`recreate_schema()\``, ~line 215), `docs/how-to-guides.md` (new section before `## Troubleshooting`), `docs/explanation.md` (RegionFinder entry ~117), `CLAUDE.md` (Lazy region ingestion section, ~66-72)

**Interfaces:** documents Task 4's functions; no code.

- [ ] **Step 1: Reference**

Insert in `docs/reference.md` before `#### \`recreate_schema()\``:

````markdown
#### `list_regions()`

List Geofabrik regions, optionally those containing a shape.

```python
poiidx.list_regions(
    shape: shapely.geometry.base.BaseGeometry | None = None,
    buffer: float | None = None,
) -> list[dict[str, Any]]
```

Without `shape`, every region that has a PBF download is returned, all with `used=False`. With `shape`, every region intersecting the (optionally buffered) shape is returned, smallest first. `used` is `True` for the regions poiidx would actually load for that shape. A large region such as DACH can contain a point and still be unused, because a smaller region already covers it.

**Returns:** list of dicts with keys `id` (str), `name` (str), `url` (str), `used` (bool), `downloaded` (bool, PBF is in the persistent cache; always `False` with `pbf_cache=False`), `imported` (bool, the region has POIs or administrative boundaries in the database). All five region functions return this shape.

```python
from shapely.geometry import Point

for region in poiidx.list_regions(Point(8.8, 53.1)):
    print(region["id"], region["used"])
# bremen True
# germany False
```

---

#### `download_region()` / `import_region()`

Fetch one region by Geofabrik id or name.

```python
poiidx.download_region(name_or_id: str) -> list[dict[str, Any]]
poiidx.import_region(name_or_id: str) -> list[dict[str, Any]]
```

`download_region` stores the PBF in the cache without importing it. `import_region` downloads if necessary and imports; importing an already imported region does nothing.

An exact id match wins; otherwise names are matched case-insensitively. **Raises** `ValueError` for an empty value, an unknown value, or a name shared by several regions (the message lists the ids). `download_region` **raises** `RuntimeError` when connected with `pbf_cache=False`. The result has one element, with `used=False`.

---

#### `download_regions_at()` / `import_regions_at()`

Fetch the regions poiidx selects for a shape, exactly as `get_nearest_pois()` would.

```python
poiidx.download_regions_at(shape: BaseGeometry, buffer: float | None = None) -> list[dict[str, Any]]
poiidx.import_regions_at(shape: BaseGeometry, buffer: float | None = None) -> list[dict[str, Any]]
```

Returns the selected regions (`used=True`); an empty list if the shape lies outside every region. `download_regions_at` **raises** `RuntimeError` when connected with `pbf_cache=False`.

---

````

- [ ] **Step 2: How-to**

Insert in `docs/how-to-guides.md` before `## Troubleshooting`:

````markdown
## Managing Regions

### How to Pre-fetch a Region

Queries download regions lazily, which can block for minutes the first time. Fetch ahead of time instead:

```python
import poiidx
from shapely.geometry import Point

poiidx.import_region("Bremen")                      # by name or Geofabrik id
poiidx.import_regions_at(Point(8.8, 53.1))          # whatever poiidx would use here
poiidx.download_region("germany")                   # PBF only, import later
```

`download_*` needs the default `pbf_cache=True`.

### How to See Which Regions Cover a Point

```python
for region in poiidx.list_regions(Point(8.8, 53.1)):
    print(region["id"], "used" if region["used"] else "unused",
          "imported" if region["imported"] else "not imported")
```

````

- [ ] **Step 3: Explanation and CLAUDE.md**

In `docs/explanation.md`, replace the **RegionFinder** entry (the R-tree wording is inaccurate; the finder is a greedy cover over the Geofabrik polygons) with:

```markdown
**RegionFinder**
: Chooses which Geofabrik regions to load for a shape. It greedily takes the smallest region that covers part of the remaining shape until nothing is left, so a large region such as DACH can contain a point yet never be used. `list_regions()` shows both the candidates and the ones actually used.
```

In `CLAUDE.md`, change step 2 of "Lazy region ingestion" from "For each region without POIs in the DB (`has_region_data`)" to "For each region without POIs or administrative boundaries in the DB (`has_region_data`)", and add after the numbered list:

```markdown
The same path is exposed explicitly: `list_regions`, `download_region`, `import_region`,
`download_regions_at`, `import_regions_at` (`__init__.py`) delegate to `PoiIdx.list_regions`,
`fetch_named` and `fetch_at`, which reuse `find_regions_by_shape`, `import_region` and the
per-region locks. `download_*` require `pbf_cache=True`.
```

Note: `CLAUDE.md` is currently untracked, so do not stage it unless the user has added it.

- [ ] **Step 4: Verify and commit**

Run: `hatch test` → PASS; `hatch run ruff:ruff check` → clean.

```bash
git add docs/reference.md docs/how-to-guides.md docs/explanation.md
git commit -m "docs: Document region listing, download and import functions"
```

---

## Self-Review

- **Spec coverage:** five functions and result dict (Tasks 3-4); lookup rules and `ValueError`s (Task 1); `RuntimeError` for download without cache, before lookup (Tasks 3-4); `RegionFinder.find_candidates` + shared selection so `used` cannot drift (Tasks 1, 3); shared buffer helper (Task 2); shared cache-dir helper (Task 3); `import_region` under lock with re-check, `init_regions_by_shape` rebuilt on it (Task 2); `has_region_data` change (Task 2); docs (Task 5); out-of-scope items untouched.
- **Additions beyond the spec** (flag to the user): the `buffer` bug fix (`convex_hull()` raised `TypeError` on every `buffer=` call; no existing test covered it), and skipping Geofabrik features that lack a `urls.pbf` entry, which also changes `find_regions` (previously a `KeyError` if such a region was picked).
- **Types:** `Region`, `region_info`, `fetch_named/fetch_at`, `import_region(region_id: str)`, `download_region_pbf(region: Region)` are used consistently across tasks.
