# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Commands

Everything goes through `hatch` — never call `pip`, `pytest`, `ruff`, or `mypy` directly
(`.github/copilot-instructions.md`).

```bash
hatch test                          # unit tests
hatch test tests/test_poi.py        # single file
hatch test -- -k test_poi_insert    # single test
hatch run types:check               # mypy over src/poiidx and tests
hatch run ruff:ruff check           # lint  (detached, skip-install env)
hatch run ruff:ruff format          # format
hatch run example -- --password-file poiidx_user.pwd   # end-to-end demo script
hatch run poiidx-cli admin 52.52 13.405 --short        # CLI entry point
```

`hatch test` requires a working **Docker daemon**: `tests/test_poi.py` starts a
`postgis/postgis:latest` container via `testcontainers` (module-scoped fixture). There is no
mock database — tests that touch models need the container.

`hatch run example` and the CLI need a **real local PostgreSQL+PostGIS** (`poiidx_db` /
`poiidx_user`, see README) and will download OSM data on first run.

Commits are gated by `.pre-commit-config.yaml` (yamllint → ruff check → ruff format → mypy →
tests) and commit messages must follow **conventional commits** (gitlint, `contrib-title-conventional-commits`).

`hatch run mkdocs:serve` is declared in `pyproject.toml` but the `mkdocs` env has no
`mkdocs`/`mkdocs-material` dependency — it will fail until those are added. CI installs them
manually (`.github/workflows/publish-docs.yml`).

## Architecture

Three layers, worth reading in this order:

1. **`__init__.py`** — the public API (`init`, `get_nearest_pois`,
   `get_administrative_hierarchy`, `get_administrative_hierarchy_string`, `close`). Thin: it
   asserts initialization, delegates to `PoiIdx`, and converts peewee models to dicts via
   `model_to_dict`.
2. **`poiIdx.py` / `PoiIdx`** — a classmethod-only orchestrator (no instances). Owns the
   connection, the schema lifecycle, region resolution, and the actual queries.
3. **Models + scanner** — peewee models (`poi.py`, `administrativeBoundary.py`, `country.py`,
   `system.py`, `schemaHash.py`) sharing the uninitialized `PostgresqlDatabase(None)` in
   `baseModel.py`, populated by `scanner.py` from OSM PBF files.

### The database is a regenerable cache, not storage

`PoiIdx.init_if_new()` hashes the CREATE TABLE + index SQL of every model
(`get_schema_hash`) and compares it against the `SchemaHash` row, and compares the stored
`System.filter_config` JSON against the caller's. **Any mismatch drops and recreates all
tables**, discarding every downloaded region.

Consequence: adding a field, an index, or changing a model's `Meta` silently wipes user data
on their next `init()`. Same for any change to the filter config. There are no migrations by
design; treat the DB as a cache.

`System` and `SchemaHash` are singleton tables — one row pinned by a `unique` boolean column
(`system=True` / `instance=True`). `System` holds the Geofabrik region index
(`index-v1.json`, downloaded by `geofabrik.py`) and the filter config, both as JSON text.

### Lazy region ingestion — queries can trigger downloads

Every public query calls `PoiIdx.init_regions_by_shape(shape, buffer)` *before* querying:

1. `RegionFinder` (`regionFinder.py`) greedily picks the smallest-area Geofabrik regions
   covering the shape, subtracting each match from the remaining geometry until it is empty.
2. For each region without POIs or administrative boundaries in the DB (`has_region_data`), `initialize_pois_for_region`
   downloads the region's `.pbf` (cached under `platformdirs.user_cache_dir("poiidx","bytehexe")/pbf`
   unless `connect(pbf_cache=False)`) and runs `poi_scan` → `administrative_scan` →
   `process_admin_centre_relations`.

So a "read-only" call to `get_nearest_pois` may block for minutes on network + parsing the
first time a region is touched. Keep that in mind when adding query paths or tests.

The same path is exposed explicitly: `list_regions`, `download_region`, `import_region`,
`download_regions_at`, `import_regions_at` (`__init__.py`) delegate to `PoiIdx.list_regions`,
`fetch_named` and `fetch_at`, which reuse `find_regions_by_shape`, `import_region` and the
per-region locks. `download_*` require `pbf_cache=True`.

### PostGIS access through peewee

Peewee has no spatial support; `ext.py` supplies it:

- `GeographyField` / `GeometryField` — `db_value` wraps shapely geometries in
  `SQL("ST_GeogFromText(%s)", …)`, `python_value` parses the hex WKB back to shapely.
- `knn(lhs, rhs)` builds the PostGIS `<->` operator so `order_by` uses the KNN index.
- Predicates that have no peewee equivalent (`ST_DWithin`, `ST_Covers`) are written as raw
  `SQL(...)` inside `.where()`.

Index types are chosen per geometry kind: `Poi.coordinates` uses **SPGIST** (points),
`AdministrativeBoundary.coordinates` uses **GIST** (polygons).

### OSM parsing details

- `encode_osm_id` (`scanner.py`) normalizes IDs to prefixed strings (`n123`, `w456`, `r789`).
  This is required because osmium's `.with_areas()` rewrites way/relation IDs as
  `way_id*2` / `relation_id*2+1`; the function decodes them back.
- POI matching: `filter_config` is a list of `{symbol, description, filters}` items; within a
  `filters` entry all tags must match (AND), entries are alternatives (OR). A value of `True`
  means "key present, any value". Default config: `src/poiidx/poi_filter_config.yaml`.
- `osm.py` assigns a nominatim-style `rank` (MIN 13 … MAX 23) from a POI's bounding radius or
  its `place` tag. Lower rank = more important.
- `process_admin_centre_relations` back-fills `Poi.admin_level` from boundary relations whose
  member role is `admin_centre`, so a city node inherits the level of the boundary it centers.
- `countryQuery.py` is the fallback when the admin hierarchy has no `admin_level == 2`: it
  resolves the country via Wikidata (P17 → P1705/labels) using a module-global rate limiter
  with `Retry-After` handling, and caches the result in the `Country` table.

## Conventions

- Module filenames are **lowerCamelCase** (`poiIdx.py`, `regionFinder.py`, `baseModel.py`,
  `administrativeBoundary.py`) — unusual for Python but consistent; follow it.
- Ruff enforces `ANN` (full type annotations, including `-> None`) and `COM`; `E501` is off.
- Version is VCS-derived; `src/poiidx/__about__.py` is generated and gitignored — never edit it.
- The docs under `docs/` are Diátaxis-shaped (tutorial / how-to / reference / explanation) and
  are hand-written, not generated. Public API changes need matching edits in
  `docs/reference.md` and usually `docs/explanation.md`.
