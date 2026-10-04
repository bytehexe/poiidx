# Region management API

Date: 2026-10-04

## Goal

Let callers (1) download or import a Geofabrik region explicitly, by name/id or by
point/shape, and (2) inspect which regions contain a point or shape and which of them
poiidx would actually use.

Today the region selection is internal: `PoiIdx.init_regions_by_shape` runs
`RegionFinder.find_regions`, then downloads and ingests whatever it picked, and the only
way to trigger this is a query function. Nothing public exposes `Region` or the
selection.

## Background

- `RegionFinder.find_regions(shape)` greedily picks the smallest-area Geofabrik region
  that removes part of the remaining shape, repeating until the shape is empty. A large
  region that contains the shape (e.g. DACH for a point in Germany) is a candidate but is
  never picked when a smaller region already covers it.
- `Pbf.get_pbf_filename` downloads into the cache dir (atomic, checksum-verified) or
  returns the cached file. The cache dir is only persistent when `connect(pbf_cache=True)`
  (default); otherwise a `TemporaryDirectory` is used and deleted after ingestion.
- Ingestion is guarded by per-region locks (`_region_lock`) and an all-or-nothing
  `database.atomic()` block.
- "Imported" is currently `has_region_data`: any `Poi` row with that region key.

## Public API (`poiidx/__init__.py`)

All functions call `assert_initialized()` and return `list[dict[str, Any]]`, matching
the existing functions. `shape` is a shapely geometry; `buffer` has the same meaning as
in `get_nearest_pois`.

| Function | Behaviour |
|---|---|
| `list_regions(shape=None, buffer=None)` | No shape: all Geofabrik regions. With shape: every region intersecting the (buffered) shape. |
| `download_region(name_or_id)` | Ensure the PBF is in the cache. No import. |
| `import_region(name_or_id)` | Download if needed, then import. |
| `download_regions_at(shape, buffer=None)` | `download_region` for each region the finder selects. Returns them. |
| `import_regions_at(shape, buffer=None)` | `import_region` for each region the finder selects. Returns them. |

Import implies download. Importing an already imported region is a no-op.

### Result dict

| Key | Meaning |
|---|---|
| `id` | Geofabrik region id |
| `name` | Region name |
| `url` | PBF URL |
| `used` | The finder selects this region for the given shape. Always `False` when `list_regions` has no shape. |
| `downloaded` | The PBF is present in the persistent cache. Always `False` when `pbf_cache=False`. |
| `imported` | The region has data in the database. |

The `*_at` functions only return selected regions, so `used` is `True` there.

### Name or id lookup

1. Exact id match wins.
2. Otherwise a case-insensitive name match.
3. If a name matches several regions, raise `ValueError` listing the candidate ids.
4. If nothing matches, raise `ValueError`.

### Errors

- `download_region` / `download_regions_at` raise `RuntimeError` when `pbf_cache=False`,
  since the file would be deleted immediately. `import_*` work in both modes.
- Network failures propagate from `Pbf` unchanged.

## Internals

- `PoiIdx.find_regions_by_shape` stays the single selection implementation. Add
  `RegionFinder.find_candidates(shape)` returning all intersecting regions; `list_regions`
  marks the subset returned by `find_regions` as `used`, so the flag cannot drift from
  what ingestion does.
- Move the buffer logic (`LocalProjection` convex hull + buffer) out of
  `init_regions_by_shape` into a shared helper so all shape-based entry points apply it
  identically.
- Extract the cache-dir selection from `initialize_pois_for_region` into one helper used
  by both download and import.
- Add `PoiIdx.download_region_pbf(region_id)` and `PoiIdx.import_region(region_id)`;
  the latter takes `_region_lock(region_id)` and re-checks `has_region_data` inside it,
  exactly like `init_regions_by_shape`. `init_regions_by_shape` is rewritten on top of it.
- `downloaded` is `(cachedir / f"{id}.pbf").exists()`.
- `has_region_data` becomes "region has Poi rows **or** AdministrativeBoundary rows".
  Today a region with no POIs matching the filter config is re-ingested on every call,
  and would be reported as not imported. No schema change (that would wipe user data,
  see CLAUDE.md). A region with neither POIs nor boundaries still counts as not imported.
- `Region` (NamedTuple) stays internal; the public surface is the dicts above.

## Testing

- No Docker: `RegionFinder` candidates vs. selected, using a small hand-made GeoJSON index
  with nested regions (a DACH-like parent containing two children); name/id lookup
  including ambiguity and unknown names.
- Existing testcontainers module: `import_region` / `import_regions_at` with the PBF fetch
  stubbed (idempotence, `imported` flag, the zero-POI-with-boundaries case), and
  `download_*` raising under `pbf_cache=False`.
- Concurrency: two threads calling `import_region` for the same id ingest once.

## Docs

- `docs/reference.md`: the five functions and the result dict.
- `docs/how-to-guides.md`: "Pre-fetch a region" (by name, by point).
- `docs/explanation.md`: candidate vs. used regions, with the DACH example, and the
  change to what counts as imported.

## Out of scope

- Deleting or evicting regions or cached PBFs.
- Reporting why a region was not used.
- Refreshing a region's data or the Geofabrik index.
- CLI commands (can follow once the API exists).
