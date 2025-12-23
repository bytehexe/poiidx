# API Reference

Complete API reference for poiidx.

## Module: `poiidx`

The main module provides high-level functions for working with POIs and administrative boundaries.

### Functions

#### `init()`

Initialize the database connection and optionally create/update the schema.

```python
poiidx.init(
    filter_config: list[dict[str, Any]],
    recreate: bool = False,
    **kwargs: Any
) -> None
```

!!! warning "Automatic Data Recreation"
    Even when `recreate=False`, poiidx will **automatically drop and recreate all data** if it detects:
    
    - Schema changes (from library updates)
    - Filter configuration changes
    - Missing or corrupted schema metadata
    
    All data should be considered temporary.

**Parameters:**

- `filter_config` (list[dict]): List of POI filter configurations. Each dict should contain:
    - `symbol` (str): Symbol category for grouping POIs
    - `description` (str): Human-readable description
    - `filters` (list[dict]): List of OSM tag combinations to match
- `recreate` (bool, optional): If True, forces immediate drop and recreation of the database schema. Default: False (but automatic recreation may still occur based on schema detection)
- `**kwargs`: Database connection parameters:
    - `host` (str): Database host
    - `database` (str): Database name
    - `user` (str): Database user
    - `password` (str): Database password
    - `port` (int): Database port (default: 5432)
    - `pbf_cache` (bool): Enable PBF file caching (default: True)

**Returns:** None

**Raises:**
- `RuntimeError`: If initialization fails

**Example:**

```python
import yaml
import poiidx

with open('filters.yaml') as f:
    filters = yaml.safe_load(f)

poiidx.init(
    filter_config=filters,
    host='localhost',
    database='poiidx_db',
    user='poiidx_user',
    password='secret',
    port=5432
)
```

---

#### `close()`

Close the database connection.

```python
poiidx.close() -> None
```

**Parameters:** None

**Returns:** None

**Example:**

```python
poiidx.close()
```

---

#### `get_nearest_pois()`

Find nearest Points of Interest to a given geometry.

```python
poiidx.get_nearest_pois(
    shape: shapely.geometry.base.BaseGeometry,
    buffer: float | None = None,
    **kwargs: Any
) -> list[dict[str, Any]]
```

**Parameters:**

- `shape` (BaseGeometry): Shapely geometry to search from (Point, Polygon, etc.)
- `buffer` (float, optional): Buffer distance in meters for region initialization
- `**kwargs`: Additional parameters passed to `PoiIdx.get_nearest_pois()`:
    - `max_distance` (float, optional): Maximum distance in meters
    - `limit` (int): Number of results to return (default: 1)
    - `regions` (list[str], optional): Filter by specific regions
    - `rank_range` (tuple[int, int], optional): Filter by rank range (min, max)

**Returns:** list[dict] - List of POI dictionaries with keys:
- `osm_id` (str): OpenStreetMap ID
- `name` (str): POI name
- `region` (str): Region identifier
- `coordinates` (str): Geometry as WKT string
- `filter_item` (str): OSM tag key
- `filter_expression` (str): OSM tag value
- `rank` (int): Priority rank
- `symbol` (str): Symbol category

**Example:**

```python
from shapely.geometry import Point

berlin = Point(13.4050, 52.5200)
pois = poiidx.get_nearest_pois(
    berlin,
    max_distance=1000,
    limit=5
)
```

---

#### `get_administrative_hierarchy()`

Get administrative boundaries containing the given geometry.

```python
poiidx.get_administrative_hierarchy(
    shape: shapely.geometry.base.BaseGeometry,
    buffer: float | None = None
) -> list[dict[str, Any]]
```

**Parameters:**

- `shape` (BaseGeometry): Shapely geometry to query
- `buffer` (float, optional): Buffer distance in meters for region initialization

**Returns:** list[dict] - List of administrative boundary dictionaries with keys:
- `osm_id` (str): OpenStreetMap ID
- `name` (str): Boundary name
- `admin_level` (int): Administrative level (2=country, 4=state, 6=county, etc.)
- `geometry` (str): Boundary geometry as WKT string
- `tags` (dict): Additional OSM tags (may include localized names)

**Example:**

```python
from shapely.geometry import Point

location = Point(13.4050, 52.5200)
hierarchy = poiidx.get_administrative_hierarchy(location)

for admin in hierarchy:
    print(f"Level {admin['admin_level']}: {admin['name']}")
```

---

#### `get_administrative_hierarchy_string()`

Get administrative hierarchy as a formatted string.

```python
poiidx.get_administrative_hierarchy_string(
    shape: shapely.geometry.base.BaseGeometry,
    lang: str | None = None,
    buffer: float | None = None
) -> str
```

**Parameters:**

- `shape` (BaseGeometry): Shapely geometry to query
- `lang` (str, optional): ISO 639-1 language code for localized names (e.g., 'de', 'fr', 'es')
- `buffer` (float, optional): Buffer distance in meters for region initialization

**Returns:** str - Formatted administrative hierarchy string (comma-separated)

**Example:**

```python
from shapely.geometry import Point

location = Point(13.4050, 52.5200)

# Default (English)
hierarchy = poiidx.get_administrative_hierarchy_string(location)
# "Berlin, Germany"

# French
hierarchy_fr = poiidx.get_administrative_hierarchy_string(location, lang='fr')
# "Berlin, Allemagne"
```

---

#### `recreate_schema()`

Drop and recreate the database schema.

```python
poiidx.recreate_schema() -> None
```

**Parameters:** None

**Returns:** None

**Warning:** This will delete all data!

---

#### `drop_schema()`

Drop all database tables.

```python
poiidx.drop_schema() -> None
```

**Parameters:** None

**Returns:** None

**Warning:** This will delete all data!

---

## Class: `PoiIdx`

Low-level class providing direct database access. Most users should use the module-level functions instead.

Located in: `poiidx.poiIdx`

### Class Methods

#### `PoiIdx.connect()`

Initialize database connection.

```python
PoiIdx.connect(
    pbf_cache: bool = True,
    **kwargs: Any
) -> None
```

#### `PoiIdx.get_nearest_pois()`

Low-level POI search with additional options.

```python
PoiIdx.get_nearest_pois(
    shape: shapely.geometry.base.BaseGeometry,
    max_distance: float | None = None,
    limit: int = 1,
    regions: list[str] | None = None,
    rank_range: tuple[int, int] | None = None
) -> list[Poi]
```

**Returns:** list[Poi] - List of Poi model instances (not dictionaries)

#### `PoiIdx.get_administrative_hierarchy()`

Low-level administrative boundary query.

```python
PoiIdx.get_administrative_hierarchy(
    shape: shapely.geometry.base.BaseGeometry
) -> list[AdministrativeBoundary]
```

**Returns:** list[AdministrativeBoundary] - List of model instances

---

## Models

Database models using Peewee ORM.

### `Poi`

Point of Interest model.

Located in: `poiidx.poi`

**Fields:**

- `osm_id` (CharField): OpenStreetMap identifier
- `name` (CharField): POI name
- `region` (CharField): Region identifier (e.g., 'europe/germany')
- `coordinates` (GeometryField): PostGIS point geometry (SRID 4326)
- `filter_item` (CharField): OSM tag key
- `filter_expression` (CharField): OSM tag value
- `rank` (IntegerField): Priority rank
- `symbol` (CharField): Symbol category

**Indexes:**

- SPGIST index on `coordinates` for efficient spatial queries
- Index on `region`
- Index on `rank`

---

### `AdministrativeBoundary`

Administrative boundary model.

Located in: `poiidx.administrativeBoundary`

**Fields:**

- `osm_id` (CharField): OpenStreetMap identifier
- `name` (CharField): Boundary name
- `admin_level` (IntegerField): Administrative level
- `geometry` (GeometryField): PostGIS geometry (SRID 4326)
- `tags` (JSONField): Additional OSM tags (including localized names)

**Indexes:**

- GIST index on `geometry`
- Index on `admin_level`

---

### `Country`

Country information model.

Located in: `poiidx.country`

**Fields:**

- `id` (CharField, primary key): Country code
- `name` (CharField): Country name
- `geometry` (GeometryField): Country boundary geometry

---

### `System`

System configuration model.

Located in: `poiidx.system`

**Fields:**

- `system` (BooleanField, primary key): System flag
- `filter_config` (TextField): JSON-encoded filter configuration
- `region_index` (TextField): Serialized region index

---

## Geometry Types

poiidx uses Shapely for geometry handling. All geometries use WGS84 (SRID 4326).

### Supported Geometry Types

- **Point**: Single location
  ```python
  from shapely.geometry import Point
  point = Point(longitude, latitude)
  ```

- **Polygon**: Area boundary
  ```python
  from shapely.geometry import Polygon
  polygon = Polygon([(lon1, lat1), (lon2, lat2), ...])
  ```

- **LineString**: Path or route
  ```python
  from shapely.geometry import LineString
  line = LineString([(lon1, lat1), (lon2, lat2), ...])
  ```

- **MultiPoint**, **MultiPolygon**, **MultiLineString**: Collections

---

## Filter Configuration Schema

POI filters are defined in YAML or as Python dictionaries:

```yaml
- symbol: string                # Symbol category for grouping
  description: string           # Human-readable description
  filters:                      # List of tag combinations
    - tag_key: tag_value        # All tags in a dict must match (AND)
      tag_key2: tag_value2
    - tag_key3: tag_value3      # Different dicts are alternatives (OR)
```

**Example:**

```yaml
- symbol: food
  description: Food and dining establishments
  filters:
    - amenity: restaurant
    - amenity: cafe
    - amenity: bar

- symbol: tourism
  description: Tourist attractions and hotels
  filters:
    - tourism: hotel
    - museum
    - attraction
  symbol: tourism
  rank: 2
```

---

## Distance Units

All distance parameters use **meters** as the unit:

- `max_distance`: meters
- `buffer`: meters

Distance calculations use PostGIS geography functions for accurate results on the Earth's surface.

---

## Region Identifiers

Regions follow the Geofabrik naming scheme:

- `europe/germany` - Germany
- `north-america/us` - United States
- `asia/japan` - Japan
- etc.

Data is downloaded from Geofabrik on-demand when a region is first queried.

---

## Error Handling

### Common Exceptions

**RuntimeError: "PoiIdx not initialized"**
: Call `poiidx.init()` before using other functions

**Database connection errors**
: Check PostgreSQL is running and credentials are correct

**PostGIS not found**
: Install PostGIS extension: `CREATE EXTENSION postgis;`

### Best Practices

Always use try-finally or context managers:

```python
import poiidx

try:
    poiidx.init(...)
    # Your queries
finally:
    poiidx.close()
```

---

## Type Hints

poiidx includes comprehensive type hints. Use with mypy:

```bash
mypy your_script.py
```

Common type imports:

```python
from typing import Any
from shapely.geometry.base import BaseGeometry
from poiidx.poi import Poi
from poiidx.administrativeBoundary import AdministrativeBoundary
```
