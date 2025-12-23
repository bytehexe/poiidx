# Explanation

Deep dives into the concepts and architecture of poiidx.

## Overview

poiidx is designed to solve a common problem in location-based applications: efficiently finding nearby Points of Interest (POIs) from OpenStreetMap data. This section explains the design decisions, architecture, and concepts behind poiidx.

!!! warning "Automatic Schema Management and Data Lifecycle"
    poiidx implements automatic schema detection to ensure data integrity. On each initialization (`init()` or `init_if_new()`), poiidx:
    
    1. Computes a hash of the current database schema (from model definitions)
    2. Compares it with the stored schema hash in the database
    3. Checks if the filter configuration has changed
    4. **Automatically drops and recreates all tables** if any mismatch is detected
    
    This means:
    
    - Data in the database should be considered **temporary and regeneratable**
    - Updating the poiidx library may trigger a full data recreation
    - Changing your filter configuration will trigger a full data recreation
    - You cannot add custom tables to the poiidx database reliably
    - The database acts as a managed cache, not persistent storage
    
    This design ensures that the data structure always matches the code, preventing schema migration issues and data corruption.

## Architecture

### System Components

```
┌─────────────────────────────────────────────────────────┐
│               Your Python Application                   │
└───────────────────────────┬─────────────────────────────┘
                            │
                            ▼
                ┌───────────────────────┐
                │   poiidx.__init__     │  High-level API
                │  (init, get_nearest   │  functions
                │   get_admin_hierarchy)│
                └───────────┬───────────┘
                            │
                            ▼
         ┌──────────────────────────────────────┐
         │          PoiIdx (Manager)            │
         │  ┌────────────────────────────────┐  │
         │  │ • connect/init_if_new          │  │
         │  │ • init_regions_by_shape        │  │
         │  │ • get_nearest_pois             │  │
         │  │ • get_administrative_hierarchy │  │
         │  └────────────────────────────────┘  │
         └──┬───────────┬───────────┬───────────┘
            │           │           │
            ▼           ▼           ▼
    ┌──────────┐  ┌──────────┐  ┌──────────────┐
    │ Region   │  │   PBF    │  │   Scanner    │
    │ Finder   │  │ Handler  │  │ (poi_scan,   │
    │          │  │          │  │  admin_scan) │
    └──────────┘  └────┬─────┘  └──────┬───────┘
                       │                │
                       ▼                │
              ┌─────────────────┐       │
              │   Geofabrik     │       │
              │   Downloader    │       │
              └────────┬────────┘       │
                       │                │
                       ▼                ▼
              ┌──────────────────────────────┐
              │    Local PBF Cache           │
              │  (~/.cache/poiidx/)          │
              └──────────────────────────────┘
                       │
                       │ Read PBF files
                       ▼
              ┌──────────────────────────────┐
              │     Osmium Library           │
              │ (OSM data parsing)           │
              └──────────────┬───────────────┘
                             │
                             ▼
         ┌──────────────────────────────────────┐
         │      Peewee ORM Models               │
         │  ┌────────┐ ┌────────┐ ┌──────────┐  │
         │  │  Poi   │ │ Admin  │ │ Country  │  │
         │  │        │ │Boundary│ │          │  │
         │  └────────┘ └────────┘ └──────────┘  │
         │  ┌────────────┐                      │
         │  │   System   │                      │
         │  └────────────┘                      │
         └──────────────┬───────────────────────┘
                        │
                        ▼
         ┌──────────────────────────────────────┐
         │      PostgreSQL + PostGIS            │
         │  ┌────────────────────────────────┐  │
         │  │  Tables with Spatial Indexes   │  │
         │  │  • poi (SPGIST on coordinates) │  │
         │  │  • administrativeboundary      │  │
         │  │    (GIST on coordinates)       │  │
         │  │  • country (GIST on geometry)  │  │
         │  │  • system (region index +      │  │
         │  │    filter config)              │  │
         │  └────────────────────────────────┘  │
         └──────────────────────────────────────┘

Data Source: OpenStreetMap via Geofabrik (downloads on-demand)
```

### Key Components

**poiidx module (`__init__.py`)**
: High-level API that wraps PoiIdx functionality and converts results to dictionaries for easy use.

**PoiIdx Manager**
: Central orchestrator that manages database connections, schema initialization, region detection, and coordinates data loading.

**RegionFinder**
: Spatial index for efficiently determining which Geofabrik regions contain a query point. Uses a serialized R-tree stored in the System table.

**Geofabrik Downloader**
: Downloads regional OSM extracts (PBF files) from Geofabrik's servers on-demand when a region is first queried.

**PBF Handler**
: Manages local cache of PBF files in `~/.cache/poiidx/` and provides access for scanning.

**Scanner (poi_scan, administrative_scan)**
: Uses Osmium library to parse PBF files, filter OSM data based on tags, and extract POIs and administrative boundaries.

**Peewee ORM Models**
: Database models representing POIs, administrative boundaries, countries, and system configuration.

**System Table**
: Stores filter configuration and a serialized region index (R-tree) for fast region lookups.

## Spatial Indexing

### Why Spatial Indexes Matter

Finding the nearest POI to a location is computationally expensive without proper indexing. A naive approach would calculate the distance from the query point to every POI in the database - for millions of POIs, this is impractical.

### PostGIS Spatial Indexes

poiidx uses PostGIS spatial indexes (specifically SPGIST for points) to achieve efficient queries:

**SPGIST (Space-Partitioned GIST)**
: Optimized for point geometries. Creates a spatial tree structure that allows the database to quickly eliminate irrelevant POIs without calculating distances.

**GIST (Generalized Search Tree)**
: Used for administrative boundaries which are polygons. Supports efficient containment queries.

### KNN Index Operator

The KNN (K-Nearest Neighbor) operator (`<->`) in PostGIS allows finding the K nearest geometries efficiently:

```sql
SELECT * FROM poi
ORDER BY coordinates <-> ST_GeogFromText('POINT(13.4050 52.5200)')
LIMIT 5;
```

This query runs in logarithmic time O(log n) instead of linear time O(n) thanks to the spatial index.

## Distance Filtering

### Geography vs Geometry

poiidx uses PostGIS **geography** types for distance calculations:

**Geography**
: Treats coordinates as points on an ellipsoid (Earth). Distance calculations account for the Earth's curvature, providing accurate results in meters.

**Geometry**
: Treats coordinates as points in a flat Cartesian plane. Faster but less accurate for distance calculations.

### The `max_distance` Parameter

When you specify `max_distance`, poiidx uses PostGIS's `ST_DWithin` function:

```sql
ST_DWithin(coordinates, query_point, max_distance)
```

This function:
1. Uses the spatial index to find candidates
2. Calculates exact geodesic distances
3. Returns only POIs within the threshold

The combination with KNN ordering ensures you get the nearest POIs that are also within your distance limit.

## Region Management

### Why Regions?

OpenStreetMap data is massive (hundreds of GB globally). poiidx uses a regional approach:

1. **On-demand download**: Only downloads data for regions you query
2. **Efficient storage**: Stores only the filtered POIs you care about
3. **Fast initialization**: Subsequent queries to the same region are instant

### Region Hierarchy

Regions follow the Geofabrik structure:

```
world
├── africa
│   ├── egypt
│   └── south-africa
├── europe
│   ├── germany
│   │   ├── berlin
│   │   └── bavaria
│   └── france
└── north-america
    └── us
        ├── california
        └── new-york
```

### Region Initialization Process

When you query a location:

1. **Region Detection**: Determine which region(s) contain the query point
2. **Download Check**: If region data isn't in the database, download it
3. **PBF Processing**: Extract POIs matching your filters from the PBF file
4. **Database Insert**: Store POIs with spatial indexes
5. **Cache**: Store PBF file locally for future use

## Filter Configuration

### How Filters Work

Filters define which POIs to extract from OpenStreetMap data:

```yaml
- symbol: food                # Category identifier
  description: Restaurants    # Human-readable description
  filters:                    # List of tag combinations
    - amenity: restaurant     # Match amenity=restaurant
    - amenity: cafe          # OR amenity=cafe
```

**Filter Logic:**
- Each item in `filters` is a dictionary of OSM tags
- All tags within a single dict must match (AND logic)
- Different dicts in the list are alternatives (OR logic)

**Example - Simple OR filter:**
```yaml
filters:
  - amenity: restaurant   # Match this
  - amenity: cafe        # OR this
  - amenity: bar         # OR this
```

**Example - Complex AND filter:**
```yaml
filters:
  - public_transport: station   # Match BOTH of these
    train: 'yes'
  - railway: station            # OR match this
```

When processing OSM data, poiidx:
1. Scans all POI nodes and ways
2. Checks if their tags match any filter combination
3. Extracts matching features
4. Stores them with the specified symbol

### Rank System

Ranks are **calculated automatically** based on:

- **POI size**: Larger areas get lower ranks (higher priority)
- **Place tag**: POIs with `place=city` rank higher than `place=village`
- **Default rank**: POIs without size/place info get maximum rank

The rank calculation ensures that important, larger POIs are prioritized in queries.

You can filter queries by rank range to focus on important POIs using the `rank_range` parameter.

## Administrative Boundaries

### OSM Admin Levels

OpenStreetMap uses standardized admin levels:

- **Level 2**: Countries
- **Level 4**: States/Provinces
- **Level 6**: Counties/Regions
- **Level 8**: Cities/Municipalities
- **Level 9**: City districts
- **Level 10**: Suburbs/Neighborhoods

Different countries may use levels slightly differently, but the general hierarchy is consistent.

### Localization

Administrative boundaries in OSM often include localized names:

```json
{
  "name": "Germany",
  "name:de": "Deutschland",
  "name:fr": "Allemagne",
  "name:es": "Alemania"
}
```

poiidx's `get_administrative_hierarchy_string()` uses these tags to provide localized names when available.

### Country Resolution

When country information is not directly available in the OpenStreetMap administrative boundaries, poiidx uses **Wikidata** to resolve country names:

1. Administrative boundaries in OSM often include a `wikidata` tag with the entity ID
2. poiidx queries the Wikidata API to find the country (P17 property) for that entity
3. Country names and localized labels are retrieved from Wikidata
4. Results are cached in the database for future queries

This approach ensures comprehensive country information across different regions, even where OSM data may be incomplete.

## Database Schema

### POI Table

```sql
CREATE TABLE poi (
    id SERIAL PRIMARY KEY,
    osm_id VARCHAR(255),
    name VARCHAR(255),
    region VARCHAR(255),
    coordinates GEOGRAPHY(POINT, 4326),  -- WGS84
    filter_item VARCHAR(255),
    filter_expression VARCHAR(255),
    rank INTEGER,
    symbol VARCHAR(255)
);

CREATE INDEX poi_coordinates_idx 
    ON poi USING SPGIST(coordinates);
```

### Why SPGIST?

SPGIST is ideal for point data because:
- **Faster for points**: Outperforms GIST for point-only datasets
- **Better KNN**: Optimized for nearest neighbor queries
- **Space efficient**: Smaller index size than GIST

### Administrative Boundary Table

```sql
CREATE TABLE administrativeboundary (
    id SERIAL PRIMARY KEY,
    osm_id VARCHAR(255),
    name VARCHAR(255),
    admin_level INTEGER,
    geometry GEOGRAPHY(GEOMETRY, 4326),
    tags JSONB
);

CREATE INDEX admin_geometry_idx 
    ON administrativeboundary USING GIST(geometry);
```

GIST is used here because boundaries are polygons, not points.

## Performance Considerations

### Query Performance

Typical query times (depends on database size and hardware):

- **Nearest POI query**: 1-10ms
- **Admin hierarchy query**: 5-20ms
- **Region initialization** (first time): Seconds to minutes

### Optimization Strategies

**Use appropriate max_distance**
: Smaller distances = faster queries. Don't query the entire world if you only need nearby POIs.

**Filter by region**
: If you know the region, specify it to avoid unnecessary spatial filtering.

**Use rank filtering**
: Limiting to high-priority POIs reduces the search space.

**Batch queries**
: If querying multiple points, keep the connection open rather than reinitializing.

### Index Maintenance

PostGIS indexes are automatically maintained, but you can optimize:

```sql
VACUUM ANALYZE poi;  -- Update statistics
REINDEX TABLE poi;   -- Rebuild indexes
```

## Data Flow

### Initial Setup

```
User calls poiidx.init()
    ↓
Creates database tables
    ↓
Downloads region index from Geofabrik
    ↓
Stores filter configuration
```

### First Query to a Region

```
User calls get_nearest_pois(berlin_point)
    ↓
Detect region (europe/germany)
    ↓
Check if region data exists (No)
    ↓
Download PBF file for germany
    ↓
Scan PBF for matching POIs
    ↓
Insert POIs into database
    ↓
Create spatial indexes
    ↓
Execute query
    ↓
Return results
```

### Subsequent Queries

```
User calls get_nearest_pois(another_berlin_point)
    ↓
Detect region (europe/germany)
    ↓
Check if region data exists (Yes)
    ↓
Execute query (fast!)
    ↓
Return results
```

## Design Decisions

### Why PostgreSQL + PostGIS?

**PostgreSQL**
: Industry-standard open-source database with excellent spatial support.

**PostGIS**
: The most mature and feature-rich spatial database extension. Provides:
- Accurate geodesic calculations
- Sophisticated spatial indexes
- Rich set of spatial functions
- Active development and community

### Why Peewee ORM?

**Peewee** is a lightweight Python ORM that:
- Has excellent PostgreSQL support
- Supports spatial extensions
- Minimal boilerplate
- Good performance
- Easy to use

### Why Geofabrik?

**Geofabrik** provides:
- Regional extracts of OSM data
- Regular updates (daily/weekly)
- Reliable hosting
- Free for non-commercial use
- Standardized naming scheme

### Why On-Demand Loading?

Pre-loading all OSM data globally would require:
- Hundreds of GB of disk space
- Hours of processing time
- Constant updates

On-demand loading provides:
- Fast startup
- Minimal storage
- Only download what you need

## Limitations

### Coverage

- Limited to regions available on Geofabrik
- Data freshness depends on Geofabrik update frequency
- Some regions may have incomplete OSM data

### Accuracy

- Distance calculations assume WGS84 ellipsoid (accurate to ~1m)
- POI locations depend on OSM data quality
- Administrative boundaries may have political disputes

### Performance

- First query to a new region requires download and processing
- Very large regions (e.g., entire continents) may be slow
- Database size grows with number of regions

### Concurrency

- Database connections are not thread-safe by default
- Use connection pooling for multi-threaded applications

## Future Directions

Potential enhancements:

- **Real-time updates**: Sync with OSM change feeds
- **Custom data sources**: Import POIs from other sources
- **Routing integration**: Calculate distances along roads
- **Clustering**: Group nearby POIs for visualization
- **Caching layer**: Redis/Memcached for frequently accessed queries

## Related Technologies

- **OpenStreetMap**: Source of geographic data
- **Geofabrik**: OSM data distribution
- **Wikidata**: Knowledge base for country information when not available in OSM
- **PostGIS**: Spatial database extension
- **Shapely**: Python library for geometric objects
- **Osmium**: Fast OSM data processing
- **Pyproj**: Coordinate transformations

## Further Reading

- [PostGIS Documentation](https://postgis.net/docs/)
- [OpenStreetMap Wiki](https://wiki.openstreetmap.org/)
- [Shapely Documentation](https://shapely.readthedocs.io/)
- [Peewee Documentation](http://docs.peewee-orm.com/)
