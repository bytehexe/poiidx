# How-to Guides

Practical guides for accomplishing specific tasks with poiidx.

## Installation & Setup

### How to Install poiidx

Install using pip:

```bash
pip install poiidx
```

For development installation:

```bash
git clone https://github.com/bytehexe/poiidx.git
cd poiidx
pip install -e .
```

### How to Set Up PostgreSQL with PostGIS

#### On Ubuntu/Debian

```bash
sudo apt update
sudo apt install postgresql postgis
sudo systemctl start postgresql
sudo systemctl enable postgresql
```

#### On macOS

```bash
brew install postgresql@16 postgis
brew services start postgresql@16
```

#### On Arch Linux

```bash
sudo pacman -S postgresql postgis
sudo -u postgres initdb -D /var/lib/postgres/data
sudo systemctl start postgresql
sudo systemctl enable postgresql
```

### How to Create a Database for poiidx

```bash
# Create user
sudo -u postgres createuser -P poiidx_user

# Create database
sudo -u postgres createdb -O poiidx_user poiidx_db

# Enable PostGIS
sudo -u postgres psql -d poiidx_db -c "CREATE EXTENSION postgis;"
```

## Configuration

### How to Configure POI Filters

Create a YAML file with your desired POI filters:

```yaml
# poi_filters.yaml
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
    - tourism: museum
```

Filter parameters:

- **symbol**: Symbol category for grouping POIs (e.g., 'food', 'tourism', 'shop')
- **description**: Human-readable description of what this filter matches
- **filters**: List of OSM tag combinations. Each item is a dict of tag key-value pairs that must all match (AND logic). Different items in the list are alternatives (OR logic)

Example: The above `food` filter matches POIs that have:
- `amenity=restaurant` OR
- `amenity=cafe` OR
- `amenity=bar`

For more complex filters with multiple required tags:
```yaml
- symbol: train_station
  description: Railway stations
  filters:
    - public_transport: station
      train: 'yes'
    - railway: station
```

### How to Initialize with Custom Configuration

```python
import yaml
import poiidx

with open('poi_filters.yaml') as f:
    filter_config = yaml.safe_load(f)

poiidx.init(
    filter_config=filter_config,
    host='localhost',
    database='poiidx_db',
    user='poiidx_user',
    password='your_password',
    port=5432
)
```

### How to Reinitialize the Database

To recreate the schema and reload data:

```python
import yaml
import poiidx

with open('poi_filters.yaml') as f:
    filter_config = yaml.safe_load(f)

poiidx.init(
    filter_config=filter_config,
    host='localhost',
    database='poiidx_db',
    user='poiidx_user',
    password='your_password',
    port=5432,
    recreate=True  # This will drop and recreate tables
)
```

## Querying POIs

### How to Find Nearest POIs

```python
from shapely.geometry import Point
import poiidx

# Initialize first (see Configuration section)

# Create a point
location = Point(13.4050, 52.5200)  # Berlin

# Find nearest POIs
pois = poiidx.get_nearest_pois(
    location,
    limit=5
)

for poi in pois:
    print(f"{poi['name']} - {poi['symbol']}")
```

### How to Limit Search by Distance

Find POIs within a specific radius:

```python
from shapely.geometry import Point
import poiidx

location = Point(13.4050, 52.5200)

# Find POIs within 500 meters
nearby_pois = poiidx.get_nearest_pois(
    location,
    max_distance=500,  # in meters
    limit=10
)
```

### How to Filter POIs by Rank

```python
from shapely.geometry import Point
from poiidx.poiIdx import PoiIdx

# For direct access to PoiIdx methods
location = Point(13.4050, 52.5200)

pois = PoiIdx.get_nearest_pois(
    location,
    max_distance=1000,
    limit=10,
    rank_range=(1, 2)  # Only ranks 1 and 2
)
```

### How to Search in Specific Regions

```python
from shapely.geometry import Point
from poiidx.poiIdx import PoiIdx

location = Point(13.4050, 52.5200)

pois = PoiIdx.get_nearest_pois(
    location,
    limit=10,
    regions=['europe/germany']  # Limit to specific regions
)
```

### How to Query with Custom Shapes

You can use any Shapely geometry:

```python
from shapely.geometry import Polygon
import poiidx

# Define a polygon area
area = Polygon([
    (13.3, 52.5),
    (13.5, 52.5),
    (13.5, 52.6),
    (13.3, 52.6),
    (13.3, 52.5)
])

# Find POIs in or near this area
pois = poiidx.get_nearest_pois(
    area,
    limit=20
)
```

## Administrative Boundaries

### How to Get Administrative Hierarchy

```python
from shapely.geometry import Point
import poiidx

location = Point(13.4050, 52.5200)

# Get hierarchy as list of dictionaries
hierarchy = poiidx.get_administrative_hierarchy(location)

for admin in hierarchy:
    print(f"Level {admin['admin_level']}: {admin['name']}")
```

### How to Get Administrative Hierarchy as String

```python
from shapely.geometry import Point
import poiidx

location = Point(13.4050, 52.5200)

# Get formatted string
hierarchy_str = poiidx.get_administrative_hierarchy_string(location)
print(hierarchy_str)
# Output: "Berlin, Deutschland"
```

### How to Get Multilingual Administrative Names

```python
from shapely.geometry import Point
import poiidx

location = Point(13.4050, 52.5200)

# Get in French
hierarchy_fr = poiidx.get_administrative_hierarchy_string(
    location,
    lang='fr'
)
print(hierarchy_fr)
# Output: "Berlin, Allemagne"

# Get in Spanish
hierarchy_es = poiidx.get_administrative_hierarchy_string(
    location,
    lang='es'
)
```

## Advanced Usage

### How to Use Buffer Zones

Add a buffer around your query point:

```python
from shapely.geometry import Point
import poiidx

location = Point(13.4050, 52.5200)

# Add 5000 meter buffer for region initialization
pois = poiidx.get_nearest_pois(
    location,
    buffer=5000,  # meters
    limit=10
)
```

### How to Access the Database Directly

For advanced queries, you can access the underlying database:

```python
from poiidx.poiIdx import PoiIdx
from poiidx.poi import Poi

# Make sure PoiIdx is initialized first

# Direct database query
all_restaurants = Poi.select().where(
    Poi.filter_expression == 'restaurant'
).limit(100)

for poi in all_restaurants:
    print(poi.name, poi.coordinates)
```

### How to Close the Connection

Always close the connection when done:

```python
import poiidx

# ... your queries ...

poiidx.close()
```

### How to Use Context Managers

For automatic cleanup:

```python
from contextlib import contextmanager
import yaml
import poiidx

@contextmanager
def poiidx_connection(config_file, **db_params):
    with open(config_file) as f:
        filter_config = yaml.safe_load(f)
    
    poiidx.init(filter_config=filter_config, **db_params)
    try:
        yield
    finally:
        poiidx.close()

# Usage
with poiidx_connection('poi_filters.yaml',
                       host='localhost',
                       database='poiidx_db',
                       user='poiidx_user',
                       password='password'):
    # Your queries here
    pois = poiidx.get_nearest_pois(point, limit=5)
```

## Troubleshooting

### How to Check if PostGIS is Installed

```bash
sudo -u postgres psql -d poiidx_db -c "SELECT PostGIS_version();"
```

### How to Verify Database Connection

```python
from poiidx.baseModel import database

try:
    database.init(
        database='poiidx_db',
        user='poiidx_user',
        password='your_password',
        host='localhost',
        port=5432
    )
    print("Connection successful!")
    database.close()
except Exception as e:
    print(f"Connection failed: {e}")
```

### How to Clear Cached PBF Files

POI data is cached locally. To clear the cache:

```python
import platformdirs
import shutil
from pathlib import Path

cache_dir = Path(platformdirs.user_cache_dir('poiidx'))
if cache_dir.exists():
    shutil.rmtree(cache_dir)
    print(f"Cleared cache at {cache_dir}")
```

### How to Enable Debug Logging

```python
import logging
import poiidx

# Enable debug logging
logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger('poiidx')
logger.setLevel(logging.DEBUG)

# Now run your queries
```
