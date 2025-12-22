# poiidx

**Efficient spatial indexing and querying of Points of Interest using PostgreSQL and PostGIS**

[![PyPI - Version](https://img.shields.io/pypi/v/poiidx.svg)](https://pypi.org/project/poiidx)
[![PyPI - Python Version](https://img.shields.io/pypi/pyversions/poiidx.svg)](https://pypi.org/project/poiidx)

poiidx is a Python library that makes it easy to work with OpenStreetMap Points of Interest (POIs) and administrative boundaries. It provides fast spatial queries using PostgreSQL/PostGIS and handles data downloading and indexing automatically.

## Features

- 🚀 **Fast spatial queries** using PostGIS spatial indexes
- 📍 **Find nearest POIs** with distance filtering and ranking
- 🗺️ **Administrative boundaries** with hierarchical queries and localization
- 🔄 **Automatic data management** - downloads OSM data on-demand from Geofabrik
- 🎯 **Flexible filtering** - configure which POI types to index
- 🌍 **Global coverage** - supports all regions available on Geofabrik
- 💾 **Efficient storage** - only stores the POI types you need

## Quick Example

```python
import yaml
import poiidx
from shapely.geometry import Point

# Configure which POI types to index
filter_config = [
    {
        'symbol': 'restaurant',
        'description': 'Restaurants and dining',
        'filters': [
            {'amenity': 'restaurant'},
            {'amenity': 'cafe'},
            {'amenity': 'bar'}
        ]
    }
]

# Initialize database
poiidx.init(
    filter_config=filter_config,
    host='localhost',
    database='poiidx_db',
    user='poiidx_user',
    password='your_password'
)

# Find nearby restaurants in Berlin
berlin = Point(13.4050, 52.5200)
pois = poiidx.get_nearest_pois(
    berlin,
    max_distance=1000,  # within 1km
    limit=5
)

for poi in pois:
    print(f"{poi['name']} - {poi['symbol']}")

# Get administrative hierarchy
hierarchy = poiidx.get_administrative_hierarchy_string(berlin)
print(hierarchy)  # "Berlin, Deutschland"

poiidx.close()
```

## Installation

```bash
pip install poiidx
```

### Requirements

- Python 3.8+
- PostgreSQL 12+ with PostGIS extension

## Documentation Structure

This documentation follows the [Diátaxis framework](https://diataxis.fr/):

### [Tutorial](tutorial.md)
**Learning-oriented** - Start here if you're new to poiidx. A hands-on introduction that guides you through setting up poiidx and making your first queries.

### [How-to Guides](how-to-guides.md)
**Problem-oriented** - Practical guides for accomplishing specific tasks. Find solutions for common use cases and advanced scenarios.

### [Reference](reference.md)
**Information-oriented** - Complete API documentation. Look up function signatures, parameters, and return values.

### [Explanation](explanation.md)
**Understanding-oriented** - Deep dives into how poiidx works. Learn about the architecture, design decisions, and underlying concepts.

## Getting Help

- **GitHub Issues**: [Report bugs or request features](https://github.com/bytehexe/poiidx/issues)
- **Source Code**: [View on GitHub](https://github.com/bytehexe/poiidx)

## License

poiidx is distributed under the [MIT License](https://spdx.org/licenses/MIT.html).

## Data Sources & Attribution

### OpenStreetMap

This project uses data from [OpenStreetMap](https://www.openstreetmap.org/), distributed via [Geofabrik](https://www.geofabrik.de/). OpenStreetMap data is © OpenStreetMap contributors and available under the [Open Database License (ODbL)](https://opendatacommons.org/licenses/odbl/).

### Wikidata

When country information is not available in OpenStreetMap data, it is supplemented using [Wikidata](https://www.wikidata.org/). Wikidata content is © Wikidata contributors and available under [CC0 1.0 Universal (CC0 1.0) Public Domain Dedication](https://creativecommons.org/publicdomain/zero/1.0/).
