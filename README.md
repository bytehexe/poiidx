# poiidx

[![PyPI - Version](https://img.shields.io/pypi/v/poiidx.svg)](https://pypi.org/project/poiidx)
[![PyPI - Python Version](https://img.shields.io/pypi/pyversions/poiidx.svg)](https://pypi.org/project/poiidx)

-----

## Table of Contents

- [Installation](#installation)
- [Database Setup](#database-setup)
- [Usage](#usage)
- [License](#license)

## Installation

```console
pip install poiidx
```

## Database Setup

### Install PostgreSQL and PostGIS

**Ubuntu/Debian:**
```console
sudo apt update
sudo apt install postgresql postgis
```

**macOS:**
```console
brew install postgresql@16 postgis
brew services start postgresql@16
```

**Arch Linux:**
```console
sudo pacman -S postgresql postgis
sudo -u postgres initdb -D /var/lib/postgres/data
sudo systemctl start postgresql
```

### Create Database and User

```console
# Create user
sudo -u postgres createuser -P poiidx_user
# Enter password when prompted

# Create database
sudo -u postgres createdb -O poiidx_user poiidx_db

# Enable PostGIS extension
sudo -u postgres psql -d poiidx_db -c "CREATE EXTENSION postgis;"
```

## Usage

```python
import poiidx
from shapely.geometry import Point

# Initialize the database connection
poiidx.init(
    host='localhost',
    database='poiidx_db',
    user='poiidx_user',
    password='your_secure_password',
    port=5432
)

# Find nearest POIs to a point (e.g., Berlin)
berlin_point = Point(13.4050, 52.5200)
nearest_pois = poiidx.get_nearest_pois(
    berlin_point,
    max_distance=1000,  # meters
    limit=5
)

for poi in nearest_pois:
    print(f"{poi['name']} - Region: {poi['region']}, Rank: {poi['rank']}")

# Get administrative hierarchy for a point
admin_hierarchy = poiidx.get_administrative_hierarchy(berlin_point)
for admin in admin_hierarchy:
    print(f"Level {admin['admin_level']}: {admin['name']}")

# Get administrative hierarchy as a formatted string
admin_str = poiidx.get_administrative_hierarchy_string(berlin_point)
print(admin_str)

# Get administrative hierarchy in a specific language (e.g., French)
admin_str_fr = poiidx.get_administrative_hierarchy_string(berlin_point, lang='fr')
print(admin_str_fr)

# Close the connection when done
poiidx.close()
```

## License

`poiidx` is distributed under the terms of the [MIT](https://spdx.org/licenses/MIT.html) license.
