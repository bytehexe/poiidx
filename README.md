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
from poiidx import POIIdex

# Connect to the database
poi = POIIdex(
    host='localhost',
    database='poiidx_db',
    user='poiidx_user',
    password='your_secure_password',
    port=5432
)

# The schema is automatically initialized on first connection

# Close the connection when done
poi.close()
```

## License

`poiidx` is distributed under the terms of the [MIT](https://spdx.org/licenses/MIT.html) license.
