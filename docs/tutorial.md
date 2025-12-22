# Tutorial: Getting Started with poiidx

This tutorial will guide you through setting up and using poiidx for the first time. By the end, you'll be able to query Points of Interest and administrative boundaries from OpenStreetMap data.

## What You'll Learn

- How to install and configure poiidx
- How to set up a PostgreSQL database with PostGIS
- How to find nearby Points of Interest
- How to retrieve administrative boundaries

## Prerequisites

- Python 3.8 or higher
- Basic knowledge of Python
- Access to install PostgreSQL on your system

## Step 1: Install PostgreSQL and PostGIS

First, you need a PostgreSQL database with PostGIS extension.

=== "Ubuntu/Debian"
    ```bash
    sudo apt update
    sudo apt install postgresql postgis
    ```

=== "macOS"
    ```bash
    brew install postgresql@16 postgis
    brew services start postgresql@16
    ```

=== "Arch Linux"
    ```bash
    sudo pacman -S postgresql postgis
    sudo -u postgres initdb -D /var/lib/postgres/data
    sudo systemctl start postgresql
    ```

## Step 2: Create Database and User

Create a dedicated database and user for poiidx:

```bash
# Create user
sudo -u postgres createuser -P poiidx_user
# Enter password when prompted: your_secure_password

# Create database
sudo -u postgres createdb -O poiidx_user poiidx_db

# Enable PostGIS extension
sudo -u postgres psql -d poiidx_db -c "CREATE EXTENSION postgis;"
```

## Step 3: Install poiidx

Install poiidx using pip:

```bash
pip install poiidx
```

## Step 4: Configure POI Filters

Create a configuration file to specify which types of POIs you want to index. Save this as `poi_config.yaml`:

```yaml
- symbol: restaurant
  description: Restaurants and dining
  filters:
    - amenity: restaurant
    - amenity: cafe
    - amenity: bar
    - amenity: pub

- symbol: tourism
  description: Tourist attractions and hotels
  filters:
    - tourism: hotel
    - tourism: museum
    - tourism: attraction

- symbol: shop
  description: Shops and stores
  filters:
    - shop: supermarket
    - shop: bakery
```

## Step 5: Initialize the Database

Create a Python script to initialize poiidx:

```python
import yaml
import poiidx

# Load filter configuration
with open('poi_config.yaml') as f:
    filter_config = yaml.safe_load(f)

# Initialize database connection
poiidx.init(
    filter_config=filter_config,
    host='localhost',
    database='poiidx_db',
    user='poiidx_user',
    password='your_secure_password',
    port=5432
)

print("Database initialized successfully!")
```

Run this script. The first time you run it, poiidx will:

1. Create the necessary database tables
2. Download OpenStreetMap data for regions
3. Index POIs based on your filter configuration

!!! note "First Run Takes Time"
    The first initialization may take several minutes as it downloads and processes OpenStreetMap data.

## Step 6: Find Nearby POIs

Now let's find POIs near a location:

```python
from shapely.geometry import Point
import poiidx
import yaml

# Initialize (assuming you've already run step 5)
with open('poi_config.yaml') as f:
    filter_config = yaml.safe_load(f)

poiidx.init(
    filter_config=filter_config,
    host='localhost',
    database='poiidx_db',
    user='poiidx_user',
    password='your_secure_password',
    port=5432
)

# Create a point for Berlin city center
berlin = Point(13.4050, 52.5200)

# Find the 5 nearest POIs within 1000 meters
nearest_pois = poiidx.get_nearest_pois(
    berlin,
    max_distance=1000,  # meters
    limit=5
)

# Display results
for poi in nearest_pois:
    print(f"{poi['name']} ({poi['symbol']})")
    print(f"  Region: {poi['region']}")
    print(f"  Rank: {poi['rank']}")
    print()
```

## Step 7: Get Administrative Boundaries

You can also retrieve administrative boundary information:

```python
from shapely.geometry import Point
import poiidx

# Use the same initialization as before
berlin = Point(13.4050, 52.5200)

# Get administrative hierarchy
admin_hierarchy = poiidx.get_administrative_hierarchy(berlin)

for admin in admin_hierarchy:
    print(f"Level {admin['admin_level']}: {admin['name']}")

# Get as formatted string
admin_string = poiidx.get_administrative_hierarchy_string(berlin)
print(admin_string)

# Get in different language (if available)
admin_string_fr = poiidx.get_administrative_hierarchy_string(berlin, lang='fr')
print(admin_string_fr)
```

## Step 8: Clean Up

When you're done, close the database connection:

```python
poiidx.close()
```

## What's Next?

Now that you've completed the tutorial, you can:

- Explore the [How-to Guides](how-to-guides.md) for specific tasks
- Read the [Reference](reference.md) for detailed API documentation
- Learn more in the [Explanation](explanation.md) section about how poiidx works

## Troubleshooting

**Database connection error**
: Make sure PostgreSQL is running and the credentials are correct.

**No POIs found**
: The first query might trigger data download for that region. Wait and try again.

**PostGIS extension error**
: Ensure PostGIS is installed and the extension is created in your database.
