import requests
from .system import System
from .__about__ import __version__

HEADERS = {
    "User-Agent": f"poiidx/{__version__} (https://github.com/bytehexe/poiidx)"
}

def download_region_data():
    response = requests.get("https://download.geofabrik.de/index-v1.json", headers=HEADERS)
    response.raise_for_status()
    geofabrik_data = response.text

    # Save the region index to the system model
    system, created = System.get_or_create(system=True)
    system.region_index = geofabrik_data
    system.save()