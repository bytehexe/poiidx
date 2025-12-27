import logging

import requests

from .__about__ import __version__
from .system import System

HEADERS = {"User-Agent": f"poiidx/{__version__} (https://github.com/bytehexe/poiidx)"}

logger = logging.getLogger(__name__)


def download_region_data() -> None:
    logger.debug("Downloading Geofabrik region index")

    response = requests.get(
        "https://download.geofabrik.de/index-v1.json", headers=HEADERS
    )
    response.raise_for_status()
    geofabrik_data = response.text

    # Save the region index to the system model
    system, created = System.get_or_create(system=True)
    system.region_index = geofabrik_data
    system.save()
