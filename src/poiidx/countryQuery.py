import time
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime

import requests

from .__about__ import __version__
from .administrativeBoundary import AdministrativeBoundary
from .country import Country

HEADERS = {
    "User-Agent": f"poiidx/{__version__} (https://github.com/bytehexe/poiidx)"
}

# Global variable to track next allowed execution time
_next_allowed_execution = 0.0

def _rate_limited_get(url: str, headers: dict) -> requests.Response:
    """Perform a rate-limited GET request with automatic retry on 429."""
    global _next_allowed_execution

    while True:
        # Wait until we're allowed to make the next request
        now = time.time()
        if now < _next_allowed_execution:
            time.sleep(_next_allowed_execution - now)

        # Make the request
        response = requests.get(url, headers=headers)

        # Check if we got a 429 (Too Many Requests)
        if response.status_code == 429:
            # Get Retry-After header (can be in seconds or HTTP date)
            retry_after = response.headers.get('Retry-After')
            if retry_after:
                try:
                    # Try to parse as seconds
                    wait_time = float(retry_after)
                except ValueError:
                    # Parse as HTTP date
                    retry_date = parsedate_to_datetime(retry_after)
                    wait_time = (retry_date - datetime.now(timezone.utc)).total_seconds()

                # Add 1 second backoff
                _next_allowed_execution = time.time() + wait_time + 1.0
            else:
                # No Retry-After header, use default backoff
                _next_allowed_execution = time.time() + 2.0
            continue

        # For successful requests, set next allowed execution to 0.2s from now
        if response.status_code == 200:
            _next_allowed_execution = time.time() + 0.2

        return response

def r(rank: str) -> int:
    """Return a numeric value for the given rank string."""
    if rank == "preferred":
        return 0
    elif rank == "normal":
        return 1
    elif rank == "deprecated":
        return 2
    else:
        return 1

def country_query(admin_with_wikidata: AdministrativeBoundary) -> tuple[str | None, dict] | None:
    """Check if the given Wikidata ID is in a country and return the country name."""
    base_url = "https://www.wikidata.org/w/rest.php/wikibase/v1"

    # Check database first
    if admin_with_wikidata.country and admin_with_wikidata.country.name:
        return admin_with_wikidata.country.name, admin_with_wikidata.country.localized_names

    wikidata_id = admin_with_wikidata.wikidata_id

    # Check if the entity exists and has a country (P17) property
    url = f"{base_url}/entities/items/{wikidata_id}/statements?property=P17"
    response = _rate_limited_get(url, headers=HEADERS)
    response.raise_for_status()
    data = response.json()
    country_id = sorted(data.get("P17", [{}]), key=lambda x: r(x.get("rank", "normal")))[0].get("value", {}).get("content", None)
    if not country_id:
        return None

    # Check the database for the country
    country = Country.get_or_none(Country.wikidata_id == country_id)
    if country is not None:
        # Save the country reference in the administrative boundary
        admin_with_wikidata.country = country
        admin_with_wikidata.save()

        return country.name, country.localized_names

    # Fetch the native labels for the country
    url = f"{base_url}/entities/items/{country_id}/statements?property=P1705"
    response = _rate_limited_get(url, headers=HEADERS)
    response.raise_for_status()
    data = response.json()
    native_labels = [x.get("value", {}).get("content", None).get("text") for x in data.get("P1705", [])]

    # Fetch all labels for the country
    url = f"{base_url}/entities/items/{country_id}/labels"
    response = _rate_limited_get(url, headers=HEADERS)
    response.raise_for_status()
    label_data = response.json()
    all_labels = label_data.values()

    # Find a matching label
    matching_labels = list(set(native_labels).intersection(set(all_labels)))
    if matching_labels:
        label = matching_labels[0]
    else:
        # Fallback to English label
        label = label_data.get("en", None)

    if label is None:
        return None
    else:
        # Store in database for future queries
        c = Country.create(wikidata_id=country_id, name=label, localized_names=label_data)
        admin_with_wikidata.country = c
        admin_with_wikidata.save()

        return label, label_data
