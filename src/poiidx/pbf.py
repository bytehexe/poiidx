import hashlib
import logging
import os
import pathlib
import tempfile

import requests

from .__about__ import __version__

logger = logging.getLogger(__name__)

HEADERS = {"User-Agent": f"poiidx/{__version__} (https://github.com/bytehexe/poiidx)"}

# (connect, read) timeouts in seconds. Without these a stalled peer blocks forever.
TIMEOUT = (10, 60)


class Pbf:
    def __init__(self, pbf_dir: str | pathlib.Path) -> None:
        self.pbf_dir = pbf_dir

    def get_pbf_filename(self, region_id: str, region_url: str) -> pathlib.Path:
        pbf_file_name = pathlib.Path(self.pbf_dir) / f"{region_id}.pbf"
        if pbf_file_name.exists():
            logger.info(
                f"Using cached PBF file for region {region_id} from {pbf_file_name}"
            )
            return pbf_file_name

        # Download PBF file
        logger.info(
            f"PBF file for region {region_id} not found in cache. Downloading..."
        )
        self.__download_pbf(region_id, region_url, pbf_file_name)
        return pbf_file_name

    def __download_pbf(
        self, region_key: str, region_url: str, pbf_file_name: pathlib.Path
    ) -> None:
        """Download to a sibling temp file and rename it into place once complete.

        The rename is atomic, so `pbf_file_name` never exists in a partial state:
        an interrupted download cannot poison the cache for later runs.
        """
        logger.info("Downloading PBF ...")
        expected_checksum = self.__fetch_checksum(region_url)

        temp_file = tempfile.NamedTemporaryFile(
            dir=pathlib.Path(self.pbf_dir),
            prefix=f"{region_key}.",
            suffix=".part",
            delete=False,
        )
        temp_path = pathlib.Path(temp_file.name)
        try:
            digest = hashlib.md5(usedforsecurity=False)
            with temp_file, requests.get(
                region_url, stream=True, headers=HEADERS, timeout=TIMEOUT
            ) as result:
                result.raise_for_status()
                for chunk in result.iter_content(chunk_size=8192):
                    temp_file.write(chunk)
                    digest.update(chunk)
                temp_file.flush()
                os.fsync(temp_file.fileno())

            if (
                expected_checksum is not None
                and digest.hexdigest() != expected_checksum
            ):
                raise ValueError(
                    f"PBF checksum mismatch for region {region_key}: "
                    f"expected {expected_checksum}, got {digest.hexdigest()}"
                )

            os.replace(temp_path, pbf_file_name)
        except BaseException:
            temp_path.unlink(missing_ok=True)
            raise

    def __fetch_checksum(self, region_url: str) -> str | None:
        """Return the published md5 for a region, or None if there is none.

        Geofabrik publishes `<region>.osm.pbf.md5` alongside each extract. A missing
        or unreachable checksum must not block ingestion, so this only warns.
        """
        try:
            response = requests.get(
                f"{region_url}.md5", headers=HEADERS, timeout=TIMEOUT
            )
            response.raise_for_status()
        except requests.RequestException as error:
            logger.warning(f"No checksum available for {region_url}: {error}")
            return None
        return response.text.split()[0]
