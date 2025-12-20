import logging
import pathlib

import requests

from .__about__ import __version__

logger = logging.getLogger(__name__)

HEADERS = {"User-Agent": f"poiidx/{__version__} (https://github.com/bytehexe/poiidx)"}


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
        self.__download_pbf(region_id, region_url, pbf_file_name)
        return pbf_file_name

    def __download_pbf(
        self, region_key: str, region_url: str, pbf_file_name: pathlib.Path
    ) -> None:
        logger.info("Downloading PBF ...")
        with requests.get(region_url, stream=True, headers=HEADERS) as result:
            result.raise_for_status()
            with open(pbf_file_name, "wb") as pbf_file:
                for chunk in result.iter_content(chunk_size=8192):
                    pbf_file.write(chunk)
