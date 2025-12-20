import sys

import poiidx


def test_import() -> None:
    dir(poiidx)  # Ensure package can be used
    assert "poiidx" in sys.modules
