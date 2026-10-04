import pytest  # type: ignore[import-not-found]
from shapely.geometry import MultiPoint, Point

from poiidx.regionFinder import Region, RegionFinder

from .helpers import build_index

IN_BREMEN = Point(8.7, 53.1)
IN_OCEAN = Point(-150, 0)


@pytest.fixture(scope="module")
def finder() -> RegionFinder:
    return RegionFinder(build_index())


def _ids(regions: list[Region]) -> list[str]:
    return [region.id for region in regions]


def test_candidates_include_larger_parents_smallest_first(finder: RegionFinder) -> None:
    assert _ids(finder.find_candidates(IN_BREMEN)) == ["bremen", "germany", "dach"]


def test_find_regions_selects_only_the_smallest_cover(finder: RegionFinder) -> None:
    assert _ids(finder.find_regions(IN_BREMEN)) == ["bremen"]


def test_find_regions_for_two_countries(finder: RegionFinder) -> None:
    shape = MultiPoint([(8.7, 53.1), (13, 47.5)])
    assert _ids(finder.find_regions(shape)) == ["bremen", "austria"]
    assert set(_ids(finder.find_candidates(shape))) == {
        "bremen",
        "austria",
        "germany",
        "dach",
    }


def test_shape_outside_every_region_yields_nothing(finder: RegionFinder) -> None:
    # "everywhere" covers this point but has no pbf url, so it must be ignored.
    assert finder.find_candidates(IN_OCEAN) == []
    assert finder.find_regions(IN_OCEAN) == []


def test_region_without_pbf_url_is_not_listed(finder: RegionFinder) -> None:
    assert "everywhere" not in _ids(finder.all_regions())


def test_lookup_by_id(finder: RegionFinder) -> None:
    assert finder.lookup("austria").id == "austria"


def test_lookup_by_name_ignores_case(finder: RegionFinder) -> None:
    assert finder.lookup("AUSTRIA").id == "austria"
    assert finder.lookup("  Austria ").id == "austria"


def test_lookup_id_wins_over_name(finder: RegionFinder) -> None:
    # "bremen-alias" has the *name* "bremen"; the id "bremen" must win.
    assert finder.lookup("bremen").id == "bremen"


def test_lookup_ambiguous_name_lists_ids(finder: RegionFinder) -> None:
    with pytest.raises(ValueError, match="georgia-asia, georgia-us"):
        finder.lookup("Georgia")


@pytest.mark.parametrize("value", ["nowhere", "everywhere"])
def test_lookup_unknown_region(finder: RegionFinder, value: str) -> None:
    with pytest.raises(ValueError, match="No Geofabrik region"):
        finder.lookup(value)


@pytest.mark.parametrize("value", ["", "   "])
def test_lookup_empty_value(finder: RegionFinder, value: str) -> None:
    with pytest.raises(ValueError, match="must not be empty"):
        finder.lookup(value)
