import math

MIN_RANK = 13
MAX_RANK = 23


def clip_rank(rank: int) -> int:
    return max(MIN_RANK, min(MAX_RANK, rank))


def calculate_rank(
    place: str | None = None,
    radius: float | None = None,
) -> int | None:
    # Use nominatim ranking based on radius as a reference
    # see: https://nominatim.org/release-docs/latest/customize/Ranking/

    if radius is not None:
        rank: int = int(math.ceil(20 - math.log(radius / 1000, 2)))
        if rank > 23:
            return 23
        if rank < 13:
            return None
        return rank

    if place in ("city", "municipality", "island"):
        return 13
    if place in ("town", "borough"):
        return 17
    if place in ("village", "suburb", "quarter"):
        return 19
    if place in ("hamlet", "farm", "neighbourhood", "islet"):
        return 20
    if place in ("isolated_dwelling", "single_dwelling", "city_block", "locality"):
        return 21
    if place in ("croft", "allotments", "garden", "plot", "square", "festival"):
        return 23
    if place is None:
        return 23

    return None
