from typing import Any

from peewee import SQL, Expression, Field
from shapely import wkb
from shapely.geometry.base import BaseGeometry


def knn(lhs: Any, rhs: Any) -> Expression:
    return Expression(lhs, "<->", rhs)


class GeometryField(Field):
    field_type = "geometry"

    def __init__(self, srid: int = 4326, *args: Any, **kwargs: Any) -> None:
        self.srid = srid
        super().__init__(*args, **kwargs)

    def db_value(self, value: Any) -> Any:
        if isinstance(value, BaseGeometry):
            return SQL("ST_GeomFromText(%s, %s)", (value.wkt, self.srid))
        return value

    def python_value(self, value: Any) -> BaseGeometry | None:
        if value is not None:
            return wkb.loads(bytes.fromhex(value))
        return None


class GeographyField(Field):
    field_type = "geography"

    def __init__(self, srid: int = 4326, *args: Any, **kwargs: Any) -> None:
        self.srid = srid
        super().__init__(*args, **kwargs)

    def db_value(self, value: Any) -> Any:
        if isinstance(value, BaseGeometry):
            return SQL("ST_GeogFromText(%s)", (value.wkt,))
        return value

    def python_value(self, value: Any) -> BaseGeometry | None:
        if value is not None:
            return wkb.loads(bytes.fromhex(value))
        return None
        return value
