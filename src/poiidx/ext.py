from peewee import SQL, Expression, Field
from shapely import wkb
from shapely.geometry.base import BaseGeometry


def knn(lhs, rhs):
    return Expression(lhs, '<->', rhs)

class GeometryField(Field):
    field_type = 'geometry'

    def __init__(self, srid=4326, *args, **kwargs) -> None:
        self.srid = srid
        super().__init__(*args, **kwargs)

    def db_value(self, value):
        if isinstance(value, BaseGeometry):
            return SQL(
                "ST_GeomFromText(%s, %s)",
                (value.wkt, self.srid)
            )
        return value

    def python_value(self, value):
        if value is not None:
            return wkb.loads(bytes.fromhex(value))
        return value

class GeographyField(Field):
    field_type = 'geography'

    def __init__(self, srid=4326, *args, **kwargs) -> None:
        self.srid = srid
        super().__init__(*args, **kwargs)

    def db_value(self, value):
        if isinstance(value, BaseGeometry):
            return SQL(
                "ST_GeogFromText(%s)",
                (value.wkt,)
            )
        return value

    def python_value(self, value):
        if value is not None:
            return wkb.loads(bytes.fromhex(value))
        return value
