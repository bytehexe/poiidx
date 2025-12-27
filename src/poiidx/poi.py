from peewee import CharField, IntegerField
from playhouse.postgres_ext import BinaryJSONField

from .baseModel import BaseModel
from .ext import GeographyField


class Poi(BaseModel):
    osm_id = CharField(index=True)
    name = CharField()
    region = CharField(index=True)
    coordinates = GeographyField(index=True, index_type="SPGIST")
    filter_item = CharField()
    filter_expression = CharField()
    rank = IntegerField(index=True)
    symbol = CharField(null=True)
    localized_names = BinaryJSONField(null=True)
    capital = IntegerField(null=True)  # Admin Level; null = not applicable
