from peewee import *
from .baseModel import BaseModel
from .ext import GeographyField
from playhouse.postgres_ext import BinaryJSONField

class Poi(BaseModel):
    osm_id = CharField()
    name = CharField()
    region = CharField(index=True)
    coordinates = GeographyField(index=True, index_type='SPGIST')
    filter_item = CharField()
    filter_expression = CharField()
    rank = IntegerField(index=True)
    symbol = CharField(null=True)
    localized_names = BinaryJSONField(null=True)
