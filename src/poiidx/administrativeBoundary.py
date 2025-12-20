from peewee import *
from .baseModel import BaseModel
from .ext import GeographyField
from .country import Country
from playhouse.postgres_ext import BinaryJSONField

class AdministrativeBoundary(BaseModel):
    osm_id = CharField()
    name = CharField()
    region = CharField(index=True)
    admin_level = IntegerField(index=True)
    coordinates = GeographyField(index=True, index_type='GIST')
    wikidata_id = CharField(null=True)
    country = ForeignKeyField(Country, null=True)
    localized_names = BinaryJSONField(null=True)