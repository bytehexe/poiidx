from peewee import *
from .baseModel import BaseModel
from .ext import GeographyField
from playhouse.postgres_ext import BinaryJSONField

class Country(BaseModel):
    wikidata_id = CharField(unique=True)
    name = CharField()
    localized_names = BinaryJSONField(null=True)
    