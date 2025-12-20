from peewee import *
from playhouse.postgres_ext import BinaryJSONField

from .baseModel import BaseModel


class Country(BaseModel):
    wikidata_id = CharField(unique=True)
    name = CharField()
    localized_names = BinaryJSONField(null=True)
