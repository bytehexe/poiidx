from peewee import *
from .baseModel import BaseModel
from .ext import GeographyField

class Country(BaseModel):
    wikidata_id = CharField(unique=True)
    name = CharField()