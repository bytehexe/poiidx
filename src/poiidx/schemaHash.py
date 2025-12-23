from peewee import BooleanField, CharField

from .baseModel import BaseModel


class SchemaHash(BaseModel):
    instance = BooleanField(unique=True, default=True)
    schema_hash = CharField()
