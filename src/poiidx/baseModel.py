from peewee import *

database = PostgresqlDatabase(None)

class BaseModel(Model):
    class Meta:
        database = database  # Use proxy for our DB.
