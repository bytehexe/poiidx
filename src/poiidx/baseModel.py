from peewee import Model, PostgresqlDatabase

database = PostgresqlDatabase(None)


class BaseModel(Model):
    class Meta:
        database = database  # Use proxy for our DB.
