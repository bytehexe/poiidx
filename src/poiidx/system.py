from peewee import *

from .baseModel import BaseModel


class System(BaseModel):
    system = BooleanField(unique=True, default=True)
    last_index_update = DateTimeField(constraints=[SQL('DEFAULT CURRENT_TIMESTAMP')])
    region_index = TextField(null=True)  # Store index metadata as JSON
