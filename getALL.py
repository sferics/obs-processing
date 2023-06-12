from database import db; db = db()
from functions import read_yaml

config = read_yaml( "config.yaml" )
print(db.config)
print(config)
