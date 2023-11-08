from database import db; db = db()
from functions import read

config = read( "config" )
print(db.config)
print(config)
