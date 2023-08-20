#!/usr/bin/env python
from database import database

db = database()

sql = "SELECT element FROM element_table WHERE role='obs'"
db.exe(sql)
data = db.fetch()

elements = ",".join(data)

print(elements)

stations = ()

for loc in stations:
    sql = ""
