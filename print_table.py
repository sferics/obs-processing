from sys import argv

if len(argv) == 2:
    table = argv[1]
else: table = "obs"

from sqlite3 import connect
db=connect("obs.db")
cur=db.cursor()
cur.execute(f"SELECT * FROM {table}")
data=cur.fetchall()
print(data)
