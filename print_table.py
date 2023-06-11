from sys import argv

if len(argv) == 2:
    table = argv[1]
else: table = "obs"

if len(argv) == 3:
    what = argv[2]
else: what = "*"

from sqlite3 import connect
db=connect("obs.db")
cur=db.cursor()
cur.execute(f"SELECT {what} FROM {table}")
data=cur.fetchall()
print(data)
