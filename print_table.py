from sys import argv

if len(argv) == 1: table = "obs"
else:              table = argv[1]

if len(argv) == 3: what = argv[2]
else:              what = "*"

if len(argv) == 4: where = f" WHERE {argv[3]}"
else:              where = ""

from sqlite3 import connect
db=connect("obs.db")
cur=db.cursor()
cur.execute(f"SELECT {what} FROM {table}{where}")
data=cur.fetchall()
for i in data:
    print(i)
