from sqlite3 import connect
db=connect("obs.db")
cur=db.cursor()
data=cur.fetchall()
print(data)
