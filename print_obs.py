from sqlite3 import connect
db=connect("obs.db")
cur=db.cursor("SELECT * FROM obs")
data=cur.fetchall()
print(data)
