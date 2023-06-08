from sqlite3 import connect
db=connect("obs.db")
cur=db.cursor()
cur.execute("SELECT * FROM obs")
data=cur.fetchall()
print(data)
