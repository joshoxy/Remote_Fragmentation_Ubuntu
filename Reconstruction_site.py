import sqlite3
import mysql.connector

#CEntral site that does fragmentation
# Connecting to sqlite
# connection object
connection_obj = sqlite3.connect('hospital_management.db')

# cursor object
cursor_obj = connection_obj.cursor()

#Communicate with the local mysql database to get records
local_stream = mysql.connector.connect(
					host = "localhost",
					user = "root",
					passwd = "",
					database = "hospital_management")
local_cursor = local_stream.cursor(buffered=True)

#Connect with Ubuntu using ip adrress on host
# upstream = mysql.connector.connect(
#     host="192.168.100.205",
#     user="test",
#     password="SiteOne",)
# upstream_cursor = upstream.cursor()  #Ubuntu cursor 

print("Reconstruction from Hybrid Fragmentation on sqlite: ")
print("Query one: ")

#Check all the available tables in the sqlite database
cursor_obj.execute("SELECT name FROM sqlite_master WHERE type='table';")
print(cursor_obj.fetchall())

query1 = "SELECT * FROM doctors"
cursor_obj.execute(query1)
update = cursor_obj.fetchall()
print(update)

#Display

connection_obj.close()


