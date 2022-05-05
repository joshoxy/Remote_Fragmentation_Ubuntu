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
upstream = mysql.connector.connect(
    host="192.168.79.182",
    user="test",
    password="SiteOne",)
upstream_cursor = upstream.cursor()  #Ubuntu cursor 

print("Reconstruction from Hybrid Fragmentation on sqlite: ")
print("")
print("Query one: ")

#Check all the available tables in the sqlite database
print("All tables available in sqlite db: ")
cursor_obj.execute("SELECT name FROM sqlite_master WHERE type='table';")
print(cursor_obj.fetchall())

#Reconstruction of Hybrid fragmentation

#Display salary_1 table
query1 = "SELECT ID, first_name, salary FROM salary_1"
cursor_obj.execute(query1)
query1_update = cursor_obj.fetchall()
print("salary_1 table: (>100,000) ")
print(query1_update)
print("")

#Display salary_2 table
query2 = "SELECT ID, first_name, salary FROM salary_2"
cursor_obj.execute(query2)
query2_update = cursor_obj.fetchall()
print("salary_2 table:  (<100,000) ")
print(query2_update)
print("")

#Doing reconstrcution of salary_1 and salary_2
#Using UNION statement to perform reconstruction
merge_query = "SELECT ID, first_name, salary FROM salary_1 UNION SELECT ID, first_name, salary FROM salary_2"
cursor_obj.execute(merge_query)
merge_update = cursor_obj.fetchall()
print("Merged table: ) ")
print(merge_update)
print("")

#Display male table
query3 = "SELECT ID, first_name, last_name, gender FROM male"
cursor_obj.execute(query3)
query3_update = cursor_obj.fetchall()
print("male table: (Gender = male:) ")
print(query3_update)
print("")

#Display not_male table
query4 = "SELECT ID, first_name, last_name, gender FROM not_male"
cursor_obj.execute(query4)
query4_update = cursor_obj.fetchall()
print("not_male table:  (Gender ! = male:) ")
print(query4_update)
print("")

#Doing reconstrcution of male and not_male
#Using UNION statement to perform reconstruction
merge_query_gender = "SELECT ID, first_name, last_name, gender FROM male UNION SELECT ID, first_name, last_name, gender FROM not_male"
cursor_obj.execute(merge_query_gender)
merge_update_gender = cursor_obj.fetchall()
print("Merged gender table: ) ")
print(merge_update_gender)
print("")
#End of hybrid reconstruction

#Reconstruction of Derived Horizontal
print("Reconstruction of Derived Fragmentation on mysql local")
print("")
local_cursor.execute("SELECT admission_id, ward_name, patient_id FROM white_house_table")
query_whitehse_update = local_cursor.fetchall()
print("white_house_table: ")
print(query_whitehse_update)
print("")

local_cursor.execute("SELECT admission_id, ward_name, patient_id FROM st_peters_table")
query_stpeters_update = local_cursor.fetchall()
print("St_peter_table: ")
print(query_stpeters_update)
print("")

#Doing reconstruction of white_house and st_peters tables
#Using UNION statement to perform reconstruction
merge_query_wards = "SELECT admission_id, ward_name, patient_id FROM white_house_table UNION SELECT admission_id, ward_name, patient_id FROM st_peters_table"
local_cursor.execute(merge_query_wards)
merge_update_wards = local_cursor.fetchall()
print("Merged admissions table: ) ")
print(merge_update_wards)
print("")
#End of derived horizontal reconstruction


connection_obj.close()


