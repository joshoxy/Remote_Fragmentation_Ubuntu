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
    host="192.168.100.205",
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

#Reconstruction of Primary Horizontal
upstream_cursor.execute("USE hospital_management")
print("Reconstruction of Primary Horizontal Fragmentation on mariadb Ubuntu: ")
print("")
upstream_cursor.execute("SELECT * FROM m1_table")
query_m1 = upstream_cursor.fetchall()
print("m1_table: ")
print(query_m1)
print("")

upstream_cursor.execute("SELECT * FROM m2_table")
query_m2 = upstream_cursor.fetchall()
print("m2_table: ")
print(query_m2)
print("")

upstream_cursor.execute("SELECT * FROM m3_table")
query_m3 = upstream_cursor.fetchall()
print("m3_table: ")
print(query_m3)
print("")

upstream_cursor.execute("SELECT * FROM m4_table")
query_m4 = upstream_cursor.fetchall()
print("m4_table: ")
print(query_m4)
print("")


#Doing reconstruction of m1, m2, m3, m4 tables
#Using UNION statement to perform reconstruction
merge_query_tables = "SELECT * FROM m1_table UNION SELECT * FROM m2_table UNION SELECT * FROM m3_table UNION SELECT * FROM m4_table"
upstream_cursor.execute(merge_query_tables)
merge_update_tables = upstream_cursor.fetchall()
print("Merged doctors table: m1, m2, m3, m4) ")
print(merge_update_tables)
print("")
#End of primary horizontal reconstruction



#Reconstruction of Primary Horizontal Query 2
upstream_cursor.execute("USE hospital_management")
print("Reconstruction of Primary Horizontal Fragmentation Query 2 on mariadb Ubuntu: ")
print("")
upstream_cursor.execute("SELECT * FROM m1_nairobi")
query_m1_nairobi = upstream_cursor.fetchall()
print("m1_nairobi: ")
print(query_m1_nairobi)
print("")

upstream_cursor.execute("SELECT * FROM m2_not_nairobi")
query_m2_not_nairobi = upstream_cursor.fetchall()
print("m2_not_nairobi: ")
print(query_m2_not_nairobi)
print("")


#Doing reconstruction of m1_nairobi, m2_not_nairobi
#Using UNION statement to perform reconstruction
merge_query_tables_loc = "SELECT * FROM m1_nairobi UNION SELECT * FROM m2_not_nairobi"
upstream_cursor.execute(merge_query_tables_loc)
merge_update_tables_loc = upstream_cursor.fetchall()
print("Merged doctors table: m1_nairobi, m2_not_nairobi) ")
print(merge_update_tables_loc)
print("")
#End of primary horizontal reconstruction



connection_obj.close()


