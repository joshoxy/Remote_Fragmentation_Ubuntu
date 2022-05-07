import sqlite3
import mysql.connector

#Site Two runs on SQLite and it performs Hybrid fragmentation

# Connecting to sqlite
# connection object
connection_obj = sqlite3.connect('hospital_management.db')

# cursor object
cursor_obj = connection_obj.cursor()
cursor_obj1 = connection_obj.cursor()

#Communicate with the local mysql database to get records
local_stream = mysql.connector.connect(
					host = "localhost",
					user = "root",
					passwd = "",
					database = "hospital_management")
local_cursor = local_stream.cursor(buffered=True)
                    
# Creating tables
cursor_obj.execute("CREATE TABLE IF NOT EXISTS doctors (ID INTEGER PRIMARY KEY, first_name VARCHAR(255), last_name VARCHAR(255), phone INT, department VARCHAR(255), salary INT, employment_date DATE)")
print("Doctor's table successfully created")
print("")

#Check all the available tables in the sqlite database
# cursor_obj.execute("SELECT name FROM sqlite_master WHERE type='table';")
# print(cursor_obj.fetchall())

print("Hybrid Fragmentation on sqlite: ")
print("")
print("Query one (>100,000): ")

local_cursor.execute("SELECT ID, first_name, salary FROM doctors WHERE salary > 100000")
local_query_result = local_cursor.fetchall()
print("Fragment fetched from localhost: ")
print(local_query_result)
print("")

#Insert data fetched into sqlite database

#First clear the table to ensure no duplicates
cursor_obj.execute("DELETE FROM doctors")

insert_query = "INSERT INTO doctors (ID, first_name, salary) VALUES (?,?,?)"
cursor_obj.executemany(insert_query, local_query_result)
connection_obj.commit()

#Display the fragment created on the sqlite database
cursor_obj.execute("CREATE TABLE IF NOT EXISTS salary_1 AS SELECT * FROM doctors WHERE salary > 100000")
cursor_obj.execute("SELECT ID, first_name, salary FROM salary_1")
updated_query_result = cursor_obj.fetchall()
print("Fragment allocated to Sqlite: salary_1 table (>100000) ")
print(updated_query_result)
print("")
#End of query one

print("Query two (<100,000): ")

local_cursor.execute("SELECT ID, first_name, salary FROM doctors WHERE salary < 100000")
local_query_result2 = local_cursor.fetchall()
print("Fragment fetched from localhost: ")
print(local_query_result2)
print("")

#Insert data fetched into sqlite database

#First clear the table to ensure no duplicates
cursor_obj.execute("DELETE FROM doctors")  #clear table before inserting to remoove duplicates
insert_query2 = "INSERT INTO doctors (ID, first_name, salary) VALUES (?,?,?)"
cursor_obj.executemany(insert_query2, local_query_result2)
connection_obj.commit()

#Display the fragment created on the sqlite database
cursor_obj.execute("CREATE TABLE IF NOT EXISTS salary_2 AS SELECT * FROM doctors WHERE salary < 100000")
cursor_obj.execute("SELECT ID, first_name, salary FROM salary_2")
updated_query_result2 = cursor_obj.fetchall()
print("Fragment allocated to Sqlite: salary_2 table (<100000)")
print(updated_query_result2)
print("")

#Query three
print("Query three: (Gender: Male) ")

#Create patients table
cursor_obj.execute("CREATE TABLE IF NOT EXISTS patients (ID INTEGER PRIMARY KEY, first_name VARCHAR(255), last_name VARCHAR(255), phone INT, gender VARCHAR(255), location VARCHAR(255), DOB DATE)")

#fetch from local host
local_cursor.execute("SELECT ID, first_name, last_name, gender FROM patients WHERE gender = 'Male'")
local_query_result1 = local_cursor.fetchall()
print("Fragment fetched from localhost: ")
print(local_query_result1)
print("")

#Insert data fetched into sqlite database

#First clear the table to ensure no duplicates
cursor_obj.execute("DELETE FROM patients")

insert_query1 = "INSERT INTO patients (ID, first_name, last_name, gender) VALUES (?,?,?,?)"
cursor_obj.executemany(insert_query1, local_query_result1)
connection_obj.commit()

#Display the fragment created on the sqlite database
cursor_obj.execute("CREATE TABLE IF NOT EXISTS male AS SELECT * FROM patients WHERE gender = 'Male'")
cursor_obj.execute("SELECT ID, first_name, last_name, gender FROM male")
updated_query_result1 = cursor_obj.fetchall()
print("Fragment allocated to Sqlite: male table ")
print(updated_query_result1)
print("")

#Query four
print("Query four: (Gender: NOT Male) ")

#fetch from localhost
local_cursor.execute("SELECT ID, first_name, last_name, gender FROM patients WHERE gender != 'Male'")
local_query_result3 = local_cursor.fetchall()
print("Fragment fetched from localhost: ")
print(local_query_result3)
print("")

#Insert data fetched into sqlite database

#First clear the table to ensure no duplicates
cursor_obj.execute("DELETE FROM patients")

insert_query3 = "INSERT INTO patients (ID, first_name, last_name, gender) VALUES (?,?,?,?)"
cursor_obj.executemany(insert_query3, local_query_result3)
connection_obj.commit()

#Display the fragment created on the sqlite database
cursor_obj.execute("CREATE TABLE IF NOT EXISTS not_male AS SELECT * FROM patients WHERE gender != 'Male'")
cursor_obj.execute("SELECT ID, first_name, last_name, gender FROM not_male")
updated_query_result3 = cursor_obj.fetchall()
print("Fragment allocated to Sqlite: not_male table ")
print(updated_query_result3)
print("")


# Close the connection
connection_obj.close()
