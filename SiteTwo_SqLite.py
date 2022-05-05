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
#cursor_obj.execute("CREATE TABLE doctors (ID INTEGER PRIMARY KEY, first_name VARCHAR(255), last_name VARCHAR(255), phone INT, department VARCHAR(255), salary INT, employment_date DATE)")
#print("Table successfully created")

#Check all the available tables in the sqlite database
# cursor_obj.execute("SELECT name FROM sqlite_master WHERE type='table';")
# print(cursor_obj.fetchall())

print("Hybrid Fragmentation on sqlite: ")
print("Query one: ")

local_cursor.execute("SELECT ID, first_name, salary FROM doctors WHERE salary > 100000")
local_query_result = local_cursor.fetchall()
print("Fragment fetched from localhost: ")
print(local_query_result)

#Insert data fetched into sqlite database

#First clear the table to ensure no duplicates
cursor_obj.execute("DELETE FROM doctors")

insert_query = "INSERT INTO doctors (ID, first_name, salary) VALUES (?,?,?)"
cursor_obj.executemany(insert_query, local_query_result)
connection_obj.commit()

#Display the fragment created on the sqlite database
updated_query = "SELECT ID, first_name, salary FROM doctors WHERE salary > 100000"
cursor_obj.execute(updated_query)
updated_query_result = cursor_obj.fetchall()
print("Fragment allocated to Sqlite: ")
print(updated_query_result)

#Query two
print("Query two: ")

#Create patients table
#cursor_obj.execute("CREATE TABLE patients (ID INTEGER PRIMARY KEY, first_name VARCHAR(255), last_name VARCHAR(255), phone INT, gender VARCHAR(255), location VARCHAR(255), DOB DATE)")

#Check if table has been created
# cursor_obj.execute("SELECT name FROM sqlite_master WHERE type='table';")
# print(cursor_obj.fetchall())

local_cursor.execute("SELECT ID, first_name, last_name FROM patients WHERE gender = 'Male'")
local_query_result1 = local_cursor.fetchall()
print("Fragment fetched from localhost: ")
print(local_query_result1)

#Insert data fetched into sqlite database

#First clear the table to ensure no duplicates
cursor_obj.execute("DELETE FROM patients")

insert_query1 = "INSERT INTO patients (ID, first_name, last_name) VALUES (?,?,?)"
cursor_obj.executemany(insert_query1, local_query_result1)
connection_obj.commit()

#Display the fragment created on the sqlite database
updated_query1 = "SELECT ID, first_name, last_name FROM patients WHERE gender = 'Male'"
cursor_obj.execute(updated_query1)
updated_query_result1 = cursor_obj.fetchall()
print("Fragment allocated to Sqlite: ")
print(updated_query_result1)


# Close the connection
connection_obj.close()
