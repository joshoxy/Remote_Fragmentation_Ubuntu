# Importing module
import mysql.connector

#Sample code to connect mysql to python
 
# Creating connection object
mydb = mysql.connector.connect(
    host = "localhost",
    user = "root",
    password = ""
)
 
# Printing the connection object
print(mydb)