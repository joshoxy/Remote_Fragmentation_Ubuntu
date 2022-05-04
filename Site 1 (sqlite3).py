import sqlite3

# Connecting to sqlite
# connection object
connection_obj = sqlite3.connect('hospital_management.db')

# cursor object
cursor_obj = connection_obj.cursor()

# Drop the GEEK table if already exists.
cursor_obj.execute("DROP TABLE IF EXISTS GEEK")

# Creating table
cursor_obj.execute("CREATE TABLE doctors ()")

print("Table is Ready")

# Close the connection
connection_obj.close()
