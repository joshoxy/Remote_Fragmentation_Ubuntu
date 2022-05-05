import mysql.connector

#Site running on Ubuntu with Mariadb. It stores the fragments for Primary Horizontal Fragmentation

#Connect with Ubuntu using ip adrress on host
upstream = mysql.connector.connect(
    host="192.168.79.182",
    user="test",
    password="SiteOne",

)
upstream_cursor = upstream.cursor()

local_stream = mysql.connector.connect(
					host = "localhost",
					user = "root",
					passwd = "",
					database = "hospital_management")

def init_fragment():    

    #Create database if not exist
    upstream_cursor.execute("CREATE DATABASE IF NOT EXISTS hospital_management")
    upstream_cursor.execute("USE hospital_management")

    #Create table doctors in Mariadb Ubuntu server 
    upstream_cursor.execute("CREATE TABLE IF NOT EXISTS doctors (ID INT NOT NULL AUTO_INCREMENT PRIMARY KEY, first_name VARCHAR(255), last_name VARCHAR(255), phone INT, department VARCHAR(255),  salary INT, employment_date   DATE )")

    
    #M1: Select doctors where department is surgery and salary < 100,000
    local_cursor = local_stream.cursor(buffered=True)
    local_data_query = "SELECT * FROM doctors WHERE department = 'Surgery' AND salary<100000"
    
    print("Primary Fragmentation: department = surgery Salary <100,000")
    print("M1 fragment fetched from localhost")
    local_cursor.execute(local_data_query)
    local_doctors_query_results = local_cursor.fetchall()
    print(local_doctors_query_results)
    print("")
     
    #Insert data into Mariadb site Ubuntu
    upstream_clear2 = "DELETE FROM doctors"  #clear table before inserting to ensure no duplicates 
    upstream_cursor.execute(upstream_clear2)
    upstream_insert_doctors_sql = "INSERT INTO doctors (ID, first_name, last_name, phone, department, salary, employment_date) VALUES (%s, %s, %s, %s, %s, %s, %s)"
    upstream_cursor.executemany(upstream_insert_doctors_sql, local_doctors_query_results)
    upstream.commit()

    #Display the resulting fragment
if __name__ == "__main__":
    #initialize fragment()
    upstream_cursor.execute("USE hospital_management")

    #M1 query on Mariadb 
    upstream_data_query = "SELECT * FROM doctors WHERE department = 'Surgery' AND salary<100000"

    print("M1 fragment fetched from Mariadb")
    upstream_cursor.execute(upstream_data_query)
    upstream_doctors_query_results = upstream_cursor.fetchall()
    print(upstream_doctors_query_results)
    print("")
    #End of M1




    #M2: Select doctors where department is surgery and salary > 100,000
    local_cursor = local_stream.cursor(buffered=True)
    local_data_query1 = "SELECT * FROM doctors WHERE department = 'Surgery' AND salary > 100000"
    
    print("Primary Fragmentation: department = surgery Salary >100,000")
    print("M2 fragment fetched from localhost")
    local_cursor.execute(local_data_query1)
    local_doctors_query_results1 = local_cursor.fetchall()
    print(local_doctors_query_results1)
    print("")
     
    #Insert data into Mariadb site Ubuntu
    upstream_clear3 = "DELETE FROM doctors"  #clear table before inserting to ensure no duplicates 
    upstream_cursor.execute(upstream_clear3)
    upstream_insert_doctors_sql1 = "INSERT INTO doctors (ID,first_name, last_name, phone, department, salary, employment_date) VALUES (%s, %s, %s, %s, %s, %s, %s)"
    upstream_cursor.executemany(upstream_insert_doctors_sql1, local_doctors_query_results1)
    upstream.commit()

    #Display the resulting fragment
if __name__ == "__main__":
    #initialize fragment()
    upstream_cursor.execute("USE hospital_management")

    #M2 query on Mariadb 
    upstream_data_query1 = "SELECT * FROM doctors WHERE department = 'Surgery' AND salary>100000"

    print("M2 fragment fetched from Mariadb")
    upstream_cursor.execute(upstream_data_query1)
    upstream_doctors_query_results1 = upstream_cursor.fetchall()
    print(upstream_doctors_query_results1)
    print("")
    #End of M2




#     #M3: Select doctors where department is not surgery and salary < 100,000
    local_cursor = local_stream.cursor(buffered=True)
    local_data_query2 = "SELECT * FROM doctors WHERE department  != 'Surgery' AND salary<100000"
    
    print("Primary Fragmentation: department != surgery Salary <100,000")
    print("M3 fragment fetched from localhost")
    local_cursor.execute(local_data_query2)
    local_doctors_query_results2 = local_cursor.fetchall()
    print(local_doctors_query_results2)
    print("")
     
    #Insert data into Mariadb site Ubuntu
    upstream_clear3 = "DELETE FROM doctors"  #clear table before inserting to ensure no duplicates 
    upstream_cursor.execute(upstream_clear3)
    upstream_insert_doctors_sql2 = "INSERT INTO doctors (ID, first_name, last_name, phone, department, salary, employment_date) VALUES (%s, %s, %s, %s, %s, %s, %s)"
    upstream_cursor.executemany(upstream_insert_doctors_sql2, local_doctors_query_results2)
    upstream.commit()

    #Display the resulting fragment
if __name__ == "__main__":
    #initialize fragment()
    upstream_cursor.execute("USE hospital_management")

    #M1 query on Mariadb 
    upstream_data_query2 = "SELECT * FROM doctors WHERE department != 'Surgery' AND salary<100000"

    print("M3 fragment fetched from Mariadb")
    upstream_cursor.execute(upstream_data_query2)
    upstream_doctors_query_results2 = upstream_cursor.fetchall()
    print(upstream_doctors_query_results2)
    print("")
#     #End of M3




#     #M4: Select doctors where department is not surgery and salary > 100,000
    local_cursor = local_stream.cursor(buffered=True)
    local_data_query3 = "SELECT * FROM doctors WHERE department != 'Surgery' AND salary>100000"
    
    print("Primary Fragmentation: department != surgery Salary >100,000")
    print("M4 fragment fetched from localhost")
    local_cursor.execute(local_data_query3)
    local_doctors_query_results3 = local_cursor.fetchall()
    print(local_doctors_query_results3)
    print("")
     
    #Insert data into Mariadb site Ubuntu
    upstream_clear4 = "DELETE FROM doctors"  #clear table before inserting to ensure no duplicates 
    upstream_cursor.execute(upstream_clear4)
    upstream_insert_doctors_sql3 = "INSERT INTO doctors (ID, first_name, last_name, phone, department, salary, employment_date) VALUES (%s, %s, %s, %s, %s, %s, %s)"
    upstream_cursor.executemany(upstream_insert_doctors_sql3, local_doctors_query_results3)
    upstream.commit()

    #Display the resulting fragment
if __name__ == "__main__":
    #initialize fragment()
    upstream_cursor.execute("USE hospital_management")

    #M4 query on Mariadb 
    upstream_data_query3 = "SELECT * FROM doctors WHERE department != 'Surgery' AND salary>100000"

    print("M4 fragment fetched from Mariadb")
    upstream_cursor.execute(upstream_data_query3)
    upstream_doctors_query_results3 = upstream_cursor.fetchall()
    print(upstream_doctors_query_results3)
    print("")
    #End of M4




    #Query 2
    #M1 where location is Nairobi
    upstream_cursor.execute("USE hospital_management")
    upstream_cursor.execute("CREATE TABLE IF NOT EXISTS admissions (admission_id INT NOT NULL AUTO_INCREMENT PRIMARY KEY, patient_id INT , doctor_id INT, ward_number INT, ward_name VARCHAR(255), location VARCHAR(255), admission_date  DATE )")

    local_cursor = local_stream.cursor(buffered=True)
    local_data_query1 = "SELECT * FROM admissions WHERE location = 'Nairobi' "

    local_cursor.execute(local_data_query1)
    local_adm_query_results = local_cursor.fetchall()
    print("M1 fragment fetched from localhost: ")
    print(local_adm_query_results)
    print("")
    
    #Insert data into Mariadb site Ubuntu
    upstream_clear = "DELETE FROM admissions"  #clear table before inserting to ensure no duplicates 
    upstream_cursor.execute(upstream_clear)
    upstream_insert_adm_sql = """INSERT INTO admissions (admission_id, patient_id, doctor_id, ward_number, ward_name, location, admission_date) VALUES (%s, %s, %s, %s, %s, %s, %s)"""
    upstream_cursor.executemany(upstream_insert_adm_sql, local_adm_query_results)
    upstream.commit()

     #Display the resulting fragment from query 2
if __name__ == "__main__":
    #initialize fragment()
    upstream_cursor.execute("USE hospital_management")

    #M1 query where location is Nairobi 
    upstream_data_query1 = "SELECT * FROM admissions WHERE location = 'Nairobi'"
    
    print("Query 2: Primary Fragmentation: location = Nairobi")
    upstream_cursor.execute(upstream_data_query1)
    upstream_adm_query_results = upstream_cursor.fetchall()
    print("M1 fragment fetched from MariaDb server: ")
    print(upstream_adm_query_results)
    print("")
    #print results from Mariadb

    #M2 where location is not Nairobi
    upstream_cursor.execute("USE hospital_management")
    upstream_cursor.execute("CREATE TABLE IF NOT EXISTS admissions (admission_id INT NOT NULL AUTO_INCREMENT PRIMARY KEY, patient_id INT , doctor_id INT, ward_number INT, ward_name VARCHAR(255), location VARCHAR(255), admission_date  DATE )")

    local_cursor = local_stream.cursor(buffered=True)
    local_data_query2 = "SELECT * FROM admissions WHERE location != 'Nairobi' "

    local_cursor.execute(local_data_query2)
    local_adm_query_results1 = local_cursor.fetchall()
    print("Query 2: Primary Fragmentation: location != Nairobi")
    print("M2 fragment fetched from localhost: ")
    print(local_adm_query_results1)
    print("")
    
    #Insert data into Mariadb site Ubuntu
    upstream_clear1 = "DELETE FROM admissions"  #clear table before inserting to ensure no duplicates 
    upstream_cursor.execute(upstream_clear1)
    upstream_insert_adm_sql1 = """INSERT INTO admissions (admission_id, patient_id, doctor_id, ward_number, ward_name, location, admission_date) VALUES (%s, %s, %s, %s, %s, %s, %s)"""
    upstream_cursor.executemany(upstream_insert_adm_sql1, local_adm_query_results1)
    upstream.commit()

     #Display the resulting fragment from query 2
if __name__ == "__main__":
    #initialize fragment()
    upstream_cursor.execute("USE hospital_management")

    #M2 query where location is not Nairobi 
    upstream_data_query2 = "SELECT * FROM admissions WHERE location != 'Nairobi'"
    
    
    upstream_cursor.execute(upstream_data_query2)
    upstream_adm_query_results1 = upstream_cursor.fetchall()
    print("M2 fragment fetched from MariaDb server: ")
    print(upstream_adm_query_results1)
    print("")
  





