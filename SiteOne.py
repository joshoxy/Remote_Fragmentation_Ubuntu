import mysql.connector

#Site running on Ubuntu with Mariadb
upstream = mysql.connector.connect(
    host="192.168.100.205",
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

    #Create table doctors
    upstream_cursor.execute("CREATE TABLE IF NOT EXISTS doctors (ID INT NOT NULL AUTO_INCREMENT PRIMARY KEY, first_name VARCHAR(255), last_name VARCHAR(255), phone INT, department VARCHAR(255),  salary INT, employment_date   DATE )")

    local_cursor = local_stream.cursor(buffered=True)
    local_data_query = "SELECT * FROM doctors WHERE department = 'Surgery' AND salary<100000"
    
    local_cursor.execute(local_data_query)
    local_doctors_query_results = local_cursor.fetchall()
     
    #Insert data into Mariadb site Ubuntu
    upstream_insert_doctors_sql = "INSERT INTO doctors (first_name, last_name, phone, department, salary, employment_date) VALUES (%s, %s, %s, %s, %s, %s, %s)"
    upstream_cursor.executemany(upstream_insert_doctors_sql, local_doctors_query_results)
    upstream.commit()

    #Display the resulting fragment
if __name__ == "__main__":
    #init_fragment()
    upstream_cursor.execute("USE hospital_management")
    upstream_data_query = "SELECT * FROM doctors WHERE department = 'Surgery' AND salary<100000"

    print("Query 1: Primary Fragmentation: Salary <100,000")
    upstream_cursor.execute(upstream_data_query)
    upstream_doctors_query_results = upstream_cursor.fetchall()
    print(upstream_doctors_query_results)
    #print results from Mariadb

    #Query 2
    upstream_cursor.execute("USE hospital_management")
    upstream_cursor.execute("CREATE TABLE IF NOT EXISTS admissions (admission_id INT NOT NULL AUTO_INCREMENT PRIMARY KEY, patient_id INT , doctor_id INT, ward_number INT, ward_name VARCHAR(255), location VARCHAR(255), admission_date  DATE )")

    local_cursor = local_stream.cursor(buffered=True)
    local_data_query1 = "SELECT * FROM admissions WHERE location = 'Nairobi' "

    local_cursor.execute(local_data_query1)
    local_adm_query_results = local_cursor.fetchall()
    
    #Insert data into Mariadb site Ubuntu
    upstream_clear = "DELETE FROM admissions"  #clear table to ensure no duplicates 
    upstream_cursor.execute(upstream_clear)
    upstream_insert_adm_sql = """INSERT INTO admissions (admission_id, patient_id, doctor_id, ward_number, ward_name, location, admission_date) VALUES (%s, %s, %s, %s, %s, %s, %s)"""
    upstream_cursor.executemany(upstream_insert_adm_sql, local_adm_query_results)
    upstream.commit()

     #Display the resulting fragment from query 2
if __name__ == "__main__":
    #init_fragment()
    upstream_cursor.execute("USE hospital_management")
    upstream_data_query1 = "SELECT * FROM admissions WHERE location = 'Nairobi'"
    
    print("Query 2: Primary Fragmentation: location = Nairobi")
    upstream_cursor.execute(upstream_data_query1)
    upstream_adm_query_results = upstream_cursor.fetchall()
    print(upstream_adm_query_results)
    #print results from Mariadb





