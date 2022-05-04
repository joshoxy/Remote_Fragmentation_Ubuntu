#Query 2
#     upstream_cursor.execute("USE hospital_management")
#     upstream_cursor.execute("CREATE TABLE IF NOT EXISTS admissions (admission_id INT(11) NOT NULL AUTO_INCREMENT PRIMARY KEY, patient_id INT, doctor_id INT, ward_number INT, ward_name VARCHAR(255), location VARCHAR(255), admission_date  DATE )")

#     local_cursor = local_stream.cursor(buffered=True)
#     local_data_query1 = "SELECT * FROM admissions WHERE location = 'Nairobi'"

#     local_cursor.execute(local_data_query1)
#     local_adm_query_results = local_cursor.fetchall()
    
#     #Insert data into Mariadb site Ubuntu
#     upstream_insert_adm_sql = "INSERT INTO admissions (admission_id, patient_id, doctor_id, ward_number, ward_name, location, admission_date) VALUES (%s, %s, %s, %s, %s, %s, %s)"
#     upstream_cursor.executemany(upstream_insert_adm_sql, local_adm_query_results)
#     upstream.commit()

#      #Display the resulting fragment from query 2
# if __name__ == "__main__":
#     #init_fragment()
#     upstream_cursor.execute("USE hospital_management")
#     upstream_data_query1 = "SELECT * FROM admissions WHERE location = 'Nairobi'"
    
#     print("Query 2: Primary Fragmentation")
#     upstream_cursor.execute(upstream_data_query1)
#     upstream_adm_query_results = upstream_cursor.fetchall()
#     print(upstream_adm_query_results)
#     #print results from Mariadb