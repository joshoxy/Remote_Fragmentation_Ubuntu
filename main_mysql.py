# importing required library
import mysql.connector

#Site running on Windows with mysql. It stores the fragments for Derived Horizontal Fragmentation and Vertical Fragmentation

# connecting to the database
dataBase = mysql.connector.connect(
					host = "localhost",
					user = "root",
					passwd = "",
					database = "hospital_management")

# preparing a cursor object
cursorObject = dataBase.cursor(buffered=True)

print("Derived Horizontal")  #loc1 table picks admissions where location = nairobi
#print("LOC1:")
#Create fragment loc1 where location is Nairobi
#cursorObject.execute("CREATE TABLE LOC1 (SELECT * FROM admissions WHERE location = 'Nairobi')")  #Query to create fragment LOC1 WHERE location = 'Nairobi";
# query2 = "SELECT * FROM admissions INNER JOIN loc1 ON admissions.admission_id = loc1.admission_id WHERE loc1.ward_name = 'White House'"
# cursorObject.execute(query2)

#create white_house_table fragment by doing inner join on admissions and loc1
cursorObject.execute("CREATE TABLE IF NOT EXISTS white_house_table (SELECT admissions.admission_id, admissions.ward_name, admissions.patient_id FROM admissions INNER JOIN loc1 ON admissions.admission_id = loc1.admission_id WHERE loc1.ward_name = 'White House')")
cursorObject.execute("SELECT * FROM white_house_table")
query_two_result = cursorObject.fetchall()

print("white_house_table: ")
print(query_two_result)

# print("LOC2:")  #loc2 table picks admissions where location != nairobi
# cursorObject.execute("CREATE TABLE LOC2 (SELECT * FROM admissions WHERE location != 'Nairobi')")  #Query to create fragment LOC2 WHERE location != 'Nairobi";
# query3 = "SELECT * FROM admissions INNER JOIN loc2 ON admissions.admission_id = loc2.admission_id WHERE loc2.ward_name = 'St Peters'"
# cursorObject.execute(query3)
# query_three_result = cursorObject.fetchall()
# print(query_three_result)
#original

#create st_peters fragment by doing innrer join on admissions and loc2
cursorObject.execute("CREATE TABLE IF NOT EXISTS st_peters_table (SELECT admissions.admission_id, admissions.ward_name, admissions.patient_id FROM admissions INNER JOIN loc2 ON admissions.admission_id = loc2.admission_id WHERE loc2.ward_name = 'St Peters')")
cursorObject.execute("SELECT * FROM st_peters_table")
query_three_result = cursorObject.fetchall()

print("st_peters_table: ")
print(query_three_result)


print("Veritcal")
print("Q1 = :")
query4 = "SELECT ID, first_name, location FROM patients"
#cursorObject.execute("CREATE TABLE patients_q1 (SELECT ID, first_name, location FROM patients)")
cursorObject.execute(query4)
query_four_result = cursorObject.fetchall()
print(query_four_result)

print("Q2 = :")
query5 = "SELECT admission_id, patient_id, ward_name FROM admissions"
#cursorObject.execute("CREATE TABLE ward_q2 (SELECT admission_id, patient_id, ward_name FROM admissions)")
cursorObject.execute(query5)
query_five_result = cursorObject.fetchall()
print(query_five_result)

print("Q3 = :")
query6 = "SELECT doctor_id, ward_name FROM admissions"
#cursorObject.execute("CREATE TABLE ward_q3 (SELECT doctor_id, ward_name FROM admissions)")
cursorObject.execute(query6)
query_six_result = cursorObject.fetchall()
print(query_six_result)

print("Q4 = :")
query7 = "SELECT ID, first_name FROM patients WHERE location = 'Nairobi'"
#cursorObject.execute("CREATE TABLE patients_q4 (SELECT ID, first_name FROM patients WHERE location = 'Nairobi')")
cursorObject.execute(query7)
query_seven_result = cursorObject.fetchall()
print(query_seven_result)

myresult = cursorObject.fetchall()
  
for x in myresult:
    print(x)
  
# disconnecting from server
dataBase.close()
