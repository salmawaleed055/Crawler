import mysql.connector

conn = mysql.connector.connect(
    host="127.0.0.1",
    port=3306,
    user="root",
    password="2552005Elmarakby$"
)
cursor = conn.cursor()
cursor.execute("CREATE DATABASE IF NOT EXISTS testdb")
cursor.execute("CREATE TABLE IF NOT EXISTS testdb.users "
"(id INT AUTO_INCREMENT PRIMARY KEY, " \
"name VARCHAR(255), " \
"email VARCHAR(255))")
