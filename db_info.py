
import mysql.connector
config = {'host': 'mysql-102a440b-aucegypt-ada5.g.aivencloud.com', 'user': 'avnadmin', 'password': 'AVNS_zPM-1nu_PwPJQvgb3JT', 'database': 'defaultdb', 'port': 25158}
conn = mysql.connector.connect(**config)
cursor = conn.cursor()
cursor.execute('SHOW TABLES')
tables = cursor.fetchall()
print('=== TABLES ===')
for t in tables:
    table_name = t[0]
    cursor.execute('DESCRIBE ' + table_name)
    cols = cursor.fetchall()
    cursor.execute('SELECT COUNT(*) FROM ' + table_name)
    count = cursor.fetchone()[0]
    print(f'{table_name}: {len(cols)} columns, {count} rows')
    for col in cols:
        print(f'  {col[0]}: {col[1]}')
conn.close()

