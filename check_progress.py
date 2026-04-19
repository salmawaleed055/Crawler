import mysql.connector

config = {
    'host': 'mysql-102a440b-aucegypt-ada5.g.aivencloud.com',
    'user': 'avnadmin',
    'password': 'AVNS_zPM-1nu_PwPJQvgb3JT',
    'database': 'defaultdb',
    'port': 25158,
}

conn = mysql.connector.connect(**config)
cursor = conn.cursor()

tables_to_check = ['Dataset', 'Dataset_Topics', 'Dataset_Tags', 'Topic', 'Tag']
print('=== Current Table Counts ===')
for table in tables_to_check:
    try:
        cursor.execute('SELECT COUNT(*) FROM ' + table)
        count = cursor.fetchone()[0]
        print(table + ': ' + str(count) + ' records')
    except:
        print(table + ': Table does not exist')

cursor.close()
conn.close()
