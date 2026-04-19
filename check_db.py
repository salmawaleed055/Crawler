import mysql.connector

config = {
    'host': 'mysql-102a440b-aucegypt-ada5.g.aivencloud.com',
    'user': 'avnadmin',
    'password': 'AVNS_zPM-1nu_PwPJQvgb3JT',
    'database': 'defaultdb',
    'port': 25158,
}

try:
    conn = mysql.connector.connect(**config)
    cursor = conn.cursor()
    
    cursor.execute('SHOW TABLES')
    tables = cursor.fetchall()
    table_names = [t[0] for t in tables]
    
    print('TABLES IN DEFAULTDB')
    print('Total tables: ' + str(len(table_names)))
    
    for table_name in table_names:
        print('\n' + '='*80)
        print('TABLE: ' + table_name)
        print('='*80)
        
        cursor.execute('DESCRIBE ' + table_name)
        columns = cursor.fetchall()
        
        print('Field'.ljust(30) + 'Type'.ljust(30) + 'Null'.ljust(8) + 'Key'.ljust(8) + 'Default'.ljust(15) + 'Extra'.ljust(20))
        print('-'*120)
        
        for col in columns:
            field = col[0]
            col_type = str(col[1])
            null_val = col[2]
            key = col[3] if col[3] else ''
            default = str(col[4]) if col[4] else 'NULL'
            extra = col[5] if col[5] else ''
            print(field.ljust(30) + col_type.ljust(30) + null_val.ljust(8) + key.ljust(8) + default.ljust(15) + extra.ljust(20))
        
        cursor.execute('SELECT COUNT(*) FROM ' + table_name)
        count = cursor.fetchone()[0]
        print('Row count: ' + str(count))
    
    print('\n' + '='*80)
    print('SUMMARY')
    print('='*80)
    for table_name in table_names:
        cursor.execute('SELECT COUNT(*) FROM ' + table_name)
        count = cursor.fetchone()[0]
        cursor.execute('DESCRIBE ' + table_name)
        col_count = len(cursor.fetchall())
        print(table_name.ljust(30) + 'Columns: ' + str(col_count).ljust(5) + 'Rows: ' + str(count))
    
    cursor.close()
    conn.close()
    
except Exception as e:
    print('Error: ' + str(e))
    import traceback
    traceback.print_exc()
