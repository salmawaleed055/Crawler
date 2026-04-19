import mysql.connector
import pandas as pd
import time

config = {
    'host': 'mysql-102a440b-aucegypt-ada5.g.aivencloud.com',
    'user': 'avnadmin',
    'password': 'AVNS_zPM-1nu_PwPJQvgb3JT',
    'database': 'defaultdb',
    'port': 25158,
}

start_time = time.time()
print("Starting optimized Dataset_Topics population...")

try:
    conn = mysql.connector.connect(**config)
    cursor = conn.cursor()

    # Read CSV
    print("Loading CSV...")
    df = pd.read_csv('Database Tables CSV/Dataset_topic.csv')
    print(f"  Loaded {len(df)} rows")
    print(f"  Unique topics: {df['TopicName'].nunique()}")

    # Clear existing data
    print("\nClearing Dataset_Topics table...")
    cursor.execute('TRUNCATE TABLE Dataset_Topics')
    conn.commit()
    print("  Cleared")

    # Get unique topics and insert into Topic table (batch)
    print("\nInserting unique topics into Topic table...")
    unique_topics = sorted(df['TopicName'].unique().tolist())
    topic_rows = [(t,) for t in unique_topics]
    
    for i in range(0, len(topic_rows), 5000):
        batch = topic_rows[i:i+5000]
        cursor.executemany('INSERT IGNORE INTO Topic (TopicName) VALUES (%s)', batch)
        conn.commit()
    print(f"  Inserted {len(unique_topics)} topics")

    # Prepare dataset-topic rows
    print("\nPreparing dataset-topic relationships...")
    dt_rows = []
    for _, row in df.iterrows():
        dt_rows.append((int(row['DatasetID']), str(row['TopicName'])))
    print(f"  Prepared {len(dt_rows)} relationships")

    # Batch insert all dataset-topic relationships (5000 rows per batch)
    print("\nInserting dataset-topic relationships (batch size: 5000)...")
    batch_size = 5000
    batch_count = (len(dt_rows) + batch_size - 1) // batch_size
    
    for i in range(0, len(dt_rows), batch_size):
        batch = dt_rows[i:i+batch_size]
        cursor.executemany(
            'INSERT IGNORE INTO Dataset_Topics (DatasetID, TopicName) VALUES (%s, %s)',
            batch
        )
        conn.commit()
        batch_num = (i // batch_size) + 1
        elapsed = time.time() - start_time
        print(f"  Batch {batch_num}/{batch_count} inserted ({len(batch)} rows) - {elapsed:.1f}s elapsed")

    # Verify
    cursor.execute('SELECT COUNT(*) FROM Dataset_Topics')
    count = cursor.fetchone()[0]
    elapsed = time.time() - start_time
    
    print(f"\n✓ Complete in {elapsed:.1f} seconds")
    print(f"✓ Dataset_Topics: {count} records")

    cursor.execute('SELECT COUNT(*) FROM Topic')
    topic_count = cursor.fetchone()[0]
    print(f"✓ Topic: {topic_count} records")

    cursor.close()
    conn.close()
    
except Exception as e:
    print(f"Error: {e}")
    import traceback
    traceback.print_exc()
