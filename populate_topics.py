"""
Populate Dataset_Topics table with actual topic data from CSV
"""

import mysql.connector
import pandas as pd

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
    
    print("Loading topics from CSV...")
    df = pd.read_csv('Database Tables CSV/Dataset_topic.csv')
    
    # Get unique topics first
    print(f"Found {len(df)} dataset-topic relationships")
    print(f"Unique topics: {df['topic'].nunique()}")
    
    # Clear existing data (optional - remove if you want to keep existing)
    print("\nClearing existing Dataset_Topics table...")
    cursor.execute('DELETE FROM Dataset_Topics')
    conn.commit()
    
    # Ensure all topics exist in Topic table
    print("Ensuring all topics exist in Topic table...")
    unique_topics = df['topic'].unique()
    for topic in unique_topics:
        cursor.execute(
            'INSERT IGNORE INTO Topic (TopicName) VALUES (%s)',
            (str(topic),)
        )
    conn.commit()
    print(f"  ✓ Inserted/verified {len(unique_topics)} unique topics")
    
    # Populate Dataset_Topics
    print("\nPopulating Dataset_Topics table...")
    rows = []
    for _, row in df.iterrows():
        dataset_id = int(row['dataset_id'])
        topic = str(row['topic'])
        rows.append((dataset_id, topic))
    
    # Batch insert
    for i in range(0, len(rows), 1000):
        batch = rows[i:i+1000]
        cursor.executemany(
            'INSERT IGNORE INTO Dataset_Topics (DatasetID, TopicName) VALUES (%s, %s)',
            batch
        )
        conn.commit()
        print(f"  ✓ Inserted batch {i//1000 + 1}")
    
    print(f"\n✓ Total dataset-topic relationships loaded: {len(rows)}")
    
    # Verify
    cursor.execute('SELECT COUNT(*) FROM Dataset_Topics')
    count = cursor.fetchone()[0]
    print(f"✓ Dataset_Topics table now contains: {count} records")
    
    cursor.execute('SELECT COUNT(*) FROM Topic')
    topic_count = cursor.fetchone()[0]
    print(f"✓ Topic table now contains: {topic_count} unique topics")
    
    cursor.close()
    conn.close()
    print("\n✓ Population complete!")
    
except Exception as e:
    print(f"Error: {e}")
    import traceback
    traceback.print_exc()
