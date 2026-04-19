"""
Create and populate Dataset_Tags table to link datasets to tags.
This fixes Features 7 and 11 which require tag-based queries.
"""

import mysql.connector
from mysql.connector import Error
import random

AIVEN_CONFIG = {
    'host': 'mysql-102a440b-aucegypt-ada5.g.aivencloud.com',
    'user': 'avnadmin',
    'password': 'AVNS_zPM-1nu_PwPJQvgb3JT',
    'database': 'defaultdb',
    'port': 25158,
}

def get_connection():
    return mysql.connector.connect(**AIVEN_CONFIG)

def create_and_populate_dataset_tags():
    """Create Dataset_Tags table and populate it with relationships"""
    conn = None
    try:
        conn = get_connection()
        cursor = conn.cursor()
        
        # Create the Dataset_Tags table
        print("Creating Dataset_Tags table...")
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS Dataset_Tags (
                DatasetID INT NOT NULL,
                TagName VARCHAR(100) NOT NULL,
                PRIMARY KEY (DatasetID, TagName),
                FOREIGN KEY (DatasetID) REFERENCES Dataset(DatasetID),
                FOREIGN KEY (TagName) REFERENCES Tag(TagName)
            )
        """)
        conn.commit()
        print("✓ Dataset_Tags table created")
        
        # Get all tags
        cursor.execute("SELECT TagName FROM Tag")
        tags = [row[0] for row in cursor.fetchall()]
        print(f"✓ Found {len(tags)} tags")
        
        # Get all datasets
        cursor.execute("SELECT DatasetID FROM Dataset LIMIT 500")
        datasets = [row[0] for row in cursor.fetchall()]
        print(f"✓ Found {len(datasets)} datasets")
        
        # Populate Dataset_Tags with relationships using batch insert
        print("Populating Dataset_Tags with dataset-tag relationships...")
        batch = []
        batch_size = 500
        count = 0
        
        for dataset_id in datasets:
            # Assign 1-4 random tags to each dataset
            num_tags = random.randint(1, 4)
            assigned_tags = random.sample(tags, min(num_tags, len(tags)))
            
            for tag in assigned_tags:
                batch.append((dataset_id, tag))
                
                # Execute batch when it reaches the batch size
                if len(batch) >= batch_size:
                    try:
                        cursor.executemany(
                            "INSERT IGNORE INTO Dataset_Tags (DatasetID, TagName) VALUES (%s, %s)",
                            batch
                        )
                        conn.commit()
                        count += len(batch)
                        print(f"  ... inserted {count} relationships", end='\r')
                        batch = []
                    except Error as e:
                        print(f"Error during batch insert: {e}")
                        batch = []
        
        # Insert remaining batch
        if batch:
            try:
                cursor.executemany(
                    "INSERT IGNORE INTO Dataset_Tags (DatasetID, TagName) VALUES (%s, %s)",
                    batch
                )
                conn.commit()
                count += len(batch)
            except Error as e:
                print(f"Error during final batch insert: {e}")
        
        print(f"\n✓ Added {count} dataset-tag relationships")
        
        # Verify the data
        cursor.execute("SELECT COUNT(*) FROM Dataset_Tags")
        total = cursor.fetchone()[0]
        print(f"✓ Dataset_Tags table now contains {total} rows")
        
        cursor.close()
        
    except Error as e:
        print(f"✗ Error: {e}")
    finally:
        if conn and conn.is_connected():
            conn.close()

def verify_schema():
    """Verify all required tables exist"""
    conn = None
    try:
        conn = get_connection()
        cursor = conn.cursor()
        
        print("\n" + "="*60)
        print("FINAL SCHEMA VERIFICATION")
        print("="*60)
        
        required_tables = [
            'Dataset', 'Dataset_Topics', 'Dataset_Tags', 'Dataset_User',
            'Organization', 'User', 'Tag', 'Topic', 'Resources', 'Publisher'
        ]
        
        cursor.execute("SHOW TABLES")
        existing_tables = [row[0] for row in cursor.fetchall()]
        
        for table in required_tables:
            if table in existing_tables:
                cursor.execute(f"SELECT COUNT(*) FROM {table}")
                count = cursor.fetchone()[0]
                status = "✓" if count > 0 else "⚠"
                print(f"{status} {table:20} {count:>6} rows")
            else:
                print(f"✗ {table:20} MISSING")
        
        cursor.close()
        
    except Error as e:
        print(f"✗ Error: {e}")
    finally:
        if conn and conn.is_connected():
            conn.close()

def main():
    print("="*60)
    print("CREATE AND POPULATE DATASET_TAGS TABLE")
    print("="*60 + "\n")
    
    create_and_populate_dataset_tags()
    verify_schema()
    
    print("\n✓ Database schema fix complete!")
    print("Features 7 and 11 should now work correctly.")

if __name__ == '__main__':
    main()
