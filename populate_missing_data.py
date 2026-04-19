"""
Populate missing data for testing all 11 features.
This script adds sample data to empty tables.
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

def populate_topics():
    """Populate Topic table with sample data"""
    conn = None
    try:
        conn = get_connection()
        cursor = conn.cursor()
        
        topics = [
            ('Agriculture', 'Food and Agriculture'),
            ('Energy', 'Energy and Utilities'),
            ('Environment', 'Environmental Data'),
            ('Health', 'Health and Medicine'),
            ('Transportation', 'Transportation Systems'),
            ('Education', 'Education and Training'),
            ('Finance', 'Financial Data'),
            ('Housing', 'Housing and Development'),
            ('Science', 'Science and Research'),
            ('Technology', 'Technology and Innovation'),
        ]
        
        print("Populating Topic table...")
        for topic_name, category in topics:
            cursor.execute(
                "INSERT IGNORE INTO Topic (TopicName, Category) VALUES (%s, %s)",
                (topic_name, category)
            )
        conn.commit()
        cursor.close()
        print(f"✓ Added {len(topics)} topics")
        
    except Error as e:
        print(f"✗ Error populating topics: {e}")
    finally:
        if conn and conn.is_connected():
            conn.close()

def populate_dataset_topics():
    """Populate Dataset_Topics junction table"""
    conn = None
    try:
        conn = get_connection()
        cursor = conn.cursor()
        
        # Get all topics
        cursor.execute("SELECT TopicName FROM Topic")
        topics = [row[0] for row in cursor.fetchall()]
        
        # Get all datasets
        cursor.execute("SELECT DatasetID FROM Dataset LIMIT 100")
        datasets = [row[0] for row in cursor.fetchall()]
        
        print("Populating Dataset_Topics junction table...")
        count = 0
        for dataset_id in datasets:
            # Assign 1-3 random topics to each dataset
            num_topics = random.randint(1, 3)
            assigned_topics = random.sample(topics, min(num_topics, len(topics)))
            
            for topic in assigned_topics:
                cursor.execute(
                    "INSERT IGNORE INTO Dataset_Topics (DatasetID, TopicName) VALUES (%s, %s)",
                    (dataset_id, topic)
                )
                count += 1
        
        conn.commit()
        cursor.close()
        print(f"✓ Added {count} dataset-topic relationships")
        
    except Error as e:
        print(f"✗ Error populating dataset topics: {e}")
    finally:
        if conn and conn.is_connected():
            conn.close()

def populate_resources():
    """Populate Resources table with sample data"""
    conn = None
    try:
        conn = get_connection()
        cursor = conn.cursor()
        
        formats = ['CSV', 'JSON', 'XML', 'API', 'RDF', 'XLSX', 'PDF', 'HTML']
        
        # Get all datasets
        cursor.execute("SELECT DatasetID FROM Dataset LIMIT 100")
        datasets = [row[0] for row in cursor.fetchall()]
        
        print("Populating Resources table...")
        count = 0
        for idx, dataset_id in enumerate(datasets):
            # Create 1-2 resources per dataset
            num_resources = random.randint(1, 2)
            for res_idx in range(num_resources):
                url = f"https://example.com/dataset/{dataset_id}/resource_{res_idx}.data"
                format_type = random.choice(formats)
                
                cursor.execute(
                    "INSERT IGNORE INTO Resources (URL, Format, DatasetID) VALUES (%s, %s, %s)",
                    (url, format_type, dataset_id)
                )
                count += 1
        
        conn.commit()
        cursor.close()
        print(f"✓ Added {count} resources")
        
    except Error as e:
        print(f"✗ Error populating resources: {e}")
    finally:
        if conn and conn.is_connected():
            conn.close()

def populate_publishers():
    """Populate Publisher table with sample data"""
    conn = None
    try:
        conn = get_connection()
        cursor = conn.cursor()
        
        publishers = [
            'Data.gov',
            'Federal Statistics Office',
            'National Institute of Standards',
            'Bureau of Labor Statistics',
            'EPA',
            'USGS',
            'Census Bureau',
            'NIH',
            'NSF',
        ]
        
        # Get all datasets
        cursor.execute("SELECT DatasetID FROM Dataset LIMIT 100")
        datasets = [row[0] for row in cursor.fetchall()]
        
        print("Populating Publisher table...")
        count = 0
        for dataset_id in datasets:
            publisher = random.choice(publishers)
            cursor.execute(
                "INSERT IGNORE INTO Publisher (DatasetID, PublisherName) VALUES (%s, %s)",
                (dataset_id, publisher)
            )
            count += 1
        
        conn.commit()
        cursor.close()
        print(f"✓ Added {count} publisher records")
        
    except Error as e:
        print(f"✗ Error populating publishers: {e}")
    finally:
        if conn and conn.is_connected():
            conn.close()

def verify_data():
    """Verify all tables have data"""
    conn = None
    try:
        conn = get_connection()
        cursor = conn.cursor()
        
        tables = ['Topic', 'Dataset_Topics', 'Resources', 'Publisher', 'User', 'Organization', 'Tag']
        
        print("\n" + "="*60)
        print("TABLE DATA VERIFICATION")
        print("="*60)
        
        for table_name in tables:
            cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
            count = cursor.fetchone()[0]
            status = "✓" if count > 0 else "✗"
            print(f"{status} {table_name:20} {count:>8} rows")
        
        cursor.close()
        
    except Error as e:
        print(f"✗ Error verifying data: {e}")
    finally:
        if conn and conn.is_connected():
            conn.close()

def main():
    print("="*60)
    print("POPULATING MISSING DATA FOR FEATURE TESTING")
    print("="*60 + "\n")
    
    populate_topics()
    populate_dataset_topics()
    populate_resources()
    populate_publishers()
    
    verify_data()
    
    print("\n✓ Data population complete!")

if __name__ == '__main__':
    main()
