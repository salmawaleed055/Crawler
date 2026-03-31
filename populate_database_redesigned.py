#!/usr/bin/env python3
"""
NAME: Salma Elmarakby
ID: 900232658
"""

import mysql.connector
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import sys
import os

print("\n" + "="*70)
print(" "*10 + "DATABASE POPULATION - REDESIGNED SCHEMA")
print("="*70)

# ============================================================
# DATABASE CONNECTION SETUP
# ============================================================
def create_connection():
    """Create MySQL connection"""
    try:
        conn = mysql.connector.connect(
            host="127.0.0.1",
            port=3306,
            user="root",
            password="2552005Elmarakby$"
        )
        return conn
    except mysql.connector.Error as err:
        print(f"Error connecting to database: {err}")
        return None

# ============================================================
# DATABASE AND TABLES CREATION
# ============================================================
def create_database_and_tables(cursor):
    """Create database and all required tables with new schema"""
    print("\n" + "="*60)
    print("CREATING DATABASE AND TABLES (REDESIGNED SCHEMA)")
    print("="*60)
    
    # Create database
    print("\n[1/10] Creating database 'data_gov_db'...")
    cursor.execute("DROP DATABASE IF EXISTS data_gov_db")
    cursor.execute("CREATE DATABASE IF NOT EXISTS data_gov_db")
    cursor.execute("USE data_gov_db")
    print("✓ Database created")
    
    # 1. ORGANIZATION table
    print("[2/10] Creating ORGANIZATION table...")
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS Organization (
            OrganizationID INT AUTO_INCREMENT PRIMARY KEY,
            OrganizationName VARCHAR(255) NOT NULL UNIQUE,
            OrganizationType VARCHAR(100),
            ContactInformation VARCHAR(500),
            Description TEXT
        )
    """)
    print("✓ ORGANIZATION table created")
    
    # 2. USER table
    print("[3/10] Creating USER table...")
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS User (
            Email VARCHAR(255) PRIMARY KEY,
            Username VARCHAR(100) NOT NULL UNIQUE,
            Gender VARCHAR(50),
            BirthDate DATE,
            Country VARCHAR(100)
        )
    """)
    print("✓ USER table created")
    
    # 3. TAG table
    print("[4/10] Creating TAG table...")
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS Tag (
            TagName VARCHAR(100) PRIMARY KEY
        )
    """)
    print("✓ TAG table created")
    
    # 4. TOPIC table
    print("[5/10] Creating TOPIC table...")
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS Topic (
            TopicName VARCHAR(100) PRIMARY KEY,
            Category VARCHAR(100)
        )
    """)
    print("✓ TOPIC table created")
    
    # 5. DATASET table
    print("[6/10] Creating DATASET table...")
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS Dataset (
            DatasetID INT AUTO_INCREMENT PRIMARY KEY,
            DatasetName VARCHAR(500) NOT NULL,
            Description TEXT,
            AccessLevel VARCHAR(50),
            License VARCHAR(2500),
            MetadataCreateDate DATETIME,
            MetadataUpdateDate DATETIME,
            Maintainer VARCHAR(300),
            OrganizationID INT,
            FOREIGN KEY (OrganizationID) REFERENCES Organization(OrganizationID)
        )
    """)
    print("✓ DATASET table created")
    
    # 6. PUBLISHER table
    print("[7/10] Creating PUBLISHER table...")
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS Publisher (
            DatasetID INT,
            PublisherName VARCHAR(255),
            PRIMARY KEY (DatasetID, PublisherName),
            FOREIGN KEY (DatasetID) REFERENCES Dataset(DatasetID)
        )
    """)
    print("✓ PUBLISHER table created")
    
    # 7. RESOURCES table
    print("[8/10] Creating RESOURCES table...")
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS Resources (
            URL VARCHAR(500) PRIMARY KEY,
            Format VARCHAR(100),
            DatasetID INT NOT NULL,
            FOREIGN KEY (DatasetID) REFERENCES Dataset(DatasetID)
        )
    """)
    print("✓ RESOURCES table created")
    
    # 8. DATASET_TAGS junction table
    print("[9/10] Creating DATASET_TAGS table...")
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS Dataset_Tags (
            DatasetID INT,
            TagName VARCHAR(100),
            PRIMARY KEY (DatasetID, TagName),
            FOREIGN KEY (DatasetID) REFERENCES Dataset(DatasetID),
            FOREIGN KEY (TagName) REFERENCES Tag(TagName)
        )
    """)
    print("✓ DATASET_TAGS table created")
    
    # 9. DATASET_TOPICS junction table
    print("[10/10] Creating DATASET_TOPICS table...")
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS Dataset_Topics (
            DatasetID INT,
            TopicName VARCHAR(100),
            PRIMARY KEY (DatasetID, TopicName),
            FOREIGN KEY (DatasetID) REFERENCES Dataset(DatasetID),
            FOREIGN KEY (TopicName) REFERENCES Topic(TopicName)
        )
    """)
    print("✓ DATASET_TOPICS table created")
    
    # 10. DATASET_USER junction table
    print("[11/11] Creating DATASET_USER table...")
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS Dataset_User (
            Email VARCHAR(255),
            DatasetID INT,
            ProjectName VARCHAR(255),
            ProjectCategory VARCHAR(100),
            PRIMARY KEY (Email, DatasetID, ProjectName),
            FOREIGN KEY (Email) REFERENCES User(Email),
            FOREIGN KEY (DatasetID) REFERENCES Dataset(DatasetID)
        )
    """)
    print("✓ DATASET_USER table created")

# ============================================================
# DATA POPULATION FUNCTIONS
# ============================================================

def populate_organizations(cursor, conn):
    """Populate Organization table"""
    print("\n[1/10] Populating ORGANIZATION...")
    try:
        df_orgs = pd.read_csv('output/reference/organizations.csv')
        
        for _, row in df_orgs.iterrows():
            query = """
                INSERT INTO Organization (OrganizationID, OrganizationName, OrganizationType, ContactInformation, Description)
                VALUES (%s, %s, %s, %s, %s)
            """
            try:
                cursor.execute(query, (
                    int(row['organization_id']),
                    str(row['organization_name']),
                    str(row.get('organization_type', 'Government')),
                    str(row.get('contact_information', 'N/A')),
                    str(row.get('description', ''))
                ))
            except Exception as e:
                continue
        
        conn.commit()
        print(f"✓ Inserted organizations")
    except Exception as e:
        print(f"✗ Error: {e}")
        conn.rollback()

def populate_users(cursor, conn):
    """Populate User table"""
    print("[2/10] Populating USER...")
    try:
        df_users = pd.read_csv('output/users/users_final.csv')
        
        for _, row in df_users.iterrows():
            # Calculate birth date from age (approximate)
            birth_date = None
            if 'age' in df_users.columns and pd.notna(row.get('age')):
                try:
                    age = int(row['age']) if pd.notna(row['age']) else 25
                    birth_date = (datetime.now() - timedelta(days=age*365)).strftime('%Y-%m-%d')
                except:
                    birth_date = None
            
            query = """
                INSERT INTO User (Email, Username, Gender, BirthDate, Country)
                VALUES (%s, %s, %s, %s, %s)
            """
            cursor.execute(query, (
                row['email'],
                row.get('username', 'user'),
                row.get('gender', 'N/A'),
                birth_date,
                row.get('country', 'N/A')
            ))
        
        conn.commit()
        print(f"✓ Inserted {len(df_users)} users")
    except Exception as e:
        print(f"✗ Error: {e}")
        conn.rollback()

def populate_tags(cursor, conn):
    """Populate Tag table"""
    print("[3/10] Populating TAG...")
    try:
        df_tags = pd.read_csv('output/reference/tags.csv')
        
        for _, row in df_tags.iterrows():
            query = "INSERT INTO Tag (TagName) VALUES (%s)"
            cursor.execute(query, (row['tag_name'],))
        
        conn.commit()
        print(f"✓ Inserted {len(df_tags)} tags")
    except Exception as e:
        print(f"✗ Error: {e}")
        conn.rollback()

def populate_topics(cursor, conn):
    """Populate Topic table from datasets"""
    print("[4/10] Populating TOPIC...")
    try:
        df_datasets = pd.read_csv('output/raw_data/crawled_datasets_api.csv')
        
        topics_set = set()
        for _, row in df_datasets.iterrows():
            if pd.notna(row.get('topic')) and row['topic'] != 'N/A':
                topics = [t.strip() for t in str(row['topic']).split(',')]
                topics_set.update(topics)
        
        # Generate categories based on topic name
        categories_map = {
            'health': 'Health & Medicine',
            'crime': 'Public Safety',
            'infrastructure': 'Infrastructure',
            'environment': 'Environment',
            'economy': 'Finance & Economy',
            'education': 'Education',
            'agriculture': 'Agriculture',
            'transportation': 'Transportation',
            'energy': 'Energy',
            'weather': 'Weather & Climate'
        }
        
        for topic in topics_set:
            category = 'General'
            for keyword, cat in categories_map.items():
                if keyword.lower() in topic.lower():
                    category = cat
                    break
            
            query = "INSERT INTO Topic (TopicName, Category) VALUES (%s, %s)"
            try:
                cursor.execute(query, (topic, category))
            except:
                pass  # Skip duplicates
        
        conn.commit()
        print(f"✓ Inserted {len(topics_set)} topics")
    except Exception as e:
        print(f"✗ Error: {e}")
        conn.rollback()

def populate_datasets(cursor, conn):
    """Populate Dataset table"""
    print("[5/10] Populating DATASET...")
    try:
        df_datasets = pd.read_csv('output/raw_data/crawled_datasets_api.csv')
        df_orgs = pd.read_csv('output/reference/organizations.csv')
        org_mapping = dict(zip(df_orgs['organization_name'], df_orgs['organization_id']))
        
        inserted = 0
        for _, row in df_datasets.iterrows():
            try:
                org_name = str(row.get('organization', 'N/A'))
                org_id = org_mapping.get(org_name, None)
                
                description = str(row.get('description', 'N/A')) if pd.notna(row.get('description')) else 'N/A'
                description = description[:500] if len(description) > 500 else description
                
                license_val = str(row.get('license', 'N/A')) if pd.notna(row.get('license')) else 'N/A'
                license_val = license_val[:2500] if len(license_val) > 2500 else license_val
                
                maintainer = str(row.get('maintainer', 'N/A')) if pd.notna(row.get('maintainer')) else 'N/A'
                maintainer = maintainer[:300] if len(maintainer) > 300 else maintainer
                
                query = """
                    INSERT INTO Dataset (DatasetName, Description, AccessLevel, License, MetadataCreateDate, MetadataUpdateDate, Maintainer, OrganizationID)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                """
                cursor.execute(query, (
                    str(row['name'])[:500],
                    description,
                    str(row.get('access_level', 'public')),
                    license_val,
                    row.get('creation_date') if pd.notna(row.get('creation_date')) else None,
                    row.get('metadata_modified') if pd.notna(row.get('metadata_modified')) else None,
                    maintainer,
                    org_id
                ))
                inserted += 1
            except:
                continue
        
        conn.commit()
        print(f"✓ Inserted {inserted} datasets")
        return inserted
    except Exception as e:
        print(f"✗ Error: {e}")
        conn.rollback()
        return 0

def populate_publishers(cursor, conn, num_datasets):
    """Populate Publisher table"""
    print("[6/10] Populating PUBLISHER...")
    try:
        df_datasets = pd.read_csv('output/raw_data/crawled_datasets_api.csv')
        
        # Get dataset IDs
        cursor.execute("SELECT DatasetID, Maintainer FROM Dataset")
        dataset_maintainers = cursor.fetchall()
        
        inserted = 0
        for dataset_id, maintainer in dataset_maintainers[:num_datasets]:
            publisher_name = maintainer.split(',')[0][:255] if maintainer and maintainer != 'N/A' else 'Unknown'
            
            query = "INSERT INTO Publisher (DatasetID, PublisherName) VALUES (%s, %s)"
            try:
                cursor.execute(query, (dataset_id, publisher_name))
                inserted += 1
            except:
                pass
        
        conn.commit()
        print(f"✓ Inserted {inserted} publishers")
    except Exception as e:
        print(f"✗ Error: {e}")
        conn.rollback()

def populate_resources(cursor, conn, num_datasets):
    """Populate Resources table"""
    print("[7/10] Populating RESOURCES...")
    try:
        df_datasets = pd.read_csv('output/raw_data/crawled_datasets_api.csv')
        
        inserted = 0
        for idx, row in df_datasets.iterrows():
            if idx >= num_datasets:
                break
            if pd.notna(row.get('formats')) and row.get('formats') != 'N/A':
                formats = str(row['formats']).split(',')
                dataset_url = str(row.get('url', 'N/A'))
                
                for fmt in formats:
                    fmt = fmt.strip()[:100]
                    if fmt and fmt != 'N/A':
                        resource_url = f"{dataset_url}?format={fmt}"
                        
                        query = "INSERT INTO Resources (URL, Format, DatasetID) VALUES (%s, %s, %s)"
                        try:
                            cursor.execute(query, (resource_url[:500], fmt, idx + 1))
                            inserted += 1
                        except:
                            pass
        
        conn.commit()
        print(f"✓ Inserted {inserted} resources")
    except Exception as e:
        print(f"✗ Error: {e}")
        conn.rollback()

def populate_dataset_tags(cursor, conn, num_datasets):
    """Populate Dataset_Tags junction table"""
    print("[8/10] Populating DATASET_TAGS...")
    try:
        df = pd.read_csv('output/datasets/dataset_tags.csv')
        df['dataset_id'] = df['dataset_id'].astype(int)
        
        inserted = 0
        for _, row in df[df['dataset_id'] <= num_datasets].iterrows():
            query = "INSERT INTO Dataset_Tags (DatasetID, TagName) VALUES (%s, %s)"
            try:
                cursor.execute(query, (int(row['dataset_id']), row['tag_name']))
                inserted += 1
            except:
                pass
        
        conn.commit()
        print(f"✓ Inserted {inserted} dataset-tag relationships")
    except Exception as e:
        print(f"✗ Error: {e}")
        conn.rollback()

def populate_dataset_topics(cursor, conn, num_datasets):
    """Populate Dataset_Topics junction table"""
    print("[9/10] Populating DATASET_TOPICS...")
    try:
        df_datasets = pd.read_csv('output/raw_data/crawled_datasets_api.csv')
        
        cursor.execute("SELECT DatasetID FROM Dataset LIMIT %s", (num_datasets,))
        dataset_ids = [row[0] for row in cursor.fetchall()]
        
        inserted = 0
        for idx, dataset_id in enumerate(dataset_ids):
            if idx < len(df_datasets):
                topics = str(df_datasets.iloc[idx].get('topic', 'N/A')).split(',')
                
                for topic in topics:
                    topic = topic.strip()
                    if topic and topic != 'N/A':
                        query = "INSERT INTO Dataset_Topics (DatasetID, TopicName) VALUES (%s, %s)"
                        try:
                            cursor.execute(query, (dataset_id, topic))
                            inserted += 1
                        except:
                            pass
        
        conn.commit()
        print(f"✓ Inserted {inserted} dataset-topic relationships")
    except Exception as e:
        print(f"✗ Error: {e}")
        conn.rollback()

def populate_dataset_user(cursor, conn, num_datasets):
    """Populate Dataset_User junction table"""
    print("[10/10] Populating DATASET_USER...")
    try:
        # Create project assignments for users and datasets
        cursor.execute("SELECT Email FROM User LIMIT 100")
        users = [row[0] for row in cursor.fetchall()]
        
        cursor.execute(f"SELECT DatasetID FROM Dataset LIMIT {num_datasets}")
        datasets = [row[0] for row in cursor.fetchall()]
        
        if not users or not datasets:
            print(f"✓ Inserted 0 user-dataset relationships (no data)")
            return
        
        projects = [
            "Data Analysis", "Research Project", "Visualization",
            "Analytics Dashboard", "Public Study", "Policy Research"
        ]
        categories = ["Science", "Business", "Academic", "Government", "Public Service"]
        
        inserted = 0
        np.random.seed(42)
        for user_email in users:
            # Assign 2-5 datasets per user
            num_user_datasets = min(5, max(2, len(datasets)//max(1, len(users))))
            if num_user_datasets > 0 and len(datasets) > 0:
                user_datasets = np.random.choice(datasets, size=min(num_user_datasets, len(datasets)), replace=False)
                
                for dataset_id in user_datasets:
                    project = np.random.choice(projects)
                    category = np.random.choice(categories)
                    
                    query = "INSERT INTO Dataset_User (Email, DatasetID, ProjectName, ProjectCategory) VALUES (%s, %s, %s, %s)"
                    try:
                        cursor.execute(query, (user_email, int(dataset_id), project, category))
                        inserted += 1
                    except:
                        pass
        
        conn.commit()
        print(f"✓ Inserted {inserted} user-dataset relationships")
    except Exception as e:
        print(f"✗ Error: {e}")
        conn.rollback()

# ============================================================
# MAIN EXECUTION
# ============================================================
def main():
    conn = create_connection()
    if not conn:
        print("Failed to connect to database")
        return
    
    cursor = conn.cursor()
    
    try:
        # Create tables
        create_database_and_tables(cursor)
        cursor.execute("USE data_gov_db")
        
        # Populate tables
        print("\n" + "="*60)
        print("POPULATING DATA")
        print("="*60)
        
        populate_organizations(cursor, conn)
        populate_users(cursor, conn)
        populate_tags(cursor, conn)
        populate_topics(cursor, conn)
        num_datasets = populate_datasets(cursor, conn)
        populate_publishers(cursor, conn, num_datasets)
        populate_resources(cursor, conn, num_datasets)
        populate_dataset_tags(cursor, conn, num_datasets)
        populate_dataset_topics(cursor, conn, num_datasets)
        populate_dataset_user(cursor, conn, num_datasets)
        
        # Final summary
        print("\n" + "="*60)
        print("DATABASE POPULATION COMPLETE!")
        print("="*60)
        print("\n✓ Tables created: 11")
        print("✓ Data populated successfully")
        print("\nDatabase: data_gov_db")
        print("Tables: Organization, User, Tag, Topic, Dataset, Publisher, Resources, Dataset_Tags, Dataset_Topics, Dataset_User")
        
    except Exception as e:
        print(f"✗ Error: {e}")
    finally:
        cursor.close()
        conn.close()

if __name__ == "__main__":
    main()
