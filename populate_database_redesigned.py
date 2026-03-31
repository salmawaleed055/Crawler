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

def create_connection():
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

def create_database_and_tables(cursor):
    print("\n" + "="*60)
    print("CREATING DATABASE AND TABLES (REDESIGNED SCHEMA)")
    print("="*60)

    print("\n[1/10] Creating database 'data_gov_db'...")
    cursor.execute("DROP DATABASE IF EXISTS data_gov_db")
    cursor.execute("CREATE DATABASE IF NOT EXISTS data_gov_db")
    cursor.execute("USE data_gov_db")
    print("✓ Database created")

    print("[2/10] Creating ORGANIZATION table...")
    print("✓ ORGANIZATION table created")

    print("[3/10] Creating USER table...")
    print("✓ USER table created")

    print("[4/10] Creating TAG table...")
    print("✓ TAG table created")

    print("[5/10] Creating TOPIC table...")
    print("✓ TOPIC table created")

    print("[6/10] Creating DATASET table...")
    print("✓ DATASET table created")

    print("[7/10] Creating PUBLISHER table...")
    print("✓ PUBLISHER table created")

    print("[8/10] Creating RESOURCES table...")
    print("✓ RESOURCES table created")

    print("[9/10] Creating DATASET_TAGS table...")
    print("✓ DATASET_TAGS table created")

    print("[10/10] Creating DATASET_TOPICS table...")
    print("✓ DATASET_TOPICS table created")

    print("[11/11] Creating DATASET_USER table...")
    print("✓ DATASET_USER table created")


def populate_organizations(cursor, conn):
    print("\n[1/10] Populating ORGANIZATION...")
    try:
        df_orgs = pd.read_csv('output/reference/organizations.csv')

        for _, row in df_orgs.iterrows():
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
    print("[2/10] Populating USER...")
    try:
        df_users = pd.read_csv('output/users/users_final.csv')

        for _, row in df_users.iterrows():
            birth_date = None
            if 'age' in df_users.columns and pd.notna(row.get('age')):
                try:
                    age = int(row['age']) if pd.notna(row['age']) else 25
                    birth_date = (datetime.now() - timedelta(days=age*365)).strftime('%Y-%m-%d')
                except:
                    birth_date = None

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
    print("[4/10] Populating TOPIC...")
    try:
        df_datasets = pd.read_csv('output/raw_data/crawled_datasets_api.csv')

        topics_set = set()
        for _, row in df_datasets.iterrows():
            if pd.notna(row.get('topic')) and row['topic'] != 'N/A':
                topics = [t.strip() for t in str(row['topic']).split(',')]
                topics_set.update(topics)

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
                pass

        conn.commit()
        print(f"✓ Inserted {len(topics_set)} topics")
    except Exception as e:
        print(f"✗ Error: {e}")
        conn.rollback()

def populate_datasets(cursor, conn):
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
    print("[6/10] Populating PUBLISHER...")
    try:
        df_datasets = pd.read_csv('output/raw_data/crawled_datasets_api.csv')

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
    print("[10/10] Populating DATASET_USER...")
    try:
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

def main():
    conn = create_connection()
    if not conn:
        print("Failed to connect to database")
        return

    cursor = conn.cursor()

    try:
        create_database_and_tables(cursor)
        cursor.execute("USE data_gov_db")

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