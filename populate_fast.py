#!/usr/bin/env python3
import mysql.connector
from mysql.connector import Error
import pandas as pd
import os

AIVEN_CONFIG = {
    'host': 'mysql-102a440b-aucegypt-ada5.g.aivencloud.com',
    'user': 'avnadmin',
    'password': 'AVNS_zPM-1nu_PwPJQvgb3JT',
    'database': 'defaultdb',
    'port': 25158,
    'use_pure': True,
    'ssl_disabled': False,
    'ssl_verify_cert': False,
    'ssl_verify_identity': False,
}

def get_connection():
    return mysql.connector.connect(**AIVEN_CONFIG)

def bulk_insert(table, columns, rows, batch_size=500):
    conn = None
    try:
        if not rows:
            print(f"  Warning: No rows to insert into {table}")
            return True
        conn = get_connection()
        cursor = conn.cursor()
        col_str = ', '.join(columns)
        placeholders = ', '.join(['%s'] * len(columns))
        sql = f"INSERT INTO {table} ({col_str}) VALUES ({placeholders})"
        
        for i in range(0, len(rows), batch_size):
            batch = rows[i:i+batch_size]
            cursor.executemany(sql, batch)
            conn.commit()
        cursor.close()
        return True
    except Exception as e:
        print(f"Error in {table}: {e}")
        return False
    finally:
        if conn and conn.is_connected():
            conn.close()

# 1. Datasets
print("[1/5] Loading Datasets...")
df = pd.read_csv('output/datasets/datasets.csv')
rows = [(int(row['dataset_id']), int(row['organization_id']), str(row['dataset_name']), str(row.get('description', 'N/A')), str(row.get('access_level', 'public')), str(row.get('license', 'N/A'))) for _, row in df.iterrows()]
bulk_insert('Dataset', ['DatasetID', 'OrganizationID', 'DatasetName', 'Description', 'AccessLevel', 'License'], rows)
print(f"  ✓ Loaded {len(rows)} datasets")

# 2. Tags
print("[2/5] Loading Tags...")
df = pd.read_csv('output/reference/tags.csv')
rows = [(str(row['tag_name']).strip(),) for _, row in df.iterrows()]
bulk_insert('Tag', ['TagName'], rows)
print(f"  ✓ Loaded {len(rows)} tags")

# 3. Dataset_Tags
print("[3/5] Loading Dataset_Tags...")
df = pd.read_csv('output/datasets/dataset_tags.csv')
rows = [(int(row['dataset_id']), str(row['tag_name']).strip()) for _, row in df.iterrows() if pd.notna(row['dataset_id'])]
bulk_insert('Dataset_Tags', ['DatasetID', 'TagName'], rows, batch_size=1000)
print(f"  ✓ Loaded {len(rows)} dataset-tag relationships")

# 4. Users
print("[4/5] Loading Users...")
df = pd.read_csv('output/users/users_final.csv')
rows = [(str(row['email']).strip(), str(row['username']).strip(), str(row.get('gender', 'Unknown')), str(row.get('country', 'Unknown'))) for _, row in df.iterrows()]
bulk_insert('User', ['Email', 'Username', 'Gender', 'Country'], rows)
print(f"  ✓ Loaded {len(rows)} users")

# 5. Resources (from datasets)
print("[5/5] Loading Resources...")
df = pd.read_csv('output/datasets/datasets.csv')
rows = []
rid = 1
for _, row in df.iterrows():
    did = int(row['dataset_id'])
    res_name = str(row['dataset_name'])
    res_url = str(row.get('dataset_url', ''))
    # Create one resource per dataset
    rows.append((rid, did, res_name, 'CSV', res_url))
    rid += 1
bulk_insert('Resources', ['ResourceID', 'DatasetID', 'Description', 'Format', 'URL'], rows, batch_size=1000)
print(f"  ✓ Loaded {len(rows)} resources")

# Verify
print("\n=== Final Counts ===")
conn = get_connection()
cursor = conn.cursor()
for table in ['Organization', 'Dataset', 'User', 'Tag', 'Resources', 'Dataset_Tags']:
    try:
        cursor.execute(f'SELECT COUNT(*) FROM {table}')
        count = cursor.fetchone()[0]
        print(f"{table}: {count} records")
    except Exception as e:
        print(f"{table}: Error - {e}")
cursor.close()
conn.close()
