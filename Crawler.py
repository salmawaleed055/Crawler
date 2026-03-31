# Name: Salma Elmarakby
# ID: 900232658

import requests
import csv
import json
import pandas as pd
import numpy as np
import random
from datetime import datetime
import sys
import os

np.random.seed(42)
random.seed(42)

output_dirs = {
    'raw': 'output/raw_data',
    'ref': 'output/reference',
    'datasets': 'output/datasets',
    'users': 'output/users'
}

for dir_path in output_dirs.values():
    os.makedirs(dir_path, exist_ok=True)

BASE_URL = "https://catalog.data.gov/api/3/action/package_search"
MAX_PAGES = 2
ROWS_PER_PAGE = 1000

print("\n" + "="*70)
print(" "*10 + "DATA.GOV CKAN API CRAWLER - COMPLETE PIPELINE")
print(" "*15 + "Milestone 2 Deliverable")
print("="*70)

print("\n[STAGE 1/3] FETCHING DATASETS FROM data.gov CKAN API")
print("-" * 70)

def fetch_datasets_via_api():
    datasets = []

    for page_num in range(MAX_PAGES):
        start_index = page_num * ROWS_PER_PAGE
        print(f"Fetching page {page_num + 1} (rows {start_index} to {start_index + ROWS_PER_PAGE})...")

        try:
            params = {
                'rows': ROWS_PER_PAGE,
                'start': start_index,
            }

            response = requests.get(BASE_URL, params=params, timeout=15)
            response.raise_for_status()
            data = response.json()

            if not data.get('success'):
                print(f"API error on page {page_num + 1}: {data.get('error')}")
                break

            results = data.get('result', {})
            packages = results.get('results', [])
            count = results.get('count', 0)

            if not packages:
                print(f"No more datasets found. Total count: {count}")
                break

            for package in packages:
                dataset_entry = extract_dataset_info(package)
                datasets.append(dataset_entry)

            print(f"  ✓ Retrieved {len(packages)} datasets (Total so far: {len(datasets)})")

        except Exception as e:
            print(f"Error on page {page_num + 1}: {e}")
            break

    return datasets

def extract_dataset_info(package):
    org_name = "N/A"
    if package.get('organization'):
        org_name = package['organization'].get('title', package['organization'].get('name', 'N/A'))

    formats = []
    if package.get('resources'):
        for resource in package['resources']:
            if resource.get('format'):
                fmt = resource['format'].strip().upper()
                if fmt and fmt not in formats:
                    formats.append(fmt)

    access_level = "public"
    if package.get('private'):
        access_level = "private" if package['private'] else "public"

    dataset_url = f"https://catalog.data.gov/dataset/{package.get('name', '')}"

    license_info = "N/A"
    if package.get('license_title') and package.get('license_title').strip():
        license_info = package['license_title'].strip()
    elif package.get('license_id') and package.get('license_id').strip():
        license_info = package['license_id'].strip()

    maintainer = "unknown@data.gov"
    maintainer_email = "N/A"
    if package.get('maintainer'):
        maintainer = package['maintainer']
    elif package.get('author'):
        maintainer = package['author']

    if package.get('maintainer_email'):
        maintainer_email = package['maintainer_email']

    description = "N/A"
    if package.get('notes'):
        description = package['notes'].strip()[:500]

    topic = "N/A"
    if package.get('tags'):
        topic_list = [tag.get('name', '') for tag in package['tags'] if tag.get('name')]
        if topic_list:
            topic = ", ".join(topic_list[:10])

    creation_date = "N/A"
    metadata_modified = "N/A"
    if package.get('metadata_created'):
        creation_date = package['metadata_created']
    if package.get('metadata_modified'):
        metadata_modified = package['metadata_modified']

    dataset_entry = {
        'name': package.get('title', 'N/A'),
        'url': dataset_url,
        'organization': org_name,
        'formats': ", ".join(formats) if formats else "N/A",
        'access_level': access_level,
        'license': license_info,
        'maintainer': maintainer,
        'maintainer_email': maintainer_email,
        'description': description,
        'topic': topic,
        'creation_date': creation_date,
        'metadata_modified': metadata_modified,
    }

    return dataset_entry

print("\n[Step 1] Fetching datasets from API...")
api_data = fetch_datasets_via_api()

if len(api_data) == 0:
    print("\n✗ No datasets fetched. Exiting.")
    exit(1)

df_datasets = pd.DataFrame(api_data)
print(f"\n✓ Successfully fetched {len(df_datasets)} datasets")

df_datasets.to_csv(os.path.join(output_dirs['raw'], 'crawled_datasets_api.csv'), index=False)
print(f"✓ Saved raw data to output/raw_data/crawled_datasets_api.csv")

print("\n[STAGE 2/3] LOADING USERS DATA")
print("-" * 70)

users_path = os.path.join('..', 'users.csv')
if os.path.exists(users_path):
    try:
        df_users = pd.read_csv(users_path)
        print(f"✓ Loaded {len(df_users)} users from users.csv")
    except Exception as e:
        print(f"✗ Error loading users: {e}")
        exit(1)
else:
    print(f"✗ users.csv not found at {users_path}")
    exit(1)

print("\n[STAGE 3/3] GENERATING CSV FILES FOR DATABASE")
print("-" * 70)

print("\n[3.1/3.7] Creating ORGANIZATIONS table...")
organizations_dict = {}

for _, row in df_datasets.iterrows():
    org_name = row['organization']
    if org_name and org_name != 'N/A' and org_name not in organizations_dict:
        organizations_dict[org_name] = {
            'organization_name': org_name,
            'description': f"Data provider: {org_name}",
            'contact_information': row['maintainer_email'] if row['maintainer_email'] != 'N/A' else 'N/A',
            'organization_type': 'Government'
        }

organizations_list = []
for idx, (org_name, org_data) in enumerate(organizations_dict.items(), 1):
    organizations_list.append({
        'organization_id': idx,
        'organization_name': org_name,
        'organization_type': org_data['organization_type'],
        'contact_information': org_data['contact_information'],
        'description': org_data['description']
    })

org_df = pd.DataFrame(organizations_list)
org_df.to_csv(os.path.join(output_dirs['ref'], 'organizations.csv'), index=False)
print(f"✓ Created output/reference/organizations.csv with {len(org_df)} organizations")

org_mapping = dict(zip(org_df['organization_name'], org_df['organization_id']))

print("[3.2/3.7] Creating TAGS table...")
all_formats = []
for formats_str in df_datasets['formats'].fillna(''):
    if formats_str != 'N/A':
        formats = [f.strip().lower() for f in str(formats_str).split(',')]
        all_formats.extend(formats)

tags = sorted(list(set([tag for tag in all_formats if tag])))
tags_list = [{'tag_name': tag} for tag in tags]

tags_df = pd.DataFrame(tags_list)
tags_df.to_csv(os.path.join(output_dirs['ref'], 'tags.csv'), index=False)
print(f"✓ Created output/reference/tags.csv with {len(tags_df)} unique tags")

tag_mapping = dict(zip(tags_df['tag_name'], range(1, len(tags_df) + 1)))

print("[3.3/3.7] Creating DATASETS table...")
datasets_list = []

for idx, row in df_datasets.iterrows():
    org_id = org_mapping.get(row['organization'], None)

    datasets_list.append({
        'dataset_id': idx + 1,
        'dataset_name': row['name'],
        'dataset_url': row['url'],
        'description': f"Dataset: {row['name']}",
        'access_level': str(row['access_level']).lower(),
        'license': str(row['license']),
        'maintainer_email': str(row['maintainer']),
        'organization_id': org_id,
        'created_date': datetime.now().strftime('%Y-%m-%d'),
        'last_updated': datetime.now().strftime('%Y-%m-%d')
    })

datasets_df = pd.DataFrame(datasets_list)
datasets_df.to_csv(os.path.join(output_dirs['datasets'], 'datasets.csv'), index=False)
print(f"✓ Created output/datasets/datasets.csv with {len(datasets_df)} datasets")

print("[3.4/3.7] Creating DATASET_TAGS junction table...")
dataset_tags_list = []

for idx, row in df_datasets.iterrows():
    if row['formats'] != 'N/A' and pd.notna(row['formats']):
        formats = [f.strip().lower() for f in str(row['formats']).split(',')]
        for fmt in formats:
            if fmt in tag_mapping:
                dataset_tags_list.append({
                    'dataset_id': idx + 1,
                    'tag_name': fmt
                })

dataset_tags_df = pd.DataFrame(dataset_tags_list)
dataset_tags_df = dataset_tags_df.drop_duplicates()
dataset_tags_df.to_csv(os.path.join(output_dirs['datasets'], 'dataset_tags.csv'), index=False)
print(f"✓ Created output/datasets/dataset_tags.csv with {len(dataset_tags_df)} mappings")

print("[3.5/3.7] Creating USERS table...")
users_list = []

for idx, row in df_users.iterrows():
    users_list.append({
        'user_id': idx + 1,
        'email': row['email'],
        'username': row['username'],
        'full_name': str(row['username']).replace('_', ' ').title(),
        'gender': str(row.get('gender', 'Unknown')),
        'age': int(row.get('age', 25)),
        'country': str(row.get('country', 'USA')),
        'registration_date': datetime.now().strftime('%Y-%m-%d')
    })

users_df = pd.DataFrame(users_list)
users_df.to_csv(os.path.join(output_dirs['users'], 'users_final.csv'), index=False)
print(f"✓ Created output/users/users_final.csv with {len(users_df)} users")

print("[3.6/3.7] Creating USAGE table with 500 random entries...")
usage_list = []

for usage_id in range(1, 501):
    user_id = random.randint(1, len(users_df))
    dataset_id = random.randint(1, len(datasets_df))

    access_count = random.randint(1, 100)
    download_count = random.randint(0, access_count)

    days_ago = random.randint(0, 180)
    usage_date = pd.Timestamp.now() - pd.Timedelta(days=days_ago)

    usage_list.append({
        'usage_id': usage_id,
        'user_id': user_id,
        'dataset_id': dataset_id,
        'access_count': access_count,
        'download_count': download_count,
        'last_accessed': usage_date.strftime('%Y-%m-%d'),
        'usage_type': random.choice(['view', 'download', 'analyze', 'share'])
    })

usage_df = pd.DataFrame(usage_list)
usage_df.to_csv(os.path.join(output_dirs['users'], 'usage.csv'), index=False)
print(f"✓ Created output/users/usage.csv with {len(usage_df)} entries")

print("\n[3.7/3.7] Finalization Summary")
print("="*70)

print("\n" + "="*70)
print(" "*10 + "API CRAWLER - END-TO-END PIPELINE COMPLETE!")
print("="*70)
print("\n✓ All CSV files successfully generated:\n")
print(f"  ✓ output/raw_data/crawled_datasets_api.csv    : {len(df_datasets)} raw datasets")
print(f"  ✓ output/reference/organizations.csv          : {len(org_df)} organizations")
print(f"  ✓ output/reference/tags.csv                   : {len(tags_df)} data format tags")
print(f"  ✓ output/datasets/datasets.csv                : {len(datasets_df)} datasets")
print(f"  ✓ output/datasets/dataset_tags.csv            : {len(dataset_tags_df)} dataset-tag mappings")
print(f"  ✓ output/users/users_final.csv                : {len(users_df)} users")
print(f"  ✓ output/users/usage.csv                      : {len(usage_df)} usage entries")

print("\n" + "="*70)
print("Next: Run populate_database.py to load data into MySQL")
print("="*70 + "\n")