import pandas as pd
import numpy as np
import random
from datetime import datetime
import csv

# Set seed for reproducibility
np.random.seed(42)
random.seed(42)

print("=" * 60)
print("MILESTONE 2 FINALIZATION - Data Population")
print("=" * 60)

# ============================================================
# STEP 1: Load the crawled datasets
# ============================================================
print("\n[1/5] Loading crawled datasets...")
try:
    df_datasets = pd.read_csv('crawled_datasets_improved.csv')
    print(f"✓ Loaded {len(df_datasets)} datasets")
except Exception as e:
    print(f"✗ Error loading crawled datasets: {e}")
    exit(1)

# ============================================================
# STEP 2: Load users data
# ============================================================
print("[2/5] Loading users data...")
try:
    df_users = pd.read_csv('users.csv')
    print(f"✓ Loaded {len(df_users)} users")
except Exception as e:
    print(f"✗ Error loading users: {e}")
    exit(1)

# ============================================================
# STEP 3: Create ORGANIZATIONS table
# ============================================================
print("[3/5] Creating ORGANIZATIONS table...")
# Extract unique organizations from datasets
organizations = df_datasets['organization'].dropna().unique()
organizations = [org for org in organizations if org != 'N/A']
organizations_list = []

for idx, org in enumerate(organizations):
    organizations_list.append({
        'organization_id': idx + 1,
        'organization_name': org,
        'description': f"Data provider: {org}"
    })

org_df = pd.DataFrame(organizations_list)
org_df.to_csv('organizations.csv', index=False)
print(f"✓ Created organizations table with {len(org_df)} organizations")

# Create mapping for organization_name to organization_id
org_mapping = dict(zip(org_df['organization_name'], org_df['organization_id']))

# ============================================================
# STEP 4: Create TAGS table (from formats)
# ============================================================
print("[4/5] Creating TAGS table...")
all_formats = []
for formats_str in df_datasets['formats'].fillna(''):
    if formats_str != 'N/A':
        formats = [f.strip() for f in formats_str.split(',')]
        all_formats.extend(formats)

tags = list(set(all_formats))
tags = [tag for tag in tags if tag]  # Remove empty strings
# Normalize tags to handle case sensitivity
tags = list(set([tag.lower() for tag in tags]))  # Convert all to lowercase for uniqueness
tags.sort()

tags_list = []
for tag in tags:
    tags_list.append({
        'tag_name': tag
    })

tags_df = pd.DataFrame(tags_list)
tags_df.to_csv('tags.csv', index=False)
print(f"✓ Created tags table with {len(tags_df)} unique tags (normalized)")

# Create mapping for tag_name
tag_mapping = dict(zip(tags_df['tag_name'], range(1, len(tags_df) + 1)))

# ============================================================
# STEP 5: Create DATASETS table
# ============================================================
print("[5/5] Creating DATASETS table...")
datasets_list = []

for idx, row in df_datasets.iterrows():
    org_id = org_mapping.get(row['organization'], None)
    
    # Handle NaN values
    access_level = row['access_level'] if (pd.notna(row['access_level']) and row['access_level'] != 'N/A') else 'public'
    license_info = row['license'] if (pd.notna(row['license']) and row['license'] != 'N/A') else 'Unknown'
    maintainer = row['maintainer'] if (pd.notna(row['maintainer']) and row['maintainer'] != 'N/A') else 'unknown@data.gov'
    
    datasets_list.append({
        'dataset_id': idx + 1,
        'dataset_name': row['name'],
        'dataset_url': row['url'],
        'description': f"Dataset: {row['name']}",
        'access_level': access_level,
        'license': license_info,
        'maintainer_email': maintainer,
        'organization_id': org_id,
        'created_date': datetime.now().strftime('%Y-%m-%d'),
        'last_updated': datetime.now().strftime('%Y-%m-%d')
    })

datasets_df = pd.DataFrame(datasets_list)
datasets_df.to_csv('datasets.csv', index=False)
print(f"✓ Created datasets table with {len(datasets_df)} datasets")

# ============================================================
# STEP 6: Create DATASET_TAGS junction table
# ============================================================
print("Creating DATASET_TAGS junction table...")
dataset_tags_list = []
junction_id = 1

for idx, row in df_datasets.iterrows():
    if row['formats'] != 'N/A' and pd.notna(row['formats']):
        formats = [f.strip().lower() for f in str(row['formats']).split(',')]  # Normalize to lowercase
        for fmt in formats:
            if fmt in tag_mapping:
                dataset_tags_list.append({
                    'dataset_id': idx + 1,
                    'tag_name': fmt
                })
                junction_id += 1

dataset_tags_df = pd.DataFrame(dataset_tags_list)
dataset_tags_df = dataset_tags_df.drop_duplicates()  # Remove any duplicate dataset-tag mappings
dataset_tags_df.to_csv('dataset_tags.csv', index=False)
print(f"✓ Created dataset_tags junction table with {len(dataset_tags_df)} mappings")

# ============================================================
# STEP 7: Create USERS table
# ============================================================
print("Creating USERS table...")
users_list = []
for idx, row in df_users.iterrows():
    users_list.append({
        'user_id': idx + 1,
        'email': row['email'],
        'username': row['username'],
        'full_name': row['username'].replace('_', ' ').title(),
        'gender': row['gender'],
        'age': row['age'],
        'country': row['country'],
        'registration_date': datetime.now().strftime('%Y-%m-%d')
    })

users_df = pd.DataFrame(users_list)
users_df.to_csv('users_final.csv', index=False)
print(f"✓ Created users table with {len(users_df)} users")

# ============================================================
# STEP 8: Create USAGE table (500 random entries)
# ============================================================
print("Creating USAGE table with 500 random entries...")
usage_list = []
usage_id = 1

for _ in range(500):
    user_id = random.randint(1, len(users_df))
    dataset_id = random.randint(1, len(datasets_df))
    
    # Generate realistic usage patterns
    access_count = random.randint(1, 100)
    download_count = random.randint(0, access_count)
    
    # Random date within last 6 months
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
    usage_id += 1

usage_df = pd.DataFrame(usage_list)
usage_df.to_csv('usage.csv', index=False)
print(f"✓ Created usage table with {len(usage_df)} entries")

# ============================================================
# SUMMARY
# ============================================================
print("\n" + "=" * 60)
print("FINALIZATION COMPLETE - FILES CREATED")
print("=" * 60)
print(f"✓ organizations.csv       : {len(org_df)} organizations")
print(f"✓ tags.csv               : {len(tags_df)} tags (unique formats)")
print(f"✓ datasets.csv           : {len(datasets_df)} datasets")
print(f"✓ dataset_tags.csv       : {len(dataset_tags_df)} dataset-tag mappings")
print(f"✓ users_final.csv        : {len(users_df)} users")
print(f"✓ usage.csv              : {len(usage_df)} usage records")
print("=" * 60)
print("\nAll non-user and user tables have been populated!")
print("Tag feedback applied: Using 'tag_name' as unique identifier (no tag_id)")
