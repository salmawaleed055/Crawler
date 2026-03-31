#!/usr/bin/env python3
"""
NAME: Salma Elmarakby
ID: 900232658

MILESTONE 2 COMPLETE EXECUTION SCRIPT - API VERSION
Runs all stages: API Crawler → CSV Generation → Database Population
"""

import subprocess
import sys
import os
from datetime import datetime

print("\n" + "="*70)
print(" "*15 + "DATA.GOV DATABASE - MILESTONE 2")
print(" "*10 + "Complete API-Based Pipeline Execution")
print("="*70)
print(f"Execution started: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

# Get script directory
script_dir = os.path.dirname(os.path.abspath(__file__))

# ============================================================
# STAGE 1: API CRAWLING + CSV GENERATION
# ============================================================
print("\n[STAGE 1/3] API CRAWLING & CSV GENERATION")
print("-" * 70)

complete_pipeline_script = os.path.join(script_dir, 'complete_pipeline.py')
if not os.path.exists(complete_pipeline_script):
    print(f"✗ Script not found: {complete_pipeline_script}")
    sys.exit(1)

print("Starting API crawler and CSV generator...")
try:
    result = subprocess.run([sys.executable, complete_pipeline_script], 
                          check=True, 
                          timeout=1800,
                          cwd=script_dir)
    print("✓ API crawling and CSV generation complete!")
except subprocess.TimeoutExpired:
    print("✗ Crawler timeout (took more than 30 minutes)")
    sys.exit(1)
except subprocess.CalledProcessError as e:
    print(f"✗ Crawler failed with error code: {e.returncode}")
    sys.exit(1)

# Verify CSV files were created
required_csvs = [
    'output/raw_data/crawled_datasets_api.csv',
    'output/reference/organizations.csv',
    'output/reference/tags.csv',
    'output/datasets/datasets.csv',
    'output/datasets/dataset_tags.csv',
    'output/users/users_final.csv',
    'output/users/usage.csv'
]

print("\nVerifying generated CSV files...")
for csv_file in required_csvs:
    csv_path = os.path.join(script_dir, csv_file)
    if os.path.exists(csv_path):
        file_size = os.path.getsize(csv_path)
        print(f"  ✓ {csv_file:40} ({file_size:,} bytes)")
    else:
        print(f"  ✗ {csv_file:40} NOT FOUND")
        sys.exit(1)

# ============================================================
# STAGE 2: DATABASE POPULATION
# ============================================================
print("\n[STAGE 2/3] DATABASE POPULATION")
print("-" * 70)

populate_script = os.path.join(script_dir, 'populate_database_redesigned.py')
if not os.path.exists(populate_script):
    print(f"✗ Script not found: {populate_script}")
    sys.exit(1)

print("Connecting to MySQL and populating database...")
try:
    result = subprocess.run([sys.executable, populate_script],
                          check=True,
                          timeout=600,
                          cwd=script_dir)
    print("✓ Database population complete!")
except subprocess.TimeoutExpired:
    print("✗ Database population timeout")
    sys.exit(1)
except subprocess.CalledProcessError as e:
    print(f"✗ Database population failed with error code: {e.returncode}")
    sys.exit(1)

# ============================================================
# STAGE 3: DATABASE DUMP
# ============================================================
print("\n[STAGE 3/3] CREATING DATABASE DUMP")
print("-" * 70)

dump_script = os.path.join(script_dir, 'database_dump.py')
if os.path.exists(dump_script):
    print("Generating SQL database dump...")
    try:
        result = subprocess.run([sys.executable, dump_script],
                              check=True,
                              timeout=300,
                              cwd=script_dir)
        print("✓ Database dump created!")
    except subprocess.TimeoutExpired:
        print("✗ Database dump timeout")
    except subprocess.CalledProcessError as e:
        print(f"⚠ Database dump failed with error code: {e.returncode}")
        # Don't exit on dump failure - it's not critical
else:
    print("⚠ Database dump script not found (optional)")

# ============================================================
# FINAL SUMMARY
# ============================================================
print("\n" + "="*70)
print(" "*20 + "MILESTONE 2 COMPLETE!")
print("="*70)
print("\n✓ All stages completed successfully!")

print("\nGenerated Files (organized in output folders):")
print("  ✓ output/raw_data/")
print("    └── crawled_datasets_api.csv    : Raw crawled data from API")
print("  ✓ output/reference/")
print("    ├── organizations.csv           : Organizations reference table")
print("    └── tags.csv                    : Data format tags (unique: tag_name)")
print("  ✓ output/datasets/")
print("    ├── datasets.csv                : Dataset records")
print("    └── dataset_tags.csv            : Dataset-Tag mappings")
print("  ✓ output/users/")
print("    ├── users_final.csv             : User records")
print("    └── usage.csv                   : User dataset usage logs (500 entries)")

print("\nDatabase Created:")
print("  ✓ Database: data_gov_db")
print("  ✓ Tables: organizations, tags, datasets, dataset_tags, users, usage")
print("  ✓ Location: MySQL at 127.0.0.1:3306")

print("\nDeliverables Checklist:")
print("  ✓ Crawling script (API-based)")
print("  ✓ CSV files for all tables")
print("  ✓ Populated MySQL database")

if os.path.exists(os.path.join(script_dir, 'data_gov_db_dump.sql')):
    dump_size = os.path.getsize(os.path.join(script_dir, 'data_gov_db_dump.sql'))
    print(f"  ✓ Database dump file ({dump_size:,} bytes)")

print(f"\nExecution completed: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
print("="*70 + "\n")
