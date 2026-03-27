#!/usr/bin/env python3
"""
MILESTONE 2 COMPLETE EXECUTION SCRIPT
Runs all stages: Web scraping → CSV Generation → Database Population
"""

import subprocess
import sys
import os

print("\n" + "="*70)
print(" "*15 + "DATA.GOV DATABASE - MILESTONE 2")
print(" "*10 + "Complete Pipeline Execution")
print("="*70)

# Stage 1: Web Scraping (if needed)
print("\n[STAGE 1/3] WEB SCRAPING - data.gov Catalog")
print("-" * 70)
if os.path.exists('crawled_datasets_improved.csv'):
    print("✓ Crawled datasets already exist (crawled_datasets_improved.csv)")
    user_input = input("Re-run scraper? (y/n): ").strip().lower()
    if user_input == 'y':
        print("Starting web scraper...")
        try:
            result = subprocess.run(['python', 'MS2.py'], check=True, timeout=3600)
            print("✓ Web scraping complete!")
        except subprocess.TimeoutExpired:
            print("✗ Scraper timeout (took more than 1 hour)")
            sys.exit(1)
        except subprocess.CalledProcessError as e:
            print(f"✗ Scraper failed with error: {e}")
            sys.exit(1)
    else:
        print("Skipping scraper, using existing data...")
else:
    print("Crawled datasets not found. Starting web scraper...")
    try:
        result = subprocess.run(['python', 'MS2.py'], check=True, timeout=3600)
        print("✓ Web scraping complete!")
    except subprocess.TimeoutExpired:
        print("✗ Scraper timeout (took more than 1 hour)")
        sys.exit(1)
    except subprocess.CalledProcessError as e:
        print(f"✗ Scraper failed with error: {e}")
        sys.exit(1)

# Stage 2: CSV Generation
print("\n[STAGE 2/3] DATA PROCESSING - CSV Generation")
print("-" * 70)
print("Generating CSV files from crawled data...")
try:
    result = subprocess.run(['python', 'finalize_milestone.py'], check=True, timeout=300)
    print("✓ CSV generation complete!")
except subprocess.TimeoutExpired:
    print("✗ CSV generation timeout")
    sys.exit(1)
except subprocess.CalledProcessError as e:
    print(f"✗ CSV generation failed: {e}")
    sys.exit(1)

# Stage 3: Database Population
print("\n[STAGE 3/3] DATABASE POPULATION")
print("-" * 70)
print("Connecting to MySQL and populating database...")
try:
    result = subprocess.run(['python', 'populate_database.py'], check=True, timeout=600)
    print("✓ Database population complete!")
except subprocess.TimeoutExpired:
    print("✗ Database population timeout")
    sys.exit(1)
except subprocess.CalledProcessError as e:
    print(f"✗ Database population failed: {e}")
    sys.exit(1)

# Summary
print("\n" + "="*70)
print(" "*20 + "MILESTONE 2 COMPLETE!")
print("="*70)
print("\n✓ All stages completed successfully!")
print("\nGenerated Files:")
print("  • crawled_datasets_improved.csv  : Raw scraped data")
print("  • organizations.csv             : Organizations reference")
print("  • tags.csv                      : Data format tags (unique: tag_name)")
print("  • datasets.csv                  : Dataset records")
print("  • dataset_tags.csv              : Dataset-Tag mappings")
print("  • users_final.csv               : User records")
print("  • usage.csv                     : User dataset usage logs")
print("\nDatabase Created:")
print("  • Database: data_gov_db")
print("  • Tables: organizations, tags, datasets, dataset_tags, users, usage")
print("  • Schema Feature: tag_name is UNIQUE PRIMARY KEY (no tag_id)")
print("="*70 + "\n")
