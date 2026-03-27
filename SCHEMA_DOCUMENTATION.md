# Milestone 2 Database Schema - Modified for Unique Tag Names
**Date:** March 27, 2026  
**Student:** Salma Elmarakby  
**ID:** 900232658

---

## Database: `data_gov_db`

### Schema Overview

#### 1. **ORGANIZATIONS** Table
```sql
CREATE TABLE organizations (
    organization_id INT AUTO_INCREMENT PRIMARY KEY,
    organization_name VARCHAR(255) NOT NULL UNIQUE,
    description TEXT,
    created_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
)
```
**Purpose:** Stores unique organizations that provide datasets  
**Records:** ~30-50 unique organizations from data.gov

---

#### 2. **TAGS** Table ⭐ **MODIFIED**
```sql
CREATE TABLE tags (
    tag_name VARCHAR(100) PRIMARY KEY,          -- UNIQUE IDENTIFIER (NO tag_id)
    created_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
)
```
**Changes from Original:**
- ❌ Removed: `tag_id INT AUTO_INCREMENT`
- ✅ Added: `tag_name VARCHAR(100)` as PRIMARY KEY
- **Reason:** Tag names are unique and semantic; eliminates redundant ID

**Purpose:** Stores data format tags (CSV, JSON, XML, ZIP, etc.)  
**Records:** ~50-80 unique formats from crawled datasets

---

#### 3. **DATASETS** Table
```sql
CREATE TABLE datasets (
    dataset_id INT AUTO_INCREMENT PRIMARY KEY,
    dataset_name VARCHAR(500) NOT NULL,
    dataset_url VARCHAR(500),
    description TEXT,
    access_level VARCHAR(50),                   -- 'public', 'restricted', etc.
    license VARCHAR(2500),
    maintainer_email VARCHAR(255),
    organization_id INT,
    created_date DATE,
    last_updated DATE,
    FOREIGN KEY (organization_id) REFERENCES organizations(organization_id) ON DELETE SET NULL
)
```
**Purpose:** Core dataset records scraped from data.gov  
**Records:** ~100 datasets (from web scraping)

---

#### 4. **DATASET_TAGS** Junction Table
```sql
CREATE TABLE dataset_tags (
    dataset_id INT NOT NULL,
    tag_name VARCHAR(100) NOT NULL,             -- REFERENCES tags(tag_name)
    PRIMARY KEY (dataset_id, tag_name),
    FOREIGN KEY (dataset_id) REFERENCES datasets(dataset_id) ON DELETE CASCADE,
    FOREIGN KEY (tag_name) REFERENCES tags(tag_name) ON DELETE CASCADE
)
```
**Purpose:** Many-to-many relationship between datasets and formats  
**Records:** ~200-300 mappings (each dataset has 2-3+ formats)

**Foreign Key Change:** `tag_name` now references `tags.tag_name` directly (no `tag_id`)

---

#### 5. **USERS** Table
```sql
CREATE TABLE users (
    user_id INT AUTO_INCREMENT PRIMARY KEY,
    email VARCHAR(255) NOT NULL UNIQUE,
    username VARCHAR(100) NOT NULL UNIQUE,
    full_name VARCHAR(255),
    gender VARCHAR(20),
    age INT,
    country VARCHAR(100),
    registration_date DATE,
    created_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
)
```
**Purpose:** User profiles  
**Records:** From `users.csv` (provided dataset)

---

#### 6. **USAGE** Table
```sql
CREATE TABLE `usage` (
    usage_id INT AUTO_INCREMENT PRIMARY KEY,
    user_id INT NOT NULL,
    dataset_id INT NOT NULL,
    access_count INT,                           -- How many times accessed
    download_count INT,                         -- How many times downloaded
    last_accessed DATE,
    usage_type VARCHAR(50),                     -- 'view', 'download', 'analyze', 'share'
    created_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (user_id) REFERENCES users(user_id) ON DELETE CASCADE,
    FOREIGN KEY (dataset_id) REFERENCES datasets(dataset_id) ON DELETE CASCADE
)
```
**Purpose:** Track user interactions with datasets  
**Records:** 500 randomly generated usage logs

---

## Entity Relationship Diagram (Conceptual)

```
┌─────────────────┐
│  ORGANIZATIONS  │
│                 │
│ organization_id │◄──┐
│ org_name        │   │
└─────────────────┘   │
                      │
                      ├──────────────┐
                      │              │
┌─────────────────┐   │   ┌──────────────────┐
│  DATASETS       │   │   │  DATASET_TAGS    │
│                 │   │   │                  │
│ dataset_id ◄───┼───┼──┤ dataset_id       │
│ dataset_name    │   │   │ tag_name ────┐  │
│ organization_id ├───┘   │              │  │
│ ...             │       └──────────────┴──┘
└─────────────────┘             ▲
       ▲                        │
       │                        │
       │        ┌───────────────┴──────────┐
       │        │                          │
    ┌──┴────────────┐             ┌────────────────┐
    │   USAGE       │             │   TAGS         │
    │               │             │                │
    │ usage_id      │             │ tag_name (PK)  │◄── CHANGED
    │ user_id ──┐   │             │ created_date   │
    │ dataset_id│   │             └────────────────┘
    │ ...       │   │
    └───────────┼───┘
                │
         ┌──────┴──────┐
         │             │
      ┌──────────┐     │
      │ USERS    │     │
      │          │     │
      │ user_id ◄┼─────┘
      │ email    │
      │ ...      │
      └──────────┘
```

---

## Key Schema Changes from Project Milestone 1

### ✅ Tags Table Modification
| Feature | Before | After |
|---------|--------|-------|
| **Primary Key** | `tag_id (INT)` | `tag_name (VARCHAR)` |
| **Identifier Semantic** | Numeric ID | Format name (CSV, JSON, etc.) |
| **UNIQUE Constraint** | Separate | Built into PRIMARY KEY |
| **Redundancy** | Stores both ID + Name | Name alone sufficient |
| **Foreign Key in dataset_tags** | `tag_id` | `tag_name` |

---

## Data Population Pipeline

### Stage 1: Web Scraping
- Script: `MS2.py`
- Output: `crawled_datasets_improved.csv` (~100 rows)
- Extracts: name, url, organization, formats, access_level, license, maintainer

### Stage 2: CSV Generation
- Script: `finalize_milestone.py`
- Inputs: `crawled_datasets_improved.csv`, `users.csv`
- Outputs:
  - `organizations.csv` - Unique organizations
  - `tags.csv` - Unique formats
  - `datasets.csv` - Dataset records
  - `dataset_tags.csv` - M:M relationships
  - `users_final.csv` - User records
  - `usage.csv` - 500 random usage logs

### Stage 3: Database Population
- Script: `populate_database.py`
- Creates: Database and tables
- Populates: All 6 tables from CSVs
- Enforces: Foreign keys and constraints

---

## Execution

Run the complete pipeline:
```bash
python run_milestone.py
```

Or run individually:
```bash
python MS2.py                  # Stage 1: Web scraping
python finalize_milestone.py   # Stage 2: CSV generation  
python populate_database.py    # Stage 3: Database population
```

---

## Verification

After execution, verify with MySQL queries:
```sql
USE data_gov_db;

-- Check table records
SELECT COUNT(*) AS organizations FROM organizations;
SELECT COUNT(*) AS tags FROM tags;
SELECT COUNT(*) AS datasets FROM datasets;
SELECT COUNT(*) AS dataset_tags FROM dataset_tags;
SELECT COUNT(*) AS users FROM users;
SELECT COUNT(*) AS usage FROM usage;

-- Verify tag_name uniqueness
SELECT tag_name, COUNT(*) FROM tags GROUP BY tag_name HAVING COUNT(*) > 1;  -- Should be empty

-- Sample join query
SELECT d.dataset_name, t.tag_name 
FROM datasets d
JOIN dataset_tags dt ON d.dataset_id = dt.dataset_id
JOIN tags t ON dt.tag_name = t.tag_name
LIMIT 10;
```

