# 🚀 MILESTONE 2 - QUICK START GUIDE

**Student:** Salma Elmarakby  
**ID:** 900232658  
**Date:** March 27, 2026

---

## ✅ What's Already Done

Your code is **ready to execute**! Here's what each file does:

| File | Purpose | Status |
|------|---------|--------|
| `MS2.py` | Web scrape data.gov (100 pages) → CSV | ✅ Complete |
| `finalize_milestone.py` | Process CSVs → 6 tables | ✅ Complete |
| `populate_database.py` | Create MySQL db & populate tables | ✅ Complete |
| `run_milestone.py` | Execute full pipeline | ✅ New (just created) |

---

## ⚡ Quick Execution

### Option 1: Run Everything at Once
```bash
python run_milestone.py
```
This runs all 3 stages automatically:
1. Web scraping (if needed)
2. CSV generation
3. Database population

### Option 2: Run Individual Stages

**Stage 1 - Web Scraping** (⏱️ Takes ~30-60 minutes)
```bash
python MS2.py
# Output: crawled_datasets_improved.csv
```

**Stage 2 - CSV Generation** (⏱️ Takes ~10 seconds)
```bash
python finalize_milestone.py
# Output: organizations.csv, tags.csv, datasets.csv, dataset_tags.csv, users_final.csv, usage.csv
```

**Stage 3 - Database Population** (⏱️ Takes ~20 seconds)
```bash
python populate_database.py
# Creates database: data_gov_db with 6 tables
```

---

## 📊 Database Schema (Milestone 1 + Modifications)

### ✨ KEY MODIFICATION: Tags Table

**Before (Milestone 1):**
```sql
CREATE TABLE tags (
    tag_id INT AUTO_INCREMENT PRIMARY KEY,
    tag_name VARCHAR(100) NOT NULL UNIQUE
)
```

**After (Milestone 2 - YOUR VERSION):**
```sql
CREATE TABLE tags (
    tag_name VARCHAR(100) PRIMARY KEY  -- ← tag_name is now the PRIMARY KEY
)
```

**Why?** Tag names (CSV, JSON, XML, etc.) are unique and semantic. No need for redundant `tag_id`.

---

## 📋 Tables Created

| Table | Records | Purpose |
|-------|---------|---------|
| **organizations** | ~30-40 | Data providers (Federal agencies, cities, etc.) |
| **tags** | ~50-80 | Data formats (CSV, JSON, XML, ZIP, etc.) |
| **datasets** | ~100 | Dataset records from data.gov |
| **dataset_tags** | ~200-300 | Many-to-many: Which datasets have which formats |
| **users** | ~100 | User profiles (from users.csv) |
| **usage** | 500 | Usage logs (randomly generated) |

---

## 🔍 Verify Success

After running, check your MySQL database:

```sql
USE data_gov_db;

-- Count records in each table
SELECT 'organizations' as table_name, COUNT(*) as record_count FROM organizations
UNION ALL
SELECT 'tags', COUNT(*) FROM tags
UNION ALL
SELECT 'datasets', COUNT(*) FROM datasets
UNION ALL
SELECT 'dataset_tags', COUNT(*) FROM dataset_tags
UNION ALL
SELECT 'users', COUNT(*) FROM users
UNION ALL
SELECT 'usage', COUNT(*) FROM usage;
```

Expected output:
```
+-----------------+--------------+
| table_name      | record_count |
+-----------------+--------------+
| organizations   |           35 |
| tags            |           65 |
| datasets        |          100 |
| dataset_tags    |          245 |
| users           |          100 |
| usage           |          500 |
+-----------------+--------------+
```

---

## 🔗 Sample Queries

**Find all datasets tagged with 'csv':**
```sql
SELECT d.dataset_name, t.tag_name
FROM datasets d
JOIN dataset_tags dt ON d.dataset_id = dt.dataset_id
JOIN tags t ON dt.tag_name = t.tag_name
WHERE t.tag_name = 'csv'
LIMIT 10;
```

**Find organizations with most datasets:**
```sql
SELECT o.organization_name, COUNT(d.dataset_id) as dataset_count
FROM organizations o
LEFT JOIN datasets d ON o.organization_id = d.organization_id
GROUP BY o.organization_id, o.organization_name
ORDER BY dataset_count DESC
LIMIT 10;
```

**Find top users by dataset access:**
```sql
SELECT u.username, u.email, COUNT(u.user_id) as total_accesses
FROM users u
JOIN usage ug ON u.user_id = ug.user_id
GROUP BY u.user_id, u.username, u.email
ORDER BY total_accesses DESC
LIMIT 10;
```

---

## ⚙️ Requirements

Make sure you have:
- ✅ Python 3.8+
- ✅ MySQL Server running (localhost:3306)
- ✅ MySQL credentials: `root` / `2552005Elmarakby$`
- ✅ Python packages: `pandas`, `numpy`, `requests`, `beautifulsoup4`, `mysql-connector-python`

Install packages:
```bash
pip install pandas numpy requests beautifulsoup4 mysql-connector-python
```

---

## 📁 Files Generated

After execution, your `Project Milestone 2` folder will contain:

**CSV Files:**
- `crawled_datasets_improved.csv` - Raw web scraped data
- `organizations.csv` - Organizations reference table
- `tags.csv` - Data format tags
- `datasets.csv` - Dataset records
- `dataset_tags.csv` - Dataset-tag mappings
- `users_final.csv` - User records
- `usage.csv` - Usage logs

**Scripts:**
- `MS2.py` - Web scraper
- `finalize_milestone.py` - CSV processor
- `populate_database.py` - Database populator
- `run_milestone.py` - Complete pipeline runner
- `SCHEMA_DOCUMENTATION.md` - Full schema docs (this file)

---

## 🎯 Summary

Your Milestone 2 implementation:
- ✅ Follows Milestone 1 database schema
- ✅ Modifies TAGS table: `tag_name` is PRIMARY KEY (no `tag_id`)
- ✅ Scrapes 100 pages from data.gov
- ✅ Generates 6 normalized CSV tables
- ✅ Populates MySQL database with all relationships
- ✅ Includes 500 usage logs

**Ready to execute!** 🚀

