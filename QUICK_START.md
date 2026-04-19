# Quick Start Guide - Data.gov Dataset Explorer

## Prerequisites
- Python 3.8+
- Virtual environment activated (`.venv`)
- MySQL Connector Python installed
- Flask and dependencies installed

## Quick Start (3 steps)

### 1. Start the Flask Application
```bash
python app.py
```
**Output:**
```
============================================================
  CSCE 2501 Milestone III — Data.gov Explorer
  Open http://localhost:5000 in your browser
============================================================
 * Running on http://127.0.0.1:5000
```

### 2. Test All Features (in another terminal)
```bash
python test_api.py
```

### 3. Access the Application
Open your browser to: **http://localhost:5000**

---

## All 11 Features Working ✅

| # | Feature | Works |
|---|---------|-------|
| 1 | Register User | ✅ |
| 2 | Add Dataset Usage | ✅ |
| 3 | View Usage History | ✅ |
| 4 | Datasets by Organization Type | ✅ |
| 5 | Top 5 Organizations | ✅ |
| 6 | Datasets by Format | ✅ |
| 7 | Datasets by Tag | ✅ |
| 8 | Total Datasets by Dimension | ✅ |
| 9 | Top 5 Datasets by Users | ✅ |
| 10 | Usage Distribution by Type | ✅ |
| 11 | Top Tags by Project Type | ✅ |

---

## API Endpoints

### User Management
```
POST /api/users/register
  Required: email, username, gender, birthdate, country
  
POST /api/usage
  Required: email, dataset_id, project_name, project_category
  
GET /api/users/<user_id>/usage
  Returns: User's dataset usage history
```

### Dataset Discovery
```
GET /api/datasets/by-org-type?type=<type>
  Example: ?type=federal
  
GET /api/datasets/by-format?format=<format>
  Example: ?format=CSV
  
GET /api/datasets/by-tag?tag=<tag>
  Example: ?tag=agriculture
  
GET /api/organizations/top5
  Returns: Top 5 contributing organizations
```

### Statistics
```
GET /api/stats/totals
  Returns: Datasets aggregated by organization, topic, format, org type
  
GET /api/stats/usage-by-project-type
  Returns: Usage distribution by analytics/ML/research
  
GET /api/datasets/top5-by-users
  Returns: Most popular datasets
  
GET /api/stats/top-tags-by-project-type
  Returns: Top 10 tags per project type
```

### Utilities
```
GET /api/health
  Returns: Database connection status
  
GET /api/meta/org-types
  Returns: Available organization types
  
GET /api/meta/formats
  Returns: Available file formats
  
GET /api/meta/tags
  Returns: Available tags
  
GET /api/dashboard/overview
  Returns: Comprehensive dashboard data
```

---

## Test Sample

Register a user:
```bash
curl -X POST http://localhost:5000/api/users/register \
  -H "Content-Type: application/json" \
  -d '{
    "email": "test@example.com",
    "username": "testuser",
    "gender": "Male",
    "birthdate": "1990-01-01",
    "country": "USA"
  }'
```

Get top organizations:
```bash
curl http://localhost:5000/api/organizations/top5
```

Search datasets by format:
```bash
curl "http://localhost:5000/api/datasets/by-format?format=CSV"
```

---

## Database Status

**Connection:** ✅ Aiven MySQL  
**Host:** mysql-102a440b-aucegypt-ada5.g.aivencloud.com  
**Database:** defaultdb  
**Tables:** 10  
**Total Records:** 2,500+  

---

## Troubleshooting

### Port 5000 already in use?
```bash
# Find process using port 5000
netstat -ano | findstr :5000

# Kill process
taskkill /PID <PID> /F
```

### Database connection error?
```bash
# Test connection
python -c "from app import get_connection; conn = get_connection(); print('Connected!')"
```

### Unicode errors in tests?
```bash
$env:PYTHONIOENCODING="utf-8"
python test_api.py
```

---

## Files Structure

```
Crawler/
├── app.py                              # Main Flask application
├── test_api.py                        # Feature testing script
├── create_dataset_tags.py            # Database setup script
├── populate_missing_data.py           # Data population
├── MILESTONE_III_COMPLETION_REPORT.md # Full documentation
├── QUICK_START.md                     # This file
├── requirements.txt                   # Python dependencies
├── static/
│   └── Index.html                     # Frontend
└── output/
    └── (CSV data files)
```

---

## Next Steps

1. ✅ All features implemented
2. ✅ All features tested
3. ✅ Database populated
4. ✅ Application running
5. Deploy to production (if needed)

---

**Status:** ✅ **READY FOR USE**

**Test Result:** 11/11 Features Passing ✅
