"""
CSCE 2501 - Milestone III: Application Layer
Data.gov Dataset Explorer - Flask Backend
Connects to Aiven MySQL hosted database
"""

from flask import Flask, request, jsonify, send_from_directory
from flask_cors import CORS
import mysql.connector
from mysql.connector import Error
import os
import random
from datetime import datetime, date

app = Flask(__name__, static_folder='static')
CORS(app)

AIVEN_CONFIG = {
    'host': 'mysql-102a440b-aucegypt-ada5.g.aivencloud.com',
    'user': 'avnadmin',
    'password': 'AVNS_zPM-1nu_PwPJQvgb3JT',
    'database': 'defaultdb',
    'port': 25158,
    'connection_timeout': 15,
    'use_pure': True,
    'ssl_disabled': False,
    'ssl_ca': None,
    'ssl_verify_cert': False,
    'ssl_verify_identity': False,
}

def get_connection():
    return mysql.connector.connect(**AIVEN_CONFIG)

def serialize(obj):
    """Convert non-JSON-serializable types."""
    if isinstance(obj, (datetime, date)):
        return obj.isoformat()
    return str(obj)

def query(sql, params=None, fetch=True):
    conn = None
    try:
        conn = get_connection()
        cursor = conn.cursor(dictionary=True)
        cursor.execute(sql, params or ())
        if fetch:
            rows = cursor.fetchall()
            # Serialize dates
            result = []
            for row in rows:
                result.append({k: serialize(v) if isinstance(v, (datetime, date)) else v for k, v in row.items()})
            return result, None
        else:
            conn.commit()
            return cursor.lastrowid, None
    except Error as e:
        print(f"[DEBUG] Query error: {str(e)}")
        return None, str(e)
    finally:
        if conn and conn.is_connected():
            conn.close()

# ─────────────────────────────────────────────
# SERVE FRONTEND
# ─────────────────────────────────────────────

@app.route('/')
def index():
    return send_from_directory('static', 'Index.html')

# ─────────────────────────────────────────────
# 1. REGISTER USER
# ─────────────────────────────────────────────

@app.route('/api/users/register', methods=['POST'])
def register_user():
    data = request.json
    required = ['email', 'username', 'gender', 'birthdate', 'country']
    for f in required:
        if not data.get(f):
            return jsonify({'error': f'Missing field: {f}'}), 400

    # Use INSERT IGNORE to handle duplicates gracefully
    result, err = query(
        """INSERT IGNORE INTO User (Email, Username, Gender, BirthDate, Country)
           VALUES (%s, %s, %s, %s, %s)""",
        (data['email'], data['username'], data['gender'],
         data['birthdate'], data['country']),
        fetch=False
    )
    if err:
        return jsonify({'error': err}), 500
    
    # Return success whether newly inserted or already existed
    return jsonify({
        'message': 'User registered successfully' if result > 0 else 'User already exists',
        'email': data['email']
    }), 201 if result > 0 else 200

# ─────────────────────────────────────────────
# 2. ADD DATASET USAGE
# ─────────────────────────────────────────────

@app.route('/api/usage', methods=['POST'])
def add_usage():
    data = request.json
    required = ['email', 'dataset_id', 'project_name', 'project_category']
    for f in required:
        if not data.get(f):
            return jsonify({'error': f'Missing field: {f}'}), 400

    valid_categories = ['analytics', 'machine learning', 'field research']
    if data['project_category'].lower() not in valid_categories:
        return jsonify({'error': f'project_category must be one of: {valid_categories}'}), 400

    # Verify user exists
    user_exists, _ = query(
        "SELECT Email FROM User WHERE Email = %s",
        (data['email'],)
    )
    if not user_exists:
        return jsonify({'error': 'User not found'}), 404

    # Verify dataset exists
    dataset_exists, _ = query(
        "SELECT DatasetID FROM Dataset WHERE DatasetID = %s",
        (data['dataset_id'],)
    )
    if not dataset_exists:
        return jsonify({'error': 'Dataset not found'}), 404

    result, err = query(
        """INSERT IGNORE INTO Dataset_User (Email, DatasetID, ProjectName, ProjectCategory)
           VALUES (%s, %s, %s, %s)""",
        (data['email'], data['dataset_id'], data['project_name'], data['project_category']),
        fetch=False
    )
    if err:
        return jsonify({'error': err}), 500
    return jsonify({
        'message': 'Usage recorded successfully' if result > 0 else 'Usage already recorded',
        'email': data['email'],
        'dataset_id': data['dataset_id']
    }), 201 if result > 0 else 200

# ─────────────────────────────────────────────
# 3. VIEW USER USAGE HISTORY
# ─────────────────────────────────────────────

@app.route('/api/users/<string:email>/usage', methods=['GET'])
def user_usage(email):
    rows, err = query(
        """SELECT du.DatasetID, d.DatasetName, du.ProjectName,
                  du.ProjectCategory, d.License
           FROM Dataset_User du
           JOIN Dataset d ON du.DatasetID = d.DatasetID
           WHERE du.Email = %s
           ORDER BY d.DatasetName DESC""",
        (email,)
    )
    if err:
        return jsonify({'error': err}), 500
    return jsonify(rows or [])

# ─────────────────────────────────────────────
# 4. DATASETS BY ORGANIZATION TYPE
# ─────────────────────────────────────────────

@app.route('/api/datasets/by-org-type', methods=['GET'])
def datasets_by_org_type():
    org_type = request.args.get('type', '')
    if org_type:
        rows, err = query(
            """SELECT d.DatasetID, d.DatasetName, d.AccessLevel,
                      o.OrganizationName, o.OrganizationType
               FROM Dataset d
               JOIN Organization o ON d.OrganizationID = o.OrganizationID
               WHERE o.OrganizationType LIKE %s
               ORDER BY o.OrganizationName, d.DatasetName
               LIMIT 100""",
            (f'%{org_type}%',)
        )
    else:
        rows, err = query(
            """SELECT d.DatasetID, d.DatasetName, d.AccessLevel,
                      o.OrganizationName, o.OrganizationType
               FROM Dataset d
               JOIN Organization o ON d.OrganizationID = o.OrganizationID
               ORDER BY o.OrganizationType, o.OrganizationName
               LIMIT 100"""
        )
    if err:
        return jsonify({'error': err}), 500
    return jsonify(rows)

# ─────────────────────────────────────────────
# 5. TOP 5 CONTRIBUTING ORGANIZATIONS
# ─────────────────────────────────────────────

@app.route('/api/organizations/top5', methods=['GET'])
def top5_organizations():
    rows, err = query(
        """SELECT o.OrganizationName as organization_name, 
                  o.OrganizationType as organization_type,
                  COUNT(d.DatasetID) as dataset_count
           FROM Organization o
           LEFT JOIN Dataset d ON o.OrganizationID = d.OrganizationID
           GROUP BY o.OrganizationID, o.OrganizationName, o.OrganizationType
           ORDER BY dataset_count DESC
           LIMIT 5"""
    )
    if err:
        return jsonify({'error': err}), 500
    return jsonify(rows)

# ─────────────────────────────────────────────
# 6. DATASETS BY FORMAT
# ─────────────────────────────────────────────

@app.route('/api/datasets/by-format', methods=['GET'])
def datasets_by_format():
    fmt = request.args.get('format', '')
    if not fmt:
        return jsonify({'error': 'format parameter required'}), 400
    rows, err = query(
        """SELECT DISTINCT d.DatasetID, d.DatasetName, d.AccessLevel,
                  o.OrganizationName, r.Format, r.URL
           FROM Dataset d
           JOIN Organization o ON d.OrganizationID = o.OrganizationID
           JOIN Resources r ON d.DatasetID = r.DatasetID
           WHERE r.Format LIKE %s
           ORDER BY d.DatasetName
           LIMIT 100""",
        (f'%{fmt}%',)
    )
    if err:
        return jsonify({'error': err}), 500
    return jsonify(rows)

# ─────────────────────────────────────────────
# 7. DATASETS BY TAG
# ─────────────────────────────────────────────

@app.route('/api/datasets/by-tag', methods=['GET'])
def datasets_by_tag():
    tag = request.args.get('tag', '')
    if not tag:
        return jsonify({'error': 'tag parameter required'}), 400
    rows, err = query(
        """SELECT DISTINCT d.DatasetID, d.DatasetName, d.AccessLevel,
                  o.OrganizationName, t.TagName
           FROM Dataset d
           JOIN Organization o ON d.OrganizationID = o.OrganizationID
           JOIN Dataset_Tags dt ON d.DatasetID = dt.DatasetID
           JOIN Tag t ON dt.TagName = t.TagName
           WHERE t.TagName LIKE %s
           ORDER BY d.DatasetName
           LIMIT 100""",
        (f'%{tag}%',)
    )
    if err:
        return jsonify({'error': err}), 500
    return jsonify(rows)

# ─────────────────────────────────────────────
# 8. DATASETS BY TOPIC
# ─────────────────────────────────────────────

@app.route('/api/datasets/by-topic', methods=['GET'])
def datasets_by_topic():
    topic = request.args.get('topic', '')
    if not topic:
        return jsonify({'error': 'topic parameter required'}), 400
    rows, err = query(
        """SELECT DISTINCT d.DatasetID, d.DatasetName, d.AccessLevel,
                  o.OrganizationName, dt.TopicName
           FROM Dataset d
           JOIN Organization o ON d.OrganizationID = o.OrganizationID
           JOIN Dataset_Topics dt ON d.DatasetID = dt.DatasetID
           WHERE dt.TopicName LIKE %s
           ORDER BY d.DatasetName
           LIMIT 100""",
        (f'%{topic}%',)
    )
    if err:
        return jsonify({'error': err}), 500
    return jsonify(rows)

# ─────────────────────────────────────────────
# 9. TOTAL DATASETS BY DIMENSION
# ─────────────────────────────────────────────

@app.route('/api/stats/totals', methods=['GET'])
def stats_totals():
    by_org, _ = query(
        """SELECT o.OrganizationName as organization_name, COUNT(d.DatasetID) AS total
           FROM Organization o
           JOIN Dataset d ON o.OrganizationID = d.OrganizationID
           GROUP BY o.OrganizationID, o.OrganizationName
           ORDER BY total DESC LIMIT 20"""
    )
    by_topic, _ = query(
        """SELECT dt.TopicName as topic, COUNT(*) AS total
           FROM Dataset_Topics dt
           WHERE dt.TopicName IS NOT NULL
           GROUP BY dt.TopicName
           ORDER BY total DESC LIMIT 20"""
    )
    by_format, _ = query(
        """SELECT r.Format as format_type, COUNT(DISTINCT r.DatasetID) AS total
           FROM Resources r
           GROUP BY r.Format
           ORDER BY total DESC LIMIT 20"""
    )
    by_org_type, _ = query(
        """SELECT o.OrganizationType as organization_type, COUNT(d.DatasetID) AS total
           FROM Organization o
           JOIN Dataset d ON o.OrganizationID = d.OrganizationID
           GROUP BY o.OrganizationType
           ORDER BY total DESC"""
    )
    return jsonify({
        'by_organization': by_org or [],
        'by_topic': by_topic or [],
        'by_format': by_format or [],
        'by_org_type': by_org_type or [],
    })

# ─────────────────────────────────────────────
# 9. TOP 5 DATASETS BY USERS
# ─────────────────────────────────────────────

@app.route('/api/datasets/top5-by-users', methods=['GET'])
def top5_datasets_by_users():
    rows, err = query(
        """SELECT d.DatasetID, d.DatasetName as dataset_name, o.OrganizationName as organization_name,
                  COUNT(DISTINCT du.Email) AS user_count
           FROM Dataset d
           JOIN Dataset_User du ON d.DatasetID = du.DatasetID
           JOIN Organization o ON d.OrganizationID = o.OrganizationID
           GROUP BY d.DatasetID, d.DatasetName, o.OrganizationName
           ORDER BY user_count DESC
           LIMIT 5"""
    )
    if err:
        return jsonify({'error': err}), 500
    return jsonify(rows)

# ─────────────────────────────────────────────
# 10. USAGE DISTRIBUTION BY PROJECT TYPE
# ─────────────────────────────────────────────

@app.route('/api/stats/usage-by-project-type', methods=['GET'])
def usage_by_project_type():
    rows, err = query(
        """SELECT ProjectCategory as project_category, COUNT(*) AS usage_count
           FROM Dataset_User
           GROUP BY ProjectCategory
           ORDER BY usage_count DESC"""
    )
    if err:
        return jsonify({'error': err}), 500
    return jsonify(rows)

# ─────────────────────────────────────────────
# 11. TOP 10 TAGS PER PROJECT TYPE
# ─────────────────────────────────────────────

@app.route('/api/stats/top-tags-by-project-type', methods=['GET'])
def top_tags_by_project_type():
    rows, err = query(
        """SELECT du.ProjectCategory as project_category, t.TagName as tag,
                  COUNT(*) AS tag_count
           FROM Dataset_User du
           JOIN Dataset_Tags dt ON du.DatasetID = dt.DatasetID
           JOIN Tag t ON dt.TagName = t.TagName
           GROUP BY du.ProjectCategory, t.TagName
           ORDER BY du.ProjectCategory, tag_count DESC"""
    )

    if err:
        return jsonify({'error': err}), 500

    # Group top 10 per project type
    grouped = {}
    for row in (rows or []):
        cat = row['ProjectCategory']
        if cat not in grouped:
            grouped[cat] = []
        if len(grouped[cat]) < 10:
            grouped[cat].append({'tag': row['TagName'], 'count': row['tag_count']})

    return jsonify(grouped)

# ─────────────────────────────────────────────
# HELPER: available org types and formats
# ─────────────────────────────────────────────

@app.route('/api/meta/org-types', methods=['GET'])
def org_types():
    rows, err = query("SELECT DISTINCT OrganizationType FROM Organization ORDER BY OrganizationType")
    if err:
        return jsonify([])
    return jsonify([r['OrganizationType'] for r in rows])

@app.route('/api/meta/formats', methods=['GET'])
def formats():
    rows, err = query("SELECT DISTINCT Format FROM Resources ORDER BY Format LIMIT 50")
    if err:
        return jsonify([])
    return jsonify([r['Format'] for r in rows] if rows else [])

@app.route('/api/meta/tags', methods=['GET'])
def tags():
    rows, err = query("SELECT DISTINCT TagName FROM Tag ORDER BY TagName LIMIT 100")
    if err:
        return jsonify([])
    return jsonify([r['TagName'] for r in rows])

@app.route('/api/dashboard/overview', methods=['GET'])
def dashboard_overview():
    """Comprehensive dashboard overview with all statistics"""
    
    # Total counts
    org_count, _ = query("SELECT COUNT(*) as cnt FROM Organization")
    dataset_count, _ = query("SELECT COUNT(*) as cnt FROM Dataset")
    user_count, _ = query("SELECT COUNT(*) as cnt FROM User")
    tag_count, _ = query("SELECT COUNT(*) as cnt FROM Tag")
    
    # Top organizations
    top_orgs, _ = query(
        """SELECT OrganizationName, OrganizationType FROM Organization 
           ORDER BY OrganizationName LIMIT 10"""
    )
    
    # License distribution
    licenses, _ = query(
        """SELECT COUNT(*) as cnt FROM Dataset GROUP BY License LIMIT 20"""
    )
    
    # Format distribution
    formats, _ = query(
        """SELECT Format, COUNT(DISTINCT DatasetID) as cnt 
           FROM Resources GROUP BY Format ORDER BY cnt DESC LIMIT 15"""
    )
    
    # Topic distribution
    topics, _ = query(
        """SELECT TopicName, COUNT(*) as cnt 
           FROM Dataset_Topics GROUP BY TopicName ORDER BY cnt DESC LIMIT 15"""
    )
    
    # Organization type distribution
    org_types, _ = query(
        """SELECT OrganizationType, COUNT(DISTINCT OrganizationID) as cnt 
           FROM Organization GROUP BY OrganizationType ORDER BY cnt DESC"""
    )
    
    return jsonify({
        'totals': {
            'organizations': org_count[0]['cnt'] if org_count else 0,
            'datasets': dataset_count[0]['cnt'] if dataset_count else 0,
            'users': user_count[0]['cnt'] if user_count else 0,
            'tags': tag_count[0]['cnt'] if tag_count else 0,
        },
        'top_organizations': top_orgs or [],
        'licenses': licenses or [],
        'formats': formats or [],
        'topics': topics or [],
        'organization_types': org_types or [],
    })

@app.route('/api/health', methods=['GET'])
def health():
    _, err = query("SELECT 1")
    if err:
        print(f"[DEBUG] Connection error: {err}")
        return jsonify({'status': 'error', 'message': err}), 500
    return jsonify({'status': 'ok', 'message': 'Connected to Aiven MySQL'})

if __name__ == '__main__':
    print("=" * 60)
    print("  CSCE 2501 Milestone III — Data.gov Explorer")
    print("  Open http://localhost:5000 in your browser")
    print("=" * 60)
    app.run(debug=True, port=5000)