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
    'port': 25162,
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
    required = ['email', 'username', 'gender', 'age', 'birthdate', 'country']
    for f in required:
        if not data.get(f):
            return jsonify({'error': f'Missing field: {f}'}), 400

    result, err = query(
        """INSERT INTO Users (email, username, gender, age, birthdate, country)
           VALUES (%s, %s, %s, %s, %s, %s)""",
        (data['email'], data['username'], data['gender'],
         data['age'], data['birthdate'], data['country']),
        fetch=False
    )
    if err:
        return jsonify({'error': err}), 500
    return jsonify({'message': 'User registered successfully', 'user_id': result}), 201

# ─────────────────────────────────────────────
# 2. ADD DATASET USAGE
# ─────────────────────────────────────────────

@app.route('/api/usage', methods=['POST'])
def add_usage():
    data = request.json
    required = ['user_id', 'dataset_id', 'project_name', 'project_category']
    for f in required:
        if not data.get(f):
            return jsonify({'error': f'Missing field: {f}'}), 400

    valid_categories = ['analytics', 'machine learning', 'field research']
    if data['project_category'].lower() not in valid_categories:
        return jsonify({'error': f'project_category must be one of: {valid_categories}'}), 400

    result, err = query(
        """INSERT INTO DatasetUsage (user_id, dataset_id, project_name, project_category, usage_date)
           VALUES (%s, %s, %s, %s, NOW())""",
        (data['user_id'], data['dataset_id'], data['project_name'], data['project_category']),
        fetch=False
    )
    if err:
        return jsonify({'error': err}), 500
    return jsonify({'message': 'Usage recorded', 'usage_id': result}), 201

# ─────────────────────────────────────────────
# 3. VIEW USER USAGE HISTORY
# ─────────────────────────────────────────────

@app.route('/api/users/<int:user_id>/usage', methods=['GET'])
def user_usage(user_id):
    rows, err = query(
        """SELECT du.usage_id, d.dataset_name, du.project_name,
                  du.project_category, du.usage_date
           FROM DatasetUsage du
           JOIN Datasets d ON du.dataset_id = d.dataset_id
           WHERE du.user_id = %s
           ORDER BY du.usage_date DESC""",
        (user_id,)
    )
    if err:
        return jsonify({'error': err}), 500
    return jsonify(rows)

# ─────────────────────────────────────────────
# 4. DATASETS BY ORGANIZATION TYPE
# ─────────────────────────────────────────────

@app.route('/api/datasets/by-org-type', methods=['GET'])
def datasets_by_org_type():
    org_type = request.args.get('type', '')
    if org_type:
        rows, err = query(
            """SELECT d.dataset_id, d.dataset_name, d.access_level,
                      o.organization_name, o.organization_type
               FROM Datasets d
               JOIN Organizations o ON d.organization_id = o.organization_id
               WHERE o.organization_type LIKE %s
               ORDER BY o.organization_name, d.dataset_name
               LIMIT 100""",
            (f'%{org_type}%',)
        )
    else:
        rows, err = query(
            """SELECT d.dataset_id, d.dataset_name, d.access_level,
                      o.organization_name, o.organization_type
               FROM Datasets d
               JOIN Organizations o ON d.organization_id = o.organization_id
               ORDER BY o.organization_type, o.organization_name
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
        """SELECT o.organization_name, o.organization_type,
                  COUNT(d.dataset_id) AS dataset_count
           FROM Organizations o
           JOIN Datasets d ON o.organization_id = d.organization_id
           GROUP BY o.organization_id, o.organization_name, o.organization_type
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
        """SELECT DISTINCT d.dataset_id, d.dataset_name, d.access_level,
                  o.organization_name, f.format_type, f.url
           FROM Datasets d
           JOIN Organizations o ON d.organization_id = o.organization_id
           JOIN DatasetFormats f ON d.dataset_id = f.dataset_id
           WHERE f.format_type LIKE %s
           ORDER BY d.dataset_name
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
        """SELECT DISTINCT d.dataset_id, d.dataset_name, d.access_level,
                  o.organization_name, t.tag_name
           FROM Datasets d
           JOIN Organizations o ON d.organization_id = o.organization_id
           JOIN DatasetTags dt ON d.dataset_id = dt.dataset_id
           JOIN Tags t ON dt.tag_id = t.tag_id
           WHERE t.tag_name LIKE %s
           ORDER BY d.dataset_name
           LIMIT 100""",
        (f'%{tag}%',)
    )
    if err:
        return jsonify({'error': err}), 500
    return jsonify(rows)

# ─────────────────────────────────────────────
# 8. TOTAL DATASETS BY DIMENSION
# ─────────────────────────────────────────────

@app.route('/api/stats/totals', methods=['GET'])
def stats_totals():
    by_org, _ = query(
        """SELECT o.organization_name, COUNT(d.dataset_id) AS total
           FROM Organizations o
           JOIN Datasets d ON o.organization_id = d.organization_id
           GROUP BY o.organization_id, o.organization_name
           ORDER BY total DESC LIMIT 20"""
    )
    by_topic, _ = query(
        """SELECT d.topic, COUNT(*) AS total
           FROM Datasets d
           WHERE d.topic IS NOT NULL AND d.topic != 'N/A'
           GROUP BY d.topic
           ORDER BY total DESC LIMIT 20"""
    )
    by_format, _ = query(
        """SELECT f.format_type, COUNT(DISTINCT f.dataset_id) AS total
           FROM DatasetFormats f
           GROUP BY f.format_type
           ORDER BY total DESC LIMIT 20"""
    )
    by_org_type, _ = query(
        """SELECT o.organization_type, COUNT(d.dataset_id) AS total
           FROM Organizations o
           JOIN Datasets d ON o.organization_id = d.organization_id
           GROUP BY o.organization_type
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
        """SELECT d.dataset_id, d.dataset_name, o.organization_name,
                  COUNT(DISTINCT du.user_id) AS user_count
           FROM Datasets d
           JOIN DatasetUsage du ON d.dataset_id = du.dataset_id
           JOIN Organizations o ON d.organization_id = o.organization_id
           GROUP BY d.dataset_id, d.dataset_name, o.organization_name
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
        """SELECT project_category, COUNT(*) AS usage_count
           FROM DatasetUsage
           GROUP BY project_category
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
        """SELECT du.project_category, t.tag_name,
                  COUNT(*) AS tag_count
           FROM DatasetUsage du
           JOIN DatasetTags dt ON du.dataset_id = dt.dataset_id
           JOIN Tags t ON dt.tag_id = t.tag_id
           GROUP BY du.project_category, t.tag_id, t.tag_name
           ORDER BY du.project_category, tag_count DESC"""
    )
    if err:
        return jsonify({'error': err}), 500

    # Group top 10 per project type
    grouped = {}
    for row in (rows or []):
        cat = row['project_category']
        if cat not in grouped:
            grouped[cat] = []
        if len(grouped[cat]) < 10:
            grouped[cat].append({'tag': row['tag_name'], 'count': row['tag_count']})

    return jsonify(grouped)

# ─────────────────────────────────────────────
# HELPER: available org types and formats
# ─────────────────────────────────────────────

@app.route('/api/meta/org-types', methods=['GET'])
def org_types():
    rows, err = query("SELECT DISTINCT organization_type FROM Organizations ORDER BY organization_type")
    if err:
        return jsonify([])
    return jsonify([r['organization_type'] for r in rows])

@app.route('/api/meta/formats', methods=['GET'])
def formats():
    rows, err = query("SELECT DISTINCT format_type FROM DatasetFormats ORDER BY format_type LIMIT 50")
    if err:
        return jsonify([])
    return jsonify([r['format_type'] for r in rows])

@app.route('/api/meta/tags', methods=['GET'])
def tags():
    rows, err = query("SELECT DISTINCT tag_name FROM Tags ORDER BY tag_name LIMIT 100")
    if err:
        return jsonify([])
    return jsonify([r['tag_name'] for r in rows])

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