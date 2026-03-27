import mysql.connector
import pandas as pd
from datetime import datetime

# ============================================================
# DATABASE CONNECTION SETUP
# ============================================================
def create_connection():
    """Create MySQL connection"""
    try:
        conn = mysql.connector.connect(
            host="127.0.0.1",
            port=3306,
            user="root",
            password="2552005Elmarakby$"
        )
        return conn
    except mysql.connector.Error as err:
        print(f"Error connecting to database: {err}")
        return None

# ============================================================
# DATABASE AND TABLES CREATION
# ============================================================
def create_database_and_tables(cursor):
    """Create database and all required tables"""
    print("\n" + "="*60)
    print("CREATING DATABASE AND TABLES")
    print("="*60)
    
    # Create database
    print("\n[1/7] Creating database 'data_gov_db'...")
    cursor.execute("DROP DATABASE IF EXISTS data_gov_db")
    cursor.execute("CREATE DATABASE IF NOT EXISTS data_gov_db")
    cursor.execute("USE data_gov_db")
    print("✓ Database created")
    
    # Create ORGANIZATIONS table
    print("[2/7] Creating ORGANIZATIONS table...")
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS organizations (
            organization_id INT AUTO_INCREMENT PRIMARY KEY,
            organization_name VARCHAR(255) NOT NULL UNIQUE,
            description TEXT,
            created_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    print("✓ ORGANIZATIONS table created")
    
    # Create TAGS table (WITHOUT tag_id - tag_name is unique identifier)
    print("[3/7] Creating TAGS table...")
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS tags (
            tag_name VARCHAR(100) PRIMARY KEY,
            created_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    print("✓ TAGS table created (tag_name as unique identifier)")
    
    # Create DATASETS table
    print("[4/7] Creating DATASETS table...")
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS datasets (
            dataset_id INT AUTO_INCREMENT PRIMARY KEY,
            dataset_name VARCHAR(500) NOT NULL,
            dataset_url VARCHAR(500),
            description TEXT,
            access_level VARCHAR(50),
            license VARCHAR(2500),
            maintainer_email VARCHAR(255),
            organization_id INT,
            created_date DATE,
            last_updated DATE,
            FOREIGN KEY (organization_id) REFERENCES organizations(organization_id) ON DELETE SET NULL
        )
    """)
    print("✓ DATASETS table created")
    
    # Create DATASET_TAGS junction table (references tag_name, not tag_id)
    print("[5/7] Creating DATASET_TAGS junction table...")
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS dataset_tags (
            dataset_id INT NOT NULL,
            tag_name VARCHAR(100) NOT NULL,
            PRIMARY KEY (dataset_id, tag_name),
            FOREIGN KEY (dataset_id) REFERENCES datasets(dataset_id) ON DELETE CASCADE,
            FOREIGN KEY (tag_name) REFERENCES tags(tag_name) ON DELETE CASCADE
        )
    """)
    print("✓ DATASET_TAGS junction table created")
    
    # Create USERS table
    print("[6/7] Creating USERS table...")
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS users (
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
    """)
    print("✓ USERS table created")
    
    # Create USAGE table
    print("[7/7] Creating USAGE table...")
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS `usage` (
            usage_id INT AUTO_INCREMENT PRIMARY KEY,
            user_id INT NOT NULL,
            dataset_id INT NOT NULL,
            access_count INT,
            download_count INT,
            last_accessed DATE,
            usage_type VARCHAR(50),
            created_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (user_id) REFERENCES users(user_id) ON DELETE CASCADE,
            FOREIGN KEY (dataset_id) REFERENCES datasets(dataset_id) ON DELETE CASCADE
        )
    """)
    print("✓ USAGE table created")

# ============================================================
# DATA POPULATION
# ============================================================
def populate_organizations(cursor, conn):
    """Populate organizations table from CSV"""
    print("\n" + "="*60)
    print("POPULATING DATA")
    print("="*60)
    
    print("\n[1/6] Populating ORGANIZATIONS...")
    try:
        df_orgs = pd.read_csv('organizations.csv')
        
        for _, row in df_orgs.iterrows():
            query = """
                INSERT INTO organizations (organization_id, organization_name, description)
                VALUES (%s, %s, %s)
            """
            cursor.execute(query, (row['organization_id'], row['organization_name'], row['description']))
        
        conn.commit()
        print(f"✓ Inserted {len(df_orgs)} organizations")
    except Exception as e:
        print(f"✗ Error populating organizations: {e}")
        conn.rollback()

def populate_tags(cursor, conn):
    """Populate tags table from CSV"""
    print("[2/6] Populating TAGS...")
    try:
        df_tags = pd.read_csv('tags.csv')
        
        for _, row in df_tags.iterrows():
            query = "INSERT INTO tags (tag_name) VALUES (%s)"
            cursor.execute(query, (row['tag_name'],))
        
        conn.commit()
        print(f"✓ Inserted {len(df_tags)} tags")
    except Exception as e:
        print(f"✗ Error populating tags: {e}")
        conn.rollback()

def populate_datasets(cursor, conn):
    """Populate datasets table from CSV"""
    print("[3/6] Populating DATASETS...")
    try:
        df_datasets = pd.read_csv('datasets.csv')
        
        for _, row in df_datasets.iterrows():
            query = """
                INSERT INTO datasets (dataset_id, dataset_name, dataset_url, description, 
                                     access_level, license, maintainer_email, 
                                     organization_id, created_date, last_updated)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """
            org_id = row['organization_id'] if pd.notna(row['organization_id']) else None
            # Handle NaN values in string columns
            maintainer_email = row['maintainer_email'] if pd.notna(row['maintainer_email']) else 'unknown@data.gov'
            
            cursor.execute(query, (
                int(row['dataset_id']),
                str(row['dataset_name']),
                str(row['dataset_url']),
                str(row['description']),
                str(row['access_level']),
                str(row['license']),
                maintainer_email,
                org_id,
                str(row['created_date']),
                str(row['last_updated'])
            ))
        
        conn.commit()
        print(f"✓ Inserted {len(df_datasets)} datasets")
    except Exception as e:
        print(f"✗ Error populating datasets: {e}")
        conn.rollback()

def populate_dataset_tags(cursor, conn):
    """Populate dataset_tags junction table from CSV"""
    print("[4/6] Populating DATASET_TAGS...")
    try:
        df_dataset_tags = pd.read_csv('dataset_tags.csv')
        
        for _, row in df_dataset_tags.iterrows():
            query = """
                INSERT INTO dataset_tags (dataset_id, tag_name)
                VALUES (%s, %s)
            """
            # Normalize tag_name to lowercase to match tags table
            tag_name = str(row['tag_name']).lower() if pd.notna(row['tag_name']) else ''
            cursor.execute(query, (int(row['dataset_id']), tag_name))
        
        conn.commit()
        print(f"✓ Inserted {len(df_dataset_tags)} dataset-tag mappings")
    except Exception as e:
        print(f"✗ Error populating dataset_tags: {e}")
        conn.rollback()

def populate_users(cursor, conn):
    """Populate users table from CSV"""
    print("[5/6] Populating USERS...")
    try:
        df_users = pd.read_csv('users_final.csv')
        
        for _, row in df_users.iterrows():
            query = """
                INSERT INTO users (user_id, email, username, full_name, gender, age, country, registration_date)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            """
            cursor.execute(query, (
                row['user_id'],
                row['email'],
                row['username'],
                row['full_name'],
                row['gender'],
                row['age'],
                row['country'],
                row['registration_date']
            ))
        
        conn.commit()
        print(f"✓ Inserted {len(df_users)} users")
    except Exception as e:
        print(f"✗ Error populating users: {e}")
        conn.rollback()

def populate_usage(cursor, conn):
    """Populate usage table from CSV"""
    print("[6/6] Populating USAGE...")
    try:
        df_usage = pd.read_csv('usage.csv')
        
        for _, row in df_usage.iterrows():
            query = """
                INSERT INTO `usage` (usage_id, user_id, dataset_id, access_count, 
                                  download_count, last_accessed, usage_type)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
            """
            cursor.execute(query, (
                row['usage_id'],
                row['user_id'],
                row['dataset_id'],
                row['access_count'],
                row['download_count'],
                row['last_accessed'],
                row['usage_type']
            ))
        
        conn.commit()
        print(f"✓ Inserted {len(df_usage)} usage records")
    except Exception as e:
        print(f"✗ Error populating usage: {e}")
        conn.rollback()

# ============================================================
# VERIFICATION & SUMMARY
# ============================================================
def verify_data(cursor):
    """Verify data was inserted correctly"""
    print("\n" + "="*60)
    print("DATA VERIFICATION")
    print("="*60)
    
    tables = {
        'organizations': 'SELECT COUNT(*) FROM organizations',
        'tags': 'SELECT COUNT(*) FROM tags',
        'datasets': 'SELECT COUNT(*) FROM datasets',
        'dataset_tags': 'SELECT COUNT(*) FROM dataset_tags',
        'users': 'SELECT COUNT(*) FROM users',
        'usage': 'SELECT COUNT(*) FROM `usage`'
    }
    
    print("\nRecord counts:")
    for table_name, query in tables.items():
        cursor.execute(query)
        count = cursor.fetchone()[0]
        print(f"  • {table_name:20s}: {count:5d} records")

# ============================================================
# MAIN EXECUTION
# ============================================================
def main():
    print("\n" + "="*60)
    print("DATA.GOV MILESTONE 2 - DATABASE POPULATION")
    print("="*60)
    
    # Connect to database
    print("\nConnecting to MySQL...")
    conn = create_connection()
    if not conn:
        return
    
    cursor = conn.cursor()
    
    try:
        # Create database and tables
        create_database_and_tables(cursor)
        
        # Populate all tables
        populate_organizations(cursor, conn)
        populate_tags(cursor, conn)
        populate_datasets(cursor, conn)
        populate_dataset_tags(cursor, conn)
        populate_users(cursor, conn)
        populate_usage(cursor, conn)
        
        # Verify data
        verify_data(cursor)
        
        print("\n" + "="*60)
        print("✓ DATABASE POPULATION COMPLETE")
        print("="*60)
        print("\nDatabase: data_gov_db")
        print("Schema implemented with feedback:")
        print("  • TAGS table uses tag_name as unique PRIMARY KEY")
        print("  • tag_id removed (redundant)")
        print("  • All foreign keys properly configured")
        print("="*60 + "\n")
        
    except Exception as e:
        print(f"\n✗ Error during execution: {e}")
        conn.rollback()
    
    finally:
        cursor.close()
        conn.close()

if __name__ == "__main__":
    main()
