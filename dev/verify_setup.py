#!/usr/bin/env python3

import os

import json
import psycopg2
import sys
from typing import Dict, List

DEFAULT_CONFIG = os.path.join(
    os.path.dirname(os.path.dirname(__file__)),
    "configs",
    "db",
    "db_config.json",
)

def load_config(config_path: str = DEFAULT_CONFIG) -> Dict:
    """Load database configuration."""
    try:
        with open(config_path, 'r') as f:
            return json.load(f)
    except Exception as e:
        print(f"❌ Error loading config: {e}")
        return {}

def test_connection(config: Dict) -> bool:
    """Test basic database connection."""
    try:
        db_config = config['database']
        conn = psycopg2.connect(
            host=db_config['host'],
            port=db_config['port'],
            database=db_config['database'],
            user=db_config['username'],
            password=db_config['password']
        )
        conn.close()
        print(f"✅ Connection to {db_config['database']} successful")
        return True
    except Exception as e:
        print(f"❌ Connection failed: {e}")
        return False

def check_user_privileges(config: Dict) -> Dict:
    """Check what privileges the user has."""
    try:
        db_config = config['database']
        conn = psycopg2.connect(
            host=db_config['host'],
            port=db_config['port'],
            database=db_config['database'],
            user=db_config['username'],
            password=db_config['password']
        )
        
        privileges = {}
        
        with conn.cursor() as cursor:
            # Check if user is superuser
            cursor.execute("SELECT usesuper FROM pg_user WHERE usename = %s", (db_config['username'],))
            result = cursor.fetchone()
            privileges['superuser'] = result[0] if result else False
            
            # Check if user can create databases
            cursor.execute("SELECT usecreatedb FROM pg_user WHERE usename = %s", (db_config['username'],))
            result = cursor.fetchone()
            privileges['createdb'] = result[0] if result else False
            
            # List available databases
            cursor.execute("SELECT datname FROM pg_database WHERE datistemplate = false ORDER BY datname")
            privileges['databases'] = [row[0] for row in cursor.fetchall()]
            
        conn.close()
        
        print(f"✅ User '{db_config['username']}' privileges:")
        print(f"   - Superuser: {privileges['superuser']}")
        print(f"   - Create DB: {privileges['createdb']}")
        print(f"   - Available databases: {', '.join(privileges['databases'])}")
        
        return privileges
        
    except Exception as e:
        print(f"❌ Error checking privileges: {e}")
        return {}

def test_maintenance_databases(config: Dict) -> List[str]:
    """Test which maintenance databases are available for creating new databases."""
    db_config = config['database']
    available_dbs = []
    
    for maintenance_db in ['template1', 'template0', 'postgres']:
        try:
            conn = psycopg2.connect(
                host=db_config['host'],
                port=db_config['port'],
                database=maintenance_db,
                user=db_config['username'],
                password=db_config['password']
            )
            conn.close()
            available_dbs.append(maintenance_db)
            print(f"✅ Can connect to {maintenance_db}")
        except Exception as e:
            print(f"❌ Cannot connect to {maintenance_db}: {e}")
    
    return available_dbs

def test_database_creation(config: Dict, maintenance_dbs: List[str]) -> bool:
    """Test if we can create a test database."""
    if not maintenance_dbs:
        print("❌ No maintenance databases available for testing database creation")
        return False
    
    db_config = config['database']
    test_db_name = "test_creation_jobscraps"
    
    for maintenance_db in maintenance_dbs:
        try:
            conn = psycopg2.connect(
                host=db_config['host'],
                port=db_config['port'],
                database=maintenance_db,
                user=db_config['username'],
                password=db_config['password']
            )
            conn.autocommit = True
            
            with conn.cursor() as cursor:
                # Clean up any existing test database
                cursor.execute(f"DROP DATABASE IF EXISTS {test_db_name}")
                
                # Try to create test database
                cursor.execute(f"CREATE DATABASE {test_db_name} OWNER {db_config['username']}")
                print(f"✅ Successfully created test database using {maintenance_db}")
                
                # Clean up
                cursor.execute(f"DROP DATABASE {test_db_name}")
                print(f"✅ Test database cleaned up")
                
            conn.close()
            return True
            
        except Exception as e:
            print(f"❌ Database creation test failed using {maintenance_db}: {e}")
    
    return False

def check_existing_tables(config: Dict) -> bool:
    """Check if the required tables exist in the main database."""
    try:
        db_config = config['database']
        conn = psycopg2.connect(
            host=db_config['host'],
            port=db_config['port'],
            database=db_config['database'],
            user=db_config['username'],
            password=db_config['password']
        )
        
        with conn.cursor() as cursor:
            # Check for required tables
            cursor.execute("""
                SELECT table_name 
                FROM information_schema.tables 
                WHERE table_schema = 'public' 
                AND table_name IN ('scraped_jobs', 'search_history')
            """)
            tables = [row[0] for row in cursor.fetchall()]
            
            for table in ['scraped_jobs', 'search_history']:
                if table in tables:
                    cursor.execute(f"SELECT COUNT(*) FROM {table}")
                    count = cursor.fetchone()[0]
                    print(f"✅ Table {table} exists with {count} rows")
                else:
                    print(f"❌ Table {table} missing")
        
        conn.close()
        return len(tables) == 2
        
    except Exception as e:
        print(f"❌ Error checking tables: {e}")
        return False

def main():
    """Run all verification tests."""
    print("🔍 JobScraps PostgreSQL Setup Verification")
    print("=" * 50)
    
    # Load configuration
    config = load_config()
    if not config:
        sys.exit(1)
    
    # Test basic connection
    if not test_connection(config):
        print("\n❌ Basic connection failed. Check your db_config.json settings.")
        sys.exit(1)
    
    # Check user privileges
    privileges = check_user_privileges(config)
    if not privileges:
        sys.exit(1)
    
    # Check tables
    tables_ok = check_existing_tables(config)
    
    # Test maintenance database connections
    print("\n🔍 Testing maintenance database connections:")
    maintenance_dbs = test_maintenance_databases(config)
    
    # Test database creation capability
    print("\n🔍 Testing database creation capability:")
    can_create_db = test_database_creation(config, maintenance_dbs)
    
    # Summary
    print("\n" + "=" * 50)
    print("📋 VERIFICATION SUMMARY")
    print("=" * 50)
    
    if privileges.get('superuser') or privileges.get('createdb'):
        print("✅ User has sufficient privileges")
    else:
        print("❌ User needs CREATEDB or SUPERUSER privileges")
        print("   Run: ALTER USER jonesy CREATEDB;")
    
    if tables_ok:
        print("✅ Required tables exist")
    else:
        print("❌ Missing required tables - run schema setup")
    
    if can_create_db:
        print("✅ Can create working databases")
    else:
        print("❌ Cannot create working databases")
    
    if maintenance_dbs:
        print(f"✅ Maintenance databases available: {', '.join(maintenance_dbs)}")
        print("\n✅ Your setup should work with the scraper!")
        print("   Try: python scraper.py --create-working-copy")
    else:
        print("❌ No maintenance databases available")
        print("   You may need to create working databases manually")
    
    print("\n🛠️  Manual working database creation command:")
    print("   ./workflow_scripts.sh manual")

if __name__ == "__main__":
    main()