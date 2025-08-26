#!/usr/bin/env python3
"""
Setup script for DataProxy local database.
"""

import sys
import os
import pymysql
from pathlib import Path

# Add parent directory to path for imports
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from dataproxy.config import Config


def create_local_database():
    """Create the local database if it doesn't exist."""
    try:
        # Connect to MySQL server (without specifying database)
        connection = pymysql.connect(
            host=Config.LOCAL_DB_HOST,
            port=Config.LOCAL_DB_PORT,
            user=Config.LOCAL_DB_USER,
            password=Config.LOCAL_DB_PASSWORD,
            charset='utf8mb4'
        )
        
        with connection.cursor() as cursor:
            # Create database if it doesn't exist
            cursor.execute(f"CREATE DATABASE IF NOT EXISTS `{Config.LOCAL_DB_NAME}`")
            print(f"✅ Local database '{Config.LOCAL_DB_NAME}' created/verified")
            
            # Use the database
            cursor.execute(f"USE `{Config.LOCAL_DB_NAME}`")
            
            # Create a simple test table to verify functionality
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS `dataproxy_metadata` (
                    `key` VARCHAR(255) NOT NULL PRIMARY KEY,
                    `value` TEXT,
                    `created_at` TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    `updated_at` TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
                )
            """)
            
            # Insert initial metadata
            cursor.execute("""
                INSERT IGNORE INTO `dataproxy_metadata` (`key`, `value`) 
                VALUES ('version', '0.1.0')
            """)
            
            connection.commit()
            print("✅ Metadata table created and initialized")
            
    except Exception as e:
        print(f"❌ Error creating local database: {e}")
        return False
    finally:
        if 'connection' in locals():
            connection.close()
    
    return True


def test_connections():
    """Test both production and local database connections."""
    print("\n🔍 Testing database connections...")
    
    # Test production connection
    try:
        prod_conn = pymysql.connect(
            host=Config.PROD_DB_HOST,
            port=Config.PROD_DB_PORT,
            user=Config.PROD_DB_USER,
            password=Config.PROD_DB_PASSWORD,
            database=Config.PROD_DB_NAME,
            charset='utf8mb4'
        )
        print(f"✅ Production database: Connected to {Config.PROD_DB_HOST}:{Config.PROD_DB_PORT}/{Config.PROD_DB_NAME}")
        prod_conn.close()
    except Exception as e:
        print(f"❌ Production database: Connection failed - {e}")
        return False
    
    # Test local connection
    try:
        local_conn = pymysql.connect(
            host=Config.LOCAL_DB_HOST,
            port=Config.LOCAL_DB_PORT,
            user=Config.LOCAL_DB_USER,
            password=Config.LOCAL_DB_PASSWORD,
            database=Config.LOCAL_DB_NAME,
            charset='utf8mb4'
        )
        print(f"✅ Local database: Connected to {Config.LOCAL_DB_HOST}:{Config.LOCAL_DB_PORT}/{Config.LOCAL_DB_NAME}")
        local_conn.close()
    except Exception as e:
        print(f"❌ Local database: Connection failed - {e}")
        return False
    
    return True


def main():
    """Main setup function."""
    print("🚀 DataProxy Local Database Setup")
    print("=" * 40)
    
    # Check if .env file exists
    env_file = Path(".env")
    if not env_file.exists():
        print("❌ .env file not found!")
        print("Please copy env.example to .env and configure your database settings")
        return False
    
    # Validate configuration
    if not Config.validate():
        print("❌ Invalid configuration!")
        print("Please check your .env file and ensure all required values are set")
        return False
    
    print("✅ Configuration validated")
    
    # Create local database
    if not create_local_database():
        return False
    
    # Test connections
    if not test_connections():
        return False
    
    print("\n🎉 Setup completed successfully!")
    print("\nNext steps:")
    print("1. Start DataProxy: python -m dataproxy.main start")
    print("2. Test connectivity: python -m dataproxy.main test")
    print("3. Sync schemas: python -m dataproxy.main sync")
    
    return True


if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)
