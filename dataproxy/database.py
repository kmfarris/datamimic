"""
Database connection management for DataProxy.
"""

import logging
from typing import Optional, Dict, Any
import pymysql
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine
from sqlalchemy.exc import SQLAlchemyError

from .config import Config

logger = logging.getLogger(__name__)


class DatabaseManager:
    """Manages database connections for production and local databases."""
    
    def __init__(self):
        self.prod_engine: Optional[Engine] = None
        self.local_engine: Optional[Engine] = None
        self.prod_connection: Optional[pymysql.Connection] = None
        self.local_connection: Optional[pymysql.Connection] = None
        
    def connect_production(self) -> bool:
        """Connect to production database."""
        try:
            self.prod_engine = create_engine(
                Config.get_prod_connection_string(),
                pool_pre_ping=True,
                pool_recycle=300
            )
            
            # Test connection
            with self.prod_engine.connect() as conn:
                conn.execute(text("SELECT 1"))
            
            logger.info("Connected to production database")
            return True
            
        except SQLAlchemyError as e:
            logger.error(f"Failed to connect to production database: {e}")
            return False
    
    def connect_local(self) -> bool:
        """Connect to local database."""
        try:
            self.local_engine = create_engine(
                Config.get_local_connection_string(),
                pool_pre_ping=True,
                pool_recycle=300
            )
            
            # Test connection
            with self.local_engine.connect() as conn:
                conn.execute(text("SELECT 1"))
            
            logger.info("Connected to local database")
            return True
            
        except SQLAlchemyError as e:
            logger.error(f"Failed to connect to local database: {e}")
            return False
    
    def get_production_connection(self) -> Optional[pymysql.Connection]:
        """Get a direct PyMySQL connection to production database."""
        try:
            if not self.prod_connection or not self.prod_connection.open:
                self.prod_connection = pymysql.connect(
                    host=Config.PROD_DB_HOST,
                    port=Config.PROD_DB_PORT,
                    user=Config.PROD_DB_USER,
                    password=Config.PROD_DB_PASSWORD,
                    database=Config.PROD_DB_NAME,
                    charset='utf8mb4',
                    autocommit=True
                )
            return self.prod_connection
        except Exception as e:
            logger.error(f"Failed to get production connection: {e}")
            return None
    
    def get_local_connection(self) -> Optional[pymysql.Connection]:
        """Get a direct PyMySQL connection to local database."""
        try:
            if not self.local_connection or not self.local_connection.open:
                self.local_connection = pymysql.connect(
                    host=Config.LOCAL_DB_HOST,
                    port=Config.LOCAL_DB_PORT,
                    user=Config.LOCAL_DB_USER,
                    password=Config.LOCAL_DB_PASSWORD,
                    database=Config.LOCAL_DB_NAME,
                    charset='utf8mb4',
                    autocommit=True
                )
            return self.local_connection
        except Exception as e:
            logger.error(f"Failed to get local connection: {e}")
            return None
    
    def execute_production_query(self, query: str, params: Optional[tuple] = None) -> Optional[list]:
        """Execute a query on production database."""
        try:
            with self.prod_engine.connect() as conn:
                result = conn.execute(text(query), params or {})
                if result.returns_rows:
                    return [dict(row._mapping) for row in result]
                return None
        except SQLAlchemyError as e:
            logger.error(f"Production query failed: {e}")
            return None
    
    def execute_local_query(self, query: str, params: Optional[tuple] = None) -> Optional[list]:
        """Execute a query on local database."""
        try:
            with self.local_engine.connect() as conn:
                result = conn.execute(text(query), params or {})
                if result.returns_rows:
                    return [dict(row._mapping) for row in result]
                return None
        except SQLAlchemyError as e:
            logger.error(f"Local query failed: {e}")
            return None
    
    def table_exists_local(self, table_name: str) -> bool:
        """Check if a table exists in local database."""
        query = """
        SELECT COUNT(*) as count 
        FROM information_schema.tables 
        WHERE table_schema = %s AND table_name = %s
        """
        result = self.execute_local_query(query, (Config.LOCAL_DB_NAME, table_name))
        return result and result[0]['count'] > 0
    
    def get_table_schema(self, table_name: str) -> Optional[Dict[str, Any]]:
        """Get table schema from production database."""
        query = """
        SELECT column_name, data_type, is_nullable, column_default, extra
        FROM information_schema.columns 
        WHERE table_schema = %s AND table_name = %s
        ORDER BY ordinal_position
        """
        return self.execute_production_query(query, (Config.PROD_DB_NAME, table_name))
    
    def create_local_table(self, table_name: str, schema: list) -> bool:
        """Create a table in local database based on production schema."""
        if not schema:
            return False
        
        columns = []
        for col in schema:
            col_def = f"`{col['column_name']}` {col['data_type']}"
            
            if col['is_nullable'] == 'NO':
                col_def += " NOT NULL"
            
            if col['column_default'] is not None:
                col_def += f" DEFAULT {col['column_default']}"
            
            if col['extra']:
                col_def += f" {col['extra']}"
            
            columns.append(col_def)
        
        create_query = f"CREATE TABLE IF NOT EXISTS `{table_name}` ({', '.join(columns)})"
        
        try:
            self.execute_local_query(create_query)
            logger.info(f"Created local table: {table_name}")
            return True
        except Exception as e:
            logger.error(f"Failed to create local table {table_name}: {e}")
            return False
    
    def close(self):
        """Close all database connections."""
        if self.prod_connection and self.prod_connection.open:
            self.prod_connection.close()
        if self.local_connection and self.local_connection.open:
            self.local_connection.close()
        
        if self.prod_engine:
            self.prod_engine.dispose()
        if self.local_engine:
            self.local_engine.dispose()
        
        logger.info("Closed all database connections")
