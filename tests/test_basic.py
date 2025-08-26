"""
Basic tests for DataProxy functionality.
"""

import unittest
from unittest.mock import Mock, patch
import sys
import os

# Add parent directory to path for imports
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from dataproxy.query_analyzer import QueryAnalyzer
from dataproxy.config import Config


class TestQueryAnalyzer(unittest.TestCase):
    """Test query analysis functionality."""
    
    def test_read_query_detection(self):
        """Test that read queries are properly identified."""
        read_queries = [
            "SELECT * FROM users",
            "SELECT id, name FROM products WHERE category = 'electronics'",
            "SHOW TABLES",
            "DESCRIBE users",
            "EXPLAIN SELECT * FROM orders"
        ]
        
        for query in read_queries:
            query_type, table_names, clean_query = QueryAnalyzer.analyze_query(query)
            self.assertEqual(query_type, "READ", f"Query should be READ: {query}")
    
    def test_write_query_detection(self):
        """Test that write queries are properly identified."""
        write_queries = [
            "INSERT INTO users (name, email) VALUES ('John', 'john@example.com')",
            "UPDATE users SET email = 'new@example.com' WHERE id = 1",
            "DELETE FROM users WHERE id = 1",
            "REPLACE INTO products (id, name) VALUES (1, 'New Product')",
            "TRUNCATE TABLE logs"
        ]
        
        for query in write_queries:
            query_type, table_names, clean_query = QueryAnalyzer.analyze_query(query)
            self.assertEqual(query_type, "WRITE", f"Query should be WRITE: {query}")
    
    def test_table_name_extraction(self):
        """Test that table names are properly extracted."""
        query = "SELECT * FROM users JOIN orders ON users.id = orders.user_id"
        query_type, table_names, clean_query = QueryAnalyzer.analyze_query(query)
        
        self.assertIn("users", table_names)
        self.assertIn("orders", table_names)
        self.assertEqual(len(table_names), 2)
    
    def test_safe_query_check(self):
        """Test that dangerous queries are properly identified."""
        dangerous_queries = [
            "DROP DATABASE production",
            "CREATE DATABASE test",
            "GRANT ALL PRIVILEGES ON *.* TO 'user'@'%'",
            "FLUSH PRIVILEGES"
        ]
        
        for query in dangerous_queries:
            self.assertFalse(QueryAnalyzer.is_safe_query(query), 
                           f"Query should be unsafe: {query}")
        
        safe_queries = [
            "SELECT * FROM users",
            "INSERT INTO logs (message) VALUES ('test')",
            "UPDATE settings SET value = 'new' WHERE key = 'test'"
        ]
        
        for query in safe_queries:
            self.assertTrue(QueryAnalyzer.is_safe_query(query), 
                          f"Query should be safe: {query}")


class TestConfig(unittest.TestCase):
    """Test configuration management."""
    
    def test_connection_strings(self):
        """Test database connection string generation."""
        # Mock environment variables
        with patch.dict(os.environ, {
            'PROD_DB_HOST': 'prod.example.com',
            'PROD_DB_PORT': '3306',
            'PROD_DB_USER': 'produser',
            'PROD_DB_PASSWORD': 'prodpass',
            'PROD_DB_NAME': 'proddb'
        }):
            # Reload config
            import importlib
            import dataproxy.config
            importlib.reload(dataproxy.config)
            
            connection_string = dataproxy.config.Config.get_prod_connection_string()
            self.assertIn('prod.example.com', connection_string)
            self.assertIn('produser', connection_string)
            self.assertIn('proddb', connection_string)


if __name__ == '__main__':
    unittest.main()
