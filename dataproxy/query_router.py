"""
Query routing logic for DataProxy.
"""

import logging
from typing import Optional, List, Dict, Any
import pymysql

from .database import DatabaseManager
from .query_analyzer import QueryAnalyzer

logger = logging.getLogger(__name__)


class QueryRouter:
    """Routes queries between production and local databases."""
    
    def __init__(self, db_manager: DatabaseManager):
        self.db_manager = db_manager
        self.local_tables = set()  # Track which tables exist locally
    
    def route_query(self, query: str) -> Dict[str, Any]:
        """
        Route a query to the appropriate database and return results.
        
        Returns:
            Dictionary with results, metadata, and routing information
        """
        # Analyze the query
        query_type, table_names, clean_query = QueryAnalyzer.analyze_query(query)
        
        # Security check
        if not QueryAnalyzer.is_safe_query(query):
            return {
                'success': False,
                'error': 'Query contains potentially dangerous operations',
                'query_type': query_type,
                'routed_to': 'rejected'
            }
        
        logger.info(f"Routing {query_type} query: {clean_query[:100]}...")
        
        try:
            if query_type == "READ":
                return self._handle_read_query(query, table_names)
            elif query_type == "WRITE":
                return self._handle_write_query(query, table_names)
            else:
                return self._handle_unknown_query(query)
                
        except Exception as e:
            logger.error(f"Error routing query: {e}")
            return {
                'success': False,
                'error': str(e),
                'query_type': query_type,
                'routed_to': 'error'
            }
    
    def _handle_read_query(self, query: str, table_names: List[str]) -> Dict[str, Any]:
        """Handle read queries by checking local vs production data."""
        # Check if all tables exist locally
        all_local = all(self.db_manager.table_exists_local(table) for table in table_names)
        
        if all_local and table_names:
            # All tables exist locally, try local first
            logger.info(f"All tables exist locally, reading from local database")
            result = self.db_manager.execute_local_query(query)
            
            if result is not None:
                return {
                    'success': True,
                    'data': result,
                    'query_type': 'READ',
                    'routed_to': 'local',
                    'table_names': table_names
                }
            else:
                # Local query failed, fallback to production
                logger.warning("Local query failed, falling back to production")
        
        # Read from production database
        logger.info(f"Reading from production database")
        result = self.db_manager.execute_production_query(query)
        
        return {
            'success': True,
            'data': result,
            'query_type': 'READ',
            'routed_to': 'production',
            'table_names': table_names
        }
    
    def _handle_write_query(self, query: str, table_names: List[str]) -> Dict[str, Any]:
        """Handle write queries by storing data locally."""
        if not table_names:
            return {
                'success': False,
                'error': 'No table names found in write query',
                'query_type': 'WRITE',
                'routed_to': 'error'
            }
        
        # Ensure all tables exist locally
        for table_name in table_names:
            if not self.db_manager.table_exists_local(table_name):
                logger.info(f"Creating local table: {table_name}")
                schema = self.db_manager.get_table_schema(table_name)
                if schema:
                    success = self.db_manager.create_local_table(table_name, schema)
                    if not success:
                        return {
                            'success': False,
                            'error': f'Failed to create local table: {table_name}',
                            'query_type': 'WRITE',
                            'routed_to': 'error'
                        }
                else:
                    return {
                        'success': False,
                        'error': f'Failed to get schema for table: {table_name}',
                        'query_type': 'WRITE',
                        'routed_to': 'error'
                    }
        
        # Execute write query on local database
        logger.info(f"Executing write query on local database")
        result = self.db_manager.execute_local_query(query)
        
        if result is not None:
            return {
                'success': True,
                'data': result,
                'query_type': 'WRITE',
                'routed_to': 'local',
                'table_names': table_names,
                'rows_affected': self._get_rows_affected(query)
            }
        else:
            return {
                'success': False,
                'error': 'Write query failed on local database',
                'query_type': 'WRITE',
                'routed_to': 'error'
            }
    
    def _handle_unknown_query(self, query: str) -> Dict[str, Any]:
        """Handle queries that couldn't be classified."""
        logger.warning(f"Unknown query type, attempting production execution: {query}")
        
        # Try to execute on production as a fallback
        result = self.db_manager.execute_production_query(query)
        
        return {
            'success': result is not None,
            'data': result,
            'query_type': 'UNKNOWN',
            'routed_to': 'production',
            'error': 'Query type unknown, executed on production'
        }
    
    def _get_rows_affected(self, query: str) -> Optional[int]:
        """Extract the number of rows affected from a write query result."""
        # This is a simplified approach - in practice, you'd want to capture
        # the actual rows affected from the database response
        if query.upper().startswith('INSERT'):
            return 1  # Assume single row insert
        elif query.upper().startswith('UPDATE'):
            return 1  # Assume single row update
        elif query.upper().startswith('DELETE'):
            return 1  # Assume single row delete
        return None
    
    def get_local_table_status(self) -> Dict[str, bool]:
        """Get status of all tables in local database."""
        status = {}
        # This would need to be implemented to check all tables
        # For now, return the tracked tables
        for table in self.local_tables:
            status[table] = self.db_manager.table_exists_local(table)
        return status
    
    def sync_table_schema(self, table_name: str) -> bool:
        """Synchronize a table's schema from production to local."""
        if not self.db_manager.table_exists_local(table_name):
            schema = self.db_manager.get_table_schema(table_name)
            if schema:
                return self.db_manager.create_local_table(table_name, schema)
        return False
