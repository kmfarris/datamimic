"""
Query analysis and classification for DataProxy.
"""

import re
import logging
from typing import Tuple, List, Optional

logger = logging.getLogger(__name__)


class QueryAnalyzer:
    """Analyzes SQL queries to determine their type and extract metadata."""
    
    # Keywords that indicate write operations
    WRITE_KEYWORDS = {
        'INSERT', 'UPDATE', 'DELETE', 'REPLACE', 'TRUNCATE', 'DROP', 'CREATE', 'ALTER'
    }
    
    # Keywords that indicate read operations
    READ_KEYWORDS = {
        'SELECT', 'SHOW', 'DESCRIBE', 'EXPLAIN', 'USE'
    }
    
    @staticmethod
    def analyze_query(query: str) -> Tuple[str, List[str], str]:
        """
        Analyze a SQL query to determine its type and extract table names.
        
        Returns:
            Tuple of (query_type, table_names, original_query)
        """
        # Normalize query
        normalized_query = query.strip().upper()
        
        # Remove comments and extra whitespace
        normalized_query = re.sub(r'--.*$', '', normalized_query, flags=re.MULTILINE)
        normalized_query = re.sub(r'/\*.*?\*/', '', normalized_query, flags=re.DOTALL)
        normalized_query = re.sub(r'\s+', ' ', normalized_query).strip()
        
        # Determine query type
        query_type = QueryAnalyzer._classify_query(normalized_query)
        
        # Extract table names
        table_names = QueryAnalyzer._extract_table_names(normalized_query, query_type)
        
        return query_type, table_names, query.strip()
    
    @staticmethod
    def _classify_query(query: str) -> str:
        """Classify query as READ, WRITE, or UNKNOWN."""
        words = query.split()
        
        if not words:
            return "UNKNOWN"
        
        first_word = words[0]
        
        if first_word in QueryAnalyzer.WRITE_KEYWORDS:
            return "WRITE"
        elif first_word in QueryAnalyzer.READ_KEYWORDS:
            return "READ"
        else:
            # Check for other patterns
            if "INTO" in query or "SET" in query:
                return "WRITE"
            elif "FROM" in query or "JOIN" in query:
                return "READ"
            else:
                return "UNKNOWN"
    
    @staticmethod
    def _extract_table_names(query: str, query_type: str) -> List[str]:
        """Extract table names from the query."""
        table_names = []
        
        try:
            if query_type == "READ":
                # Extract from FROM and JOIN clauses
                from_pattern = r'FROM\s+`?(\w+)`?'
                join_pattern = r'JOIN\s+`?(\w+)`?'
                
                from_matches = re.findall(from_pattern, query, re.IGNORECASE)
                join_matches = re.findall(join_pattern, query, re.IGNORECASE)
                
                table_names.extend(from_matches)
                table_names.extend(join_matches)
                
            elif query_type == "WRITE":
                # Extract from INSERT INTO, UPDATE, DELETE FROM
                if query.startswith('INSERT'):
                    into_pattern = r'INTO\s+`?(\w+)`?'
                    matches = re.findall(into_pattern, query, re.IGNORECASE)
                    table_names.extend(matches)
                    
                elif query.startswith('UPDATE'):
                    update_pattern = r'UPDATE\s+`?(\w+)`?'
                    matches = re.findall(update_pattern, query, re.IGNORECASE)
                    table_names.extend(matches)
                    
                elif query.startswith('DELETE'):
                    from_pattern = r'FROM\s+`?(\w+)`?'
                    matches = re.findall(from_pattern, query, re.IGNORECASE)
                    table_names.extend(matches)
                    
                elif query.startswith('REPLACE'):
                    into_pattern = r'INTO\s+`?(\w+)`?'
                    matches = re.findall(into_pattern, query, re.IGNORECASE)
                    table_names.extend(matches)
                    
                elif query.startswith('TRUNCATE'):
                    table_pattern = r'TABLE\s+`?(\w+)`?'
                    matches = re.findall(table_pattern, query, re.IGNORECASE)
                    table_names.extend(matches)
                    
                elif query.startswith('DROP'):
                    table_pattern = r'TABLE\s+`?(\w+)`?'
                    matches = re.findall(table_pattern, query, re.IGNORECASE)
                    table_names.extend(matches)
                    
                elif query.startswith('CREATE'):
                    table_pattern = r'TABLE\s+`?(\w+)`?'
                    matches = re.findall(table_pattern, query, re.IGNORECASE)
                    table_names.extend(matches)
                    
                elif query.startswith('ALTER'):
                    table_pattern = r'TABLE\s+`?(\w+)`?'
                    matches = re.findall(table_pattern, query, re.IGNORECASE)
                    table_names.extend(matches)
            
            # Remove duplicates and empty strings
            table_names = list(set(filter(None, table_names)))
            
        except Exception as e:
            logger.warning(f"Failed to extract table names from query: {e}")
            table_names = []
        
        return table_names
    
    @staticmethod
    def is_safe_query(query: str) -> bool:
        """Check if a query is safe to execute (basic security check)."""
        dangerous_patterns = [
            r'DROP\s+DATABASE',
            r'CREATE\s+DATABASE',
            r'ALTER\s+DATABASE',
            r'GRANT\s+.*\s+ON',
            r'REVOKE\s+.*\s+FROM',
            r'FLUSH\s+PRIVILEGES',
            r'SET\s+PASSWORD',
            r'CREATE\s+USER',
            r'DROP\s+USER'
        ]
        
        normalized_query = query.strip().upper()
        
        for pattern in dangerous_patterns:
            if re.search(pattern, normalized_query, re.IGNORECASE):
                return False
        
        return True
    
    @staticmethod
    def extract_where_conditions(query: str) -> Optional[str]:
        """Extract WHERE clause from a query for logging purposes."""
        where_match = re.search(r'WHERE\s+(.+?)(?:\s+ORDER\s+BY|\s+GROUP\s+BY|\s+LIMIT|\s+HAVING|$)', 
                               query, re.IGNORECASE | re.DOTALL)
        return where_match.group(1).strip() if where_match else None
