"""
Configuration management for DataProxy.
"""

import os
from typing import Optional
from dotenv import load_dotenv

# Load environment variables
load_dotenv()


class Config:
    """Configuration class for DataProxy."""
    
    # Production database
    PROD_DB_HOST: str = os.getenv("PROD_DB_HOST", "localhost")
    PROD_DB_PORT: int = int(os.getenv("PROD_DB_PORT", "3306"))
    PROD_DB_USER: str = os.getenv("PROD_DB_USER", "root")
    PROD_DB_PASSWORD: str = os.getenv("PROD_DB_PASSWORD", "")
    PROD_DB_NAME: str = os.getenv("PROD_DB_NAME", "test")
    
    # Local database
    LOCAL_DB_HOST: str = os.getenv("LOCAL_DB_HOST", "localhost")
    LOCAL_DB_PORT: int = int(os.getenv("LOCAL_DB_PORT", "3306"))
    LOCAL_DB_USER: str = os.getenv("LOCAL_DB_USER", "root")
    LOCAL_DB_PASSWORD: str = os.getenv("LOCAL_DB_PASSWORD", "")
    LOCAL_DB_NAME: str = os.getenv("LOCAL_DB_NAME", "dataproxy_local")
    
    # Proxy server
    PROXY_HOST: str = os.getenv("PROXY_HOST", "localhost")
    PROXY_PORT: int = int(os.getenv("PROXY_PORT", "3307"))
    
    # Logging
    LOG_LEVEL: str = os.getenv("LOG_LEVEL", "INFO")
    LOG_FILE: Optional[str] = os.getenv("LOG_FILE")
    
    # Performance
    MAX_CONNECTIONS: int = int(os.getenv("MAX_CONNECTIONS", "100"))
    QUERY_TIMEOUT: int = int(os.getenv("QUERY_TIMEOUT", "30"))
    BUFFER_SIZE: int = int(os.getenv("BUFFER_SIZE", "8192"))
    
    @classmethod
    def get_prod_connection_string(cls) -> str:
        """Get production database connection string."""
        return f"mysql+pymysql://{cls.PROD_DB_USER}:{cls.PROD_DB_PASSWORD}@{cls.PROD_DB_HOST}:{cls.PROD_DB_PORT}/{cls.PROD_DB_NAME}"
    
    @classmethod
    def get_local_connection_string(cls) -> str:
        """Get local database connection string."""
        return f"mysql+pymysql://{cls.LOCAL_DB_USER}:{cls.LOCAL_DB_PASSWORD}@{cls.LOCAL_DB_HOST}:{cls.LOCAL_DB_PORT}/{cls.LOCAL_DB_NAME}"
    
    @classmethod
    def validate(cls) -> bool:
        """Validate configuration."""
        required_vars = [
            cls.PROD_DB_HOST, cls.PROD_DB_USER, cls.PROD_DB_NAME,
            cls.LOCAL_DB_HOST, cls.LOCAL_DB_USER, cls.LOCAL_DB_NAME
        ]
        return all(var for var in required_vars)
