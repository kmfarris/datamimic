# DataProxy

A Python application that acts as a proxy layer between applications and production databases, creating local clones of production data while intercepting write operations.

## Overview

DataProxy sits between your application and production database, providing:
- **Read-through caching**: Initial reads go to production, subsequent reads from local cache
- **Write interception**: Writes are stored locally, building a clone of production data
- **Transparent operation**: Applications connect to DataProxy as if it were the production database

## Features

- MySQL/MariaDB compatibility
- Automatic local database schema creation
- Query routing (reads vs writes)
- Local data persistence
- Production data safety

## Architecture

```
Application → DataProxy → Production Database (reads only)
                ↓
            Local Database (writes + cached reads)
```

## Installation

1. Clone the repository
2. Install dependencies: `pip install -r requirements.txt`
3. Copy `.env.example` to `.env` and configure your database connections
4. Run: `python -m dataproxy.main`

## Configuration

Create a `.env` file with:

```env
# Production database
PROD_DB_HOST=localhost
PROD_DB_PORT=3306
PROD_DB_USER=user
PROD_DB_PASSWORD=password
PROD_DB_NAME=database

# Local database
LOCAL_DB_HOST=localhost
LOCAL_DB_PORT=3306
LOCAL_DB_USER=user
LOCAL_DB_PASSWORD=password
LOCAL_DB_NAME=dataproxy_local

# DataProxy settings
PROXY_HOST=localhost
PROXY_PORT=3307
```

## Usage

1. Start DataProxy: `python -m dataproxy.main`
2. Connect your application to `localhost:3307` instead of your production database
3. DataProxy will automatically:
   - Forward read queries to production
   - Store write results locally
   - Build local schema as needed

## Development

- `dataproxy/` - Core application code
- `tests/` - Test suite
- `scripts/` - Utility scripts
- `config/` - Configuration files
