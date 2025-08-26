"""
Main entry point for DataProxy.
"""

import sys
import logging
import click
from rich.console import Console
from rich.table import Table
from rich.panel import Panel

from .config import Config
from .proxy_server import DataProxyServer
from .database import DatabaseManager

console = Console()


@click.group()
@click.version_option(version="0.1.0")
def cli():
    """DataProxy - Database proxy with local write caching."""
    pass


@cli.command()
@click.option('--host', default=Config.PROXY_HOST, help='Proxy server host')
@click.option('--port', default=Config.PROXY_PORT, help='Proxy server port')
@click.option('--log-level', default=Config.LOG_LEVEL, help='Logging level')
def start(host, port, log_level):
    """Start the DataProxy server."""
    console.print(Panel.fit("🚀 Starting DataProxy Server", style="bold blue"))
    
    # Update config
    Config.PROXY_HOST = host
    Config.PROXY_PORT = port
    Config.LOG_LEVEL = log_level
    
    # Configure logging
    logging.basicConfig(
        level=getattr(logging, log_level),
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.StreamHandler(),
            logging.FileHandler(Config.LOG_FILE) if Config.LOG_FILE else logging.NullHandler()
        ]
    )
    
    # Validate configuration
    if not Config.validate():
        console.print("[red]❌ Invalid configuration. Please check your .env file.[/red]")
        sys.exit(1)
    
    # Create and start server
    server = DataProxyServer()
    
    try:
        console.print(f"[green]✅ Configuration validated[/green]")
        console.print(f"[blue]📍 Proxy will listen on {host}:{port}[/blue]")
        console.print(f"[blue]🗄️  Production DB: {Config.PROD_DB_HOST}:{Config.PROD_DB_PORT}/{Config.PROD_DB_NAME}[/blue]")
        console.print(f"[blue]💾 Local DB: {Config.LOCAL_DB_HOST}:{Config.LOCAL_DB_PORT}/{Config.LOCAL_DB_NAME}[/blue]")
        console.print("\n[bold]Starting server...[/bold]")
        
        if server.start():
            console.print("[green]✅ Server started successfully![/green]")
            console.print("[yellow]Press Ctrl+C to stop the server[/yellow]")
            
            # Keep server running
            try:
                while True:
                    import time
                    time.sleep(1)
            except KeyboardInterrupt:
                console.print("\n[yellow]🛑 Received interrupt signal[/yellow]")
                
        else:
            console.print("[red]❌ Failed to start server[/red]")
            sys.exit(1)
            
    except Exception as e:
        console.print(f"[red]❌ Error starting server: {e}[/red]")
        sys.exit(1)
    finally:
        server.stop()
        console.print("[green]✅ Server stopped[/green]")


@cli.command()
def status():
    """Show DataProxy status and configuration."""
    console.print(Panel.fit("📊 DataProxy Status", style="bold blue"))
    
    # Configuration status
    config_table = Table(title="Configuration")
    config_table.add_column("Setting", style="cyan")
    config_table.add_column("Value", style="magenta")
    
    config_table.add_row("Proxy Host", Config.PROXY_HOST)
    config_table.add_row("Proxy Port", str(Config.PROXY_PORT))
    config_table.add_row("Production DB", f"{Config.PROD_DB_HOST}:{Config.PROD_DB_PORT}")
    config_table.add_row("Local DB", f"{Config.LOCAL_DB_HOST}:{Config.LOCAL_DB_PORT}")
    config_table.add_row("Log Level", Config.LOG_LEVEL)
    
    console.print(config_table)
    
    # Test database connections
    console.print("\n[bold]Testing database connections...[/bold]")
    
    db_manager = DatabaseManager()
    
    # Test production connection
    if db_manager.connect_production():
        console.print("[green]✅ Production database: Connected[/green]")
    else:
        console.print("[red]❌ Production database: Failed to connect[/red]")
    
    # Test local connection
    if db_manager.connect_local():
        console.print("[green]✅ Local database: Connected[/green]")
    else:
        console.print("[red]❌ Local database: Failed to connect[/red]")
    
    db_manager.close()


@cli.command()
@click.option('--table', help='Specific table to sync')
def sync(table):
    """Synchronize table schemas from production to local database."""
    console.print(Panel.fit("🔄 Schema Synchronization", style="bold blue"))
    
    if not Config.validate():
        console.print("[red]❌ Invalid configuration[/red]")
        sys.exit(1)
    
    db_manager = DatabaseManager()
    
    if not db_manager.connect_production():
        console.print("[red]❌ Failed to connect to production database[/red]")
        sys.exit(1)
    
    if not db_manager.connect_local():
        console.print("[red]❌ Failed to connect to local database[/red]")
        sys.exit(1)
    
    try:
        if table:
            # Sync specific table
            console.print(f"[blue]Syncing table: {table}[/blue]")
            schema = db_manager.get_table_schema(table)
            if schema:
                if db_manager.create_local_table(table, schema):
                    console.print(f"[green]✅ Table {table} synchronized successfully[/green]")
                else:
                    console.print(f"[red]❌ Failed to sync table {table}[/red]")
            else:
                console.print(f"[red]❌ Table {table} not found in production[/red]")
        else:
            # Get all tables from production
            tables_query = """
            SELECT table_name 
            FROM information_schema.tables 
            WHERE table_schema = %s
            """
            tables = db_manager.execute_production_query(tables_query, (Config.PROD_DB_NAME,))
            
            if tables:
                console.print(f"[blue]Found {len(tables)} tables in production[/blue]")
                
                for table_info in tables:
                    table_name = table_info['table_name']
                    console.print(f"[blue]Syncing table: {table_name}[/blue]")
                    
                    schema = db_manager.get_table_schema(table_name)
                    if schema:
                        if db_manager.create_local_table(table_name, schema):
                            console.print(f"[green]✅ Table {table_name} synchronized[/green]")
                        else:
                            console.print(f"[red]❌ Failed to sync table {table_name}[/red]")
                    else:
                        console.print(f"[yellow]⚠️  No schema found for table {table_name}[/yellow]")
            else:
                console.print("[yellow]No tables found in production database[/yellow]")
                
    except Exception as e:
        console.print(f"[red]❌ Error during synchronization: {e}[/red]")
    finally:
        db_manager.close()


@cli.command()
def test():
    """Run basic connectivity tests."""
    console.print(Panel.fit("🧪 Connectivity Tests", style="bold blue"))
    
    if not Config.validate():
        console.print("[red]❌ Invalid configuration[/red]")
        sys.exit(1)
    
    db_manager = DatabaseManager()
    
    # Test production connection
    console.print("[blue]Testing production database connection...[/blue]")
    if db_manager.connect_production():
        console.print("[green]✅ Production database: Connected[/green]")
        
        # Test simple query
        result = db_manager.execute_production_query("SELECT 1 as test")
        if result:
            console.print("[green]✅ Production database: Query test passed[/green]")
        else:
            console.print("[red]❌ Production database: Query test failed[/red]")
    else:
        console.print("[red]❌ Production database: Connection failed[/red]")
    
    # Test local connection
    console.print("[blue]Testing local database connection...[/blue]")
    if db_manager.connect_local():
        console.print("[green]✅ Local database: Connected[/green]")
        
        # Test simple query
        result = db_manager.execute_local_query("SELECT 1 as test")
        if result:
            console.print("[green]✅ Local database: Query test passed[/green]")
        else:
            console.print("[red]❌ Local database: Query test failed[/red]")
    else:
        console.print("[red]❌ Local database: Connection failed[/red]")
    
    db_manager.close()


if __name__ == "__main__":
    cli()
