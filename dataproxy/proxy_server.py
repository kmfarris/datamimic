"""
Main proxy server for DataProxy.
"""

import socket
import threading
import logging
import time
from typing import Optional, Dict, Any
import pymysql
from pymysql.constants import CLIENT

from .config import Config
from .database import DatabaseManager
from .query_router import QueryRouter

logger = logging.getLogger(__name__)


class DataProxyServer:
    """Main proxy server that handles MySQL client connections."""
    
    def __init__(self):
        self.db_manager = DatabaseManager()
        self.query_router = QueryRouter(self.db_manager)
        self.server_socket: Optional[socket.socket] = None
        self.clients: Dict[int, 'ClientHandler'] = {}
        self.running = False
        self.client_counter = 0
        
    def start(self):
        """Start the proxy server."""
        if not Config.validate():
            logger.error("Invalid configuration")
            return False
        
        # Connect to databases
        if not self.db_manager.connect_production():
            logger.error("Failed to connect to production database")
            return False
        
        if not self.db_manager.connect_local():
            logger.error("Failed to connect to local database")
            return False
        
        # Create server socket
        try:
            self.server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self.server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            self.server_socket.bind((Config.PROXY_HOST, Config.PROXY_PORT))
            self.server_socket.listen(Config.MAX_CONNECTIONS)
            
            logger.info(f"DataProxy server started on {Config.PROXY_HOST}:{Config.PROXY_PORT}")
            self.running = True
            
            # Accept client connections
            while self.running:
                try:
                    client_socket, address = self.server_socket.accept()
                    logger.info(f"New client connection from {address}")
                    
                    # Create client handler
                    client_id = self.client_counter
                    self.client_counter += 1
                    
                    client_handler = ClientHandler(
                        client_socket, 
                        address, 
                        client_id,
                        self.query_router
                    )
                    
                    self.clients[client_id] = client_handler
                    
                    # Start client handler thread
                    client_thread = threading.Thread(
                        target=client_handler.handle,
                        daemon=True
                    )
                    client_thread.start()
                    
                except socket.error as e:
                    if self.running:
                        logger.error(f"Socket error: {e}")
                    break
                    
        except Exception as e:
            logger.error(f"Failed to start server: {e}")
            return False
        
        return True
    
    def stop(self):
        """Stop the proxy server."""
        logger.info("Stopping DataProxy server...")
        self.running = False
        
        # Close all client connections
        for client_id, client_handler in list(self.clients.items()):
            client_handler.close()
            del self.clients[client_id]
        
        # Close server socket
        if self.server_socket:
            self.server_socket.close()
        
        # Close database connections
        self.db_manager.close()
        
        logger.info("DataProxy server stopped")


class ClientHandler:
    """Handles individual client connections."""
    
    def __init__(self, client_socket: socket.socket, address: tuple, client_id: int, query_router: QueryRouter):
        self.client_socket = client_socket
        self.address = address
        self.client_id = client_id
        self.query_router = query_router
        self.authenticated = False
        self.current_database = None
        
    def handle(self):
        """Handle client connection."""
        try:
            # Send MySQL handshake
            self._send_handshake()
            
            # Handle authentication
            if not self._handle_authentication():
                return
            
            self.authenticated = True
            logger.info(f"Client {self.client_id} authenticated")
            
            # Main query handling loop
            while True:
                try:
                    # Read query packet
                    packet = self._read_packet()
                    if not packet:
                        break
                    
                    # Parse and execute query
                    query = packet.decode('utf-8', errors='ignore').strip()
                    if query:
                        self._handle_query(query)
                        
                except Exception as e:
                    logger.error(f"Error handling query from client {self.client_id}: {e}")
                    self._send_error(f"Query error: {e}")
                    break
                    
        except Exception as e:
            logger.error(f"Error handling client {self.client_id}: {e}")
        finally:
            self.close()
    
    def _send_handshake(self):
        """Send MySQL protocol handshake."""
        # Simplified handshake - in practice, you'd implement full MySQL protocol
        handshake = b'\x0a' + b'mysql_native_password' + b'\x00' * 20
        self.client_socket.send(handshake)
    
    def _handle_authentication(self) -> bool:
        """Handle client authentication."""
        try:
            # Read auth packet
            auth_packet = self._read_packet()
            if not auth_packet:
                return False
            
            # For now, accept all connections
            # In production, implement proper authentication
            self._send_ok_packet()
            return True
            
        except Exception as e:
            logger.error(f"Authentication failed for client {self.client_id}: {e}")
            return False
    
    def _handle_query(self, query: str):
        """Handle a SQL query."""
        try:
            logger.info(f"Client {self.client_id} query: {query[:100]}...")
            
            # Route query through query router
            result = self.query_router.route_query(query)
            
            if result['success']:
                if result['routed_to'] == 'local':
                    logger.info(f"Query routed to local database")
                elif result['routed_to'] == 'production':
                    logger.info(f"Query routed to production database")
                
                # Send results back to client
                self._send_results(result)
            else:
                logger.error(f"Query failed: {result.get('error', 'Unknown error')}")
                self._send_error(result.get('error', 'Query failed'))
                
        except Exception as e:
            logger.error(f"Error handling query: {e}")
            self._send_error(f"Internal error: {e}")
    
    def _send_results(self, result: Dict[str, Any]):
        """Send query results back to client."""
        try:
            if result['query_type'] == 'READ' and result.get('data'):
                # Send result set
                self._send_result_set(result['data'])
            else:
                # Send OK packet for write operations
                rows_affected = result.get('rows_affected', 0)
                self._send_ok_packet(rows_affected)
                
        except Exception as e:
            logger.error(f"Error sending results: {e}")
            self._send_error("Failed to send results")
    
    def _send_result_set(self, data: list):
        """Send a result set to the client."""
        # Simplified result set - in practice, implement full MySQL protocol
        if data:
            # Send column count
            self.client_socket.send(b'\x01')
            
            # Send column definitions (simplified)
            for column in data[0].keys():
                col_def = f"`{column}` varchar(255)".encode('utf-8')
                self.client_socket.send(len(col_def).to_bytes(1, 'little') + col_def)
            
            # Send data rows
            for row in data:
                row_data = '|'.join(str(v) for v in row.values()).encode('utf-8')
                self.client_socket.send(len(row_data).to_bytes(1, 'little') + row_data)
            
            # Send EOF packet
            self.client_socket.send(b'\xfe')
        else:
            # No data
            self.client_socket.send(b'\x00')
            self.client_socket.send(b'\xfe')
    
    def _send_ok_packet(self, rows_affected: int = 0):
        """Send OK packet to client."""
        ok_packet = b'\x00' + rows_affected.to_bytes(8, 'little') + b'\x00'
        self.client_socket.send(ok_packet)
    
    def _send_error(self, message: str):
        """Send error packet to client."""
        error_packet = b'\xff' + message.encode('utf-8')
        self.client_socket.send(error_packet)
    
    def _read_packet(self) -> Optional[bytes]:
        """Read a MySQL packet from the client."""
        try:
            # Read packet length (3 bytes) and sequence number (1 byte)
            header = self.client_socket.recv(4)
            if len(header) < 4:
                return None
            
            packet_length = int.from_bytes(header[:3], 'little')
            packet_data = self.client_socket.recv(packet_length)
            
            if len(packet_data) < packet_length:
                return None
            
            return packet_data
            
        except Exception as e:
            logger.error(f"Error reading packet: {e}")
            return None
    
    def close(self):
        """Close client connection."""
        try:
            self.client_socket.close()
            logger.info(f"Client {self.client_id} disconnected")
        except Exception as e:
            logger.error(f"Error closing client {self.client_id}: {e}")


def main():
    """Main entry point."""
    # Configure logging
    logging.basicConfig(
        level=getattr(logging, Config.LOG_LEVEL),
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.StreamHandler(),
            logging.FileHandler(Config.LOG_FILE) if Config.LOG_FILE else logging.NullHandler()
        ]
    )
    
    # Create and start server
    server = DataProxyServer()
    
    try:
        server.start()
    except KeyboardInterrupt:
        logger.info("Received interrupt signal")
    finally:
        server.stop()


if __name__ == "__main__":
    main()
