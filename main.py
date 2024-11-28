import os
import json
import logging
from datetime import datetime
from typing import Dict, Any, Optional
import psycopg2
from psycopg2.extras import RealDictCursor
from tqdm import tqdm
import subprocess

# Configure logging
logging.basicConfig(level=logging.INFO, 
                    format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class PostgreSQLBackup:
    def __init__(self, config_path='/config/config.json'):
        # Load configuration
        with open(config_path, 'r') as config_file:
            self.config = json.load(config_file)
        
        # Database connection parameters from environment
        self.db_params = {
            'host': os.getenv('DB_HOST'),
            'port': os.getenv('DB_PORT', '5432'),
            'database': os.getenv('DB_NAME'),
            'user': os.getenv('DB_USER'),
            'password': os.getenv('DB_PASSWORD')
        }
        
        # Backup directory
        self.backup_dir = '/backups'
        os.makedirs(self.backup_dir, exist_ok=True)

    def _get_connection(self):
        """Create a database connection."""
        return psycopg2.connect(**self.db_params)

    def get_total_rows(self, table_name: str, where_clause: Optional[str] = None) -> int:
        """Get total number of rows in a table."""
        query = f"SELECT COUNT(*) as count FROM {table_name}"
        if where_clause:
            query += f" WHERE {where_clause}"
        
        with self._get_connection() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                cursor.execute(query)
                return cursor.fetchone()['count']

    def backup_table(self, table_name: str, batch_size: int = 100000, 
                     where_clause: Optional[str] = None):
        """
        Backup a specific table with optional filtering and batching.
        
        :param table_name: Name of the table to backup
        :param batch_size: Number of rows to process in each batch
        :param where_clause: Optional WHERE clause for selective dumping
        """
        # Get total rows for progress tracking
        total_rows = self.get_total_rows(table_name, where_clause)
        logger.info(f"Backing up table {table_name}: {total_rows} total rows")

        # Prepare backup filename
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        backup_filename = f"{table_name}_{timestamp}"
        
        # Batch processing
        offset = 0
        batch_number = 1
        
        while offset < total_rows:
            # Construct query with optional where clause and pagination
            query = f"SELECT * FROM {table_name}"
            if where_clause:
                query += f" WHERE {where_clause}"
            query += f" ORDER BY ctid LIMIT {batch_size} OFFSET {offset}"
            
            # Backup batch to CSV
            batch_backup_file = f"{backup_filename}_batch_{batch_number}.csv"
            full_backup_path = os.path.join(self.backup_dir, batch_backup_file)
            
            # Use COPY command for efficient CSV export
            psql_command = [
                'psql',
                f'-h{self.db_params["host"]}',
                f'-p{self.db_params["port"]}',
                f'-U{self.db_params["user"]}',
                f'-d{self.db_params["database"]}',
                '-c', 
                f"\\copy ({query}) TO '{full_backup_path}' WITH CSV HEADER"
            ]
            
            # Set password via environment to avoid command line exposure
            env = os.environ.copy()
            env['PGPASSWORD'] = self.db_params['password']
            
            try:
                subprocess.run(psql_command, env=env, check=True)
                logger.info(f"Backed up batch {batch_number} for {table_name}")
            except subprocess.CalledProcessError as e:
                logger.error(f"Failed to backup batch {batch_number}: {e}")
                break
            
            # Update for next iteration
            offset += batch_size
            batch_number += 1

    def backup_database(self):
        """Backup tables based on configuration."""
        for table_config in self.config.get('tables', []):
            table_name = table_config.get('name')
            where_clause = table_config.get('where_clause')
            batch_size = table_config.get('batch_size', 100000)
            
            if not table_name:
                logger.warning("Skipping table with no name")
                continue
            
            try:
                self.backup_table(
                    table_name, 
                    batch_size=batch_size, 
                    where_clause=where_clause
                )
            except Exception as e:
                logger.error(f"Error backing up table {table_name}: {e}")

def main():
    backup = PostgreSQLBackup()
    backup.backup_database()

if __name__ == "__main__":
    main()