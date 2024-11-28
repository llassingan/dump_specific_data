
import os
import json
import logging
from datetime import datetime
from typing import Dict, Any, Optional
from sqlalchemy import create_engine, text

# Configure logging

# Create the backups directory if it doesn't exist
log_dir = '/logs'  
os.makedirs(log_dir, exist_ok=True)  # Create the directory if it doesn't exist

# Create a timestamp for the log file name
timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")

log_file_name = os.path.join(log_dir, f'sql_dump_{timestamp}.log')  # Combine path and file name


# Create a logger
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)  # Set the logger level

# Create file handler which logs even debug messages
file_handler = logging.FileHandler(log_file_name)
file_handler.setLevel(logging.INFO)  # Set the file handler level

# Create console handler with the same level
console_handler = logging.StreamHandler()
console_handler.setLevel(logging.INFO)  # Set the console handler level

# Create a formatter and set it for both handlers
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
file_handler.setFormatter(formatter)
console_handler.setFormatter(formatter)

# Add the handlers to the logger
logger.addHandler(file_handler)
logger.addHandler(console_handler)



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
            'password': os.getenv('DB_PASSWORD'),
            'default_schema': self.config.get('default_schema', 'public')
        }
        
        # Backup directory
        self.backup_dir = '/backups'
        os.makedirs(self.backup_dir, exist_ok=True)

    def _get_connection_string(self):
        """Create SQLAlchemy connection string."""
        return f"postgresql://{self.db_params['user']}:{self.db_params['password']}@{self.db_params['host']}:{self.db_params['port']}/{self.db_params['database']}"

    def _get_fully_qualified_table_name(self, table_name: str, schema: Optional[str] = None):
        """Generate fully qualified table name with schema."""
        if schema:
            return f"{schema}.{table_name}"
        return f"{self.db_params['default_schema']}.{table_name}"

    def backup_table(self, 
                     table_name: str, 
                     batch_size: int = 10000, 
                     where_clause: Optional[str] = None,
                     schema: Optional[str] = None,
                     max_batches: Optional[int] = None):
        """Backup a specific table with optional filtering and batching."""
        qualified_table_name = self._get_fully_qualified_table_name(table_name, schema)
        engine = create_engine(self._get_connection_string())
        logger.info("====================================================================================")
        logger.info(f"Starting backup table: {table_name} from schema: {schema}")
        with engine.connect() as connection:
            query = text("""
                SELECT column_name, data_type 
                FROM information_schema.columns 
                WHERE table_name = :table_name 
                  AND table_schema = :schema
                ORDER BY ordinal_position
            """)
            result = connection.execute(query, {
                'table_name': table_name, 
                'schema': schema or self.db_params['default_schema']
            })
            columns = [row for row in result]
        
        column_names = [col[0] for col in columns]
        
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        backup_filename = f"{schema or 'public'}_{table_name}_{timestamp}.sql"
        full_backup_path = os.path.join(self.backup_dir, backup_filename)
        
        base_query = f"SELECT * FROM {qualified_table_name}"
        if where_clause:
            base_query += f" WHERE {where_clause}"
        
        with open(full_backup_path, 'w') as backup_file:
            backup_file.write(f"-- Backup of {qualified_table_name}\n")
            backup_file.write(f"SET session_replication_role = 'replica';\n\n")
            backup_file.write(f"ALTER TABLE {qualified_table_name} DISABLE TRIGGER ALL;\n\n")
            backup_file.write(f"TRUNCATE TABLE {qualified_table_name};\n\n")
            
            offset = 0
            batch_number = 1
            recordsnum = 0
            while True:
                if max_batches and batch_number > max_batches:
                    logger.info(f"Reached maximum number of batches ({max_batches}) for {qualified_table_name}")
                    break
                paginated_query = f"{base_query} ORDER BY ctid LIMIT {batch_size} OFFSET {offset}"
                
                with engine.connect() as connection:
                    result = connection.execute(text(paginated_query))
                    rows = result.fetchall()
                    
                    if not rows:
                        break
                    
                    for row in rows:
                        row_dict = dict(zip(column_names, row))
                        formatted_values = []
                        for col in column_names:
                            value = row_dict[col]
                            
                            if value is None:
                                formatted_values.append('NULL')
                            elif isinstance(value, str):
                                # Escape single quotes by replacing them with two single quotes
                                escaped_value = value.replace("'", "''")
                                formatted_values.append(f"'{escaped_value}'")
                            elif isinstance(value, (int, float)):
                                formatted_values.append(str(value))
                            elif isinstance(value, bool):
                                # Depending on your SQL dialect, this might need to be 1/0 or TRUE/FALSE
                                formatted_values.append('TRUE' if value else 'FALSE')
                            else:
                                # For other data types like dates, format them as strings
                                # Adjust the formatting as needed for your specific use case
                                escaped_value = str(value).replace("'", "''")
                                formatted_values.append(f"'{escaped_value}'")
                        insert_stmt = f"INSERT INTO {qualified_table_name} ({', '.join(column_names)}) VALUES ({', '.join(formatted_values)});\n"
                        backup_file.write(insert_stmt)

                    
            
                
                offset += batch_size
                batch_number += 1
                recordsnum += len(rows)
                
                logger.info(f"Processed batch {batch_number-1} for {qualified_table_name}. Total Data: {recordsnum}")
            
            backup_file.write(f"\nALTER TABLE {qualified_table_name} ENABLE TRIGGER ALL;\n")
            backup_file.write(f"\nSET session_replication_role = 'origin';\n")
        
        logger.info(f"Completed backup for {qualified_table_name}: {full_backup_path}. Total backup data: {recordsnum}")
        logger.info("====================================================================================")

    def backup_custom_query(self, 
                             query: str, 
                             output_table_name: str, 
                             batch_size: int = 10000, 
                             params: Optional[Dict] = None,
                             output_schema: Optional[str] = None, 
                             max_batches: Optional[int] = None):
        """Backup results of a custom query with batched processing."""
        qualified_output_table = self._get_fully_qualified_table_name(output_table_name, output_schema)
        engine = create_engine(self._get_connection_string())
        
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        backup_filename = f"{output_schema or 'public'}_{output_table_name}_{timestamp}_custom_query.sql"
        full_backup_path = os.path.join(self.backup_dir, backup_filename)
        logger.info("====================================================================================")
        logger.info(f"Starting backup from query: {query}")
        with engine.connect() as connection:
            query_with_limit = text(f"{query} LIMIT 1")
            result = connection.execute(query_with_limit, params or {})
            column_names = list(result.keys())
        
        with open(full_backup_path, 'w') as backup_file:
            backup_file.write(f"-- Custom Query Backup for {qualified_output_table}\n")
            backup_file.write(f"-- Original Query: {query}\n\n")
            
            offset = 0
            batch_number = 1
            recordsnum = 0
            while True:
                if max_batches and batch_number > max_batches:
                    logger.info(f"Reached maximum number of batches ({max_batches}) for custom query")
                    break
                paginated_query = text(f"{query} LIMIT :batch_size OFFSET :offset")
                query_params = (params or {}).copy()
                query_params.update({
                    'batch_size': batch_size,
                    'offset': offset
                })
                
                with engine.connect() as connection:
                    result = connection.execute(paginated_query, query_params)
                    rows = result.fetchall()
                    
                    if not rows:
                        break
                    
                    for row in rows:
                        row_dict = dict(zip(column_names, row))
                        formatted_values = []
                        for col in column_names:
                            value = row_dict[col]
                            
                            if value is None:
                                formatted_values.append('NULL')
                            elif isinstance(value, str):
                                # Escape single quotes by replacing them with two single quotes
                                escaped_value = value.replace("'", "''")
                                formatted_values.append(f"'{escaped_value}'")
                            elif isinstance(value, (int, float)):
                                formatted_values.append(str(value))
                            elif isinstance(value, bool):
                                # Depending on your SQL dialect, this might need to be 1/0 or TRUE/FALSE
                                formatted_values.append('TRUE' if value else 'FALSE')
                            else:
                                # For other data types like dates, format them as strings
                                # Adjust the formatting as needed for your specific use case
                                escaped_value = str(value).replace("'", "''")
                                formatted_values.append(f"'{escaped_value}'")
                        insert_stmt = f"INSERT INTO {qualified_output_table} ({', '.join(column_names)}) VALUES ({', '.join(formatted_values)});\n"
                        backup_file.write(insert_stmt)
                
                offset += batch_size
                batch_number += 1
                recordsnum += len(rows)
                
                logger.info(f"Processed batch {batch_number-1} for custom query. Total Data: {recordsnum}")
            
        logger.info(f"Completed custom query backup: {full_backup_path}. Total backup data: {recordsnum}")
        logger.info("====================================================================================")

    def backup_database(self):
        """Backup tables and custom queries based on configuration."""
        for table_config in self.config.get('tables', []):
            table_name = table_config.get('name')
            schema = table_config.get('schema')
            where_clause = table_config.get('where_clause')
            batch_size = table_config.get('batch_size', 10000)
            max_batches = table_config.get('max_batches')
            
            if not table_name:
                logger.warning("Skipping table with no name")
                continue
            
            try:
                self.backup_table(
                    table_name, 
                    batch_size=batch_size, 
                    where_clause=where_clause,
                    schema=schema,
                    max_batches=max_batches
                )
            except Exception as e:
                logger.error(f"Error backing up table {schema}.{table_name}: {e}")
        
        for query_config in self.config.get('custom_queries', []):
            query = query_config.get('query')
            output_table_name = query_config.get('output_table_name', 'custom_query_result')
            output_schema = query_config.get('output_schema')
            batch_size = query_config.get('batch_size', 10000)
            max_batches = query_config.get('max_batches')
            params = query_config.get('params')
            
            
            if not query:
                logger.warning("Skipping custom query with no query defined")
                continue
            
            try:
                self.backup_custom_query(
                    query=query,
                    output_table_name=output_table_name,
                    output_schema=output_schema,
                    batch_size=batch_size,
                    params=params,
                    max_batches=max_batches
                )
            except Exception as e:
                logger.error(f"Error backing up custom query: {e}")

def main():
    backup = PostgreSQLBackup()
    backup.backup_database()

if __name__ == "__main__":
    main()
