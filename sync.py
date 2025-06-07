from azure.storage.blob import BlobServiceClient
from pyspark.sql import SparkSession
from psycopg2 import pool, OperationalError, sql
from typing import Dict
import logging
import os
import psycopg2
import re
import traceback
import time
from pyspark.sql.functions import current_timestamp, lit

try:
    from pyspark.dbutils import DBUtils
    dbutils = DBUtils(SparkSession.builder.getOrCreate())
except Exception:  # pragma: no cover - running outside Databricks
    class _DummySecrets:
        def get(self, scope: str, key: str) -> str:
            env_key = f"{scope}_{key}".upper()
            value = os.getenv(env_key)
            if value is None:
                raise ValueError(f"Missing secret for {env_key}")
            return value

    class _DummyFS:
        def ls(self, path: str):
            raise NotImplementedError("dbutils.fs.ls not implemented outside Databricks")

        def mkdirs(self, path: str):
            logging.info(f"[DRY-RUN] Would create directory: {path}")

    class _DummyDBUtils:
        secrets = _DummySecrets()
        fs = _DummyFS()

    dbutils = _DummyDBUtils()
    logging.warning("DBUtils not available; using environment variables for secrets")

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)

# Create SparkSession
spark = SparkSession.builder.getOrCreate()


# Postgres Handler
class PostgresDataHandler:
    """Handle PostgreSQL interactions using a connection pool."""

    def __init__(self, pg_pool: pool.ThreadedConnectionPool, pg_config: Dict[str, str]):
        self.pg_pool = pg_pool
        self.pg_config = pg_config

    @staticmethod
    def connect_to_postgres(pg_config: Dict[str, str]) -> pool.ThreadedConnectionPool:
        """Create a connection pool to PostgreSQL."""
        try:
            return psycopg2.pool.ThreadedConnectionPool(1, 20, **pg_config)
        except OperationalError as e:
            logging.error(f"Failed to connect to PostgreSQL: {str(e)}")
            raise

    def is_connection_alive(self):
        """Check if a connection from the pool is alive."""
        conn = self.pg_pool.getconn()
        try:
            with conn.cursor() as cursor:
                cursor.execute("SELECT 1")
                return True
        except OperationalError:
            return False
        finally:
            self.pg_pool.putconn(conn)

    @staticmethod
    def _is_valid_table_name(table: str) -> bool:
        """Validate table name to contain only allowed characters."""
        return bool(re.fullmatch(r"[\w\.\"]+", table))

    @staticmethod
    def _parse_table_name(table: str) -> (str, str):
        """Split schema.table into components and remove quotes."""
        clean = table.replace('"', '')
        if '.' in clean:
            schema, tbl = clean.split('.', 1)
        else:
            schema, tbl = 'public', clean
        return schema, tbl

    def get_table_count(self, table: str) -> int:
        """Get actual row count from a PostgreSQL table in a safe manner."""
        if not self._is_valid_table_name(table):
            raise ValueError(f"Invalid table name: {table}")

        schema, tbl = self._parse_table_name(table)
        conn = self.pg_pool.getconn()
        try:
            with conn.cursor() as cursor:
                query = sql.SQL("SELECT COUNT(*) FROM {}.{}").format(
                    sql.Identifier(schema), sql.Identifier(tbl)
                )
                cursor.execute(query)
                count = cursor.fetchone()[0]
                logging.info(f"PostgreSQL count for {table}: {count}")
                return count
        finally:
            self.pg_pool.putconn(conn)

    def export_table_to_delta(self, table: str, stage: str, db: str, fetchsize: int = 10000) -> None:
        """
        Export a table directly from PostgreSQL to Delta Lake using JDBC.
        This bypasses CSV completely and avoids all the row count issues.
        Row counts are NOT computed due to performance considerations.
        """
        try:
            if not self._is_valid_table_name(table):
                raise ValueError(f"Invalid table name: {table}")

            logging.info(f"Starting direct JDBC export for {table}")

            # Create JDBC URL and properties
            jdbc_url = f"jdbc:postgresql://{self.pg_config['host']}:{self.pg_config['port']}/{self.pg_config['database']}"
            properties = {
                "user": self.pg_config['user'],
                "password": self.pg_config['password'],
                "driver": "org.postgresql.Driver",
                # Increase fetch size for better performance
                "fetchsize": str(fetchsize),
            }

            # Table name without quotes for JDBC
            schema, tbl = self._parse_table_name(table)

            # Use Spark's JDBC reader to load directly from PostgreSQL
            df = spark.read.jdbc(url=jdbc_url, table=f"{schema}.{tbl}", properties=properties)

            # Log the schema to verify correct data types
            logging.info(f"JDBC schema for {table}:")
            for field in df.schema.fields:
                logging.info(f"  {field.name}: {field.dataType}")

            # Add metadata columns
            df = df.withColumns(
                {
                    "ETL_CREATED_DATE": current_timestamp(),
                    "ETL_LAST_UPDATE_DATE": current_timestamp(),
                    "CREATED_BY": lit("ETL_PROCESS"),
                    "TO_PROCESS": lit(True),
                    "EDW_EXTERNAL_SOURCE_SYSTEM": lit("LeadCustodyRepository"),
                }
            )

            # Calculate Delta Lake path
            clean_table = tbl
            flp = f"abfss://dataarchitecture@quilitydatabricks.dfs.core.windows.net/{stage}/{db}/{clean_table}"

            # Write to Delta Lake
            df.write.format("delta").mode("overwrite").option(
                "overwriteSchema", "true"
            ).save(flp)

            # Instead, log export completion
            logging.info(f"Successfully exported table {table} to Delta (row counts skipped for performance).")

        except Exception as e:
            logging.error(f"Failed to export table '{table}': {str(e)}")
            logging.error(traceback.format_exc())
            raise


# Azure Data Handler
class AzureDataHandler:
    def __init__(self, blob_service_client):
        self.blob_service_client = blob_service_client

    @staticmethod
    def connect_to_azure_storage(storage_config: dict) -> BlobServiceClient:
        try:
            connection_string = f"DefaultEndpointsProtocol=https;AccountName={storage_config['account_name']};AccountKey={storage_config['account_key']};EndpointSuffix=core.windows.net"
            return BlobServiceClient.from_connection_string(connection_string)
        except Exception as e:
            logging.error(f"Failed to connect to Azure Storage: {str(e)}")
            raise

    def ensure_directory_exists(self, stage: str, db: str) -> None:
        db_path = f"abfss://dataarchitecture@quilitydatabricks.dfs.core.windows.net/{stage}/{db}"
        if hasattr(dbutils, "fs"):
            try:
                dbutils.fs.ls(db_path)
            except Exception:
                dbutils.fs.mkdirs(db_path)
        else:
            logging.warning(f"DBUtils.fs not available; ensure directory {db_path} exists")


# Data Sync
class PostgresAzureDataSync:
    def __init__(
        self, postgres_handler: PostgresDataHandler, azure_handler: AzureDataHandler
    ):
        self.postgres_handler = postgres_handler
        self.azure_handler = azure_handler

    def perform_operation(
        self, db: str, tables_to_copy: list
    ) -> None:
        """Export specified tables from PostgreSQL to Delta storage."""

        if not self.postgres_handler.is_connection_alive():
            logging.error("PostgreSQL connection is not alive. Aborting operation.")
            return

        for table in tables_to_copy:
            try:
                logging.info(f"Processing table {table}")
                # Ensure the directory exists
                self.azure_handler.ensure_directory_exists("RAW", db)
                # Export directly to Delta
                start = time.time()
                self.postgres_handler.export_table_to_delta(table, "RAW", db)
                duration = time.time() - start
                logging.info(f"Successfully processed table {table} in {duration:.2f}s")
            except Exception as e:
                logging.error(f"Failed to process table {table}: {str(e)}")
                logging.error(traceback.format_exc())


# PostgreSQL Configurations
pg_config = {
    "host": dbutils.secrets.get(
        scope="key-vault-secret", key="DataProduct-LCR-Host-PROD"
    ),
    "port": dbutils.secrets.get(
        scope="key-vault-secret", key="DataProduct-LCR-Port-PROD"
    ),
    "database": "LeadCustodyRepository",
    "user": dbutils.secrets.get(
        scope="key-vault-secret", key="DataProduct-LCR-User-PROD"
    ),
    "password": dbutils.secrets.get(
        scope="key-vault-secret", key="DataProduct-LCR-Pass-PROD"
    ),
}

if not all(pg_config.values()):
    raise ValueError("PostgreSQL configuration is incomplete")

# Azure Configurations
storage_config = {
    "account_name": "quilitydatabricks",
    "account_key": dbutils.secrets.get(
        scope="key-vault-secret", key="DataProduct-ADLS-Key"
    ),
    "container_name": "dataarchitecture",
}

if not storage_config.get("account_key"):
    raise ValueError("Azure storage configuration is incomplete")

# Table Configurations
tables_to_copy = [
    'public."leadassignment"',
    'public."leadxref"',
    'public."lead"',
]

# Execution
try:
    pg_pool = PostgresDataHandler.connect_to_postgres(pg_config)
    postgres_handler = PostgresDataHandler(pg_pool, pg_config)
    blob_service_client = AzureDataHandler.connect_to_azure_storage(storage_config)
    azure_handler = AzureDataHandler(blob_service_client)
    sync = PostgresAzureDataSync(postgres_handler, azure_handler)
    sync.perform_operation(
        pg_config["database"],
        tables_to_copy,
    )
finally:
    if 'pg_pool' in locals():
        pg_pool.closeall()
