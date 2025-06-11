import os
import re
import time
import logging
import traceback
from typing import Dict, List, Optional, Tuple
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import current_timestamp, lit
from azure.storage.blob import BlobServiceClient
import psycopg2
from psycopg2 import pool, sql, OperationalError

# DBUtils fallback
try:
    from pyspark.dbutils import DBUtils

    dbutils = DBUtils(SparkSession.builder.getOrCreate())
except Exception:

    class _DummySecrets:
        def get(self, scope: str, key: str) -> str:
            env_key = f"{scope}_{key}".upper()
            value = os.getenv(env_key)
            if value is None:
                raise ValueError(f"Missing secret for {env_key}")
            return value

    class _DummyFS:
        def ls(self, path: str):
            raise NotImplementedError(
                "dbutils.fs.ls not implemented outside Databricks"
            )

        def mkdirs(self, path: str):
            logging.info(f"[DRY-RUN] Would create directory: {path}")

    class _DummyDBUtils:
        secrets = _DummySecrets()
        fs = _DummyFS()

    dbutils = _DummyDBUtils()
    logging.warning("DBUtils not available; using environment variables for secrets.")

# Logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)

# Spark Session
spark = SparkSession.builder.getOrCreate()

# OOP HANDLERS
class PostgresDataHandler:
    def __init__(self, pg_pool: pool.ThreadedConnectionPool, pg_config: Dict[str, str]):
        self.pg_pool = pg_pool
        self.pg_config = pg_config

    @staticmethod
    def connect_to_postgres(pg_config: Dict[str, str]) -> pool.ThreadedConnectionPool:
        try:
            return psycopg2.pool.ThreadedConnectionPool(1, 20, **pg_config)
        except OperationalError as e:
            logging.error(f"Failed to connect to PostgreSQL: {str(e)}")
            raise

    def is_connection_alive(self) -> bool:
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
        return bool(re.fullmatch(r"[\w\.\"]+", table))

    @staticmethod
    def _parse_table_name(table: str) -> Tuple[str, str]:
        clean = table.replace('"', "")
        if "." in clean:
            schema, tbl = clean.split(".", 1)
        else:
            schema, tbl = "public", clean
        return schema, tbl

    def _get_numeric_pk_column(self, table: str) -> Optional[str]:
        """Automatically detect a suitable partition column (integer PK preferred)"""
        schema, tbl = self._parse_table_name(table)
        conn = self.pg_pool.getconn()
        try:
            with conn.cursor() as cursor:
                cursor.execute(
                    """
                    SELECT a.attname
                    FROM pg_index i
                    JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = ANY(i.indkey)
                    WHERE i.indrelid = %s::regclass AND i.indisprimary
                """,
                    (f"{schema}.{tbl}",),
                )
                pk_cols = [row[0] for row in cursor.fetchall()]
                if pk_cols:
                    # Check if PK is integer-like
                    cursor.execute(
                        """
                        SELECT attname, format_type(atttypid, atttypmod)
                        FROM pg_attribute
                        WHERE attrelid = %s::regclass
                        AND attname = ANY(%s)
                    """,
                        (f"{schema}.{tbl}", pk_cols),
                    )
                    for attname, atttype in cursor.fetchall():
                        if "int" in atttype or "serial" in atttype:
                            return attname
                # Fallback | Integer
                cursor.execute(
                    """
                    SELECT column_name, data_type
                    FROM information_schema.columns
                    WHERE table_schema = %s AND table_name = %s
                """,
                    (schema, tbl),
                )
                for cname, ctype in cursor.fetchall():
                    if ctype in ("integer", "bigint", "smallint"):
                        return cname
        except Exception as e:
            logging.warning(f"Could not determine numeric PK column for {table}: {e}")
        finally:
            self.pg_pool.putconn(conn)
        return None

    def _get_partition_bounds(
        self, table: str, partition_column: str
    ) -> Optional[Tuple[int, int]]:
        schema, tbl = self._parse_table_name(table)
        conn = self.pg_pool.getconn()
        try:
            with conn.cursor() as cursor:
                query = sql.SQL("SELECT MIN({0}), MAX({0}) FROM {1}.{2}").format(
                    sql.Identifier(partition_column),
                    sql.Identifier(schema),
                    sql.Identifier(tbl),
                )
                cursor.execute(query)
                bounds = cursor.fetchone()
                if bounds and None not in bounds:
                    return bounds
        except Exception as e:
            logging.warning(
                f"Could not get partition bounds for {table}.{partition_column}: {e}"
            )
        finally:
            self.pg_pool.putconn(conn)
        return None

    def _jdbc_read(
        self, table: str, fetchsize: int = 20000, num_partitions: int = 8
    ) -> DataFrame:
        schema, tbl = self._parse_table_name(table)
        jdbc_url = f"jdbc:postgresql://{self.pg_config['host']}:{self.pg_config['port']}/{self.pg_config['database']}"
        options = {
            "url": jdbc_url,
            "dbtable": f"{schema}.{tbl}",
            "user": self.pg_config["user"],
            "password": self.pg_config["password"],
            "driver": "org.postgresql.Driver",
            "fetchsize": str(fetchsize),
            "pushDownPredicate": "true",
        }
        partition_column = self._get_numeric_pk_column(table)
        if partition_column:
            bounds = self._get_partition_bounds(table, partition_column)
            if bounds:
                lower, upper = bounds
                options.update(
                    {
                        "partitionColumn": partition_column,
                        "lowerBound": str(lower),
                        "upperBound": str(upper),
                        "numPartitions": str(num_partitions),
                    }
                )
                logging.info(
                    f"Reading in parallel: {tbl} partition_column={partition_column}, bounds=({lower}, {upper}), numPartitions={num_partitions}"
                )
            else:
                logging.warning(
                    f"No bounds for partition column {partition_column}, reading single partition for {tbl}"
                )
        else:
            logging.warning(
                f"No suitable partition column found for {tbl}, reading single partition."
            )
        df = spark.read.format("jdbc").options(**options).load()
        return df

    def export_table_to_delta(
        self,
        table: str,
        stage: str,
        db: str,
        fetchsize: int = 20000,
        num_partitions: int = 8,
    ):
        if not self._is_valid_table_name(table):
            raise ValueError(f"Invalid table name: {table}")
        schema, tbl = self._parse_table_name(table)
        try:
            logging.info(f"--- Starting JDBC export for {table} ---")
            start = time.time()
            df = self._jdbc_read(
                table, fetchsize=fetchsize, num_partitions=num_partitions
            )
            row_count = df.count()
            logging.info(f"Rows read from {table}: {row_count}")
            df = df.withColumns(
                {
                    "ETL_CREATED_DATE": current_timestamp(),
                    "ETL_LAST_UPDATE_DATE": current_timestamp(),
                    "CREATED_BY": lit("ETL_PROCESS"),
                    "TO_PROCESS": lit(True),
                    "EDW_EXTERNAL_SOURCE_SYSTEM": lit("LeadCustodyRepository"),
                }
            )
            delta_path = f"abfss://dataarchitecture@quilitydatabricks.dfs.core.windows.net/{stage}/{db}/{tbl}"
            df.write.format("delta").mode("overwrite").option(
                "overwriteSchema", "true"
            ).save(delta_path)
            duration = time.time() - start
            logging.info(
                f"Exported {table} to Delta (rows: {row_count}) in {duration:.2f}s"
            )
            logging.info("Spark 4 EXPLAIN (extended):")
            df.explain("extended")
        except Exception as e:
            logging.error(f"Failed to export table '{table}': {e}")
            logging.error(traceback.format_exc())
            raise


class AzureDataHandler:
    def __init__(self, blob_service_client: BlobServiceClient):
        self.blob_service_client = blob_service_client

    @staticmethod
    def connect_to_azure_storage(storage_config: Dict[str, str]) -> BlobServiceClient:
        try:
            connection_string = (
                f"DefaultEndpointsProtocol=https;"
                f"AccountName={storage_config['account_name']};"
                f"AccountKey={storage_config['account_key']};"
                "EndpointSuffix=core.windows.net"
            )
            return BlobServiceClient.from_connection_string(connection_string)
        except Exception as e:
            logging.error(f"Failed to connect to Azure Storage: {e}")
            raise

    def ensure_directory_exists(self, stage: str, db: str):
        db_path = f"abfss://dataarchitecture@quilitydatabricks.dfs.core.windows.net/{stage}/{db}"
        if hasattr(dbutils, "fs"):
            try:
                dbutils.fs.ls(db_path)
            except Exception:
                dbutils.fs.mkdirs(db_path)
        else:
            logging.warning(
                f"DBUtils.fs not available; ensure directory {db_path} exists."
            )


class PostgresAzureDataSync:
    def __init__(
        self, postgres_handler: PostgresDataHandler, azure_handler: AzureDataHandler
    ):
        self.postgres_handler = postgres_handler
        self.azure_handler = azure_handler

    def perform_operation(self, db: str, tables_to_copy: List[str]):
        if not self.postgres_handler.is_connection_alive():
            logging.error("PostgreSQL connection is not alive. Aborting operation.")
            return
        for table in tables_to_copy:
            try:
                logging.info(f"Processing table {table}")
                self.azure_handler.ensure_directory_exists("RAW", db)
                self.postgres_handler.export_table_to_delta(table, "RAW", db)
                logging.info(f"Successfully processed table {table}")
            except Exception as e:
                logging.error(f"Failed to process table {table}: {e}")
                logging.error(traceback.format_exc())


# CONFIGURATION

pg_config = {
    "host": dbutils.secrets.get("key-vault-secret", "DataProduct-LCR-Host-PROD"),
    "port": dbutils.secrets.get("key-vault-secret", "DataProduct-LCR-Port-PROD"),
    "database": "LeadCustodyRepository",
    "user": dbutils.secrets.get("key-vault-secret", "DataProduct-LCR-User-PROD"),
    "password": dbutils.secrets.get("key-vault-secret", "DataProduct-LCR-Pass-PROD"),
}

if not all(pg_config.values()):
    raise ValueError("PostgreSQL configuration is incomplete.")
storage_config = {
    "account_name": "quilitydatabricks",
    "account_key": dbutils.secrets.get("key-vault-secret", "DataProduct-ADLS-Key"),
    "container_name": "dataarchitecture",
}

if not storage_config.get("account_key"):
    raise ValueError("Azure storage configuration is incomplete.")
tables_to_copy = [
    'public."leadassignment"',
    'public."leadxref"',
    'public."lead"',
]

# EXECUTION
try:
    pg_pool = PostgresDataHandler.connect_to_postgres(pg_config)
    postgres_handler = PostgresDataHandler(pg_pool, pg_config)
    blob_service_client = AzureDataHandler.connect_to_azure_storage(storage_config)
    azure_handler = AzureDataHandler(blob_service_client)
    sync = PostgresAzureDataSync(postgres_handler, azure_handler)
    sync.perform_operation(pg_config["database"], tables_to_copy)
finally:
    if "pg_pool" in locals():
        pg_pool.closeall()
