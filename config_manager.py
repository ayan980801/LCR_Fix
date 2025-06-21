import os

from pyspark.sql import SparkSession


class ConfigManager:
    """Handles configuration and secrets."""

    TIMEZONE = "America/New_York"
    RAW_BASE_PATH = (
        "abfss://dataarchitecture@quilitydatabricks.dfs.core.windows.net/RAW/LeadCustodyRepository"
    )
    METADATA_BASE_PATH = (
        "dbfs:/FileStore/DataProduct/DataArchitecture/Pipelines/LCR_EDW/Metadata"
    )
    TABLES = ["lead_assignment", "lead_xref", "lead"]
    TABLE_PROCESSING_CONFIG = {
        "lead": True,
        "lead_xref": True,
        "lead_assignment": True,
    }

    @classmethod
    def get_secret(cls, scope: str, key: str) -> str:
        try:
            from pyspark.dbutils import DBUtils

            dbutils = DBUtils(cls.get_spark())
            return dbutils.secrets.get(scope=scope, key=key)
        except Exception:
            env_key = f"{scope}_{key}".upper()
            value = os.getenv(env_key)
            if value is None:
                raise ValueError(f"Missing secret for {env_key}")
            return value

    @classmethod
    def get_sf_config_stg(cls) -> dict:
        return {
            "sfURL": "hmkovlx-nu26765.snowflakecomputing.com",
            "sfDatabase": "DEV",
            "sfWarehouse": "INTEGRATION_COMPUTE_WH",
            "sfRole": "SG-SNOWFLAKE-DEVELOPERS",
            "sfSchema": "QUILITY_EDW_STAGE",
            "sfUser": cls.get_secret("key-vault-secret", "DataProduct-SF-EDW-User"),
            "sfPassword": cls.get_secret("key-vault-secret", "DataProduct-SF-EDW-Pass"),
            "on_error": "CONTINUE",
        }

    @staticmethod
    def get_spark() -> SparkSession:
        if not hasattr(ConfigManager, "_spark"):
            ConfigManager._spark = SparkSession.builder.getOrCreate()
        return ConfigManager._spark
