from datetime import datetime
from typing import Dict, List, Optional
import logging
import pytz
import dateutil.parser
import traceback
import json
import os
import time

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import (
    col, current_timestamp, lit, udf, to_date, to_timestamp, when, lower,
    coalesce, length, regexp_replace, year
)
from pyspark.sql.types import (
    BooleanType, DateType, DecimalType, DoubleType, StringType,
    StructField, StructType, TimestampType
)

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Create SparkSession
spark: SparkSession = SparkSession.builder.getOrCreate()
# set Spark log level for concise output
auto_level = os.getenv("SPARK_LOG", "WARN").upper()
spark.sparkContext.setLogLevel(auto_level)

try:
    from pyspark.dbutils import DBUtils
    dbutils = DBUtils(spark)
except Exception:  # pragma: no cover
    class _DummySecrets:
        def get(self, scope: str, key: str) -> str:
            env_key = f"{scope}_{key}".upper()
            value = os.getenv(env_key)
            if value is None:
                raise ValueError(f"Missing secret for {env_key}")
            return value

    class _DummyDBUtils:
        secrets = _DummySecrets()


    dbutils = _DummyDBUtils()
    logger.warning("DBUtils not available; using environment variables for secrets")

# --------------------------------------------------------------------
# Stand-alone pg helper – ingest must not depend on sync.py
# --------------------------------------------------------------------
try:
    import psycopg2
    import psycopg2.pool  # Databricks clusters often ship psycopg2-binary
except ImportError as _pg_err:  # pragma: no cover
    psycopg2 = None  # Let the RuntimeError below explain what to do

# ---- pg_config (copied from sync.py so this file is self-contained) --
pg_config: Dict[str, str] = {
    "host":     dbutils.secrets.get("key-vault-secret", "DataProduct-LCR-Host-PROD"),
    "port":     dbutils.secrets.get("key-vault-secret", "DataProduct-LCR-Port-PROD"),
    "database": "LeadCustodyRepository",
    "user":     dbutils.secrets.get("key-vault-secret", "DataProduct-LCR-User-PROD"),
    "password": dbutils.secrets.get("key-vault-secret", "DataProduct-LCR-Pass-PROD"),
}

class PostgresDataHandler:
    """Lightweight wrapper used by ingest — only the methods ingest.py calls."""

    def __init__(self, pool, config):
        self.pool = pool
        self.config = config

    # ----------------------------------------------------------------
    # Static: build a connection-pool (identical signature to sync.py)
    # ----------------------------------------------------------------
    @staticmethod
    def connect_to_postgres(config):
        if psycopg2 is None:
            raise RuntimeError(
                "psycopg2 wheel not found; install psycopg2-binary or attach the library "
                "to this Databricks cluster before running ingest.py."
            )
        return psycopg2.pool.SimpleConnectionPool(
            minconn=1,
            maxconn=int(os.getenv("PG_MAX_CONN", "4")),
            host=config["host"],
            port=config["port"],
            database=config["database"],
            user=config["user"],
            password=config["password"],
        )

    # ----------------------------------------------------------------
    # Query helpers – ingest only needs get_table_count right now
    # ----------------------------------------------------------------
    def get_table_count(self, table_name: str) -> int:
        conn = self.pool.getconn()
        try:
            with conn.cursor() as cur:
                cur.execute(f"SELECT COUNT(*) FROM {table_name}")
                return cur.fetchone()[0]
        finally:
            self.pool.putconn(conn)
# --------------------------------------------------------------------
 
# Constants
TIMEZONE = "America/New_York"
RAW_BASE_PATH = (
    "abfss://dataarchitecture@quilitydatabricks.dfs.core.windows.net/"
    "RAW/LeadCustodyRepository"
)
METADATA_BASE_PATH = (
    "dbfs:/FileStore/DataProduct/DataArchitecture/Pipelines/LCR_EDW/Metadata"
)

# Map "lead_assignment" ➜ "leadassignment", etc.  This keeps all the
# existing variable-names in the ingest script intact while converting
# them to the naming convention used by sync.py when it writes paths.
def _clean_name(name: str) -> str:
    return name.replace("_", "")

# Define Snowflake connection configuration for the staging schema
sf_config_stg: Dict[str, str] = {
    "sfURL": "hmkovlx-nu26765.snowflakecomputing.com",
    "sfDatabase": "DEV",
    "sfWarehouse": "INTEGRATION_COMPUTE_WH",
    "sfRole": "SG-SNOWFLAKE-DEVELOPERS",
    "sfSchema": "QUILITY_EDW_STAGE",
    "sfUser": dbutils.secrets.get(
        scope="key-vault-secret", key="DataProduct-SF-EDW-User"
    ),
    "sfPassword": dbutils.secrets.get(
        scope="key-vault-secret", key="DataProduct-SF-EDW-Pass"
    ),
    "on_error": "CONTINUE",
}

# Table Configurations
tables: List[str] = ["lead_assignment", "lead_xref", "lead"]

# Table Processing Configuration
table_processing_config: Dict[str, bool] = {
    "lead": True,
    "lead_xref": True,
    "lead_assignment": True,
}

# JSON columns that need special handling to prevent flattening
json_columns = {
    "lead_assignment": ["METADATA"],
    "lead": ["LEAD_ATTRIBUTES"],
    "lead_xref": []
}

# Schema definitions
table_schemas: Dict[str, StructType] = {
    "lead": StructType(
        [
            StructField("STG_LCR_LEAD_KEY", StringType(), True),
            StructField("LEAD_GUID", StringType(), True),
            StructField("LEGACY_LEAD_ID", StringType(), True),
            StructField("INDIV_ID", StringType(), True),
            StructField("HH_ID", StringType(), True),
            StructField("ADDR_ID", StringType(), True),
            StructField("LEAD_CODE", StringType(), True),
            StructField("LEAD_TYPE_ID", DecimalType(38, 0), True),
            StructField("LEAD_TYPE", StringType(), True),
            StructField("LEAD_SOURCE", StringType(), True),
            StructField("LEAD_CREATE_DATE", TimestampType(), True),
            StructField("FIRST_NAME", StringType(), True),
            StructField("MIDDLE_NAME", StringType(), True),
            StructField("LAST_NAME", StringType(), True),
            StructField("SUFFIX", StringType(), True),
            StructField("BIRTH_DATE", StringType(), True),
            StructField("AGE", DecimalType(38, 0), True),
            StructField("SEX", StringType(), True),
            StructField("STREET_1", StringType(), True),
            StructField("STREET_2", StringType(), True),
            StructField("CITY", StringType(), True),
            StructField("STATE_ID", DecimalType(38, 0), True),
            StructField("STATE", StringType(), True),
            StructField("ZIP", StringType(), True),
            StructField("ZIP5", StringType(), True),
            StructField("COUNTY", StringType(), True),
            StructField("COUNTRY", StringType(), True),
            StructField("PHONE", StringType(), True),
            StructField("HOME_PHONE", StringType(), True),
            StructField("CELL_PHONE", StringType(), True),
            StructField("WORK_PHONE", StringType(), True),
            StructField("DO_NOT_CALL", StringType(), True),
            StructField("CALLER_ID", StringType(), True),
            StructField("EMAIL", StringType(), True),
            StructField("DYNAMIC_LEAD", StringType(), True),
            StructField("PROSPECT_ID", StringType(), True),
            StructField("EXT_PARTNER_ID", StringType(), True),
            StructField("CHANNEL_ID", DecimalType(38, 0), True),
            StructField("CHANNEL", StringType(), True),
            StructField("OPT_SOURCE_ID", StringType(), True),
            StructField("SOURCE_ID", DecimalType(38, 0), True),
            StructField("SUB_SOURCE_ID", BooleanType(), True),
            StructField("SOURCE_OF_REFERRAL", StringType(), True),
            StructField("DIVISION", StringType(), True),
            StructField("LEAD_SUB_SOURCE", StringType(), True),
            StructField("LEAD_SUB_SOURCE_ID", StringType(), True),
            StructField("LENDER", StringType(), True),
            StructField("LOAN_AMOUNT", StringType(), True),
            StructField("LOAN_DATE", DateType(), True),
            StructField("DIABETES", StringType(), True),
            StructField("HEALTH_PROBLEMS", StringType(), True),
            StructField("HEART_PROBLEMS", StringType(), True),
            StructField("HEIGHT", StringType(), True),
            StructField("HIGH_BP_CHOL", StringType(), True),
            StructField("IS_INSURED", StringType(), True),
            StructField("SMOKER", StringType(), True),
            StructField("OCCUPATION", StringType(), True),
            StructField("SPOUSE", StringType(), True),
            StructField("COBORROWER_AGE", DoubleType(), True),
            StructField("COBORROWER_BIRTH_DATE", TimestampType(), True),
            StructField("COBORROWER_HEIGHT", StringType(), True),
            StructField("COBORROWER_ON_MORTGAGE", StringType(), True),
            StructField("COBORROWER_NAME", StringType(), True),
            StructField("COBORROWER_RELATION", StringType(), True),
            StructField("COBORROWER_SEX", StringType(), True),
            StructField("COBORROWER_SMOKER", StringType(), True),
            StructField("COBORROWER_WEIGHT", StringType(), True),
            StructField("COBORROWER_OCCUPATION", StringType(), True),
            StructField("DATA_SOURCE", StringType(), True),
            StructField("LEAD_ORIGIN_URL", StringType(), True),
            StructField("MAILING_ID", StringType(), True),
            StructField("SUSPECT_CAMPAIGN_ID", DecimalType(38, 0), True),
            StructField("CONSUMER_DEBT", DoubleType(), True),
            StructField("MORTGAGE_DEBT", DoubleType(), True),
            StructField("UTM_CAMPAIGN", StringType(), True),
            StructField("UTM_MEDIUM", StringType(), True),
            StructField("UTM_SOURCE", StringType(), True),
            StructField("REFERRING_URL", StringType(), True),
            StructField("PCS_POLICIES_ID", DecimalType(38, 0), True),
            StructField("CREATE_DATE", TimestampType(), True),
            StructField("MODIFY_DATE", TimestampType(), True),
            StructField("SOURCE_TABLE", StringType(), True),
            StructField("IS_DELETED_SOURCE", StringType(), True),
            StructField("EXP_DATE", TimestampType(), True),
            StructField("SOURCE_TYPE", StringType(), True),
            StructField("SOURCE_TYPE_ID", DecimalType(38, 0), True),
            StructField("PRODUCT_TYPE", StringType(), True),
            StructField("LEAD_ATTRIBUTES", StringType(), True),
            StructField("CUSTODY_TARGET_AUDIENCE", StringType(), True),
            StructField("SOURCE", StringType(), True),
            StructField("PRODUCT_TYPE_ID", DecimalType(38, 0), True),
            StructField("LEAD_SOURCE_ID", StringType(), True),
            StructField("ORIGIN_SYSTEM_ID", StringType(), True),
            StructField("ORIGIN_SYSTEM", StringType(), True),
            StructField("ORIGIN_SYSTEM_ORIG", StringType(), True),
            StructField("LEAD_INGESTION_METHOD", StringType(), True),
            StructField("ETL_CREATED_DATE", TimestampType(), False),
            StructField("ETL_LAST_UPDATE_DATE", TimestampType(), False),
            StructField("CREATED_BY", StringType(), False),
            StructField("TO_PROCESS", BooleanType(), False),
            StructField("EDW_EXTERNAL_SOURCE_SYSTEM", StringType(), False),
        ]
    ),
    "lead_xref": StructType(
        [
            StructField("STG_LCR_LEAD_XREF_KEY", StringType(), True),
            StructField("LEAD_XREF_GUID", StringType(), True),
            StructField("LEGACY_LEAD_ID", StringType(), True),
            StructField("LEAD_CODE", StringType(), True),
            StructField("LEAD_LEVEL_ID", StringType(), True),
            StructField("LEAD_LEVEL", StringType(), True),
            StructField("DATA_SOURCE_ID", StringType(), True),
            StructField("LEVEL_DATE", TimestampType(), True),
            StructField("CREATE_DATE", TimestampType(), True),
            StructField("MODIFY_DATE", TimestampType(), True),
            StructField("AVAILABLE_FOR_PURCHASE_IND", StringType(), True),
            StructField("IS_DELETED_SOURCE", StringType(), True),
            StructField("LEAD_LEVEL_ALIAS", StringType(), True),
            StructField("ETL_CREATED_DATE", TimestampType(), False),
            StructField("ETL_LAST_UPDATE_DATE", TimestampType(), False),
            StructField("CREATED_BY", StringType(), False),
            StructField("TO_PROCESS", BooleanType(), False),
            StructField("EDW_EXTERNAL_SOURCE_SYSTEM", StringType(), False),
        ]
    ),
    "lead_assignment": StructType(
        [
            StructField("STG_LCR_LEAD_ASSIGNMENT_KEY", StringType(), True),
            StructField("LEAD_ASSIGNMENT_GUID", StringType(), True),
            StructField("LEAD_XREF_GUID", StringType(), True),
            StructField("AGENT_CODE", StringType(), True),
            StructField("PURCHASE_DATE", TimestampType(), True),
            StructField("PURCHASE_PRICE", DoubleType(), True),
            StructField("ASSIGN_DATE", TimestampType(), True),
            StructField("INACTIVE_IND", StringType(), True),
            StructField("STATUS", StringType(), True),
            StructField("AGENT_EXTUID", StringType(), True),
            StructField("ALLOCATE_IND", StringType(), True),
            StructField("COMMENTS", StringType(), True),
            StructField("SFG_DIRECT_AGENT_ID", StringType(), True),
            StructField("BASE_SHOP_OWNER_AGENT_ID", StringType(), True),
            StructField("TOTAL_UPLINE_AGENT_CODES", StringType(), True),
            StructField("UNPAID_IND", StringType(), True),
            StructField("APP_COUNT", StringType(), True),
            StructField("APP_APV", StringType(), True),
            StructField("ACTUAL_APP_COUNT", StringType(), True),
            StructField("ACTUAL_APV", StringType(), True),
            StructField("CREATE_DATE", TimestampType(), True),
            StructField("MODIFY_DATE", TimestampType(), True),
            StructField("SOURCE_TABLE", StringType(), True),
            StructField("METADATA", StringType(), True),
            StructField("STATUS_DATE", TimestampType(), True),
            StructField("IS_DELETED_SOURCE", BooleanType(), True),
            StructField("ORDER_NUMBER", StringType(), True),
            StructField("LEAD_STATUS_ID", StringType(), True),
            StructField("LEAD_STATUS", StringType(), True),
            StructField("HQ_PURCHASE_AMOUNT", DoubleType(), True),
            StructField("LEAD_ORDER_SYSTEM_ID", StringType(), True),
            StructField("LEAD_ORDER_SYSTEM", StringType(), True),
            StructField("ORDER_SYSTEM_ID", StringType(), True),
            StructField("ORDER_SYSTEM", StringType(), True),
            StructField("ORDER_SYSTEM_ORIG", StringType(), True),
            StructField("EXCLUSIVITY_END_DATE", TimestampType(), True),
            StructField("ETL_CREATED_DATE", TimestampType(), False),
            StructField("ETL_LAST_UPDATE_DATE", TimestampType(), False),
            StructField("CREATED_BY", StringType(), False),
            StructField("TO_PROCESS", BooleanType(), False),
            StructField("EDW_EXTERNAL_SOURCE_SYSTEM", StringType(), False),
        ]
    ),
}

# Column mappings
column_mappings = {
    "lead": {
        "leadguid": "LEAD_GUID",
        "legacyleadid": "LEGACY_LEAD_ID",
        "individ": "INDIV_ID",
        "hhid": "HH_ID",
        "addrid": "ADDR_ID",
        "leadcode": "LEAD_CODE",
        "leadtypeid": "LEAD_TYPE_ID",
        "leadtype": "LEAD_TYPE",
        "leadsource": "LEAD_SOURCE",
        "leadcreatedate": "LEAD_CREATE_DATE",
        "firstname": "FIRST_NAME",
        "middlename": "MIDDLE_NAME",
        "lastname": "LAST_NAME",
        "suffix": "SUFFIX",
        "birthdate": "BIRTH_DATE",
        "age": "AGE",
        "sex": "SEX",
        "street1": "STREET_1",
        "street2": "STREET_2",
        "city": "CITY",
        "stateid": "STATE_ID",
        "state": "STATE",
        "zip": "ZIP",
        "zip5": "ZIP5",
        "county": "COUNTY",
        "country": "COUNTRY",
        "phone": "PHONE",
        "homephone": "HOME_PHONE",
        "cellphone": "CELL_PHONE",
        "workphone": "WORK_PHONE",
        "donotcall": "DO_NOT_CALL",
        "callerid": "CALLER_ID",
        "email": "EMAIL",
        "dynamiclead": "DYNAMIC_LEAD",
        "prospectid": "PROSPECT_ID",
        "extpartnerid": "EXT_PARTNER_ID",
        "channelid": "CHANNEL_ID",
        "channel": "CHANNEL",
        "optsourceid": "OPT_SOURCE_ID",
        "sourceid": "SOURCE_ID",
        "subsourceid": "SUB_SOURCE_ID",
        "sourceofreferral": "SOURCE_OF_REFERRAL",
        "division": "DIVISION",
        "leadsubsource": "LEAD_SUB_SOURCE",
        "leadsubsourceid": "LEAD_SUB_SOURCE_ID",
        "lender": "LENDER",
        "loanamount": "LOAN_AMOUNT",
        "loandate": "LOAN_DATE",
        "diabetes": "DIABETES",
        "healthproblems": "HEALTH_PROBLEMS",
        "heartproblems": "HEART_PROBLEMS",
        "height": "HEIGHT",
        "highbpchol": "HIGH_BP_CHOL",
        "isinsured": "IS_INSURED",
        "smoker": "SMOKER",
        "occupation": "OCCUPATION",
        "spouse": "SPOUSE",
        "coborrowerage": "COBORROWER_AGE",
        "coborrowerbirthdate": "COBORROWER_BIRTH_DATE",
        "coborrowerheight": "COBORROWER_HEIGHT",
        "coborroweronmortgage": "COBORROWER_ON_MORTGAGE",
        "coborrowername": "COBORROWER_NAME",
        "coborrowerrelation": "COBORROWER_RELATION",
        "coborrowersex": "COBORROWER_SEX",
        "coborrowersmoker": "COBORROWER_SMOKER",
        "coborrowerweight": "COBORROWER_WEIGHT",
        "coborroweroccupation": "COBORROWER_OCCUPATION",
        "datasource": "DATA_SOURCE",
        "leadoriginurl": "LEAD_ORIGIN_URL",
        "mailingid": "MAILING_ID",
        "suspectcampaignid": "SUSPECT_CAMPAIGN_ID",
        "consumerdebt": "CONSUMER_DEBT",
        "mortgagedebt": "MORTGAGE_DEBT",
        "utmcampaign": "UTM_CAMPAIGN",
        "utmmedium": "UTM_MEDIUM",
        "utmsource": "UTM_SOURCE",
        "referringurl": "REFERRING_URL",
        "pcspoliciesid": "PCS_POLICIES_ID",
        "createdate": "CREATE_DATE",
        "modifydate": "MODIFY_DATE",
        "sourcetable": "SOURCE_TABLE",
        "isdeletedsource": "IS_DELETED_SOURCE",
        "expdate": "EXP_DATE",
        "sourcetype": "SOURCE_TYPE",
        "sourcetypeid": "SOURCE_TYPE_ID",
        "producttype": "PRODUCT_TYPE",
        "leadattributes": "LEAD_ATTRIBUTES",
        "custodytargetaudience": "CUSTODY_TARGET_AUDIENCE",
        "source": "SOURCE",
        "producttypeid": "PRODUCT_TYPE_ID",
        "leadsourceid": "LEAD_SOURCE_ID",
        "originsystemid": "ORIGIN_SYSTEM_ID",
        "originsystem": "ORIGIN_SYSTEM",
        "originsystem_orig": "ORIGIN_SYSTEM_ORIG",
        "leadingestionmethod": "LEAD_INGESTION_METHOD",
    },
    "lead_xref": {
        "leadxrefguid": "LEAD_XREF_GUID",
        "legacyleadid": "LEGACY_LEAD_ID",
        "leadcode": "LEAD_CODE",
        "leadlevelid": "LEAD_LEVEL_ID",
        "leadlevel": "LEAD_LEVEL",
        "datasourceid": "DATA_SOURCE_ID",
        "leveldate": "LEVEL_DATE",
        "createdate": "CREATE_DATE",
        "modifydate": "MODIFY_DATE",
        "availableforpurchaseind": "AVAILABLE_FOR_PURCHASE_IND",
        "isdeletedsource": "IS_DELETED_SOURCE",
        "leadlevelalias": "LEAD_LEVEL_ALIAS",
    },
    "lead_assignment": {
        "leadassignmentguid": "LEAD_ASSIGNMENT_GUID",
        "leadxrefguid": "LEAD_XREF_GUID",
        "agentcode": "AGENT_CODE",
        "purchasedate": "PURCHASE_DATE",
        "purchaseprice": "PURCHASE_PRICE",
        "assigndate": "ASSIGN_DATE",
        "inactiveind": "INACTIVE_IND",
        "status": "STATUS",
        "agentextuid": "AGENT_EXTUID",
        "allocateind": "ALLOCATE_IND",
        "comments": "COMMENTS",
        "sfgdirectagentid": "SFG_DIRECT_AGENT_ID",
        "baseshopowneragentid": "BASE_SHOP_OWNER_AGENT_ID",
        "totaluplineagentcodes": "TOTAL_UPLINE_AGENT_CODES",
        "unpaidind": "UNPAID_IND",
        "appcount": "APP_COUNT",
        "appapv": "APP_APV",
        "actualappcount": "ACTUAL_APP_COUNT",
        "actualapv": "ACTUAL_APV",
        "createdate": "CREATE_DATE",
        "modifydate": "MODIFY_DATE",
        "sourcetable": "SOURCE_TABLE",
        "metadata": "METADATA",
        "statusdate": "STATUS_DATE",
        "isdeletedsource": "IS_DELETED_SOURCE",
        "ordernumber": "ORDER_NUMBER",
        "leadstatusid": "LEAD_STATUS_ID",
        "leadstatus": "LEAD_STATUS",
        "hqpurchaseamount": "HQ_PURCHASE_AMOUNT",
        "leadordersystemid": "LEAD_ORDER_SYSTEM_ID",
        "leadordersystem": "LEAD_ORDER_SYSTEM",
        "ordersystemid": "ORDER_SYSTEM_ID",
        "ordersystem": "ORDER_SYSTEM",
        "ordersystemorig": "ORDER_SYSTEM_ORIG",
        "exclusivityenddate": "EXCLUSIVITY_END_DATE",
    },
}

# Define Boolean-like String Columns
boolean_string_columns = {
    "IS_DELETED_SOURCE",
}

@udf(TimestampType())
def enhanced_parse_timestamp_udf(date_str):
    """
    Safely parse timestamp values with fallback for fuzzy parsing,
    ignoring invalid short or non-numeric strings.
    """
    if not date_str:
        return None

    if isinstance(date_str, str) and (len(date_str) <= 3 or not any(c.isdigit() for c in date_str)):
        return None

    try:
        parsed_date = dateutil.parser.parse(str(date_str), fuzzy=False)
        ny_timezone = pytz.timezone(TIMEZONE)

        if parsed_date.tzinfo is None:
            parsed_date = ny_timezone.localize(parsed_date)
        else:
            parsed_date = parsed_date.astimezone(ny_timezone)

        current_datetime = datetime.now(ny_timezone)
        if parsed_date > current_datetime:
            return current_datetime

        return parsed_date
    except Exception:
        return None

@udf(DateType())
def enhanced_parse_date_udf(date_str):
    """
    Safely parse date values with fallback for fuzzy parsing,
    ignoring invalid short or non-numeric strings.
    """
    if not date_str:
        return None

    if isinstance(date_str, str) and (len(date_str) <= 3 or not any(c.isdigit() for c in date_str)):
        return None

    try:
        parsed_date = dateutil.parser.parse(str(date_str), fuzzy=False).date()
        current_date = datetime.now(pytz.timezone(TIMEZONE)).date()
        if parsed_date > current_date:
            return None
        return parsed_date
    except Exception:
        return None

def validate_dataframe(df: DataFrame, target_schema: StructType, check_types: bool = True) -> None:
    """
    Validates that the DataFrame has all columns with correct data types according to the target schema.
    """
    logger.info("Validating DataFrame against target schema")
    errors = []
    
    for field in target_schema.fields:
        col_name = field.name
        col_type = field.dataType
        
        if col_name not in df.columns:
            error_msg = f"Column {col_name} is missing from the DataFrame"
            errors.append(error_msg)
            logger.error(error_msg)
        elif check_types and not isinstance(df.schema[col_name].dataType, type(col_type)):
            error_msg = (
                f"Column {col_name} has type {df.schema[col_name].dataType}, "
                f"but should be {col_type}"
            )
            errors.append(error_msg)
            logger.error(error_msg)
            
    if errors:
        raise ValueError(
            "DataFrame validation failed with errors:\n" + "\n".join(errors)
        )
        
    logger.info("DataFrame validation completed successfully")

def get_last_runtime(table_name: str) -> datetime:
    """
    Retrieves the last runtime for the given table from DBFS.
    If not found, returns a past date to include all records.
    """
    try:
        last_runtime_path = f"{METADATA_BASE_PATH}/last_runtime_{table_name}.txt"
        last_runtime_str = spark.read.text(last_runtime_path).first()[0]
        last_runtime = datetime.strptime(
            last_runtime_str, "%Y-%m-%d %H:%M:%S.%f"
        ).replace(tzinfo=pytz.timezone(TIMEZONE))
        logger.info(f"Last runtime for table {table_name}: {last_runtime}")
        return last_runtime
    except Exception as e:
        logger.warning(f"Could not read last runtime for table {table_name}. Error: {str(e)}")
        past_date = datetime(1900, 1, 1, tzinfo=pytz.timezone(TIMEZONE))
        logger.info(f"Setting last_runtime to {past_date} for table {table_name}")
        return past_date

def update_last_runtime(table_name: str, new_runtime: datetime) -> None:
    """
    Updates the last runtime for the given table in DBFS.
    """
    try:
        last_runtime_path = f"{METADATA_BASE_PATH}/last_runtime_{table_name}.txt"
        new_runtime_str = new_runtime.strftime("%Y-%m-%d %H:%M:%S.%f")
        spark.createDataFrame([(new_runtime_str,)], ["last_runtime"]).coalesce(1)\
            .write.mode("overwrite").text(last_runtime_path)
        logger.info(f"Updated last runtime for table {table_name} to {new_runtime}")
    except Exception as e:
        logger.error(f"Could not update last runtime for table {table_name}. Error: {str(e)}")

def snowflake_table_exists(table_name: str) -> bool:
    """Check if a table exists in Snowflake."""
    query = (
        f"SELECT 1 FROM information_schema.tables WHERE table_schema = '{sf_config_stg['sfSchema']}'"
        f" AND table_name = '{table_name.upper()}'"
    )
    try:
        df = spark.read.format("net.snowflake.spark.snowflake").options(**sf_config_stg).option("query", query).load()
        # Efficient table existence check: only fetch up to 1 row
        return len(df.take(1)) > 0
    except Exception as e:
        logger.error(f"Failed to check table existence: {e}")
        return False

def create_checkpoint(table_name: str) -> None:
    """Create a checkpoint file after table processing."""
    path = f"{METADATA_BASE_PATH}/checkpoint_{table_name}.txt"
    spark.createDataFrame([(datetime.now().isoformat(),)], ["ts"]).coalesce(1).write.mode("overwrite").text(path)

def truncate_table(table_name: str) -> None:
    """Truncate a staging table in Snowflake."""
    opts = {
        **sf_config_stg,
        "dbtable": f"STG_LCR_{table_name.upper()}",
        "TRUNCATE_TABLE": "ON",
    }
    spark.createDataFrame([], table_schemas[table_name]) \
        .write.format("net.snowflake.spark.snowflake") \
        .options(**opts).mode("overwrite").save()

def swap_temp_into_target(temp_table: str, target_table: str) -> None:
    """Atomically replace target with temp and *always* drop the temp table."""
    # atomic metadata-preserving swap
    spark.read.format("net.snowflake.spark.snowflake")\
        .options(**sf_config_stg)\
        .option("query", f"ALTER TABLE {target_table} SWAP WITH {temp_table}")\
        .load()
    # always drop the (now empty) temp
    spark.read.format("net.snowflake.spark.snowflake")\
        .options(**sf_config_stg)\
        .option("query", f"DROP TABLE IF EXISTS {temp_table}")\
        .load()

def clean_invalid_timestamps(df: DataFrame) -> DataFrame:
    """
    Removes obviously invalid timestamp values from timestamp columns,
    setting them to null or a default as needed.
    """
    timestamp_cols = [
        field.name
        for field in df.schema.fields
        if isinstance(field.dataType, TimestampType)
    ]
    
    for ts_col in timestamp_cols:
        df = df.withColumn(
            ts_col,
            when(
                col(ts_col).isNull()
                | col(ts_col).cast("string").rlike("^[A-Za-z]{1,3}$")
                | (length(col(ts_col).cast("string")) <= 3)
                | (~col(ts_col).cast("string").rlike(".*\\d+.*"))
                | (year(col(ts_col)) < 1900)
                | (year(col(ts_col)) > year(current_timestamp()) + 1),
                lit(None),
            ).otherwise(col(ts_col))
        )
        
        if ts_col.startswith("ETL_"):
            df = df.withColumn(
                ts_col,
                coalesce(col(ts_col), current_timestamp())
            )
    
    return df

@udf(StringType())
def validate_json_udf(val: str) -> Optional[str]:
    """Return the JSON string if valid, otherwise None."""
    if val is None:
        return None
    try:
        json.loads(val)
        return val
    except Exception:
        return None

def transform_column(df: DataFrame, col_name: str, col_type, table_name: str) -> DataFrame:
    """
    Transforms/cleans a single column to match the target data type, with special handling for JSON columns, etc.
    """
    # Handle JSON columns
    if table_name in json_columns and col_name in json_columns[table_name]:
        logger.info(
            f"Applying JSON validation for column {col_name} in table {table_name}"
        )
        return df.withColumn(
            col_name,
            validate_json_udf(col(col_name).cast(StringType()))
        )
    
    # Timestamp
    if isinstance(col_type, TimestampType):
        df = df.withColumn(
            col_name,
            when(
                (col(col_name).cast("string").rlike("^[A-Za-z]{1,3}$")) |
                (length(col(col_name).cast("string")) <= 3) |
                (~col(col_name).cast("string").rlike(".*\\d+.*")),
                lit(None)
            ).otherwise(col(col_name))
        )
        return df.withColumn(
            col_name,
            when(col(col_name).isNull(), None).otherwise(
                coalesce(
                    to_timestamp(col(col_name)),
                    enhanced_parse_timestamp_udf(col(col_name)),
                )
            ),
        )
    
    # Date
    elif isinstance(col_type, DateType):
        return df.withColumn(
            col_name,
            when(col(col_name).isNull(), None).otherwise(
                coalesce(
                    to_date(col(col_name)),
                    enhanced_parse_date_udf(col(col_name)),
                )
            ),
        )
    
    # Decimal
    elif isinstance(col_type, DecimalType):
        precision, scale = col_type.precision, col_type.scale
        return df.withColumn(col_name, col(col_name).cast(DecimalType(precision, scale)))
    
    # Double
    elif isinstance(col_type, DoubleType):
        return df.withColumn(col_name, col(col_name).cast(DoubleType()))
    
    # Boolean
    elif isinstance(col_type, BooleanType):
        return df.withColumn(
            col_name,
            when(lower(col(col_name)).isin("true", "1", "yes"), lit(True))
            .when(lower(col(col_name)).isin("false", "0", "no"), lit(False))
            .when(col(col_name).isNull(), lit(None))
            .otherwise(
                when(
                    length(col(col_name)) == 1,
                    when(lower(col(col_name)) == "t", lit(True))
                    .when(lower(col(col_name)) == "f", lit(False))
                    .otherwise(lit(None))
                ).otherwise(lit(None))
            ),
        )
    
    # Boolean strings
    elif isinstance(col_type, StringType) and col_name in boolean_string_columns:
        return df.withColumn(
            col_name,
            when(lower(col(col_name).cast("string")).isin("true", "1", "yes", "t"), lit("TRUE"))
            .when(lower(col(col_name).cast("string")).isin("false", "0", "no", "f"), lit("FALSE"))
            .when(col(col_name).isNull(), lit(None))
            .otherwise(col(col_name).cast(StringType()))   # ← cast fixes the mismatch
        )
    
    # Fallback to String
    else:
        return df.withColumn(col_name, col(col_name).cast(StringType()))

def load_raw_data(table_name: str) -> DataFrame:
    """
    Loads raw data for the given table from Delta storage.
    IMPORTANT: Ensure path matches the sync script's location so data is not duplicated.
    """
    # Use the same folder name that sync.py wrote
    raw_table_name: str = _clean_name(table_name)
    raw_dataset_path: str = f"{RAW_BASE_PATH}/{raw_table_name}"

    if table_name == "lead_assignment":
        logger.info(f"Loading {table_name} with special JSON handling")
        df = (
            spark.read.format("delta")
            .option("header", "true")
            .option("inferSchema", "false")
            .option("multiLine", "true")
            .option("mode", "PERMISSIVE")
            .load(raw_dataset_path)
        )
        return df
    else:
        return (
            spark.read.format("delta")
            .option("header", "true")
            .option("inferSchema", "false")
            .load(raw_dataset_path)
        )

def verify_row_count(df: DataFrame, table_name: str, postgres_handler) -> None:
    """Validate that Delta row count matches the source Postgres count."""
    pg_table = _clean_name(table_name)
    expected = postgres_handler.get_table_count(f'public."{pg_table}"')
    actual = df.count()
    if expected != actual:
        logger.error(
            f"Row count mismatch for {table_name}: Postgres {expected} vs Delta {actual}"
        )
        raise ValueError(
            f"Row count mismatch for {table_name}: Postgres {expected} vs Delta {actual}"
        )
    logger.info(f"Row count validation passed for {table_name}: {actual} rows")

def rename_and_add_columns(df: DataFrame, table_name: str) -> DataFrame:
    """
    Renames columns based on column_mappings and adds missing columns as null, matching the target schema.
    """
    df_columns_lower = {column.lower(): column for column in df.columns}
    
    # Rename columns
    for old_col, new_col in column_mappings[table_name].items():
        if old_col.lower() in df_columns_lower:
            original_col = df_columns_lower[old_col.lower()]
            df = df.withColumnRenamed(original_col, new_col)
            
    # Add missing columns
    target_schema: StructType = table_schemas[table_name]
    missing_columns = set(field.name for field in target_schema.fields) - set(df.columns)
    for col_name in missing_columns:
        df = df.withColumn(col_name, lit(None).cast(target_schema[col_name].dataType))
        
    return df

def transform_columns(df: DataFrame, target_schema: StructType, table_name: str) -> DataFrame:
    """
    Cleans invalid timestamps first, then applies transform_column for each target column.
    """
    df = clean_invalid_timestamps(df)
    for field in target_schema.fields:
        df = transform_column(df, field.name, field.dataType, table_name)
    return df

def add_metadata_columns(df: DataFrame, target_schema: StructType) -> DataFrame:
    """
    Adds ETL metadata columns with consistent timestamps and default values.
    """
    etl_timestamp = current_timestamp()
    metadata_defaults = {
        "ETL_CREATED_DATE": etl_timestamp,
        "ETL_LAST_UPDATE_DATE": etl_timestamp,
        "CREATED_BY": lit("ETL_PROCESS"),
        "TO_PROCESS": lit(True),
        "EDW_EXTERNAL_SOURCE_SYSTEM": lit("LeadCustodyRepository"),
    }
    
    for col_name, default_value in metadata_defaults.items():
        df = df.withColumn(
            col_name,
            default_value.cast(target_schema[col_name].dataType)
        )
        
    return df

def process_table(
    table_name: str,
    write_mode: str,
    historical_load: bool = False,
    postgres_handler=None
) -> None:
    """
    Main workflow for a single table: load raw data, rename columns,
    transform data, handle special logic, validate, and write to Snowflake.
    """
    logger.info(f"Starting processing for table: {table_name}")
    try:
        if historical_load and write_mode != "append":
            truncate_table(table_name)
        # 1) Load raw data
        raw_df = load_raw_data(table_name)
        if postgres_handler:
            verify_row_count(raw_df, table_name, postgres_handler)
        logger.info(f"Loaded raw records from source for table {table_name} (row count skipped for performance).")
        
        # 2) Rename columns and add missing ones
        raw_df = rename_and_add_columns(raw_df, table_name)
        validate_dataframe(raw_df, table_schemas[table_name], check_types=False)
        logger.info(f"Renamed columns for table {table_name} (row count skipped for performance).")
        
        # 3) Transform columns
        target_schema = table_schemas[table_name]
        raw_df = transform_columns(raw_df, target_schema, table_name)
        validate_dataframe(raw_df, target_schema)
        logger.info(f"Data transformation completed for table {table_name} (row count skipped for performance).")

        # 4) Special handling for lead_assignment
        if table_name == "lead_assignment":
            date_columns = [
                "PURCHASE_DATE",
                "ASSIGN_DATE",
                "CREATE_DATE",
                "MODIFY_DATE",
                "STATUS_DATE",
                "EXCLUSIVITY_END_DATE",
            ]
            current_date = current_timestamp()
            for date_col in date_columns:
                raw_df = raw_df.withColumn(
                    date_col,
                    when(col(date_col) > current_date, current_date).otherwise(col(date_col))
                )
            raw_df = raw_df.withColumn(
                "METADATA",
                when(col("METADATA").isNull(), lit(None)).otherwise(col("METADATA").cast(StringType()))
            )
            logger.info("Applied lead assignment specific handling")
        
        # 5) Add metadata columns
        raw_df = add_metadata_columns(raw_df, target_schema)
        
        # 6) Reorder columns to match target schema
        target_columns = [field.name for field in target_schema.fields]
        raw_df = raw_df.select(*target_columns)
        
        # 7) Final timestamp cleanup
        raw_df = clean_invalid_timestamps(raw_df)

        timestamp_cols = [
            field.name
            for field in target_schema.fields
            if isinstance(field.dataType, TimestampType)
        ]
        for ts_col in timestamp_cols:
            raw_df = raw_df.withColumn(
                ts_col,
                when(
                    col(ts_col).isNull() |
                    regexp_replace(col(ts_col).cast("string"), "[0-9\\-:. ]", "").rlike(".+"),
                    current_timestamp() if ts_col.startswith("ETL_") else lit(None)
                ).otherwise(col(ts_col))
            )

        logger.info(f"DataFrame finalization completed for table {table_name} (row count skipped for performance).")

        # 9) Write to Snowflake
        if not snowflake_table_exists(f"STG_LCR_{table_name.upper()}"):
            logger.error(f"Target table STG_LCR_{table_name.upper()} does not exist in Snowflake")
            return

        # Write records
        if write_mode == "append":
            import uuid
            temp_table = (
                f"TMP_LCR_{table_name.upper()}_{int(time.time())}_"
                f"{uuid.uuid4().hex.upper()}"
            )
            write_options = {
                **sf_config_stg,
                "dbtable": temp_table,
                "on_error": "CONTINUE",
                "column_mapping": "name"
            }
            for attempt in range(3):
                try:
                    raw_df.write.format("net.snowflake.spark.snowflake").options(**write_options).mode("overwrite").save()
                    break
                except Exception as w_err:
                    if attempt == 2:
                        raise
                    logger.warning(f"Snowflake write failed, retrying... {w_err}")
                    time.sleep(5)
            # Atomically replace target and clean up temp
            swap_temp_into_target(temp_table, f"STG_LCR_{table_name.upper()}")
            # Prevent full reload on next incremental run
            update_last_runtime(table_name, datetime.now(pytz.timezone(TIMEZONE)))
            logger.info(f"Successfully refreshed STG_LCR_{table_name.upper()} via temp swap (row count skipped for performance).")
            create_checkpoint(table_name)

        elif write_mode == "incremental_insert":
            last_runtime = get_last_runtime(table_name)
            raw_df = raw_df.withColumn("MODIFY_DATE", coalesce(col("MODIFY_DATE"), col("CREATE_DATE")))
            # NOTE: We no longer check if DataFrame is empty due to performance. Snowflake will ignore empty writes.
            raw_df_filtered = (
                raw_df
                if historical_load
                else raw_df.filter(col("MODIFY_DATE") > last_runtime)
            )

            validate_dataframe(raw_df_filtered, target_schema)
            write_options = {
                **sf_config_stg,
                "dbtable": f"STG_LCR_{table_name.upper()}",
                "column_mapping": "name",
                "on_error": "CONTINUE"
            }
            for attempt in range(3):
                try:
                    raw_df_filtered.write.format("net.snowflake.spark.snowflake").options(**write_options).mode("append").save()
                    break
                except Exception as w_err:
                    if attempt == 2:
                        raise
                    logger.warning(f"Snowflake write failed, retrying... {w_err}")
                    time.sleep(5)
            update_last_runtime(table_name, datetime.now(pytz.timezone(TIMEZONE)))
            logger.info(f"Appended records to table STG_LCR_{table_name.upper()} (row count skipped for performance).")
            create_checkpoint(table_name)

        else:
            raise ValueError(f"Invalid write mode: {write_mode}")

        logger.info(f"Completed processing for table: {table_name}")

    except Exception as e:
        logger.error(f"Unexpected error processing table {table_name}: {str(e)}")
        logger.error(traceback.format_exc())
        raise

def main():
    """
    Main entry point: iterate over tables, process each with chosen write_mode & historical_load options.
    """
    write_mode = "append"
    historical_load = "true"

    pg_pool = None
    try:
        pg_pool = PostgresDataHandler.connect_to_postgres(pg_config)
    except Exception as e:
        logger.error(f"Failed to connect to PostgreSQL: {e}")
        return

    postgres_handler = PostgresDataHandler(pg_pool, pg_config)

    try:
        for table in tables:
            should_process = table_processing_config.get(table, False)
            if should_process:
                process_table(table, write_mode, historical_load, postgres_handler)
            else:
                logger.info(
                    f"Skipping processing for table: {table} as per configuration."
                )

        logger.info("ETL process completed successfully.")
    finally:
        if pg_pool:
            pg_pool.closeall()

if __name__ == "__main__":
    main()
