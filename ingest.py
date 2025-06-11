from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple
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
    coalesce, length, regexp_replace, year, max as spark_max
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
 
# Constants
TIMEZONE = "America/New_York"
RAW_BASE_PATH = "abfss://dataarchitecture@quilitydatabricks.dfs.core.windows.net/RAW/LeadCustodyRepository"
METADATA_BASE_PATH = "dbfs:/FileStore/DataProduct/DataArchitecture/Pipelines/LCR_EDW/Metadata"

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

# Helper to build StructType from simple tuple definitions
def build_struct_type(fields: List[Tuple[str, Any, bool]]) -> StructType:
    """Convert `(name, type, nullable)` tuples to a StructType."""
    struct_fields = []
    for name, dtype, nullable in fields:
        dtype_obj = dtype() if isinstance(dtype, type) else dtype
        struct_fields.append(StructField(name, dtype_obj, nullable))
    return StructType(struct_fields)

# Schema definitions stored as `(name, type, nullable)` tuples
table_schema_defs: Dict[str, List[Tuple[str, Any, bool]]] = {
    "lead": [
        ("STG_LCR_LEAD_KEY", StringType(), True),
        ("LEAD_GUID", StringType(), True),
        ("LEGACY_LEAD_ID", StringType(), True),
        ("INDIV_ID", StringType(), True),
        ("HH_ID", StringType(), True),
        ("ADDR_ID", StringType(), True),
        ("LEAD_CODE", StringType(), True),
        ("LEAD_TYPE_ID", DecimalType(38, 0), True),
        ("LEAD_TYPE", StringType(), True),
        ("LEAD_SOURCE", StringType(), True),
        ("LEAD_CREATE_DATE", TimestampType(), True),
        ("FIRST_NAME", StringType(), True),
        ("MIDDLE_NAME", StringType(), True),
        ("LAST_NAME", StringType(), True),
        ("SUFFIX", StringType(), True),
        ("BIRTH_DATE", StringType(), True),
        ("AGE", DecimalType(38, 0), True),
        ("SEX", StringType(), True),
        ("STREET_1", StringType(), True),
        ("STREET_2", StringType(), True),
        ("CITY", StringType(), True),
        ("STATE_ID", DecimalType(38, 0), True),
        ("STATE", StringType(), True),
        ("ZIP", StringType(), True),
        ("ZIP5", StringType(), True),
        ("COUNTY", StringType(), True),
        ("COUNTRY", StringType(), True),
        ("PHONE", StringType(), True),
        ("HOME_PHONE", StringType(), True),
        ("CELL_PHONE", StringType(), True),
        ("WORK_PHONE", StringType(), True),
        ("DO_NOT_CALL", StringType(), True),
        ("CALLER_ID", StringType(), True),
        ("EMAIL", StringType(), True),
        ("DYNAMIC_LEAD", StringType(), True),
        ("PROSPECT_ID", StringType(), True),
        ("EXT_PARTNER_ID", StringType(), True),
        ("CHANNEL_ID", DecimalType(38, 0), True),
        ("CHANNEL", StringType(), True),
        ("OPT_SOURCE_ID", StringType(), True),
        ("SOURCE_ID", DecimalType(38, 0), True),
        ("SUB_SOURCE_ID", BooleanType(), True),
        ("SOURCE_OF_REFERRAL", StringType(), True),
        ("DIVISION", StringType(), True),
        ("LEAD_SUB_SOURCE", StringType(), True),
        ("LEAD_SUB_SOURCE_ID", StringType(), True),
        ("LENDER", StringType(), True),
        ("LOAN_AMOUNT", StringType(), True),
        ("LOAN_DATE", DateType(), True),
        ("DIABETES", StringType(), True),
        ("HEALTH_PROBLEMS", StringType(), True),
        ("HEART_PROBLEMS", StringType(), True),
        ("HEIGHT", StringType(), True),
        ("HIGH_BP_CHOL", StringType(), True),
        ("IS_INSURED", StringType(), True),
        ("SMOKER", StringType(), True),
        ("OCCUPATION", StringType(), True),
        ("SPOUSE", StringType(), True),
        ("COBORROWER_AGE", DoubleType(), True),
        ("COBORROWER_BIRTH_DATE", TimestampType(), True),
        ("COBORROWER_HEIGHT", StringType(), True),
        ("COBORROWER_ON_MORTGAGE", StringType(), True),
        ("COBORROWER_NAME", StringType(), True),
        ("COBORROWER_RELATION", StringType(), True),
        ("COBORROWER_SEX", StringType(), True),
        ("COBORROWER_SMOKER", StringType(), True),
        ("COBORROWER_WEIGHT", StringType(), True),
        ("COBORROWER_OCCUPATION", StringType(), True),
        ("DATA_SOURCE", StringType(), True),
        ("LEAD_ORIGIN_URL", StringType(), True),
        ("MAILING_ID", StringType(), True),
        ("SUSPECT_CAMPAIGN_ID", DecimalType(38, 0), True),
        ("CONSUMER_DEBT", DoubleType(), True),
        ("MORTGAGE_DEBT", StringType(), True),
        ("UTM_CAMPAIGN", StringType(), True),
        ("UTM_MEDIUM", StringType(), True),
        ("UTM_SOURCE", StringType(), True),
        ("REFERRING_URL", StringType(), True),
        ("PCS_POLICIES_ID", DecimalType(38, 0), True),
        ("CREATE_DATE", TimestampType(), True),
        ("MODIFY_DATE", TimestampType(), True),
        ("SOURCE_TABLE", StringType(), True),
        ("IS_DELETED_SOURCE", StringType(), True),
        ("EXP_DATE", TimestampType(), True),
        ("SOURCE_TYPE", StringType(), True),
        ("SOURCE_TYPE_ID", DecimalType(38, 0), True),
        ("PRODUCT_TYPE", StringType(), True),
        ("LEAD_ATTRIBUTES", StringType(), True),
        ("CUSTODY_TARGET_AUDIENCE", StringType(), True),
        ("SOURCE", StringType(), True),
        ("PRODUCT_TYPE_ID", DecimalType(38, 0), True),
        ("LEAD_SOURCE_ID", StringType(), True),
        ("ORIGIN_SYSTEM_ID", StringType(), True),
        ("ORIGIN_SYSTEM", StringType(), True),
        ("ORIGIN_SYSTEM_ORIG", StringType(), True),
        ("LEAD_INGESTION_METHOD", StringType(), True),
        ("ETL_CREATED_DATE", TimestampType(), False),
        ("ETL_LAST_UPDATE_DATE", TimestampType(), False),
        ("CREATED_BY", StringType(), False),
        ("TO_PROCESS", BooleanType(), False),
        ("EDW_EXTERNAL_SOURCE_SYSTEM", StringType(), False),
    ],
    "lead_xref": [
        ("STG_LCR_LEAD_XREF_KEY", StringType(), True),
        ("LEAD_XREF_GUID", StringType(), True),
        ("LEGACY_LEAD_ID", StringType(), True),
        ("LEAD_CODE", StringType(), True),
        ("LEAD_LEVEL_ID", StringType(), True),
        ("LEAD_LEVEL", StringType(), True),
        ("DATA_SOURCE_ID", StringType(), True),
        ("LEVEL_DATE", TimestampType(), True),
        ("CREATE_DATE", TimestampType(), True),
        ("MODIFY_DATE", TimestampType(), True),
        ("AVAILABLE_FOR_PURCHASE_IND", StringType(), True),
        ("IS_DELETED_SOURCE", StringType(), True),
        ("LEAD_LEVEL_ALIAS", StringType(), True),
        ("ETL_CREATED_DATE", TimestampType(), False),
        ("ETL_LAST_UPDATE_DATE", TimestampType(), False),
        ("CREATED_BY", StringType(), False),
        ("TO_PROCESS", BooleanType(), False),
        ("EDW_EXTERNAL_SOURCE_SYSTEM", StringType(), False),
    ],
    "lead_assignment": [
        ("STG_LCR_LEAD_ASSIGNMENT_KEY", StringType(), True),
        ("LEAD_ASSIGNMENT_GUID", StringType(), True),
        ("LEAD_XREF_GUID", StringType(), True),
        ("AGENT_CODE", StringType(), True),
        ("PURCHASE_DATE", TimestampType(), True),
        ("PURCHASE_PRICE", DoubleType(), True),
        ("ASSIGN_DATE", TimestampType(), True),
        ("INACTIVE_IND", StringType(), True),
        ("STATUS", StringType(), True),
        ("AGENT_EXTUID", StringType(), True),
        ("ALLOCATE_IND", StringType(), True),
        ("COMMENTS", StringType(), True),
        ("SFG_DIRECT_AGENT_ID", StringType(), True),
        ("BASE_SHOP_OWNER_AGENT_ID", StringType(), True),
        ("TOTAL_UPLINE_AGENT_CODES", StringType(), True),
        ("UNPAID_IND", StringType(), True),
        ("APP_COUNT", StringType(), True),
        ("APP_APV", StringType(), True),
        ("ACTUAL_APP_COUNT", StringType(), True),
        ("ACTUAL_APV", StringType(), True),
        ("CREATE_DATE", TimestampType(), True),
        ("MODIFY_DATE", TimestampType(), True),
        ("SOURCE_TABLE", StringType(), True),
        ("METADATA", StringType(), True),
        ("STATUS_DATE", TimestampType(), True),
        ("IS_DELETED_SOURCE", BooleanType(), True),
        ("ORDER_NUMBER", StringType(), True),
        ("LEAD_STATUS_ID", StringType(), True),
        ("LEAD_STATUS", StringType(), True),
        ("HQ_PURCHASE_AMOUNT", DoubleType(), True),
        ("LEAD_ORDER_SYSTEM_ID", StringType(), True),
        ("LEAD_ORDER_SYSTEM", StringType(), True),
        ("ORDER_SYSTEM_ID", StringType(), True),
        ("ORDER_SYSTEM", StringType(), True),
        ("ORDER_SYSTEM_ORIG", StringType(), True),
        ("EXCLUSIVITY_END_DATE", TimestampType(), True),
        ("ETL_CREATED_DATE", TimestampType(), False),
        ("ETL_LAST_UPDATE_DATE", TimestampType(), False),
        ("CREATED_BY", StringType(), False),
        ("TO_PROCESS", BooleanType(), False),
        ("EDW_EXTERNAL_SOURCE_SYSTEM", StringType(), False),
    ],
}

# Build StructTypes from the tuple definitions
table_schemas: Dict[str, StructType] = {
    name: build_struct_type(fields) for name, fields in table_schema_defs.items()
}

# Column mappings

# Raw-to-target column mappings stored concisely
column_mapping_defs: Dict[str, List[Tuple[str, str]]] = {
    "lead": [
        ("leadguid", "LEAD_GUID"),
        ("legacyleadid", "LEGACY_LEAD_ID"),
        ("individ", "INDIV_ID"),
        ("hhid", "HH_ID"),
        ("addrid", "ADDR_ID"),
        ("leadcode", "LEAD_CODE"),
        ("leadtypeid", "LEAD_TYPE_ID"),
        ("leadtype", "LEAD_TYPE"),
        ("leadsource", "LEAD_SOURCE"),
        ("leadcreatedate", "LEAD_CREATE_DATE"),
        ("firstname", "FIRST_NAME"),
        ("middlename", "MIDDLE_NAME"),
        ("lastname", "LAST_NAME"),
        ("suffix", "SUFFIX"),
        ("birthdate", "BIRTH_DATE"),
        ("age", "AGE"),
        ("sex", "SEX"),
        ("street1", "STREET_1"),
        ("street2", "STREET_2"),
        ("city", "CITY"),
        ("stateid", "STATE_ID"),
        ("state", "STATE"),
        ("zip", "ZIP"),
        ("zip5", "ZIP5"),
        ("county", "COUNTY"),
        ("country", "COUNTRY"),
        ("phone", "PHONE"),
        ("homephone", "HOME_PHONE"),
        ("cellphone", "CELL_PHONE"),
        ("workphone", "WORK_PHONE"),
        ("donotcall", "DO_NOT_CALL"),
        ("callerid", "CALLER_ID"),
        ("email", "EMAIL"),
        ("dynamiclead", "DYNAMIC_LEAD"),
        ("prospectid", "PROSPECT_ID"),
        ("extpartnerid", "EXT_PARTNER_ID"),
        ("channelid", "CHANNEL_ID"),
        ("channel", "CHANNEL"),
        ("optsourceid", "OPT_SOURCE_ID"),
        ("sourceid", "SOURCE_ID"),
        ("subsourceid", "SUB_SOURCE_ID"),
        ("sourceofreferral", "SOURCE_OF_REFERRAL"),
        ("division", "DIVISION"),
        ("leadsubsource", "LEAD_SUB_SOURCE"),
        ("leadsubsourceid", "LEAD_SUB_SOURCE_ID"),
        ("lender", "LENDER"),
        ("loanamount", "LOAN_AMOUNT"),
        ("loandate", "LOAN_DATE"),
        ("diabetes", "DIABETES"),
        ("healthproblems", "HEALTH_PROBLEMS"),
        ("heartproblems", "HEART_PROBLEMS"),
        ("height", "HEIGHT"),
        ("highbpchol", "HIGH_BP_CHOL"),
        ("isinsured", "IS_INSURED"),
        ("smoker", "SMOKER"),
        ("occupation", "OCCUPATION"),
        ("spouse", "SPOUSE"),
        ("coborrowerage", "COBORROWER_AGE"),
        ("coborrowerbirthdate", "COBORROWER_BIRTH_DATE"),
        ("coborrowerheight", "COBORROWER_HEIGHT"),
        ("coborroweronmortgage", "COBORROWER_ON_MORTGAGE"),
        ("coborrowername", "COBORROWER_NAME"),
        ("coborrowerrelation", "COBORROWER_RELATION"),
        ("coborrowersex", "COBORROWER_SEX"),
        ("coborrowersmoker", "COBORROWER_SMOKER"),
        ("coborrowerweight", "COBORROWER_WEIGHT"),
        ("coborroweroccupation", "COBORROWER_OCCUPATION"),
        ("datasource", "DATA_SOURCE"),
        ("leadoriginurl", "LEAD_ORIGIN_URL"),
        ("mailingid", "MAILING_ID"),
        ("suspectcampaignid", "SUSPECT_CAMPAIGN_ID"),
        ("consumerdebt", "CONSUMER_DEBT"),
        ("mortgagedebt", "MORTGAGE_DEBT"),
        ("utmcampaign", "UTM_CAMPAIGN"),
        ("utmmedium", "UTM_MEDIUM"),
        ("utmsource", "UTM_SOURCE"),
        ("referringurl", "REFERRING_URL"),
        ("pcspoliciesid", "PCS_POLICIES_ID"),
        ("createdate", "CREATE_DATE"),
        ("modifydate", "MODIFY_DATE"),
        ("sourcetable", "SOURCE_TABLE"),
        ("isdeletedsource", "IS_DELETED_SOURCE"),
        ("expdate", "EXP_DATE"),
        ("sourcetype", "SOURCE_TYPE"),
        ("sourcetypeid", "SOURCE_TYPE_ID"),
        ("producttype", "PRODUCT_TYPE"),
        ("leadattributes", "LEAD_ATTRIBUTES"),
        ("custodytargetaudience", "CUSTODY_TARGET_AUDIENCE"),
        ("source", "SOURCE"),
        ("producttypeid", "PRODUCT_TYPE_ID"),
        ("leadsourceid", "LEAD_SOURCE_ID"),
        ("originsystemid", "ORIGIN_SYSTEM_ID"),
        ("originsystem", "ORIGIN_SYSTEM"),
        ("originsystem_orig", "ORIGIN_SYSTEM_ORIG"),
        ("leadingestionmethod", "LEAD_INGESTION_METHOD"),
    ],
    "lead_xref": [
        ("leadxrefguid", "LEAD_XREF_GUID"),
        ("legacyleadid", "LEGACY_LEAD_ID"),
        ("leadcode", "LEAD_CODE"),
        ("leadlevelid", "LEAD_LEVEL_ID"),
        ("leadlevel", "LEAD_LEVEL"),
        ("datasourceid", "DATA_SOURCE_ID"),
        ("leveldate", "LEVEL_DATE"),
        ("createdate", "CREATE_DATE"),
        ("modifydate", "MODIFY_DATE"),
        ("availableforpurchaseind", "AVAILABLE_FOR_PURCHASE_IND"),
        ("isdeletedsource", "IS_DELETED_SOURCE"),
        ("leadlevelalias", "LEAD_LEVEL_ALIAS"),
    ],
    "lead_assignment": [
        ("leadassignmentguid", "LEAD_ASSIGNMENT_GUID"),
        ("leadxrefguid", "LEAD_XREF_GUID"),
        ("agentcode", "AGENT_CODE"),
        ("purchasedate", "PURCHASE_DATE"),
        ("purchaseprice", "PURCHASE_PRICE"),
        ("assigndate", "ASSIGN_DATE"),
        ("inactiveind", "INACTIVE_IND"),
        ("status", "STATUS"),
        ("agentextuid", "AGENT_EXTUID"),
        ("allocateind", "ALLOCATE_IND"),
        ("comments", "COMMENTS"),
        ("sfgdirectagentid", "SFG_DIRECT_AGENT_ID"),
        ("baseshopowneragentid", "BASE_SHOP_OWNER_AGENT_ID"),
        ("totaluplineagentcodes", "TOTAL_UPLINE_AGENT_CODES"),
        ("unpaidind", "UNPAID_IND"),
        ("appcount", "APP_COUNT"),
        ("appapv", "APP_APV"),
        ("actualappcount", "ACTUAL_APP_COUNT"),
        ("actualapv", "ACTUAL_APV"),
        ("createdate", "CREATE_DATE"),
        ("modifydate", "MODIFY_DATE"),
        ("sourcetable", "SOURCE_TABLE"),
        ("metadata", "METADATA"),
        ("statusdate", "STATUS_DATE"),
        ("isdeletedsource", "IS_DELETED_SOURCE"),
        ("ordernumber", "ORDER_NUMBER"),
        ("leadstatusid", "LEAD_STATUS_ID"),
        ("leadstatus", "LEAD_STATUS"),
        ("hqpurchaseamount", "HQ_PURCHASE_AMOUNT"),
        ("leadordersystemid", "LEAD_ORDER_SYSTEM_ID"),
        ("leadordersystem", "LEAD_ORDER_SYSTEM"),
        ("ordersystemid", "ORDER_SYSTEM_ID"),
        ("ordersystem", "ORDER_SYSTEM"),
        ("ordersystemorig", "ORDER_SYSTEM_ORIG"),
        ("exclusivityenddate", "EXCLUSIVITY_END_DATE"),
    ],
}

# Build mapping dictionaries from tuple lists
column_mappings: Dict[str, Dict[str, str]] = {
    tbl: {src: tgt for src, tgt in mapping}
    for tbl, mapping in column_mapping_defs.items()
}

# Define Boolean-like String Columns
boolean_string_columns = {
    "IS_DELETED_SOURCE",
}


class IngestPipeline:
    """Pipeline for ingesting Lead Custody Repository data."""

    def __init__(
        self,
        spark: SparkSession = spark,
        dbutils=dbutils,
        timezone: str = TIMEZONE,
        raw_base_path: str = RAW_BASE_PATH,
        metadata_base_path: str = METADATA_BASE_PATH,
        sf_config_stg: Dict[str, str] = sf_config_stg,
        tables: List[str] = tables,
        table_processing_config: Dict[str, bool] = table_processing_config,
        table_schemas: Dict[str, StructType] = table_schemas,
        column_mappings: Dict[str, Dict[str, str]] = column_mappings,
        json_columns: Dict[str, List[str]] = json_columns,
    ) -> None:
        self.spark = spark
        self.dbutils = dbutils
        self.TIMEZONE = timezone
        self.RAW_BASE_PATH = raw_base_path
        self.METADATA_BASE_PATH = metadata_base_path
        self.sf_config_stg = sf_config_stg
        self.tables = tables
        self.table_processing_config = table_processing_config
        self.table_schemas = table_schemas
        self.column_mappings = column_mappings
        self.json_columns = json_columns
        self.boolean_string_columns = boolean_string_columns

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

    def validate_dataframe(
        self, df: DataFrame, target_schema: StructType, check_types: bool = True
    ) -> None:
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

    def get_etl_last_update_date(self, table_name: str) -> Optional[str]:
        """Read the watermark for a table as a string timestamp."""
        path = f"{self.METADATA_BASE_PATH}/etl_last_update_{table_name}.txt"
        try:
            df = self.spark.read.text(path)
            return df.first()[0].strip()
        except Exception as ex:  # pragma: no cover - watermark may not exist
            logger.info(f"No watermark found for {table_name}: {ex}")
            return None


    def update_etl_last_update_date(self, table_name: str, new_value: str) -> None:
        """Write the watermark for a table as a string timestamp."""
        path = f"{self.METADATA_BASE_PATH}/etl_last_update_{table_name}.txt"
        try:
            self.spark.createDataFrame([(new_value,)], ["watermark"]).coalesce(1).write.mode("overwrite").text(path)
            logger.info(f"Watermark for {table_name} updated to {new_value}")
        except Exception as ex:
            logger.error(f"Could not update watermark for {table_name}: {ex}")

    def snowflake_table_exists(self, table_name: str) -> bool:
        """Check if a table exists in Snowflake."""
        query = (
            f"SELECT 1 FROM information_schema.tables WHERE table_schema = '{self.sf_config_stg['sfSchema']}'"
            f" AND table_name = '{table_name.upper()}'"
        )
        try:
            df = (
                self.spark.read.format("net.snowflake.spark.snowflake")
                .options(**self.sf_config_stg)
                .option("query", query)
                .load()
            )
            # Efficient table existence check: only fetch up to 1 row
            return len(df.take(1)) > 0
        except Exception as e:
            logger.error(f"Failed to check table existence: {e}")
            return False

    def create_checkpoint(self, table_name: str) -> None:
        """Create a checkpoint file after table processing."""
        path = f"{self.METADATA_BASE_PATH}/checkpoint_{table_name}.txt"
        self.spark.createDataFrame([(datetime.now().isoformat(),)], ["ts"]).coalesce(1).write.mode("overwrite").text(path)

    def clean_invalid_timestamps(self, df: DataFrame) -> DataFrame:
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

    def transform_column(self, df: DataFrame, col_name: str, col_type, table_name: str) -> DataFrame:
        """
        Transforms/cleans a single column to match the target data type,
        with special handling for JSON columns, etc.
        """
        # Handle JSON columns
        if table_name in self.json_columns and col_name in self.json_columns[table_name]:
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

        # Boolean strings (preserve "TRUE"/"FALSE" as string)
        elif isinstance(col_type, StringType) and col_name in self.boolean_string_columns:
            return df.withColumn(
                col_name,
                when(lower(col(col_name).cast("string")).isin("true", "1", "yes", "t"), lit("TRUE"))
                .when(lower(col(col_name).cast("string")).isin("false", "0", "no", "f"), lit("FALSE"))
                .when(col(col_name).isNull(), lit(None))
                .otherwise(col(col_name).cast(StringType()))
            )

        # Fallback to String for all other columns (including MORTGAGE_DEBT)
        else:
            return df.withColumn(col_name, col(col_name).cast(StringType()))
    

    def load_raw_data(self, table_name: str) -> DataFrame:
        """
        Loads raw data for the given table from Delta storage.
        IMPORTANT: Ensure path matches the sync script's location so data is not duplicated.
        """
        raw_table_name: str = table_name.replace("_", "")
        # This path is now corrected (removed the "public." prefix) to match the sync script
        raw_dataset_path: str = f"{self.RAW_BASE_PATH}/{raw_table_name}"

        if table_name == "lead_assignment":
            logger.info(f"Loading {table_name} with special JSON handling")
            df = (
                self.spark.read.format("delta")
                .option("header", "true")
                .option("inferSchema", "false")
                .option("multiLine", "true")
                .option("mode", "PERMISSIVE")
                .load(raw_dataset_path)
            )
            return df
        else:
            return (
                self.spark.read.format("delta")
                .option("header", "true")
                .option("inferSchema", "false")
                .load(raw_dataset_path)
            )

    def rename_and_add_columns(self, df: DataFrame, table_name: str) -> DataFrame:
        """
        Renames columns based on column_mappings and adds missing columns as null, matching the target schema.
        """
        df_columns_lower = {column.lower(): column for column in df.columns}

        # Rename columns
        for old_col, new_col in self.column_mappings[table_name].items():
            if old_col.lower() in df_columns_lower:
                original_col = df_columns_lower[old_col.lower()]
                df = df.withColumnRenamed(original_col, new_col)

        # Add missing columns
        target_schema: StructType = self.table_schemas[table_name]
        missing_columns = set(field.name for field in target_schema.fields) - set(df.columns)
        for col_name in missing_columns:
            df = df.withColumn(col_name, lit(None).cast(target_schema[col_name].dataType))

        return df

    def transform_columns(self, df: DataFrame, target_schema: StructType, table_name: str) -> DataFrame:
        """
        Cleans invalid timestamps first, then applies transform_column for each target column.
        """
        df = self.clean_invalid_timestamps(df)
        for field in target_schema.fields:
            df = self.transform_column(df, field.name, field.dataType, table_name)
        return df

    def add_metadata_columns(self, df: DataFrame, target_schema: StructType) -> DataFrame:
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
        self,
        table_name: str,
        write_mode: str,
        historical_load: bool = False
    ) -> None:
        """
        Main workflow for a single table: load raw data, rename columns,
        transform data, handle special logic, validate, and write to Snowflake.

        When ``write_mode`` is ``"append"`` and ``historical_load`` is ``True``, the
        corresponding staging table is truncated before new data is inserted.
        """
        logger.info(f"Starting processing for table: {table_name}")
        try:
            # 1) Load raw data
            raw_df = self.load_raw_data(table_name)
            logger.info(
                f"Loaded raw records from source for table {table_name} (row count skipped for performance)."
            )

            # 2) Rename columns and add missing ones
            raw_df = self.rename_and_add_columns(raw_df, table_name)
            self.validate_dataframe(
                raw_df, self.table_schemas[table_name], check_types=False
            )
            logger.info(
                f"Renamed columns for table {table_name} (row count skipped for performance)."
            )

            # 3) Transform columns
            target_schema = self.table_schemas[table_name]
            raw_df = self.transform_columns(raw_df, target_schema, table_name)
            self.validate_dataframe(raw_df, target_schema)
            logger.info(
                f"Data transformation completed for table {table_name} (row count skipped for performance)."
            )

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
                        when(col(date_col) > current_date, current_date).otherwise(
                            col(date_col)
                        ),
                    )
                raw_df = raw_df.withColumn(
                    "METADATA",
                    when(col("METADATA").isNull(), lit(None)).otherwise(
                        col("METADATA").cast(StringType())
                    ),
                )
                logger.info("Applied lead assignment specific handling")

            # 5) Add metadata columns
            raw_df = self.add_metadata_columns(raw_df, target_schema)

            # 6) Reorder columns to match target schema
            target_columns = [field.name for field in target_schema.fields]
            raw_df = raw_df.select(*target_columns)

            # 7) Final timestamp cleanup
            raw_df = self.clean_invalid_timestamps(raw_df)

            timestamp_cols = [
                field.name
                for field in target_schema.fields
                if isinstance(field.dataType, TimestampType)
            ]
            for ts_col in timestamp_cols:
                raw_df = raw_df.withColumn(
                    ts_col,
                    when(
                        col(ts_col).isNull()
                        | regexp_replace(col(ts_col).cast("string"), "[0-9\\-:. ]", "").rlike(".+"),
                        current_timestamp() if ts_col.startswith("ETL_") else lit(None),
                    ).otherwise(col(ts_col)),
                )

            logger.info(
                f"DataFrame finalization completed for table {table_name} (row count skipped for performance)."
            )

            # 9) Write to Snowflake
            if not self.snowflake_table_exists(f"STG_LCR_{table_name.upper()}"):
                logger.error(
                    f"Target table STG_LCR_{table_name.upper()} does not exist in Snowflake"
                )
                return

            if write_mode == "append":
                if historical_load:
                    truncate_options = {
                        **self.sf_config_stg,
                        "dbtable": f"STG_LCR_{table_name.upper()}",
                        "truncate_table": "on",
                    }
                    (
                        self.spark.createDataFrame([], target_schema)
                        .write.format("net.snowflake.spark.snowflake")
                        .options(**truncate_options)
                        .mode("overwrite")
                        .save()
                    )
                    logger.info(
                        f"Table STG_LCR_{table_name.upper()} truncated (historical append)"
                    )

                write_options = {
                    **self.sf_config_stg,
                    "dbtable": f"STG_LCR_{table_name.upper()}",
                    "on_error": "CONTINUE",
                    "column_mapping": "name",
                }
                for attempt in range(3):
                    try:
                        raw_df.write.format("net.snowflake.spark.snowflake").options(**write_options).mode("append").save()
                        break
                    except Exception as w_err:
                        if attempt == 2:
                            raise
                        logger.warning(f"Snowflake write failed, retrying... {w_err}")
                        time.sleep(5)
                logger.info(
                    f"Successfully wrote to Snowflake for table {table_name} (row count skipped for performance)."
                )
                self.create_checkpoint(table_name)

                if historical_load and "ETL_LAST_UPDATE_DATE" in raw_df.columns:
                    max_val = raw_df.agg(spark_max(col("ETL_LAST_UPDATE_DATE"))).collect()[0][0]
                    if max_val:
                        self.update_etl_last_update_date(table_name, str(max_val))

            elif write_mode == "incremental_insert":
                watermark = self.get_etl_last_update_date(table_name)
                if watermark and not historical_load:
                    raw_df_filtered = raw_df.filter(
                        col("ETL_LAST_UPDATE_DATE") > to_timestamp(lit(watermark))
                    )
                else:
                    raw_df_filtered = raw_df

                self.validate_dataframe(raw_df_filtered, target_schema)
                write_options = {
                    **self.sf_config_stg,
                    "dbtable": f"STG_LCR_{table_name.upper()}",
                    "column_mapping": "name",
                    "on_error": "CONTINUE",
                    "truncate": "true",
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

                if "ETL_LAST_UPDATE_DATE" in raw_df_filtered.columns:
                    max_val = raw_df_filtered.agg(spark_max(col("ETL_LAST_UPDATE_DATE"))).collect()[0][0]
                    if max_val:
                        self.update_etl_last_update_date(table_name, str(max_val))

                logger.info(
                    f"Appended records to table STG_LCR_{table_name.upper()} (row count skipped for performance)."
                )
                self.create_checkpoint(table_name)

            else:
                raise ValueError(f"Invalid write mode: {write_mode}")

            logger.info(f"Completed processing for table: {table_name}")
        except Exception as e:
            logger.error(f"Unexpected error processing table {table_name}: {str(e)}")
            logger.error(traceback.format_exc())
            raise

    def run(self, write_mode: str = "append", historical_load: bool = True) -> None:
        """Execute processing for all configured tables."""
        for table in self.tables:
            should_process = self.table_processing_config.get(table, False)
            if should_process:
                self.process_table(table, write_mode, historical_load)
            else:
                logger.info(f"Skipping processing for table: {table} as per configuration.")

        logger.info("ETL process completed successfully.")


def main():
    """
    Main entry point: iterate over tables, process each with chosen write_mode & historical_load options.
    """
    pipeline = IngestPipeline()
    pipeline.run()

if __name__ == "__main__":
    main()
