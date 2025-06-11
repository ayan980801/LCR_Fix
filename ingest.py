# Standard Library Imports
import os
import time
import json
import logging
import traceback
from datetime import datetime
from typing import Dict, List, Optional
# Third Party Imports
import pytz
import dateutil.parser
# PySpark Imports
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import (
    col, current_timestamp, lit, udf, when, lower, coalesce,
    length, regexp_replace, year, to_date, to_timestamp, max as spark_max
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
            StructField("MORTGAGE_DEBT", StringType(), True),
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

from dataclasses import dataclass


@dataclass
class TableConfig:
    name: str
    schema: StructType
    column_mappings: Dict[str, str]
    json_columns: List[str]


class LeadCustodyIngestor:
    def __init__(self):
        self.spark = spark
        self.dbutils = dbutils
        self.timezone = TIMEZONE
        self.raw_base_path = RAW_BASE_PATH
        self.metadata_base_path = METADATA_BASE_PATH
        self.sf_config = sf_config_stg
        self.boolean_string_columns = boolean_string_columns

        self.table_configs = {
            name: TableConfig(
                name=name,
                schema=table_schemas[name],
                column_mappings=column_mappings[name],
                json_columns=json_columns.get(name, []),
            )
            for name in tables
        }

        self.parse_timestamp_udf = udf(
            LeadCustodyIngestor.enhanced_parse_timestamp_udf, TimestampType()
        )
        self.parse_date_udf = udf(
            LeadCustodyIngestor.enhanced_parse_date_udf, DateType()
        )

    @staticmethod
    def enhanced_parse_timestamp_udf(date_str):
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

    @staticmethod
    def enhanced_parse_date_udf(date_str):
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

    def _validate_dataframe(self, df: DataFrame, schema: StructType, check_types: bool = True) -> None:
        logger.info("Validating DataFrame against target schema")
        errors = []
        for field in schema.fields:
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
            raise ValueError("DataFrame validation failed with errors:\n" + "\n".join(errors))
        logger.info("DataFrame validation completed successfully")

    def _get_watermark(self, table_name: str) -> Optional[str]:
        path = f"{self.metadata_base_path}/etl_last_update_{table_name}.txt"
        try:
            df = self.spark.read.text(path)
            return df.first()[0].strip()
        except Exception as ex:
            logger.info(f"No watermark found for {table_name}: {ex}")
            return None

    def _update_watermark(self, table_name: str, new_value: str) -> None:
        path = f"{self.metadata_base_path}/etl_last_update_{table_name}.txt"
        try:
            self.spark.createDataFrame([(new_value,)], ["watermark"]).coalesce(1).write.mode("overwrite").text(path)
            logger.info(f"Watermark for {table_name} updated to {new_value}")
        except Exception as ex:
            logger.error(f"Could not update watermark for {table_name}: {ex}")

    def _snowflake_table_exists(self, table_name: str) -> bool:
        query = (
            f"SELECT 1 FROM information_schema.tables WHERE table_schema = '{self.sf_config['sfSchema']}'"
            f" AND table_name = '{table_name.upper()}'"
        )
        try:
            df = self.spark.read.format("net.snowflake.spark.snowflake").options(**self.sf_config).option("query", query).load()
            return len(df.take(1)) > 0
        except Exception as e:
            logger.error(f"Failed to check table existence: {e}")
            return False

    def _create_checkpoint(self, table_name: str) -> None:
        path = f"{self.metadata_base_path}/checkpoint_{table_name}.txt"
        self.spark.createDataFrame([(datetime.now().isoformat(),)], ["ts"]).coalesce(1).write.mode("overwrite").text(path)

    def _clean_invalid_timestamps(self, df: DataFrame) -> DataFrame:
        timestamp_cols = [
            field.name for field in df.schema.fields if isinstance(field.dataType, TimestampType)
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
                df = df.withColumn(ts_col, coalesce(col(ts_col), current_timestamp()))
        return df

    def _load_raw_data(self, config: TableConfig) -> DataFrame:
        raw_table_name = config.name.replace("_", "")
        path = f"{self.raw_base_path}/{raw_table_name}"
        if config.name == "lead_assignment":
            return (
                self.spark.read.format("delta")
                .option("header", "true")
                .option("inferSchema", "false")
                .option("multiLine", "true")
                .option("mode", "PERMISSIVE")
                .load(path)
            )
        return (
            self.spark.read.format("delta")
            .option("header", "true")
            .option("inferSchema", "false")
            .load(path)
        )

    def _rename_and_add_columns(self, df: DataFrame, config: TableConfig) -> DataFrame:
        df_columns_lower = {c.lower(): c for c in df.columns}
        for old_col, new_col in config.column_mappings.items():
            if old_col.lower() in df_columns_lower:
                df = df.withColumnRenamed(df_columns_lower[old_col.lower()], new_col)
        missing = set(f.name for f in config.schema.fields) - set(df.columns)
        for col_name in missing:
            df = df.withColumn(col_name, lit(None).cast(config.schema[col_name].dataType))
        return df

    def _transform_columns(self, df: DataFrame, config: TableConfig) -> DataFrame:
        df = self._clean_invalid_timestamps(df)
        for field in config.schema.fields:
            col_name = field.name
            col_type = field.dataType
            if col_name in config.json_columns:
                df = df.withColumn(col_name, validate_json_udf(col(col_name).cast(StringType())))
                continue
            if isinstance(col_type, TimestampType):
                df = df.withColumn(col_name, self.parse_timestamp_udf(col(col_name)))
            elif isinstance(col_type, DateType):
                df = df.withColumn(col_name, self.parse_date_udf(col(col_name)))
            elif isinstance(col_type, DecimalType):
                precision, scale = col_type.precision, col_type.scale
                df = df.withColumn(col_name, col(col_name).cast(DecimalType(precision, scale)))
            elif isinstance(col_type, DoubleType):
                df = df.withColumn(col_name, col(col_name).cast(DoubleType()))
            elif isinstance(col_type, BooleanType):
                df = df.withColumn(
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
            elif isinstance(col_type, StringType) and col_name in self.boolean_string_columns:
                df = df.withColumn(
                    col_name,
                    when(lower(col(col_name).cast("string")).isin("true", "1", "yes", "t"), lit("TRUE"))
                    .when(lower(col(col_name).cast("string")).isin("false", "0", "no", "f"), lit("FALSE"))
                    .when(col(col_name).isNull(), lit(None))
                    .otherwise(col(col_name).cast(StringType()))
                )
            else:
                df = df.withColumn(col_name, col(col_name).cast(StringType()))
        return df

    def _add_metadata_columns(self, df: DataFrame, schema: StructType) -> DataFrame:
        etl_timestamp = current_timestamp()
        defaults = {
            "ETL_CREATED_DATE": etl_timestamp,
            "ETL_LAST_UPDATE_DATE": etl_timestamp,
            "CREATED_BY": lit("ETL_PROCESS"),
            "TO_PROCESS": lit(True),
            "EDW_EXTERNAL_SOURCE_SYSTEM": lit("LeadCustodyRepository"),
        }
        for col_name, default in defaults.items():
            df = df.withColumn(col_name, default.cast(schema[col_name].dataType))
        return df

    def _write_with_retry(self, df: DataFrame, options: Dict[str, str], mode: str = "append", attempts: int = 3) -> None:
        for attempt in range(attempts):
            try:
                df.write.format("net.snowflake.spark.snowflake").options(**options).mode(mode).save()
                return
            except Exception as w_err:
                if attempt == attempts - 1:
                    raise
                logger.warning(f"Snowflake write failed, retrying... {w_err}")
                time.sleep(5)

    def process_table(self, config: TableConfig, write_mode: str, historical_load: bool = False) -> None:
        logger.info(f"Starting processing for table: {config.name}")
        try:
            raw_df = self._load_raw_data(config)
            raw_df = self._rename_and_add_columns(raw_df, config)
            self._validate_dataframe(raw_df, config.schema, check_types=False)
            raw_df = self._transform_columns(raw_df, config)
            self._validate_dataframe(raw_df, config.schema)

            if config.name == "lead_assignment":
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
                        when(col(date_col) > current_date, current_date).otherwise(col(date_col)),
                    )
                raw_df = raw_df.withColumn(
                    "METADATA",
                    when(col("METADATA").isNull(), lit(None)).otherwise(col("METADATA").cast(StringType())),
                )

            raw_df = self._add_metadata_columns(raw_df, config.schema)
            raw_df = raw_df.select(*[f.name for f in config.schema.fields])
            raw_df = self._clean_invalid_timestamps(raw_df)

            timestamp_cols = [
                f.name for f in config.schema.fields if isinstance(f.dataType, TimestampType)
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

            if not self._snowflake_table_exists(f"STG_LCR_{config.name.upper()}"):
                logger.error(f"Target table STG_LCR_{config.name.upper()} does not exist in Snowflake")
                return

            if write_mode == "append":
                if historical_load:
                    truncate_options = {**self.sf_config, "dbtable": f"STG_LCR_{config.name.upper()}", "truncate_table": "on"}
                    self.spark.createDataFrame([], config.schema).write.format("net.snowflake.spark.snowflake").options(**truncate_options).mode("overwrite").save()
                write_options = {**self.sf_config, "dbtable": f"STG_LCR_{config.name.upper()}", "on_error": "CONTINUE", "column_mapping": "name"}
                self._write_with_retry(raw_df, write_options)
                self._create_checkpoint(config.name)
                if historical_load and "ETL_LAST_UPDATE_DATE" in raw_df.columns:
                    max_val = raw_df.agg(spark_max(col("ETL_LAST_UPDATE_DATE"))).collect()[0][0]
                    if max_val:
                        self._update_watermark(config.name, str(max_val))
            elif write_mode == "incremental_insert":
                watermark = self._get_watermark(config.name)
                if watermark and not historical_load:
                    raw_df = raw_df.filter(col("ETL_LAST_UPDATE_DATE") > to_timestamp(lit(watermark)))
                self._validate_dataframe(raw_df, config.schema)
                write_options = {
                    **self.sf_config,
                    "dbtable": f"STG_LCR_{config.name.upper()}",
                    "column_mapping": "name",
                    "on_error": "CONTINUE",
                    "truncate": "true",
                }
                self._write_with_retry(raw_df, write_options)
                if "ETL_LAST_UPDATE_DATE" in raw_df.columns:
                    max_val = raw_df.agg(spark_max(col("ETL_LAST_UPDATE_DATE"))).collect()[0][0]
                    if max_val:
                        self._update_watermark(config.name, str(max_val))
                self._create_checkpoint(config.name)
            else:
                raise ValueError(f"Invalid write mode: {write_mode}")
            logger.info(f"Completed processing for table: {config.name}")
        except Exception as e:
            logger.error(f"Unexpected error processing table {config.name}: {str(e)}")
            logger.error(traceback.format_exc())
            raise

    def run(self, write_mode: str = "append", historical_load: bool = False) -> None:
        for name, config in self.table_configs.items():
            if table_processing_config.get(name, False):
                self.process_table(config, write_mode, historical_load)
            else:
                logger.info(f"Skipping processing for table: {name} as per configuration.")
        logger.info("ETL process completed successfully.")


if __name__ == "__main__":
    LeadCustodyIngestor().run(write_mode="append", historical_load=True)

