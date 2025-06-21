import logging
import time
import traceback
from datetime import datetime

from pyspark.sql import DataFrame
from pyspark.sql.functions import col, current_timestamp, lit, regexp_replace, when
from pyspark.sql.types import StringType, TimestampType

from config_manager import ConfigManager
from schema_manager import SchemaManager
from etl_utils import (
    add_hash_key,
    add_metadata_columns,
    clean_invalid_timestamps,
    rename_and_add_columns,
    transform_columns,
    validate_dataframe,
)


class TableProcessor:
    def __init__(self, table_name: str, write_mode: str, historical_load: bool, config: ConfigManager, schema: SchemaManager):
        self.table_name = table_name
        self.write_mode = write_mode
        self.historical_load = historical_load
        self.config = config
        self.schema = schema

    def load_raw_data(self) -> DataFrame:
        spark = self.config.get_spark()
        raw_table_name = self.table_name.replace("_", "")
        raw_dataset_path = f"{self.config.RAW_BASE_PATH}/{raw_table_name}"
        if self.table_name == "lead_assignment":
            return (
                spark.read.format("delta")
                .option("header", "true")
                .option("inferSchema", "false")
                .option("multiLine", "true")
                .option("mode", "PERMISSIVE")
                .load(raw_dataset_path)
            )
        return (
            spark.read.format("delta")
            .option("header", "true")
            .option("inferSchema", "false")
            .load(raw_dataset_path)
        )

    def snowflake_table_exists(self) -> bool:
        spark = self.config.get_spark()
        sf_config_stg = self.config.get_sf_config_stg()
        query = (
            f"SELECT 1 FROM information_schema.tables WHERE table_schema = '{sf_config_stg['sfSchema']}'"
            f" AND table_name = 'STG_LCR_{self.table_name.upper()}'"
        )
        try:
            df = (
                spark.read.format("net.snowflake.spark.snowflake")
                .options(**sf_config_stg)
                .option("query", query)
                .load()
            )
            return len(df.take(1)) > 0
        except Exception as e:
            logging.error(f"Failed to check table existence: {e}")
            return False

    def create_checkpoint(self) -> None:
        spark = self.config.get_spark()
        path = f"{self.config.METADATA_BASE_PATH}/checkpoint_{self.table_name}.txt"
        spark.createDataFrame([(datetime.now().isoformat(),)], ["ts"]).coalesce(1).write.mode("overwrite").text(path)

    def process(self) -> None:
        spark = self.config.get_spark()
        sf_config_stg = self.config.get_sf_config_stg()
        try:
            raw_df = self.load_raw_data()
            logging.info(f"Loaded data for {self.table_name}")
            raw_df = rename_and_add_columns(raw_df, self.table_name)
            validate_dataframe(raw_df, self.schema.get_schema(self.table_name), check_types=False)
            target_schema = self.schema.get_schema(self.table_name)
            raw_df = transform_columns(raw_df, target_schema, self.table_name)
            validate_dataframe(raw_df, target_schema)
            if self.table_name == "lead_assignment":
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
                logging.info("Applied lead assignment specific handling")
            metadata_cols = [
                "ETL_CREATED_DATE",
                "ETL_LAST_UPDATE_DATE",
                "CREATED_BY",
                "TO_PROCESS",
                "EDW_EXTERNAL_SOURCE_SYSTEM",
            ]
            key_col_mapping = {
                "lead": "STG_LCR_LEAD_KEY",
                "lead_assignment": "STG_LCR_LEAD_ASSIGNMENT_KEY",
                "lead_xref": "STG_LCR_LEAD_XREF_KEY",
            }
            key_col = key_col_mapping[self.table_name]
            raw_df = add_hash_key(raw_df, metadata_cols + [key_col], key_col)
            raw_df = add_metadata_columns(raw_df, target_schema)
            target_columns = [field.name for field in target_schema.fields]
            raw_df = raw_df.select(*target_columns)
            raw_df = clean_invalid_timestamps(raw_df)
            timestamp_cols = [
                field.name for field in target_schema.fields if isinstance(field.dataType, TimestampType)
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
            logging.info(f"DataFrame finalization completed for table {self.table_name}")
            if not self.snowflake_table_exists():
                logging.error(
                    f"Target table STG_LCR_{self.table_name.upper()} does not exist in Snowflake"
                )
                return
            if self.write_mode == "append":
                if self.historical_load:
                    truncate_options = {
                        **sf_config_stg,
                        "dbtable": f"STG_LCR_{self.table_name.upper()}",
                        "truncate_table": "on",
                    }
                    (
                        spark.createDataFrame([], target_schema)
                        .write.format("net.snowflake.spark.snowflake")
                        .options(**truncate_options)
                        .mode("overwrite")
                        .save()
                    )
                    logging.info(
                        f"Table STG_LCR_{self.table_name.upper()} truncated (historical append)"
                    )
                write_options = {
                    **sf_config_stg,
                    "dbtable": f"STG_LCR_{self.table_name.upper()}",
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
                        logging.warning(f"Snowflake write failed, retrying... {w_err}")
                        time.sleep(5)
                logging.info(f"Successfully wrote to Snowflake for table {self.table_name}")
                self.create_checkpoint()
            elif self.write_mode == "incremental_insert":
                target = (
                    spark.read.format("net.snowflake.spark.snowflake")
                    .options(**sf_config_stg)
                    .option("dbtable", f"STG_LCR_{self.table_name.upper()}")
                    .load()
                )
                key_col_mapping = {
                    "lead": "STG_LCR_LEAD_KEY",
                    "lead_assignment": "STG_LCR_LEAD_ASSIGNMENT_KEY",
                    "lead_xref": "STG_LCR_LEAD_XREF_KEY",
                }
                key_col = key_col_mapping[self.table_name]
                raw_df_filtered = raw_df.join(target.select(key_col), on=key_col, how="left_anti")
                validate_dataframe(raw_df_filtered, target_schema)
                write_options = {
                    **sf_config_stg,
                    "dbtable": f"STG_LCR_{self.table_name.upper()}",
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
                        logging.warning(f"Snowflake write failed, retrying... {w_err}")
                        time.sleep(5)
                logging.info(f"Appended records to table STG_LCR_{self.table_name.upper()}")
                self.create_checkpoint()
            else:
                raise ValueError(f"Invalid write mode: {self.write_mode}")
            logging.info(f"Completed processing for table: {self.table_name}")
        except Exception as e:
            logging.error(f"Unexpected error processing table {self.table_name}: {str(e)}")
            logging.error(traceback.format_exc())
            raise
