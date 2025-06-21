import json
import hashlib
from datetime import datetime
from typing import List, Optional

import pytz
import dateutil.parser
from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    col,
    concat_ws,
    coalesce,
    current_timestamp,
    length,
    lit,
    lower,
    regexp_replace,
    to_date,
    to_timestamp,
    trim,
    udf,
    when,
    year,
)
from pyspark.sql.types import (
    BooleanType,
    DateType,
    DecimalType,
    DoubleType,
    StringType,
    StructType,
    TimestampType,
)

from schema_manager import SchemaManager


TIMEZONE = "America/New_York"
NUMERIC_REGEX = r"^-?\d+(\.\d+)?$"


@udf(TimestampType())
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


@udf(DateType())
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


@udf(StringType())
def validate_json_udf(val: str) -> Optional[str]:
    if val is None:
        return None
    try:
        json.loads(val)
        return val
    except Exception:
        return None


@udf(StringType())
def sha3_512_udf(val: str) -> Optional[str]:
    if val is None:
        return None
    return hashlib.sha3_512(val.encode("utf-8")).hexdigest()


def clean_invalid_timestamps(df: DataFrame) -> DataFrame:
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
            ).otherwise(col(ts_col)),
        )
        if ts_col.startswith("ETL_"):
            df = df.withColumn(ts_col, coalesce(col(ts_col), current_timestamp()))
    return df


def validate_dataframe(df: DataFrame, target_schema: StructType, check_types: bool = True) -> None:
    errors = []
    for field in target_schema.fields:
        col_name = field.name
        col_type = field.dataType
        if col_name not in df.columns:
            errors.append(f"Column {col_name} is missing from the DataFrame")
        elif check_types and not isinstance(df.schema[col_name].dataType, type(col_type)):
            errors.append(
                f"Column {col_name} has type {df.schema[col_name].dataType}, but should be {col_type}"
            )
    if errors:
        raise ValueError("DataFrame validation failed with errors:\n" + "\n".join(errors))


def transform_column(df: DataFrame, col_name: str, col_type, table_name: str) -> DataFrame:
    json_columns = SchemaManager.get_json_columns(table_name)
    if table_name in SchemaManager.JSON_COLUMNS and col_name in json_columns:
        return df.withColumn(col_name, validate_json_udf(col(col_name).cast(StringType())))
    if isinstance(col_type, TimestampType):
        df = df.withColumn(
            col_name,
            when(
                col(col_name).cast("string").rlike("^[A-Za-z]{1,3}$")
                | (length(col(col_name).cast("string")) <= 3)
                | (~col(col_name).cast("string").rlike(".*\\d+.*")),
                lit(None),
            ).otherwise(col(col_name)),
        )
        return df.withColumn(
            col_name,
            when(col(col_name).isNull(), None).otherwise(
                coalesce(to_timestamp(col(col_name)), enhanced_parse_timestamp_udf(col(col_name)))
            ),
        )
    if isinstance(col_type, DateType):
        return df.withColumn(
            col_name,
            when(col(col_name).isNull(), None)
            .when(col(col_name).rlike(r"^\d{4}-\d{2}-\d{2}$"), to_date(col(col_name)))
            .otherwise(enhanced_parse_date_udf(col(col_name))),
        )
    if isinstance(col_type, DecimalType):
        precision, scale = col_type.precision, col_type.scale
        cleaned = trim(regexp_replace(col(col_name), ",", ""))
        return df.withColumn(
            col_name,
            when(
                col(col_name).isNull() | (cleaned == "") | (~cleaned.rlike(NUMERIC_REGEX)),
                None,
            ).otherwise(cleaned.cast(DecimalType(precision, scale))),
        )
    if isinstance(col_type, DoubleType):
        cleaned = trim(regexp_replace(col(col_name), ",", ""))
        return df.withColumn(
            col_name,
            when(
                col(col_name).isNull() | (cleaned == "") | (~cleaned.rlike(NUMERIC_REGEX)),
                None,
            ).otherwise(cleaned.cast(DoubleType())),
        )
    if isinstance(col_type, BooleanType):
        return df.withColumn(
            col_name,
            when(lower(col(col_name)).isin("true", "1", "yes"), lit(True))
            .when(lower(col(col_name)).isin("false", "0", "no"), lit(False))
            .when(col(col_name).isNull(), lit(None))
            .otherwise(
                when(
                    length(col(col_name)) == 1,
                    when(lower(col(col_name)) == "t", lit(True)).when(lower(col(col_name)) == "f", lit(False)).otherwise(lit(None)),
                ).otherwise(lit(None))
            ),
        )
    if isinstance(col_type, StringType) and SchemaManager.is_boolean_string_column(col_name):
        return df.withColumn(
            col_name,
            when(lower(col(col_name).cast("string")).isin("true", "1", "yes", "t"), lit("TRUE"))
            .when(lower(col(col_name).cast("string")).isin("false", "0", "no", "f"), lit("FALSE"))
            .when(col(col_name).isNull(), lit(None))
            .otherwise(col(col_name).cast(StringType())),
        )
    return df.withColumn(col_name, col(col_name).cast(StringType()))


def add_metadata_columns(df: DataFrame, target_schema: StructType) -> DataFrame:
    etl_timestamp = current_timestamp()
    metadata_defaults = {
        "ETL_CREATED_DATE": etl_timestamp,
        "ETL_LAST_UPDATE_DATE": etl_timestamp,
        "CREATED_BY": lit("ETL_PROCESS"),
        "TO_PROCESS": lit(True),
        "EDW_EXTERNAL_SOURCE_SYSTEM": lit("LeadCustodyRepository"),
    }
    for col_name, default_value in metadata_defaults.items():
        df = df.withColumn(col_name, default_value.cast(target_schema[col_name].dataType))
    return df


def rename_and_add_columns(df: DataFrame, table_name: str) -> DataFrame:
    df_columns_lower = {column.lower(): column for column in df.columns}
    column_mappings = SchemaManager.get_column_mapping(table_name)
    for old_col, new_col in column_mappings.items():
        if old_col.lower() in df_columns_lower:
            df = df.withColumnRenamed(df_columns_lower[old_col.lower()], new_col)
    target_schema = SchemaManager.get_schema(table_name)
    missing_columns = set(field.name for field in target_schema.fields) - set(df.columns)
    for col_name in missing_columns:
        df = df.withColumn(col_name, lit(None).cast(target_schema[col_name].dataType))
    return df


def add_hash_key(df: DataFrame, metadata_cols: List[str], key_col: str) -> DataFrame:
    non_meta_cols = [c for c in df.columns if c not in metadata_cols and c != key_col]
    concatenated = concat_ws("||", *[col(c).cast("string") for c in non_meta_cols])
    return df.withColumn(key_col, sha3_512_udf(concatenated))


def transform_columns(df: DataFrame, target_schema: StructType, table_name: str) -> DataFrame:
    df = clean_invalid_timestamps(df)
    for field in target_schema.fields:
        df = transform_column(df, field.name, field.dataType, table_name)
    return df
