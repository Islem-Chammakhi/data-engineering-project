from pyspark.sql import SparkSession
from pyspark.sql.functions import col, date_format, from_unixtime, round as spark_round, to_timestamp, trim, when


# 1. create spark session
def create_spark_session(app_name: str, master: str = "spark://spark-master:7077") -> SparkSession: 
    return SparkSession.builder.master(master).appName(app_name).config("spark.sql.session.timeZone", "UTC").getOrCreate()


# 2. convert  timestamp to a standardized string event_time.
def normalize_timestamp( df, source_col: str, target_col: str = "event_time", input_fmt: str = None, output_fmt: str = "yyyy-MM-dd'T'HH:mm:ss", epoch_millis: bool = False, ):
    if source_col not in df.columns:
        return df
    if epoch_millis:
        parsed = from_unixtime((col(source_col) / 1000.0).cast("double"), output_fmt)
    elif input_fmt:
        parsed = date_format(to_timestamp(col(source_col), input_fmt), output_fmt)
    else:
        parsed = date_format(to_timestamp(col(source_col)), output_fmt)
    return df.withColumn(target_col, parsed)


# convert a single field to numeric if it is stored as text,
def normalize_numeric_column(df, field_name: str, float_scale: int = 3, round_value: bool = False):
    if field_name not in df.columns:
        return df

    field_type = df.schema[field_name].dataType.simpleString()
    if field_type == "string":
        numeric_column = trim(col(field_name))
        decimal_cast = numeric_column.cast("double")
        parsed_column = when(
            numeric_column.rlike(r"^-?\d+$"),
            numeric_column.cast("long"),
        ).when(
            numeric_column.rlike(r"^-?\d+\.\d+$"),
            spark_round(decimal_cast, float_scale) if round_value else decimal_cast,
        ).otherwise(col(field_name))
        return df.withColumn(field_name, parsed_column)

    if field_type in {"double", "float", "decimal"} and round_value:
        return df.withColumn(field_name, spark_round(col(field_name).cast("double"), float_scale))

    return df


# 3. converts strings to numeric (long or double) and optional rounding (arrondi optionnel)
def normalize_numeric_columns( df, columns: list[str], float_scale: int = 2, round_cols: list[str] = None, ):
    if not columns:
        return df
    round_cols = set(round_cols or [])
    for field_name in columns:
        df = normalize_numeric_column(
            df,
            field_name,
            float_scale=float_scale,
            round_value=field_name in round_cols,
        )
    return df


# 4. drop rows with null values (or a specific subset of columns with nulls)
def drop_null_values(df, subset: list[str] = None, how: str = "any"):
    if subset is None:
        return df.na.drop(how=how)
    return df.na.drop(how=how, subset=subset)


# 5. drop duplicate rows based on a specific subset of columns or the full row if none are provided.
def drop_duplicates(df, subset: list[str] = None):
    if subset is None:
        return df.dropDuplicates()
    return df.dropDuplicates(subset)


# read a JSON file into a Spark DataFrame
def read_json(spark, path: str):
    return spark.read.option("multiline", "true").json(path)


# write a Spark DataFrame to parquet (overwrite mode by default)
def write_parquet(df, path: str, mode: str = "overwrite"):
    df.write.mode(mode).parquet(path)
