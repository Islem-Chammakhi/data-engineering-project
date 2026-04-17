import os

from src.spark.utils import ( create_spark_session, drop_duplicates, drop_null_values, normalize_numeric_columns, normalize_timestamp, read_json, write_parquet, )


def main():
    # create spark session for the oil pipeline
    spark = create_spark_session(app_name="oil")

    # choose input and output paths
    input_path = os.getenv("OIL_INPUT_PATH", "/app/src/data/oilgold.json")
    output_path = os.getenv("OIL_OUTPUT_PATH", "/app/data/silver/oil")

    # read the raw local JSON data (in production, read from minio)
    oil_df = read_json(spark, input_path)

    # drop rows with any null values
    oil_df = drop_null_values(oil_df)

    # drop duplicate rows 
    oil_df = drop_duplicates(oil_df)

    # normalize the source timestamp into a standardized event_time field
    oil_df = normalize_timestamp(
        oil_df,
        source_col="Datetime",
        target_col="event_time",
        input_fmt="yyyy-MM-dd HH:mm:ssXXX",
    )

    # normalize numeric fields and round the price fields to 3 decimal places.
    oil_df = normalize_numeric_columns(
        oil_df,
        columns=["Open", "High", "Low", "Close", "Volume", "Dividends", "Stock Splits"],
        float_scale=3,
        round_cols=["Open", "High", "Low", "Close"],
    )

    # write the transformed silver data to parquet
    write_parquet(oil_df, output_path)


if __name__ == "__main__":
    main()
