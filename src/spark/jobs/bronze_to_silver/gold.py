import os

from src.spark.utils import ( create_spark_session, drop_duplicates, drop_null_values, normalize_numeric_columns, normalize_timestamp, read_json, write_parquet, )


def main():
    # create spark session for the gold pipeline
    spark = create_spark_session(app_name="gold")

    # choose input and output paths
    input_path = os.getenv("GOLD_INPUT_PATH", "/app/src/data/oilgold.json")
    output_path = os.getenv("GOLD_OUTPUT_PATH", "/app/data/silver/gold")

    # read the raw local JSON data (in production, read from minio)
    gold_df = read_json(spark, input_path)

    # drop rows with any null values
    gold_df = drop_null_values(gold_df)

    # drop duplicate rows 
    gold_df = drop_duplicates(gold_df)

    # normalize the source timestamp into a standardized event_time field
    gold_df = normalize_timestamp(
        gold_df,
        source_col="Datetime",
        target_col="event_time",
        input_fmt="yyyy-MM-dd HH:mm:ssXXX",
    )

    # normalize numeric fields and round the price fields to 3 decimal places.
    gold_df = normalize_numeric_columns(
        gold_df,
        columns=["Open", "High", "Low", "Close", "Volume", "Dividends", "Stock Splits"],
        float_scale=3,
        round_cols=["Open", "High", "Low", "Close"],
    )

    # write the transformed silver data to parquet
    write_parquet(gold_df, output_path)


if __name__ == "__main__":
    main()
