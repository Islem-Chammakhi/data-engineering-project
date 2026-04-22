import os

from transformation.utils import ( create_spark_session, drop_duplicates, drop_null_values, normalize_numeric_columns, normalize_timestamp, read_json, write_parquet, )


def main():
    # create spark session for the crypto stream pipeline
    spark = create_spark_session(app_name="crypto_stream")

    # choose input and output paths
    input_path = os.getenv("CRYPTO_STREAM_INPUT_PATH", "/app/src/data/crypto_stream.json")
    output_path = os.getenv("CRYPTO_STREAM_OUTPUT_PATH", "/app/data/silver/crypto_stream")

    # read the raw local JSON data (in production, read from minio)
    stream_df = read_json(spark, input_path)

    # drop rows with any null values
    stream_df = drop_null_values(stream_df)

    # drop duplicate rows 
    stream_df = drop_duplicates(stream_df, subset=["trade_id"])

    # convert epoch milliseconds into a standardized event_time string
    stream_df = normalize_timestamp(
        stream_df,
        source_col="event_time",
        target_col="event_time",
        epoch_millis=True,
    )

    # convert selected numeric fields and round the trade price.
    stream_df = normalize_numeric_columns(
        stream_df,
        columns=["price", "quantity"],
        round_cols=["price"],
    )

    # write the transformed silver data to parquet
    write_parquet(stream_df, output_path)


if __name__ == "__main__":
    main()
