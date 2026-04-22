import os

from transformation.utils import ( create_spark_session, drop_duplicates, drop_null_values, normalize_numeric_columns, normalize_timestamp, read_json, write_parquet, )


def main():
    # create spark session for the crypto batch pipeline
    spark = create_spark_session(app_name="crypto_batch")

    # choose input and output paths
    input_path = "s3a://raw-data-staging/binance/batch/bitcoin/"
    output_path = "s3a://silver-data-staging/binance/bitcoin/"

    # read the raw local JSON data (in production, read from minio)
    trades = read_json(spark, input_path)

    # drop rows with any null values
    trades = drop_null_values(trades)

    # drop duplicate rows 
    trades = drop_duplicates(trades)

    # convert epoch milliseconds into a standardized event_time string
    trades = normalize_timestamp(
        trades,
        source_col="open_time",
        target_col="event_time",
        epoch_millis=True,
    )

    # convert selected numeric fields and round the primary price fields.
    trades = normalize_numeric_columns(
        trades,
        columns=["open", "high", "low", "close", "volume", "quote_asset_volume"],
        round_cols=["open", "high", "low", "close"],
    )

    # write the transformed silver data to parquet
    write_parquet(trades, output_path)


if __name__ == "__main__":
    main()
