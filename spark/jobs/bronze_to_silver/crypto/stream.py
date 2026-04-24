import os

from transformation.utils import ( create_spark_session, drop_duplicates, drop_null_values, normalize_numeric_columns, normalize_timestamp, )

from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, LongType, BooleanType


def main():
    # create spark session for the crypto stream pipeline
    spark = create_spark_session(app_name="crypto_stream")
    spark.sparkContext.setLogLevel("WARN")

    # choose input and output paths (empty for now)
    input_path = ""
    output_path = ""
    checkpoint_path = "/data/checkpoints/crypto_stream"

    # define schema for the incoming kafka json (candle format)
    schema = StructType([
        StructField("symbol", StringType(), True),
        StructField("open_time", LongType(), True),
        StructField("open", StringType(), True),
        StructField("high", StringType(), True),
        StructField("low", StringType(), True),
        StructField("close", StringType(), True),
        StructField("volume", StringType(), True),
        StructField("close_time", LongType(), True),
        StructField("quote_asset_volume", StringType(), True),
        StructField("number_of_trades", LongType(), True),
        StructField("is_final", BooleanType(), True)
    ])

    # read from kafka topic
    stream_df = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", "kafka:9092")
        .option("subscribe", "binance-trades")
        .option("startingOffsets", "latest")
        .load()
    )

    # deserialize json
    stream_df = (
        stream_df.selectExpr("CAST(value AS STRING)")
        .select(from_json(col("value"), schema).alias("data"))
        .select("data.*")
    )

    # drop rows with any null values
    stream_df = drop_null_values(stream_df)

    # drop duplicate rows 
    stream_df = drop_duplicates(stream_df)

    # convert epoch milliseconds into a standardized event_time string
    stream_df = normalize_timestamp(
        stream_df,
        source_col="open_time",
        target_col="event_time",
        epoch_millis=True,
    )

    # convert selected numeric fields and round the primary price fields.
    stream_df = normalize_numeric_columns(
        stream_df,
        columns=["open", "high", "low", "close", "volume", "quote_asset_volume"],
        round_cols=["open", "high", "low", "close"],
    )

    # consolidates all the data from a micro-batch into one single partition.
    stream_df = stream_df.repartition(1)

    # write to minio (silver layer) with a trigger for micro-batching
    query = (
        stream_df.writeStream
        .format("parquet")
        .outputMode("append")
        .option("path", output_path)
        .option("checkpointLocation", checkpoint_path)
        .trigger(processingTime="5 minute")
        .start()
    )

    print(f"Streaming started. Writing to {output_path}...")
    query.awaitTermination()



if __name__ == "__main__":
    main()
