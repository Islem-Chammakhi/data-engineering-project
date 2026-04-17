import os

from src.spark.utils import ( create_spark_session, drop_duplicates, drop_null_values, normalize_timestamp, read_json, write_parquet )


def main():
    # create spark session
    spark = create_spark_session(app_name="news")

    # choose input and output paths
    input_path = os.getenv("NEWS_INPUT_PATH", "/app/src/data/news.json")
    output_path = os.getenv("NEWS_OUTPUT_PATH", "/app/data/silver/news")

    # read the local json data (read data from minio in production)
    news_df = read_json(spark, input_path)

    # drop rows with any null values
    news_df = drop_null_values(news_df)

    # drop duplicate rows by url and publishedAt
    news_df = drop_duplicates(news_df, subset=["url", "publishedAt"])

    # normalize timestamp
    news_df = normalize_timestamp(
        news_df,
        source_col="publishedAt",
        target_col="event_time",
        input_fmt="yyyy-MM-dd'T'HH:mm:ss'Z'",
    )

    # write the transformed data to parquet (column storage) (write to minio in production)
    write_parquet(news_df, output_path)


if __name__ == "__main__":
    main()
