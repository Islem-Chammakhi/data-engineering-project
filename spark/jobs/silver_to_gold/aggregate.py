
from transformation.utils import create_spark_session, get_db_configs
from pyspark.sql.functions import abs, unix_timestamp,row_number,expr,col,lit
from pyspark.sql.window import Window

asset_mapping = {
    "BTC": "45be0d5a-024e-4738-a223-40021b51b3bb",
    "GOLD": "b3f804bb-5148-47b7-b4d8-1be81ee095eb",
    "OIL": "3fdaf722-3322-4b2f-ad2a-d25ee66baf4c"
}

def is_df_valid(df):
    try:
        return df is not None and df.limit(1).count() > 0
    except Exception:
        return False
    
def safe_read_parquet(spark, path):
    try:
        df = spark.read.parquet(path)
        return df
    except Exception as e:
        print(f"Error reading {path}: {e}")
        return None
    
def transform_and_aggregate(df_news, df_asset, asset_name):
    

    df_news = df_news.withColumnRenamed("event_time", "news_time")
    df_asset = df_asset.withColumnRenamed("event_time", "asset_time")

    # horizons
    df_news = df_news.withColumn("news_time_1h", expr("news_time + interval 1 hour"))
    df_news = df_news.withColumn("news_time_4h", expr("news_time + interval 4 hours"))
    df_news = df_news.withColumn("news_time_24h", expr("news_time + interval 24 hours"))
    
    df_join = df_news.crossJoin(df_asset)
    

    df_join = df_join.withColumn(
    "time_diff_event",
        abs(unix_timestamp(col("news_time")) - unix_timestamp(col("asset_time")))
        ).withColumn(
    "time_diff_1h",
        abs(unix_timestamp(col("news_time_1h")) - unix_timestamp(col("asset_time")))
        ).withColumn(
    "time_diff_4h",
        abs(unix_timestamp(col("news_time_4h")) - unix_timestamp(col("asset_time")))
        ).withColumn(
    "time_diff_24h",
        abs(unix_timestamp(col("news_time_24h")) - unix_timestamp(col("asset_time")))
        ).withColumn("event_time", col("news_time"))
    
    # price at event time
    w_event = Window.partitionBy("news_id").orderBy("time_diff_event")
    df_event = df_join.withColumn("rank_event", row_number().over(w_event)) \
                  .filter("rank_event = 1") \
                  .select("news_id", col("close").alias("price_at_event"),
                          col("asset_time").alias("asset_time_at_event"),
                          col("news_time")
                          )
    
    # price at 1h
    w_1h = Window.partitionBy("news_id").orderBy("time_diff_1h")
    df_1h = df_join.withColumn("rank_1h", row_number().over(w_1h)) \
               .filter("rank_1h = 1") \
               .select("news_id", col("close").alias("price_1h"))
    
    # price at 4h
    w_4h = Window.partitionBy("news_id").orderBy("time_diff_4h")
    df_4h = df_join.withColumn("rank_4h", row_number().over(w_4h)) \
               .filter("rank_4h = 1") \
               .select("news_id", col("close").alias("price_4h"))
    
    # price at 24h
    w_24h = Window.partitionBy("news_id").orderBy("time_diff_24h")
    df_24h = df_join.withColumn("rank_24h", row_number().over(w_24h)) \
                .filter("rank_24h = 1") \
                .select("news_id", col("close").alias("price_24h"))
    
    df_final = df_event \
    .join(df_1h, "news_id") \
    .join(df_4h, "news_id") \
    .join(df_24h, "news_id")

    df_final = df_final.withColumn(
    "return_1h",
        (col("price_1h") - col("price_at_event")) / col("price_at_event")
        ).withColumn(
    "return_4h",
        (col("price_4h") - col("price_at_event")) / col("price_at_event")
        ).withColumn(
    "return_24h",
        (col("price_24h") - col("price_at_event")) / col("price_at_event"),        
        ).withColumn(
    "spike_flag",
        (abs(col("price_1h") - col("price_at_event")) / col("price_at_event")) > 0.005
        ).withColumn(
    "asset_id",
        lit(asset_mapping[asset_name])).withColumn("event_time", col("news_time"))
    # df_final.show()
    df_fact = df_final.select(
    "news_id",
    "asset_id",
    "event_time",
    "asset_time_at_event",
    "price_at_event",
    "price_1h",
    "price_4h",
    "price_24h",
    "return_1h",
    "return_4h",
    "return_24h",
    "spike_flag"
)
    return df_fact

def main():
    spark = create_spark_session(app_name="silver_to_gold")

    # ! Read the parquet silver files
    df_news = safe_read_parquet(spark, "s3a://silver-data-staging/news/arabic/")
    df_btc = safe_read_parquet(spark, "s3a://silver-data-staging/binance/bitcoin/")
    df_gold = safe_read_parquet(spark, "s3a://silver-data-staging/yfinance/gold/")
    df_oil  = safe_read_parquet(spark, "s3a://silver-data-staging/yfinance/oil/")

    news_ok = is_df_valid(df_news)

    assets = {
    "BTC": df_btc,
    "GOLD": df_gold,
    "OIL": df_oil
    }

    valid_assets = {
    name: df for name, df in assets.items()
    if is_df_valid(df)
    }

    if not news_ok:
        raise Exception("STOP: News data not available")

    if len(valid_assets) == 0:
        raise Exception("STOP: No valid asset data")

    print(f"Pipeline continues with assets: {list(valid_assets.keys())}")

    # ! explore schema of the dataframes
    # print("Schema for news:")
    df_news = df_news.withColumn("news_id", expr("uuid()"))
    # df_news.printSchema()
    df_news_db = df_news.select(
    "news_id",
    col("title"),
    col("source"),
    col("description"),
    col("event_time").alias("published_time")
    ).dropDuplicates(["news_id"])
    jdbc_url, properties = get_db_configs()
    df_news_db.write \
            .mode("append") \
            .jdbc(url=jdbc_url, table="dim_news", properties=properties)
    transformations={}
    for name, df in valid_assets.items():
        df_fact=transform_and_aggregate(df_news, df, name)
        df_fact.write \
            .mode("append") \
            .jdbc(url=jdbc_url, table="fact_news_market", properties=properties)
        transformations[name] = df_fact

    print("Transformation and aggregation completed for assets: ", list(transformations.keys()))


if __name__ == "__main__":
    main()
