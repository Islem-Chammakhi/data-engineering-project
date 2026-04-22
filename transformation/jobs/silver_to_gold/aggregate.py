import os

from transformation.utils import create_spark_session, write_parquet


def main():
    spark = create_spark_session(app_name="silver_to_gold")
    silver_root = os.getenv("SILVER_ROOT", "/app/data/silver")
    gold_output = os.getenv("GOLD_ROOT", "/app/data/gold")

    silver_df = spark.read.parquet(silver_root)

    # Add silver-to-gold transformation logic here.

    write_parquet(silver_df, gold_output)


if __name__ == "__main__":
    main()
