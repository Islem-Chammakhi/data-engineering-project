
from prefect import flow
from pipeline.pipeline_per_source import  run_pipeline
from sources.yfinance_client.batch import ingest_gold_data,ingest_oil_data
from sources.binance_client.batch import ingest_bitcoin_data
from sources.news_client.batch import ingest_newsapi_data

@flow(name="market-pipeline")
def main_flow(staging_bucket):

    futures = [
        run_pipeline.submit("binance-bitcoin", ingest_bitcoin_data, staging_bucket),
        run_pipeline.submit("yfinance-gold", ingest_gold_data, staging_bucket),
        run_pipeline.submit("yfinance-oil", ingest_oil_data, staging_bucket),
        run_pipeline.submit("newsapi-arabic", ingest_newsapi_data, staging_bucket),
    ]

    results = [f.result() for f in futures]

    print(results)

main_flow("raw-data-staging")