
from prefect import flow
from pipeline.pipeline_per_source import pipeline_per_source
from sources.yfinance_client.batch import ingest_gold_data,ingest_oil_data
from sources.binance_client.batch import ingest_bitcoin_data
from sources.news_client.batch import ingest_newsapi_data


# @flow(name="market-pipeline")
# def main_flow(staging_bucket):

#     pipelines = [
#         pipeline_per_source.submit(
#             "binance-bitcoin",
#             ingest_bitcoin_data,
#             staging_bucket
#         ),
#         pipeline_per_source.submit(
#             "yfinance-gold",
#             ingest_gold_data,
#             staging_bucket
#         ),
#         pipeline_per_source.submit(
#             "yfinance-oil",
#             ingest_oil_data,
#             staging_bucket
#         ),
#         pipeline_per_source.submit(
#             "newsapi-arabic",
#             ingest_newsapi_data,
#             staging_bucket
#         ),
#     ]

#     [p.result() for p in pipelines]


@flow(name="market-pipeline")
def main_flow(staging_bucket):

        # print(pipeline_per_source(
        #     "binance-bitcoin",
        #     ingest_bitcoin_data,
        #     staging_bucket
        # ))
        print(pipeline_per_source(
            "yfinance-gold",
            ingest_gold_data,
            staging_bucket
        ))

        print(pipeline_per_source(
            "yfinance-oil",
            ingest_oil_data,
            staging_bucket
        ))
        print(pipeline_per_source(
            "newsapi-arabic",
            ingest_newsapi_data,
            staging_bucket
        ))


main_flow("raw-data-staging")