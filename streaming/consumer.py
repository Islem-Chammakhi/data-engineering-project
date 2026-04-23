# consumer script (read kafka binance trade records and display them)


# import necessary libraries and modules
import json
import logging
import time
from kafka import KafkaConsumer
from kafka.errors import NoBrokersAvailable
from config.configuration import ( KAFKA_BOOTSTRAP_SERVERS, TOPIC_NAME, CONSUMER_GROUP )


# configure logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")


# create a kafka consumer and retry until the broker is available
def create_consumer() -> KafkaConsumer:
    while True:
        try:
            consumer_instance = KafkaConsumer(
                TOPIC_NAME,
                bootstrap_servers=[server.strip() for server in KAFKA_BOOTSTRAP_SERVERS.split(",")],
                auto_offset_reset="earliest",
                enable_auto_commit=True,
                group_id=CONSUMER_GROUP,
                value_deserializer=lambda value: json.loads(value.decode("utf-8")),
            )
            logging.info("Connected to Kafka broker: %s", KAFKA_BOOTSTRAP_SERVERS)
            return consumer_instance
        except NoBrokersAvailable:
            logging.info("Kafka broker not available yet, retrying in 2 seconds...")
            time.sleep(2)


# consume records from the kafka topic and print each candle
def consume_klines() -> None:
    consumer = create_consumer()
    logging.info("Starting Kafka consumer for topic %s", TOPIC_NAME)
    try:
        for message in consumer:
            candle = message.value
            print(json.dumps({
                "topic": message.topic,
                "partition": message.partition,
                "offset": message.offset,
                "symbol": candle.get("symbol"),
                "open_time": candle.get("open_time"),
                "open": candle.get("open"),
                "high": candle.get("high"),
                "low": candle.get("low"),
                "close": candle.get("close"),
                "volume": candle.get("volume"),
                "close_time": candle.get("close_time"),
                "quote_asset_volume": candle.get("quote_asset_volume"),
                "number_of_trades": candle.get("number_of_trades"),
                "is_final": candle.get("is_final"),
            }, indent=2))
    finally:
        consumer.close()


# main entry point to start the consumer
if __name__ == "__main__":
    logging.info("Kafka bootstrap: %s", KAFKA_BOOTSTRAP_SERVERS)
    logging.info("Kafka topic: %s", TOPIC_NAME)
    try:
        consume_klines()
    except KeyboardInterrupt:
        logging.info("Consumer interrupted by user")
