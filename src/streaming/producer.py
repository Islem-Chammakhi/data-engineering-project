# producer script (read config, connect to kafka, and stream binance trades into kafka)


# import necessary libraries and modules
import json
import logging
import time
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable
from binance import ThreadedWebsocketManager
from src.config.configuration import ( KAFKA_BOOTSTRAP_SERVERS, TOPIC_NAME, BINANCE_SYMBOL, BINANCE_API_KEY, BINANCE_API_SECRET, )

# configure logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")


# global producer instance
producer: KafkaProducer | None = None


# create a kafka producer and retry until the broker is available
def create_producer() -> KafkaProducer:
    while True:
        try:
            producer_instance = KafkaProducer(
                bootstrap_servers=[server.strip() for server in KAFKA_BOOTSTRAP_SERVERS.split(",")],
                value_serializer=lambda value: json.dumps(value).encode("utf-8"),
                key_serializer=lambda key: str(key).encode("utf-8") if key is not None else None,
            )
            logging.info("Connected to Kafka broker: %s", KAFKA_BOOTSTRAP_SERVERS)
            return producer_instance
        except NoBrokersAvailable:
            logging.info("Kafka broker not available yet, retrying in 2 seconds...")
            time.sleep(2)


# process each binance trade event and publish it to kafka
def handle_trade_message(msg):
    if not msg or msg.get("e") != "trade":
        return

    record = {
        "stream_symbol": BINANCE_SYMBOL,
        "event_time": msg.get("E"),
        "trade_time": msg.get("T"),
        "price": msg.get("p"),
        "quantity": msg.get("q"),
        "buyer_is_maker": msg.get("m"),
        "trade_id": msg.get("t"),
        "raw": msg,
    }
    producer.send(TOPIC_NAME, key=msg.get("s"), value=record)
    producer.flush()
    logging.debug("Published trade %s -> topic %s", msg.get("t"), TOPIC_NAME)


# start the websocket manager and keep the producer running
def run_producer() -> None:
    global producer
    logging.info("Starting Binance trade producer")
    producer = create_producer()

    if BINANCE_API_KEY and BINANCE_API_SECRET:
        twm = ThreadedWebsocketManager(api_key=BINANCE_API_KEY, api_secret=BINANCE_API_SECRET)
    else:
        twm = ThreadedWebsocketManager()

    twm.start()
    twm.start_trade_socket(callback=handle_trade_message, symbol=BINANCE_SYMBOL.upper())
    logging.info("Binance websocket started for %s", BINANCE_SYMBOL.upper())

    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        logging.info("Producer interrupted by user")
    finally:
        twm.stop()
        producer.close()


# main entry point to start the producer
if __name__ == "__main__":
    logging.info("Kafka bootstrap: %s", KAFKA_BOOTSTRAP_SERVERS)
    logging.info("Kafka topic: %s", TOPIC_NAME)
    logging.info("Binance symbol: %s", BINANCE_SYMBOL)
    run_producer()
