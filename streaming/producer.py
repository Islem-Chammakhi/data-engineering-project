# producer script (read config, connect to kafka, and stream binance klines into kafka)


# import necessary libraries and modules
import json
import logging
import time
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable
from binance import ThreadedWebsocketManager
from binance.enums import KLINE_INTERVAL_1MINUTE
from config.configuration import ( KAFKA_BOOTSTRAP_SERVERS, TOPIC_NAME, BINANCE_SYMBOL )

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


# process each binance kline event and publish it to kafka
def handle_kline_message(msg):
    if not msg or msg.get("e") != "kline":
        return

    k = msg.get("k") or {}
    is_final = bool(k.get("x"))

    # Emit the same candle attributes as batch ingestion, plus symbol + is_final.
    record = {
        "symbol": (k.get("s") or BINANCE_SYMBOL).lower(),
        "open_time": k.get("t"),
        "open": k.get("o"),
        "high": k.get("h"),
        "low": k.get("l"),
        "close": k.get("c"),
        "volume": k.get("v"),
        "close_time": k.get("T"),
        "quote_asset_volume": k.get("q"),
        "number_of_trades": k.get("n"),
        "is_final": is_final,
    }

    # For your plan, we only want closed candles.
    if not is_final:
        return

    producer.send(TOPIC_NAME, key=record["symbol"], value=record)
    producer.flush()
    logging.debug("Published kline open_time=%s -> topic %s", k.get("t"), TOPIC_NAME)


# start the websocket manager and keep the producer running
def run_producer() -> None:
    global producer
    logging.info("Starting Binance kline producer (1m)")
    producer = create_producer()

    twm = ThreadedWebsocketManager()
    twm.start()
    twm.start_kline_socket(
        callback=handle_kline_message,
        symbol=BINANCE_SYMBOL.upper(),
        interval=KLINE_INTERVAL_1MINUTE,
    )
    logging.info("Binance websocket started (kline 1m) for %s", BINANCE_SYMBOL.upper())

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
