import os

from dotenv import load_dotenv

load_dotenv()


def _getenv(name: str, default: str | None = None) -> str:
    value = os.getenv(name, default)
    if value is None or value == "":
        raise RuntimeError(f"Missing required env var: {name}")
    return value


KAFKA_BOOTSTRAP_SERVERS = _getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:29092")
TOPIC_NAME = _getenv("TOPIC_NAME", "binance-trades")
BINANCE_SYMBOL = _getenv("BINANCE_SYMBOL", "btcusdt").lower()
CONSUMER_GROUP = _getenv("CONSUMER_GROUP", "binance-consumer-group")
BINANCE_API_KEY = os.getenv("BINANCE_API_KEY")
BINANCE_API_SECRET = os.getenv("BINANCE_API_SECRET")
