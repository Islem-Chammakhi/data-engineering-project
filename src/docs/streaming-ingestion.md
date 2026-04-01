# Streaming Data Ingestion

This document describes the current streaming ingestion workflow for Binance trade data.

## Workflow
Config -> Kafka -> Producer -> Topic -> Consumer

- Config loads env values
- Docker starts Zookeeper then Kafka
- Producer connects to Binance websocket
- Each trade -> producer sends to `binance-trades` Kafka topic
- Consumer reads from that topic and prints messages

## File Structure
- `src/config/configuration.py` — loads `.env` and env values
- `src/streaming/producer.py` — Binance websocket producer
- `src/streaming/consumer.py` — Kafka consumer reader
- `docker-compose.yml` — starts zookeeper, kafka, producer, consumer
- `Dockerfile` + `requirements.txt` — Python runtime for services

## Quick Start
1. Start Kafka and Zookeeper:
   ```bash
   docker compose up -d kafka zookeeper
   ```
2. Start the streaming pipeline:
   ```bash
   docker compose up -d producer consumer
   ```

## Trade Message Attributes

Each Kafka message contains trade data from Binance with the following attributes:

- `topic` - Kafka topic name
- `partition` - Partition number inside the topic
- `offset` - Position of the message inside the partition
- `symbol` - `"btcusdt"` = Bitcoin price in USDT
- `event_time` - Unix timestamp in milliseconds (can be converted to a readable date/time)
- `price` - Price of **1 BTC** in USDT
- `quantity` - Amount of BTC traded
- `trade_id` - Unique trade identifier

## Example Docker Console Output

Below is an example of the Docker console showing streamed Binance trade records:

![Docker Console - Binance Trade Stream Example](https://i.imgur.com/yiY4MsK.png)


## Notes
- `producer` Writes Binance trades into Kafka
- `consumer` Reads from the Kafka topic and prints records
- `.env.example` Contains all the necessary environment variables
