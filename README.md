# Project Summary

## Objective
Build a **data engineering pipeline** to correlate **news events** with **price movements** in:
- **Crypto** (e.g. BTC)
- **Oil**
- **Gold**

The goal is to detect whether relevant news causes:
- Sudden price moves
- Measurable returns after the event
- Stronger reactions in one asset vs another
- Abnormal spikes compared to normal volatility



## Main Analysis Questions

### 1. Did a sudden price move happen after relevant news?
**Example:** Iran/war news → oil spikes after

### 2. How much did the asset move after the news?
**Example:** BTC moves **+2% in 1 hour** after a headline

### 3. Which asset reacted the most to the same news?
**Example:** oil **+3%**, gold **+1%**, BTC **-0.5%**

### 4. Was the move abnormal?
**Example:** oil jumps above its normal volatility range after news



## Data Sources

### Crypto
- **Binance WebSocket API** → real-time / streaming  
  Docs:  
  - [Binance WebSocket API Documentation](https://developers.binance.com/docs/binance-spot-api-docs/websocket-api/general-api-information)  
  - [python-binance Library Documentation](https://python-binance.readthedocs.io/en/latest/)

- **Binance REST API** → historical / batch  
  Docs:  
  - [Binance REST API Documentation](https://developers.binance.com/docs/binance-spot-api-docs/rest-api/general-api-information)

### Oil & Gold
- **Yahoo Finance (yfinance)** → periodic polling / batch  
  Docs:  
  - [yfinance Documentation](https://ranaroussi.github.io/yfinance/)

### News
- **GDELT 2.0** → near-real-time / micro-batch news ingestion  
  Docs:  
  - [GDELT 2.0 API Documentation](https://blog.gdeltproject.org/gdelt-doc-2-0-api-debuts/)

> All selected APIs provide the minimum required fields for the MVP:
> **timestamp, price, headline/title, asset name (when needed)**



## ETL Stack

### 1. Ingestion
- API calls for batch ingestion
- Simulated streaming with **Kafka**

### 2. Database
- **Neon (PostgreSQL)**  
Key advantages:
- Free tier
- Autoscaling + scale to zero
- **Database branching** for team collaboration
- Supports extensions like **TimescaleDB**, **pgvector**, **PostGIS**

### 3. Data Lake
- **MinIO** (S3-compatible, local with Docker)

Used to store:
- Raw JSON / CSV / logs
- Cleaned or enriched intermediate files

### 4. Orchestration
- **Prefect** or **Airflow**

### 5. Visualization
- **Streamlit**



## High-Level Pipeline Flow

1. **Ingest** market + news data from APIs  
2. **Store raw data** in **MinIO**  
3. **Transform** data:
   - Normalize timestamps
   - Align news with price windows
   - Calculate returns / volatility / spikes
4. **Load curated data** into **Neon**
5. **Orchestrate** jobs with **Prefect/Airflow**
6. **Visualize insights** in **Streamlit**



## MVP Focus
Start with:
- **Binance REST** for BTC
- **Yahoo Finance** for oil & gold
- **GDELT** for news
- **MinIO** for raw storage
- **Neon** for analytics tables
- **Streamlit** dashboard for results



## One-Line Summary
A **free-tier event-driven ETL pipeline** that ingests **news + market data**, aligns them by **timestamp**, and analyzes how **crypto, oil, and gold react to breaking news**.




## Data Ingestion Documentation
Detailed streaming and batch ingestion docs are now kept inside the `src/docs` folder.

- Streaming ingestion: `src/docs/streaming-ingestion.md`
- Batch ingestion: `src/docs/batch-ingestion.md`

This README stays broad and high-level, while the docs folder contains the detailed workflow and commands.
