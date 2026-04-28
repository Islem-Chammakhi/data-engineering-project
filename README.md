# News-Driven Market Analysis ETL Pipeline

## 📊 Project Overview

A **free-tier event-driven ETL pipeline** that ingests **news events** and **market data**, aligns them by timestamp, and analyzes how **crypto (BTC)**, **oil**, and **gold** react to breaking news.

This project correlates news events with price movements to detect:

- **Sudden price moves** after relevant news
- **Measurable returns** following market-moving events
- **Asset reaction comparisons** (which asset reacts most to the same news)
- **Abnormal spikes** compared to normal volatility patterns

---

## 🎯 Main Analysis Questions

1. **Did a sudden price move happen after relevant news?**
   - Example: Iran/war news → oil spikes after

2. **How much did the asset move after the news?**
   - Example: BTC moves +2% in 1 hour after a headline

3. **Which asset reacted the most to the same news?**
   - Example: oil +3%, gold +1%, BTC -0.5%

4. **Was the move abnormal?**
   - Example: oil jumps above its normal volatility range after news

---

## 📡 Data Sources

### Crypto (Real-time & Batch)

- **Binance WebSocket API** → real-time streaming data
- **Binance REST API** → historical/batch data
- Primary asset: **Bitcoin (BTC)**
- Documentation: [Binance API Docs](https://binance-docs.github.io/apidocs/)

### Commodities (Periodic Polling)

- **Yahoo Finance (yfinance)** → batch ingestion
- Assets: **Oil (Crude)** & **Gold**
- Documentation: [yfinance Docs](https://github.com/ranaroussi/yfinance)

### News Events (Micro-batch)

- **GDELT 2.0** → near-real-time news ingestion
- Documentation: [GDELT 2.0 API](https://www.gdeltproject.org/data.html)

---

## 🏗️ Architecture & Tech Stack

### Core Technologies

| Component            | Technology                        | Purpose                                                  |
| -------------------- | --------------------------------- | -------------------------------------------------------- |
| **Data Ingestion**   | Python (requests, python-binance) | Batch & real-time API polling                            |
| **Streaming**        | Apache Kafka                      | Event streaming for real-time market data                |
| **Transformation**   | Apache Spark (PySpark)            | Distributed data processing & analysis                   |
| **Data Lake**        | MinIO (S3-compatible)             | Raw & processed data storage (Bronze/Silver/Gold layers) |
| **Database**         | Neon PostgreSQL                   | Analytical tables & metadata                             |
| **Orchestration**    | Prefect                           | Workflow scheduling & monitoring                         |
| **Visualization**    | Streamlit                         | Interactive dashboards & insights                        |
| **Containerization** | Docker & Docker Compose           | Multi-service deployment                                 |
| **Monitoring**       | Grafana                           | Metrics & observability                                  |

---

## 📂 Project Structure

```
data_engineering/
├── db/                          # Database layer
│   ├── schemas/                # SQLAlchemy ORM models
│   │   ├── dim_asset.py       # Asset dimension table (BTC, Oil, Gold)
│   │   ├── dim_news.py        # News dimension table (headlines, sources)
│   │   ├── fact_news_market.py # Fact table linking news & prices
│   │   ├── pipeline_run.py    # Pipeline execution metadata
│   │   └── task.py            # Task execution tracking
│   ├── config_db.py           # Database configuration
│   ├── init_db.py             # Schema initialization
│   ├── session_db.py          # SQLAlchemy session management
│   └── base_db.py             # Base database classes
│
├── ingestion/                  # Batch ingestion layer
│   ├── ingestion.py           # Main ingestion orchestration
│   └── sources/               # Individual data source clients
│       ├── binance_client/    # Binance REST API client (BTC prices)
│       ├── news_client/       # GDELT news client
│       └── yfinance_client/   # Yahoo Finance client (Oil, Gold)
│
├── streaming/                 # Real-time streaming layer
│   ├── config/
│   │   └── configuration.py   # Kafka & streaming config
│   └── streaming/
│       ├── producer.py        # Kafka producer (publish market events)
│       └── consumer.py        # Kafka consumer (subscribe to streams)
│
├── spark/                     # PySpark transformations (core processing)
│   ├── jobs/                 # Spark job scripts
│   │   ├── bitcoin.py        # BTC analysis job
│   │   ├── gold.py           # Gold analysis job
│   │   ├── news.py           # News processing job
│   │   ├── oil.py            # Oil analysis job
│   │   └── bronze_to_silver/ # Raw → Cleaned data transformation
│   │       └── crypto/stream.py
│   └── transformation/        # Spark utilities
│       ├── utils.py          # Spark utility functions
│       └── silver_to_gold/   # Cleaned → Analytics transformation
│           └── aggregate.py  # Aggregations & correlations
│
├── pipeline/                  # Pipeline orchestration (Prefect)
│   ├── main_flow.py          # Master Prefect flow
│   └── pipeline_per_source.py # Per-source pipeline definitions
│
├── load/                      # Data loading layer
│   └── load.py               # Load transformations into Neon DB
│
├── transformation/            # Additional ETL utilities
│   └── transformation.py      # Data transformation helpers
│
├── utils/                    # Utility modules
│   ├── minio_client/         # MinIO S3 client wrapper
│   └── time_client/          # Timestamp & time utilities
│
├── docker/                   # Docker image definitions
│   ├── base/                # Base image with dependencies
│   ├── client/              # Client-side container
│   ├── master/              # Prefect Master scheduler
│   ├── worker/              # Prefect Worker executor
│   └── streamlit/           # Streamlit dashboard container
│       ├── Dockerfile
│       ├── requirements.txt
│       └── streamlit.py     # Dashboard application
│
├── grafana/                 # Monitoring & observability
│   └── provisioning/        # Grafana datasources config
│       └── datasources/postgres.yaml
│
├── minio_data/              # Local MinIO storage (S3-compatible)
│   ├── raw-data/            # Bronze layer - raw ingested data
│   │   ├── binance/bitcoin/ (JSON files)
│   │   ├── news/arabic/     (JSON files)
│   │   └── yfinance/oil/ & gold/
│   ├── raw-data-staging/    # Temporary staging area
│   ├── silver-data/         # Silver layer - cleaned data
│   │   └── {source}/{asset}/ (_SUCCESS markers)
│   ├── silver-data-staging/ # Temporary staging
│   │   └── {source}/{asset}/
│   └── gold-data/           # Gold layer - analytics-ready data (Parquet)
│
├── docs/                    # Detailed documentation
│   ├── batch-ingestion.md   # Batch ingestion workflow & commands
│   ├── streaming-ingestion.md # Kafka streaming setup
│   └── spark-transformation.md # Spark job details
│
├── docker-compose.yml       # Multi-service orchestration
├── Dockerfile              # Main application image
├── requirements.txt        # Python dependencies (Flask, Spark, etc.)
└── README.md              # This file
```

---

## 🔄 High-Level Pipeline Flow

```
┌─────────────────────────────────────────────────────────────┐
│                    1. DATA INGESTION                        │
├─────────────────────────────────────────────────────────────┤
│                                                             │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────────┐ │
│  │   Binance    │  │  Yahoo Fin.  │  │ GDELT News       │ │
│  │ REST API     │  │  (yfinance)  │  │ Micro-batches    │ │
│  │ (BTC prices) │  │ (Oil, Gold)  │  │ (Headlines)      │ │
│  └──────┬───────┘  └──────┬───────┘  └────────┬─────────┘ │
│         │                 │                   │           │
└─────────┼─────────────────┼───────────────────┼───────────┘
          │                 │                   │
┌─────────▼─────────────────▼───────────────────▼───────────┐
│          2. STORE RAW DATA (MinIO Bronze Layer)           │
├─────────────────────────────────────────────────────────────┤
│                                                             │
│  ├── minio_data/raw-data/binance/bitcoin/                │
│  ├── minio_data/raw-data/yfinance/oil/ & gold/           │
│  └── minio_data/raw-data/news/arabic/                    │
└────────────────────┬────────────────────────────────────────┘
                     │
┌────────────────────▼────────────────────────────────────────┐
│      3. SPARK TRANSFORMATION (PySpark Distributed)         │
├─────────────────────────────────────────────────────────────┤
│                                                             │
│  Bronze → Silver (spark/jobs/bronze_to_silver/)           │
│  ├── Normalize timestamps to UTC                          │
│  ├── Parse & validate JSON                               │
│  ├── Remove duplicates & null values                      │
│  ├── Partition by asset & date for optimization          │
│  └── Output: Parquet files to silver-data/               │
│                                                             │
│  Silver → Gold (spark/transformation/silver_to_gold/)    │
│  ├── Calculate returns (1h, 4h, 24h windows)             │
│  ├── Compute volatility metrics                          │
│  ├── Detect price spikes & anomalies                     │
│  ├── Correlate news events with price moves              │
│  └── Output: Analytics-ready Parquet to gold-data/       │
│                                                             │
└────────────────────┬────────────────────────────────────────┘
                     │
┌────────────────────▼────────────────────────────────────────┐
│   4. STORE PROCESSED DATA (MinIO Silver/Gold Layers)        │
├─────────────────────────────────────────────────────────────┤
│                                                             │
│  ├── minio_data/silver-data/ (cleaned Parquet)           │
│  └── minio_data/gold-data/ (analytics-ready Parquet)     │
│                                                             │
└────────────────────┬────────────────────────────────────────┘
                     │
┌────────────────────▼────────────────────────────────────────┐
│    5. LOAD INTO DATABASE (Neon PostgreSQL)                  │
├─────────────────────────────────────────────────────────────┤
│                                                             │
│  ├── dim_asset (BTC, Oil, Gold metadata)                 │
│  ├── dim_news (news headlines & metadata)                │
│  ├── fact_news_market (correlations & analysis)          │
│  └── pipeline_run & task (execution tracking)            │
│                                                             │
└────────────────────┬────────────────────────────────────────┘
                     │
┌────────────────────▼────────────────────────────────────────┐
│   6. ORCHESTRATION & MONITORING                             │
├─────────────────────────────────────────────────────────────┤
│                                                             │
│  Prefect (Orchestration)                                  │
│  ├── Schedule daily/hourly jobs                          │
│  ├── Handle retries & failures                           │
│  └── Track pipeline runs in DB                           │
│                                                             │
│  Grafana (Monitoring)                                    │
│  ├── Query metrics from PostgreSQL                       │
│  ├── Display dashboards & alerts                         │
│  └── Monitor Spark job performance                       │
│                                                             │
└────────────────────┬────────────────────────────────────────┘
                     │
┌────────────────────▼────────────────────────────────────────┐
│    7. VISUALIZATION & INSIGHTS (Streamlit)                  │
├─────────────────────────────────────────────────────────────┤
│                                                             │
│  ├── Interactive price movement charts                    │
│  ├── News event timeline visualization                    │
│  ├── Correlation heatmaps (news vs prices)               │
│  ├── Anomaly detection alerts                            │
│  └── Asset reaction comparison dashboard                 │
│                                                             │
└─────────────────────────────────────────────────────────────┘
```

---

## 🎯 MVP Focus

The initial release includes:

- ✅ **Binance REST API** for BTC historical data
- ✅ **Yahoo Finance** for Oil & Gold periodic polling
- ✅ **GDELT 2.0** for news micro-batch ingestion
- ✅ **MinIO** for data lake storage (Bronze/Silver/Gold layers)
- ✅ **Neon PostgreSQL** for analytical tables
- ✅ **Streamlit** dashboard for real-time insights
- ✅ **PySpark** for distributed data transformations
- ✅ **Kafka** for streaming architecture (simulated)
- ✅ **Prefect** for workflow orchestration
- ✅ **Docker Compose** for local development & deployment

---

## 🚀 Quick Start

### Prerequisites

- Docker & Docker Compose
- Python 3.9+
- Git

### Installation & Setup

1. **Clone the repository**

   ```bash
   git clone <repo-url>
   cd data_engineering
   ```

2. **Create environment file** (`.env`)

   ```
   # Neon Database
   DATABASE_URL=postgresql://user:password@neon-host/dbname

   # MinIO
   MINIO_ROOT_USER=minioadmin
   MINIO_ROOT_PASSWORD=minioadmin
   MINIO_ENDPOINT=http://minio:9000

   # Binance API
   BINANCE_API_KEY=your_key
   BINANCE_API_SECRET=your_secret

   # Prefect
   PREFECT_API_URL=http://prefect:4200/api
   ```

3. **Start all services**

   ```bash
   docker-compose up -d
   ```

4. **Initialize database**

   ```bash
   docker-compose exec master python -m db.init_db
   ```

5. **Run first ingestion**

   ```bash
   docker-compose exec master python -m ingestion.ingestion
   ```

6. **Run Spark transformations**

   ```bash
   docker-compose exec master python -m spark.jobs.bitcoin
   ```

7. **View Streamlit dashboard**
   ```
   http://localhost:8501
   ```

---

## 📖 Documentation

Detailed documentation for each layer is maintained in the `docs/` folder:

- **[Batch Ingestion](docs/batch-ingestion.md)** — API calls, data parsing, MinIO storage
- **[Streaming Ingestion](docs/streaming-ingestion.md)** — Kafka setup, producer/consumer configuration
- **[Spark Transformations](docs/spark-transformation.md)** — Bronze-to-Silver-to-Gold jobs, correlation analysis

---

## 🗄️ Data Schema

### Dimension Tables

**dim_asset**

```sql
├── asset_id (PK)
├── asset_name (BTC, Oil, Gold)
├── asset_type (Crypto, Commodity)
└── created_at
```

**dim_news**

```sql
├── news_id (PK)
├── headline
├── source
├── language
├── published_at
└── gdelt_event_code
```

### Fact Table

**fact_news_market**

```sql
├── fact_id (PK)
├── asset_id (FK)
├── news_id (FK)
├── price_before
├── price_after
├── price_change_pct
├── returns_1h, 4h, 24h
├── volatility_metric
├── is_anomaly
├── event_window (seconds after news)
└── created_at
```

---

## 🔧 Configuration

### MinIO Buckets (S3-compatible, 3-tier data architecture)

- `raw-data/` → **Bronze layer** (raw JSON/CSV from APIs)
- `silver-data/` → **Silver layer** (cleaned Parquet, deduplicated)
- `gold-data/` → **Gold layer** (analytics-ready, aggregated)

### Neon Database

- Multi-region support (default: US-East)
- Autoscaling enabled for query spikes
- Database branching for team collaboration

### Kafka Topics (Streaming)

- `binance-btc-trades` → Real-time BTC price updates
- `news-events` → GDELT news stream
- `market-alerts` → Anomaly detections

---

## ⚙️ Spark Configuration

### Spark Jobs Location

```
spark/jobs/          # Individual asset analysis jobs
spark/transformation/ # Shared utilities & aggregations
```

### Execution Models

- **Batch**: Run via Prefect on schedule (daily/hourly)
- **Streaming**: Consume from Kafka topics in real-time

### Performance Tuning

- **Partitioning**: By asset & date for optimized queries
- **Format**: Parquet for efficient compression & columnar reads
- **Caching**: In-memory caching for frequently accessed datasets
- **Shuffle Optimization**: Coalesce partitions before writes

---

## 📊 Monitoring & Observability

- **Prefect Dashboard** → Workflow monitoring (http://localhost:4200)
- **Grafana** → Metrics & alerts (http://localhost:3000)
- **PostgreSQL** → Query logs & execution plans
- **MinIO Console** → Data lake browser (http://localhost:9001)

---

## 🛠️ Development

### Adding a New Data Source

1. Create client in `sources/<source_name>/`
2. Add ORM schema to `db/schemas/`
3. Define ingestion task in `pipeline/pipeline_per_source.py`
4. Add Spark transformation job in `spark/jobs/`
5. Update Prefect flow in `pipeline/main_flow.py`

### Running Tests

```bash
docker-compose exec master pytest tests/
```

### Building Custom Docker Image

```bash
docker build -f Dockerfile -t data-engineering:latest .
docker build -f docker/streamlit/Dockerfile -t data-engineering-streamlit:latest .
```

---

## 📈 Performance Considerations

- **Spark**: Partitioned by asset & date for optimized queries
- **Database**: Indexes on (asset_id, timestamp) for fast lookups
- **MinIO**: Parquet format for 80% compression & columnar efficiency
- **Kafka**: 3 replicas for high availability & fault tolerance
- **Caching**: Spark RDD caching for repeated transformations

---

## 🔒 Security Best Practices

- All API credentials stored in `.env` (never commit!)
- PostgreSQL password rotation recommended monthly
- MinIO enabled with IAM policies per service
- Docker secrets for production deployments
- VPC isolation for cloud deployments

---

## 📋 Roadmap

- [ ] Real-time Kafka streaming for BTC
- [ ] Multi-language news NLP sentiment analysis
- [ ] Advanced anomaly detection (Isolation Forest, DBSCAN)
- [ ] Additional assets (Ethereum, Silver, Natural Gas)
- [ ] AWS deployment (S3 + RDS + EMR Spark)
- [ ] REST API layer for external integrations
- [ ] Machine learning price prediction models
- [ ] dbt integration for SQL transformations

---

## 🤝 Contributing

1. Create a feature branch (`git checkout -b feature/new-source`)
2. Make changes and test locally
3. Submit pull request with documentation
4. Ensure all tests pass in CI/CD

---

## 📝 License

MIT License - See LICENSE file for details

---

## 📧 Contact & Support

For questions or issues:

- Check existing [GitHub Issues](issues)
- Review documentation in `docs/` folder
- Contact: [your-email]

---

## 🙏 Acknowledgments

- **Binance** for high-quality market data APIs
- **GDELT** for global event data
- **Neon** for serverless PostgreSQL
- **Apache Spark** & **Kafka** communities
- **Prefect** for workflow orchestration
- **Streamlit** for rapid dashboard development

---

**Last Updated:** April 28, 2026  
**Version:** 0.1.0 (MVP)
