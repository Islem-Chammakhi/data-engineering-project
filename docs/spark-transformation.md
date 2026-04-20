# Spark Transformation

This document describes the Spark transformation workflow for local JSON input and Parquet output.

## Workflow

Spark is run in a standalone cluster with a master and worker. The transformation is executed manually with `spark-submit`.

- `docker compose up -d spark-master spark-worker` starts the Spark standalone cluster
- `docker compose run --rm spark spark-submit <job>` submits a PySpark job to the cluster
- Jobs read JSON from `src/data`
- Jobs write Parquet to `data/silver` or `data/gold`


## How It Works

1. Start the Spark cluster
   ```bash
   docker compose up -d spark-master spark-worker
   ```

2. Submit a batch job
   ```bash
   docker compose run --rm spark /opt/spark/bin/spark-submit /app/src/spark/jobs/bronze_to_silver/crypto/batch.py
   ```

3. Verify output
   - Batch jobs write to Parquet under `data/silver`
   - Gold aggregation writes to `data/gold`

## Notes

- The Spark submit service mounts the repository at `/app`, so jobs can access project files and data paths directly.
- `src/spark/utils.py` creates a Spark session with `spark://spark-master:7077`.
- Input JSON is read with `multiline=true` because the sample files are newline-delimited JSON.
- If Spark cannot see a file, make sure the repo is mounted into all Spark containers (`- .:/app`).

