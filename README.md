# DA3408 Assignment 3: Spark vs Ray

K Kevin Sona (DA24B007)

This repository builds the same data preprocessing pipeline in Apache Spark and in Ray Data, runs each on a two worker cluster, and compares them. The pipeline takes six months of NYC yellow taxi trips (January to June 2024, 20.3 million rows), cleans them, joins them to the taxi zone table, computes a trip speed with a custom Python function and an average speed per hour, and writes the result to Parquet. The full write-up is in `report/main.pdf`.

## Results

Both pipelines produce exactly the same output (17,865,181 rows, identical column totals and row fingerprint). Times are the median of three runs on identical clusters (2 workers, each limited to 2 CPUs and 3 GB of memory).

| | Spark | Ray |
|---|---|---|
| Total time | 143 s | 1,085 s |
| Python UDF time | 12.7 s | 6.0 s |
| Average worker CPU (of 200%) | 174% | 57% |

Spark is much faster overall because this pipeline is dominated by large joins, which Spark handles well within limited memory. Ray runs the custom Python function about twice as fast because it does not need to move data between Java and Python.

## Repository layout

| Path | What it is |
|---|---|
| `apps/pipeline_common.py` | Shared rules used by both pipelines: columns, cleaning thresholds, output schema and the Python UDF |
| `apps/spark_clean.py` | The pipeline in PySpark |
| `apps/ray_clean.py` | The same pipeline in Ray Data |
| `spark-cluster/` | Docker Compose file and Dockerfile for the Spark standalone cluster (1 master, 2 workers) |
| `ray-cluster/` | Docker Compose file for the Ray cluster (1 head, 2 workers) |
| `compare_outputs.py` | Checks that the Spark and Ray outputs are identical |
| `benchmark_analysis.ipynb` | Tables and figures from the benchmark runs |
| `results/screenshots/` | Spark UI and Ray Dashboard screenshots |
| `results/*.txt` | Terminal output of the benchmark runs |
| `report/main.pdf` | The report |

## How to run

1. Download the data into `data/raw/`: the six files `yellow_tripdata_2024-01.parquet` to `yellow_tripdata_2024-06.parquet` and `taxi_zone_lookup.csv` from the [NYC TLC Trip Record Data](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page) page. Also create an empty `results/` folder. Both are mounted into every container.

2. Spark. The Spark image builds on `cluster-apache-spark:3.3.4` from the class [docker-spark-cluster](https://github.com/sudarsun/docker-spark-cluster) repository, which must be built first.

   ```bash
   cd spark-cluster && docker compose build && docker compose up -d
   docker exec spark_master /opt/spark/bin/spark-submit \
     --master spark://spark-master:7077 \
     --driver-memory 1G --executor-memory 2G --total-executor-cores 4 \
     --py-files /opt/spark-apps/pipeline_common.py /opt/spark-apps/spark_clean.py
   docker compose down
   ```

   The Spark Master UI is at http://localhost:9090 and the application UI at http://localhost:4040 while a job runs.

3. Ray. Run only one cluster at a time.

   ```bash
   cd ray-cluster && docker compose up -d
   docker exec -it ray_head python /opt/spark-apps/ray_clean.py
   docker compose down
   ```

   The Ray Dashboard is at http://localhost:8265. A full 6 month run takes about 18 minutes with 3 GB workers.

4. Check parity (needs pandas and pyarrow on the host): `python compare_outputs.py`

Both scripts take `--months` (for example `--months 1` for a quick test on January only) and append their timings to `results/spark_runs.jsonl` or `results/ray_runs.jsonl`.
