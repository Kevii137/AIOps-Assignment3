# DA3408 A3 -- NYC Yellow Taxi preprocessing pipeline in PySpark (standalone cluster, 2 workers).
#
#   Ingestion -> Cleansing -> Transformation (heavy join + Python UDF + hourly feature) -> Export
#
# Submit from inside the master container (same pattern as the class docker-spark-cluster):
#   docker exec -it spark_master /opt/spark/bin/spark-submit --master spark://spark-master:7077 \
#       --driver-memory 1G --executor-memory 2G --total-executor-cores 4 \
#       --py-files /opt/spark-apps/pipeline_common.py /opt/spark-apps/spark_clean.py
import argparse
import json
import os
import time

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType

from pipeline_common import (KEY_COLS, MAX_DURATION_SEC, MAX_SPEED_MPH, OUTPUT_COLS, OUTPUT_DIR,
                             RESULTS_DIR, SELECT_COLS, ZONES_FILE, month_range, parse_months,
                             trip_files, trip_speed_mph)

parser = argparse.ArgumentParser()
parser.add_argument("--months", default="1-6", help="e.g. 1 or 1-6")
parser.add_argument("--skip-udf-bench", action="store_true")
args = parser.parse_args()
months = parse_months(args.months)

# initialize the spark session -- the master URL comes from spark-submit --master
spark = (
    SparkSession.builder
    .appName(f"a3-spark-clean-months-{args.months}")
    .config("spark.sql.shuffle.partitions", "8")              # tuned for a 4-core cluster (as in class)
    .config("spark.sql.autoBroadcastJoinThreshold", -1)       # force a real shuffle join ("heavy join")
    .config("spark.sql.session.timeZone", "UTC")              # TLC timestamps are naive wall-clock times
    .config("spark.sql.parquet.outputTimestampType", "TIMESTAMP_MICROS")  # modern Parquet, not INT96
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")
print("Spark version:", spark.version, "| master:", spark.sparkContext.master, "| months:", months)

metrics = {"engine": "spark", "months": args.months, "run_ts": time.strftime("%Y-%m-%d %H:%M:%S"),
           "stage_sec": {}, "rows": {}}
t_start = time.time()

# ---------------------------------------------------------------------------------------------
# 1. INGESTION -- read every monthly Parquet file from the shared directory in one call
# ---------------------------------------------------------------------------------------------
t0 = time.time()
raw = spark.read.parquet(*trip_files(months))
raw = raw.select([F.col(c).cast(t).alias(c) for c, t in SELECT_COLS.items()])
zones = spark.read.csv(ZONES_FILE, header=True, inferSchema=True)
metrics["rows"]["raw"] = raw.count()                  # action: Parquet row count (from file footers)
metrics["stage_sec"]["ingest"] = time.time() - t0
print(f"[ingest]   {metrics['rows']['raw']:,} raw rows from {len(months)} file(s), "
      f"partitions={raw.rdd.getNumPartitions()}  ({metrics['stage_sec']['ingest']:.2f}s)")

# ---------------------------------------------------------------------------------------------
# 2. CLEANSING -- nulls, duplicates, impossible values, timestamp formatting
# ---------------------------------------------------------------------------------------------
t0 = time.time()
start, end = month_range(months)
cleaned = (
    raw.dropna(subset=KEY_COLS)
       .dropDuplicates()                                                   # shuffle #1
       .withColumn("duration_sec",
                   F.col("tpep_dropoff_datetime").cast("long") - F.col("tpep_pickup_datetime").cast("long"))
       .filter((F.col("trip_distance") > 0) & (F.col("fare_amount") >= 0))
       .filter((F.col("duration_sec") > 0) & (F.col("duration_sec") <= MAX_DURATION_SEC))
       .filter((F.col("tpep_pickup_datetime") >= F.lit(start).cast("timestamp")) &
               (F.col("tpep_pickup_datetime") < F.lit(end).cast("timestamp")))
       .withColumn("pickup_date", F.to_date("tpep_pickup_datetime"))
       .withColumn("pickup_hour", F.hour("tpep_pickup_datetime"))
)
cleaned.cache()
metrics["rows"]["cleaned"] = cleaned.count()          # action: materialise the cleaned table
metrics["stage_sec"]["cleanse"] = time.time() - t0
print(f"[cleanse]  {metrics['rows']['cleaned']:,} rows kept "
      f"({metrics['rows']['raw'] - metrics['rows']['cleaned']:,} dropped)  ({metrics['stage_sec']['cleanse']:.2f}s)")

# ---------------------------------------------------------------------------------------------
# 3. TRANSFORMATION -- heavy join (pickup + dropoff zones), Python UDF, hourly average speed
# ---------------------------------------------------------------------------------------------
t0 = time.time()
pu_zones = zones.select(F.col("LocationID").alias("PULocationID"),
                        F.col("Borough").alias("PU_Borough"), F.col("Zone").alias("PU_Zone"))
do_zones = zones.select(F.col("LocationID").alias("DOLocationID"),
                        F.col("Borough").alias("DO_Borough"), F.col("Zone").alias("DO_Zone"))

speed_udf = F.udf(trip_speed_mph, DoubleType())       # row-at-a-time Python UDF: JVM <-> Python worker

enriched = (
    cleaned.join(pu_zones, "PULocationID")            # SortMergeJoin #1 (shuffle both sides)
           .join(do_zones, "DOLocationID")            # SortMergeJoin #2
           .withColumn("speed_mph", speed_udf("trip_distance", "duration_sec"))
           .filter(F.col("speed_mph").isNotNull())
)
enriched.cache()                                      # reused twice below -> run the UDF only once
enriched.count()                                      # fill the cache NOW, before two consumers race to compute it
hourly = enriched.groupBy("pickup_hour").agg(F.round(F.avg("speed_mph"), 4).alias("avg_speed_hour"))
transformed = enriched.join(hourly, "pickup_hour").select(OUTPUT_COLS)

print("\n--- Physical plan of the transformation (look for Exchange, SortMergeJoin, BatchEvalPython) ---")
transformed.explain()

metrics["rows"]["transformed"] = transformed.count()  # action: runs joins + UDF + aggregation
metrics["stage_sec"]["transform"] = time.time() - t0
print(f"[transform] {metrics['rows']['transformed']:,} rows after join + UDF  ({metrics['stage_sec']['transform']:.2f}s)")

# ---------------------------------------------------------------------------------------------
# 4. EXPORT -- ML-ready Parquet on the shared directory
# ---------------------------------------------------------------------------------------------
t0 = time.time()
transformed.write.mode("overwrite").parquet(f"{OUTPUT_DIR}/spark")   # re-reads enriched from cache
metrics["stage_sec"]["export"] = time.time() - t0
metrics["total_sec"] = time.time() - t_start
print(f"[export]   written to {OUTPUT_DIR}/spark  ({metrics['stage_sec']['export']:.2f}s)")
print(f"\nTOTAL wall-clock (ingest -> export): {metrics['total_sec']:.2f}s")

print("\nAverage speed per pickup hour:")
hourly.orderBy("pickup_hour").show(24)

# ---------------------------------------------------------------------------------------------
# 5. UDF DEEP-DIVE -- same speed rule, two ways, on the cached cleaned table
#    native  : Spark SQL expression, runs entirely inside the JVM   (baseline: the pure computation)
#    @udf    : row-at-a-time Python -- every row is pickled JVM -> Python -> JVM
#    JVM<->Python overhead = python_udf time - native time
# ---------------------------------------------------------------------------------------------
if not args.skip_udf_bench:
    mph = F.round(F.col("trip_distance") / (F.col("duration_sec") / 3600.0), 4)
    variants = {
        "native": F.when((F.col("duration_sec") > 0) & (mph <= MAX_SPEED_MPH), mph),
        "python_udf": speed_udf("trip_distance", "duration_sec"),
    }
    enriched.unpersist(blocking=True)                 # free memory so `cleaned` stays fully cached
    cleaned.count()                                   # warm-up: make sure the input is in memory
    metrics["udf_sec"] = {}
    print("\n--- UDF deep-dive (cleaned rows:", f"{metrics['rows']['cleaned']:,}, best of 2)")
    for name, expr in variants.items():
        timings = []
        for _ in range(2):
            t0 = time.time()
            row = cleaned.select(expr.alias("s")).agg(F.sum("s").alias("sum"), F.count("s").alias("n")).collect()[0]
            timings.append(time.time() - t0)
        metrics["udf_sec"][name] = min(timings)
        print(f"  {name:11s} {metrics['udf_sec'][name]:7.2f}s   sum={row['sum']:.2f}  non-null={row['n']:,}")
    overhead = metrics["udf_sec"]["python_udf"] - metrics["udf_sec"]["native"]
    print(f"  JVM<->Python overhead: {overhead:.2f}s "
          f"({100 * overhead / metrics['udf_sec']['python_udf']:.0f}% of the Python UDF time)")

# save metrics for the analysis notebook (one JSON line per run)
with open(f"{RESULTS_DIR}/spark_runs.jsonl", "a") as f:
    f.write(json.dumps(metrics) + "\n")
print(f"\nMetrics appended to {RESULTS_DIR}/spark_runs.jsonl")

# keep the driver alive so the application UI (port 4040) can be inspected/screenshotted
hold = int(os.environ.get("HOLD_UI", "0"))
if hold:
    print(f"Holding Spark UI at http://localhost:4040 for {hold}s ...")
    time.sleep(hold)

spark.stop()
