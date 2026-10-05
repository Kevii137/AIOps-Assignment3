# DA3408 A3 -- NYC Yellow Taxi preprocessing pipeline in Ray Data (head + 2 workers).
# Line-for-line translation of spark_clean.py; all rules come from pipeline_common.py.
#
#   Ingestion -> Cleansing -> Transformation (heavy join + Python UDF + hourly feature) -> Export
#
# Run from inside the head container (the head joins the running cluster with address="auto"):
#   docker exec -it ray_head python /opt/spark-apps/ray_clean.py
#
# Ray Data (required by the assignment) is built on the Ray Core ideas from class:
#   ds.map_batches(fn)   -> runs fn as a @ray.remote TASK on every block of rows
#   dataset blocks       -> objects in the shared OBJECT STORE (what ray.put() uses)
#   ds.materialize()     -> execute now and keep the blocks (like ray.get() on all futures)
#   ds.join / groupby    -> shuffle via aggregator ACTORS that collect matching rows
import argparse
import json
import logging
import os
import shutil
import time

import pandas as pd
import pyarrow as pa
import pyarrow.csv as pacsv
import ray
import ray.data as rd
from ray.data.context import ShuffleStrategy

from pipeline_common import (KEY_COLS, MAX_DURATION_SEC, OUTPUT_COLS, OUTPUT_DIR, RESULTS_DIR,
                             SELECT_COLS, ZONES_FILE, month_range, parse_months, trip_files,
                             trip_speed_mph, trip_speed_mph_vectorized)

parser = argparse.ArgumentParser()
parser.add_argument("--months", default="1-6", help="e.g. 1 or 1-6")
parser.add_argument("--skip-udf-bench", action="store_true")
args = parser.parse_args()
months = parse_months(args.months)

# connect to the running cluster (same ray.init pattern as the class notebooks).
# Workers import pipeline_common.py from the shared /opt/spark-apps mount (like Spark's --py-files).
ray.init(address="auto", logging_level="ERROR", log_to_driver=False,
         runtime_env={"env_vars": {"PYTHONPATH": "/opt/spark-apps"}})
logging.getLogger("ray.data").setLevel(logging.WARNING)   # hide Ray Data INFO chatter in the terminal
ctx = rd.DataContext.get_current()
ctx.enable_progress_bars = False

# ---------------------------------------------------------------------------------------------
# MEMORY TUNING -- needed to fit 6 months into the same 3 GB-per-worker caps Spark used.
# Ray Data's hash join sends each shuffle partition to "aggregator" actors (the class's actor
# concept), which join a whole partition in working memory that cannot spill; with the defaults
# the 6-month job deadlocks or runs out of memory.
# ---------------------------------------------------------------------------------------------
ctx.shuffle_strategy = ShuffleStrategy.SORT_SHUFFLE_PULL_BASED  # groupby/map_groups: plain tasks, no aggregators
ctx.max_hash_shuffle_aggregators = 2                 # joins: one aggregator per worker
ctx.target_max_block_size = 64 * 1024**2             # smaller blocks -> lower peak memory per task
JOIN_AGGREGATOR_ARGS = {"memory": 512 * 1024**2}     # aggregator memory reservation that fits a 3 GB node
JOIN_PARTITIONS = 32                                 # smaller join partitions -> smaller per-aggregator peak

print("Ray version:", ray.__version__, "| cluster CPUs:", ray.cluster_resources().get("CPU"), "| months:", months)

metrics = {"engine": "ray", "months": args.months, "run_ts": time.strftime("%Y-%m-%d %H:%M:%S"),
           "stage_sec": {}, "rows": {}}
t_start = time.time()

# Arrow types matching Spark's SELECT_COLS casts
ARROW_TYPES = {"int": pa.int32(), "bigint": pa.int64(), "double": pa.float64(), "timestamp": pa.timestamp("us")}
SCHEMA = pa.schema([(c, ARROW_TYPES[t]) for c, t in SELECT_COLS.items()])


def cast_columns(batch: pa.Table) -> pa.Table:
    return batch.select(list(SELECT_COLS)).cast(SCHEMA)


# ---------------------------------------------------------------------------------------------
# 1. INGESTION -- read every monthly Parquet file from the shared directory in one call
# ---------------------------------------------------------------------------------------------
t0 = time.time()
raw = rd.read_parquet(trip_files(months), columns=list(SELECT_COLS))
raw = raw.map_batches(cast_columns, batch_format="pyarrow")
# keep "N/A" as a string, exactly like Spark's CSV reader (pyarrow would otherwise make it null)
zones = rd.read_csv(ZONES_FILE, convert_options=pacsv.ConvertOptions(strings_can_be_null=False))
metrics["rows"]["raw"] = raw.count()
metrics["stage_sec"]["ingest"] = time.time() - t0
print(f"[ingest]   {metrics['rows']['raw']:,} raw rows from {len(months)} file(s)  "
      f"({metrics['stage_sec']['ingest']:.2f}s)")

# ---------------------------------------------------------------------------------------------
# 2. CLEANSING -- nulls, duplicates, impossible values, timestamp formatting
# ---------------------------------------------------------------------------------------------
t0 = time.time()
start, end = month_range(months)
EPOCH = pd.Timestamp("1970-01-01")


def drop_nulls(df: pd.DataFrame) -> pd.DataFrame:
    return df.dropna(subset=KEY_COLS)


def filter_and_format(df: pd.DataFrame) -> pd.DataFrame:
    # same as Spark's cast(timestamp as long): whole epoch seconds, then subtract
    pu_sec = (df["tpep_pickup_datetime"] - EPOCH) // pd.Timedelta(seconds=1)
    do_sec = (df["tpep_dropoff_datetime"] - EPOCH) // pd.Timedelta(seconds=1)
    df["duration_sec"] = (do_sec - pu_sec).astype("int64")
    keep = ((df["trip_distance"] > 0) & (df["fare_amount"] >= 0)
            & (df["duration_sec"] > 0) & (df["duration_sec"] <= MAX_DURATION_SEC)
            & (df["tpep_pickup_datetime"] >= pd.Timestamp(start))
            & (df["tpep_pickup_datetime"] < pd.Timestamp(end)))
    df = df[keep].copy()
    df["pickup_date"] = df["tpep_pickup_datetime"].dt.date
    df["pickup_hour"] = df["tpep_pickup_datetime"].dt.hour.astype("int32")
    return df


CLEAN_SCHEMA = SCHEMA.append(pa.field("duration_sec", pa.int64())) \
                     .append(pa.field("pickup_date", pa.date32())).append(pa.field("pickup_hour", pa.int32()))


def enforce_types(batch: pa.Table) -> pa.Table:
    # pandas round-trips and the shuffle can widen ints (int32 -> int64); joins need exact key types
    return batch.select(CLEAN_SCHEMA.names).cast(CLEAN_SCHEMA)


def drop_duplicate_rows(group: pd.DataFrame) -> pd.DataFrame:
    return group.drop_duplicates()


cleaned = (
    raw.map_batches(drop_nulls, batch_format="pandas")
       # Ray Data has no drop_duplicates: shuffle so all rows of a pickup zone meet (identical
       # rows always share a zone), then drop exact duplicates inside each zone group
       .groupby("PULocationID").map_groups(drop_duplicate_rows, batch_format="pandas")
       .map_batches(filter_and_format, batch_format="pandas")
       .map_batches(enforce_types, batch_format="pyarrow")
)
cleaned = cleaned.materialize()                          # = Spark's cache() + count()
metrics["rows"]["cleaned"] = cleaned.count()
metrics["stage_sec"]["cleanse"] = time.time() - t0
print(f"[cleanse]  {metrics['rows']['cleaned']:,} rows kept "
      f"({metrics['rows']['raw'] - metrics['rows']['cleaned']:,} dropped)  ({metrics['stage_sec']['cleanse']:.2f}s)")

# ---------------------------------------------------------------------------------------------
# 3. TRANSFORMATION -- heavy join (pickup + dropoff zones), Python UDF, hourly average speed
# ---------------------------------------------------------------------------------------------
t0 = time.time()


def zone_side(prefix):
    def rename(df: pd.DataFrame) -> pd.DataFrame:
        return pd.DataFrame({f"{prefix}LocationID": df["LocationID"].astype("int32"),
                             f"{prefix}_Borough": df["Borough"], f"{prefix}_Zone": df["Zone"]})
    return rename


pu_zones = zones.map_batches(zone_side("PU"), batch_format="pandas")
do_zones = zones.map_batches(zone_side("DO"), batch_format="pandas")


def apply_speed_udf(df: pd.DataFrame) -> pd.DataFrame:
    # the SAME row-at-a-time Python function Spark wraps in F.udf -- here it runs in-process
    df["speed_mph"] = [trip_speed_mph(d, s)
                       for d, s in zip(df["trip_distance"].tolist(), df["duration_sec"].tolist())]
    return df[df["speed_mph"].notna()]


enriched = (
    cleaned.join(pu_zones, "inner", JOIN_PARTITIONS, on=("PULocationID",),   # hash-shuffle join #1
                 aggregator_ray_remote_args=JOIN_AGGREGATOR_ARGS)
           .materialize()                                                     # finish join #1 before join #2 starts
           .join(do_zones, "inner", JOIN_PARTITIONS, on=("DOLocationID",),   # hash-shuffle join #2
                 aggregator_ray_remote_args=JOIN_AGGREGATOR_ARGS)
           .map_batches(apply_speed_udf, batch_format="pandas")
)
enriched = enriched.materialize()                        # = Spark's enriched.cache() + count()


def round_avg(df: pd.DataFrame) -> pd.DataFrame:
    return pd.DataFrame({"pickup_hour": df["pickup_hour"].astype("int32"),
                         "avg_speed_hour": df["mean(speed_mph)"].round(4)})


hourly = enriched.groupby("pickup_hour").mean("speed_mph").map_batches(round_avg, batch_format="pandas")
transformed = (enriched.join(hourly, "inner", JOIN_PARTITIONS, on=("pickup_hour",),
                             aggregator_ray_remote_args=JOIN_AGGREGATOR_ARGS)
                       .select_columns(OUTPUT_COLS))

metrics["rows"]["transformed"] = transformed.count()     # runs the hourly aggregation + join back
metrics["stage_sec"]["transform"] = time.time() - t0
print(f"[transform] {metrics['rows']['transformed']:,} rows after join + UDF  ({metrics['stage_sec']['transform']:.2f}s)")

print("\n--- Ray Data execution stats for the enriched dataset (look for Join and MapBatches(apply_speed_udf)) ---")
print("\n".join(l for l in enriched.stats().splitlines()
                if l.startswith("Operator") or "Remote wall time" in l or "* Output num rows" in l))

# ---------------------------------------------------------------------------------------------
# 4. EXPORT -- ML-ready Parquet on the shared directory
# ---------------------------------------------------------------------------------------------
t0 = time.time()
shutil.rmtree(f"{OUTPUT_DIR}/ray", ignore_errors=True)  # overwrite, like Spark's mode("overwrite")
transformed.write_parquet(f"{OUTPUT_DIR}/ray")
metrics["stage_sec"]["export"] = time.time() - t0
metrics["total_sec"] = time.time() - t_start
print(f"[export]   written to {OUTPUT_DIR}/ray  ({metrics['stage_sec']['export']:.2f}s)")
print(f"\nTOTAL wall-clock (ingest -> export): {metrics['total_sec']:.2f}s")

print("\nAverage speed per pickup hour:")
print(hourly.to_pandas().sort_values("pickup_hour").to_string(index=False))

# ---------------------------------------------------------------------------------------------
# 5. UDF DEEP-DIVE -- same speed rule, two ways, on the materialized cleaned dataset
#    native  : vectorised pandas expression (Ray's own fast path)     (baseline)
#    @udf    : the row-at-a-time Python function -- no JVM, no pickling per row
#    Python overhead = python_udf time - native time
# ---------------------------------------------------------------------------------------------
if not args.skip_udf_bench:
    del enriched, transformed                            # free object-store memory (like unpersist)

    # each batch returns ONE row (partial sum + non-null count), then the driver adds them up --
    # a single pass, like Spark's agg(sum, count)
    def partial_agg(s: pd.Series) -> pd.DataFrame:
        return pd.DataFrame({"sum": [s.sum()], "n": [int(s.notna().sum())]})

    def native_speed(df: pd.DataFrame) -> pd.DataFrame:
        return partial_agg(trip_speed_mph_vectorized(df["trip_distance"], df["duration_sec"]))

    def python_speed(df: pd.DataFrame) -> pd.DataFrame:
        return partial_agg(pd.Series([trip_speed_mph(d, s) for d, s in
                                      zip(df["trip_distance"].tolist(), df["duration_sec"].tolist())],
                                     dtype="float64"))

    variants = {"native": native_speed, "python_udf": python_speed}
    metrics["udf_sec"] = {}
    print("\n--- UDF deep-dive (cleaned rows:", f"{metrics['rows']['cleaned']:,}, best of 2)")
    for name, fn in variants.items():
        timings = []
        for _ in range(2):
            t0 = time.time()
            parts = cleaned.map_batches(fn, batch_format="pandas").to_pandas()
            total, n = parts["sum"].sum(), int(parts["n"].sum())
            timings.append(time.time() - t0)
        metrics["udf_sec"][name] = min(timings)
        print(f"  {name:11s} {metrics['udf_sec'][name]:7.2f}s   sum={total:.2f}  non-null={n:,}")
    overhead = metrics["udf_sec"]["python_udf"] - metrics["udf_sec"]["native"]
    print(f"  Python per-row overhead: {overhead:.2f}s "
          f"({100 * overhead / metrics['udf_sec']['python_udf']:.0f}% of the Python UDF time)")

# save metrics for the analysis notebook (one JSON line per run)
with open(f"{RESULTS_DIR}/ray_runs.jsonl", "a") as f:
    f.write(json.dumps(metrics) + "\n")
print(f"\nMetrics appended to {RESULTS_DIR}/ray_runs.jsonl")

ray.shutdown()
