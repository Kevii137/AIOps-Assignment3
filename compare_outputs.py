# Pipeline-parity check: do spark_clean.py and ray_clean.py produce the same table?
# Streams both Parquet outputs in batches (never holds 17.8M rows x 2 in memory).
#   run on the host:  .venv/bin/python compare_outputs.py
import sys

import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.dataset as ds

sys.path.insert(0, "apps")
from pipeline_common import OUTPUT_COLS  # noqa: E402

OUT = {"spark": "data/output/spark", "ray": "data/output/ray"}
# avg_speed_hour is checked separately: Spark's round() is HALF_UP, pandas' is half-even,
# and the two engines sum the averages in a different order
ROW_COLS = [c for c in OUTPUT_COLS if c != "avg_speed_hour"]
NUMERIC = ["passenger_count", "trip_distance", "fare_amount", "tip_amount", "total_amount",
           "duration_sec", "speed_mph"]


def normalise(batch: pa.RecordBatch) -> pd.DataFrame:
    """Same logical value -> same Python value, whichever engine wrote it."""
    cols = {}
    for name in ROW_COLS:
        col = batch.column(name)
        if pa.types.is_timestamp(col.type):              # Spark: UTC-adjusted; Ray: naive
            col = pc.cast(col, pa.timestamp("us", tz=None)) if col.type.tz is None else \
                  pc.cast(pc.local_timestamp(col), pa.timestamp("us"))
        if pa.types.is_large_string(col.type):
            col = pc.cast(col, pa.string())
        cols[name] = col
    df = pa.table(cols).to_pandas()
    df["speed_mph"] = df["speed_mph"].round(6)
    return df


def profile(engine: str):
    dset = ds.dataset(OUT[engine], format="parquet")
    rows, fingerprint = 0, np.uint64(0)
    sums = dict.fromkeys(NUMERIC, 0.0)
    for batch in dset.to_batches(columns=ROW_COLS, batch_size=500_000):
        df = normalise(batch)
        rows += len(df)
        # order-independent fingerprint of the whole row multiset (uint64 sum wraps mod 2^64)
        fingerprint += pd.util.hash_pandas_object(df[ROW_COLS], index=False).to_numpy().sum(dtype=np.uint64)
        for c in NUMERIC:
            sums[c] += float(df[c].sum())
    hourly = (dset.to_table(columns=["pickup_hour", "avg_speed_hour"]).group_by("pickup_hour")
              .aggregate([("avg_speed_hour", "max")]).to_pandas()
              .set_index("pickup_hour")["avg_speed_hour_max"].sort_index())
    return {"schema": sorted(dset.schema.names), "rows": rows, "fingerprint": fingerprint,
            "sums": sums, "hourly": hourly}


with np.errstate(over="ignore"):
    s, r = profile("spark"), profile("ray")

checks = [
    ("same column names", s["schema"] == r["schema"]),
    (f"row count  spark={s['rows']:,}  ray={r['rows']:,}", s["rows"] == r["rows"]),
]
for c in NUMERIC:
    checks.append((f"sum({c})  spark={s['sums'][c]:.2f}  ray={r['sums'][c]:.2f}",
                   abs(s["sums"][c] - r["sums"][c]) <= 1e-6 * max(1.0, abs(s["sums"][c]))))
checks.append((f"row-multiset fingerprint  spark={s['fingerprint']}  ray={r['fingerprint']}",
               s["fingerprint"] == r["fingerprint"]))
diff = (s["hourly"] - r["hourly"]).abs().max()
checks.append((f"avg_speed_hour, 24 hours, max abs difference = {diff:.6f} (tolerance 0.001)", diff <= 1e-3))

print("Pipeline parity: Spark output vs Ray output\n")
for text, ok in checks:
    print(f"  [{'MATCH' if ok else 'DIFF '}]  {text}")
print("\nRESULT:", "MATCH -- identical outputs" if all(ok for _, ok in checks) else "DIFFERENCES FOUND")
