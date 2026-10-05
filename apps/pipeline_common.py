# Shared definitions imported by BOTH spark_clean.py and ray_clean.py.
# Keeping the column lists, cleaning thresholds and the Python UDF in ONE file is what
# guarantees "pipeline parity" -- neither engine can drift from the other.
import math

DATA_DIR = "/opt/spark-data/raw"          # shared directory, bind-mounted into every container
OUTPUT_DIR = "/opt/spark-data/output"
RESULTS_DIR = "/opt/spark-results"
ZONES_FILE = f"{DATA_DIR}/taxi_zone_lookup.csv"


def trip_files(months):
    return [f"{DATA_DIR}/yellow_tripdata_2024-{m:02d}.parquet" for m in months]


def parse_months(arg):
    """'1' -> [1]   '1-6' -> [1..6]"""
    lo, _, hi = arg.partition("-")
    return list(range(int(lo), int(hi or lo) + 1))


# --- Ingestion: explicit column list + types (protects against month-to-month schema drift) ---
SELECT_COLS = {
    "VendorID": "int",
    "tpep_pickup_datetime": "timestamp",
    "tpep_dropoff_datetime": "timestamp",
    "passenger_count": "bigint",
    "trip_distance": "double",
    "RatecodeID": "bigint",
    "PULocationID": "int",
    "DOLocationID": "int",
    "payment_type": "bigint",
    "fare_amount": "double",
    "tip_amount": "double",
    "total_amount": "double",
}

# --- Cleansing rules ---
KEY_COLS = ["tpep_pickup_datetime", "tpep_dropoff_datetime", "PULocationID", "DOLocationID",
            "trip_distance", "passenger_count", "fare_amount"]
MAX_DURATION_SEC = 6 * 3600              # trips longer than 6 h are meter errors
MAX_SPEED_MPH = 100.0                    # faster than this is a GPS/meter error


def month_range(months):
    """[start, end) pickup window as 'YYYY-MM-DD' strings for the requested months."""
    start = f"2024-{months[0]:02d}-01"
    end = f"2024-{months[-1] + 1:02d}-01" if months[-1] < 12 else "2025-01-01"
    return start, end


# --- Final schema written to Parquet (same column order from both engines) ---
OUTPUT_COLS = [
    "VendorID", "tpep_pickup_datetime", "tpep_dropoff_datetime", "pickup_date", "pickup_hour",
    "duration_sec", "passenger_count", "trip_distance", "RatecodeID", "payment_type",
    "fare_amount", "tip_amount", "total_amount",
    "PULocationID", "PU_Borough", "PU_Zone", "DOLocationID", "DO_Borough", "DO_Zone",
    "speed_mph", "avg_speed_hour",
]


# --- The custom Python UDF (row-at-a-time, pure Python on purpose) ---
def trip_speed_mph(distance_miles, duration_sec):
    """Average speed of one trip in miles per hour; None if the trip is physically implausible."""
    if distance_miles is None or duration_sec is None or duration_sec <= 0:
        return None
    mph = distance_miles / (duration_sec / 3600.0)
    if math.isnan(mph) or mph > MAX_SPEED_MPH:
        return None
    return round(mph, 4)


# --- Vectorised variant, used ONLY as Ray's 'native' baseline in its UDF deep-dive ---
def trip_speed_mph_vectorized(distance, duration):
    """Same rule as trip_speed_mph, but on whole pandas Series at once."""
    mph = (distance / (duration / 3600.0)).round(4)
    return mph.where((duration > 0) & (mph <= MAX_SPEED_MPH))
