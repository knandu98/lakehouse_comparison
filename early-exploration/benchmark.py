"""
Benchmark: DuckLake vs Apache Iceberg vs Delta Lake
====================================================
Measures:
  T1  Cold filter query latency
  T2  Warm filter query latency (median of 10 runs)
  T3  Cold aggregation query latency
  T4  Warm aggregation query latency (median of 10 runs)
  T5  Append 100 rows latency
  M1  Data file count before/after append
  M2  Total metadata size on disk

All three formats use the same Gapminder dataset (1704 rows)
and query via DuckDB where possible for engine fairness.
"""

import gc
import json
import os
import shutil
import statistics
import time
from pathlib import Path

import duckdb
import pandas as pd
import pyarrow as pa

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
GAPMINDER_URL = "https://raw.githubusercontent.com/plotly/datasets/master/gapminderDataFiveYear.csv"
RUNS = 10
APPEND_ROWS = 100
CONTINENT = "Asia"

RESULTS: dict[str, dict] = {}

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def dir_size(path: str | Path) -> int:
    """Total bytes of all files under path."""
    total = 0
    for dirpath, _, filenames in os.walk(path):
        for f in filenames:
            total += os.path.getsize(os.path.join(dirpath, f))
    return total


def count_files(path: str | Path, ext: str = ".parquet") -> int:
    """Count files with given extension under path."""
    return sum(1 for _ in Path(path).rglob(f"*{ext}"))


def synthetic_rows(n: int) -> pd.DataFrame:
    """Generate n synthetic gapminder-like rows."""
    return pd.DataFrame({
        "country": [f"BenchCountry{i}" for i in range(n)],
        "continent": ["BenchContinent"] * n,
        "year": [2025] * n,
        "lifeExp": [75.0 + (i % 10) for i in range(n)],
        "pop": [1_000_000.0 * (i + 1) for i in range(n)],
        "gdpPercap": [30_000.0 + i for i in range(n)],
    })


def ms(seconds: float) -> float:
    return round(seconds * 1000, 2)


# ---------------------------------------------------------------------------
# Load source data once
# ---------------------------------------------------------------------------
print("Downloading Gapminder CSV …")
source_df = pd.read_csv(GAPMINDER_URL)
append_df = synthetic_rows(APPEND_ROWS)
print(f"Source: {len(source_df)} rows   Append batch: {len(append_df)} rows\n")

# ===================================================================
# 1. DUCKLAKE
# ===================================================================
print("=" * 60)
print("DUCKLAKE")
print("=" * 60)

dl_res: dict[str, object] = {}

# --- Setup ---
t0 = time.perf_counter()
dl_con = duckdb.connect()
dl_con.execute("INSTALL ducklake; LOAD ducklake;")
dl_con.execute("ATTACH 'ducklake:sqlite:./gapminder_ducklake.sqlite' AS gapminder (DATA_PATH './data/');")
dl_con.execute("USE gapminder;")
dl_con.execute("""
    CREATE TABLE gapminder (
        country VARCHAR, continent VARCHAR, year INTEGER,
        lifeExp DOUBLE, pop DOUBLE, gdpPercap DOUBLE
    );
""")
dl_con.execute("""
    INSERT INTO gapminder (country, continent, year, lifeExp, pop, gdpPercap)
    SELECT country, continent, year, lifeExp, pop, gdpPercap FROM source_df;
""")
dl_res["setup_ms"] = ms(time.perf_counter() - t0)
dl_res["row_count"] = dl_con.execute("SELECT count(*) FROM gapminder.gapminder").fetchone()[0]
print(f"  Setup: {dl_res['setup_ms']} ms  rows: {dl_res['row_count']}")

# --- T1: Cold filter ---
gc.collect()
t0 = time.perf_counter()
dl_con.execute("SELECT country, year, lifeExp, gdpPercap, pop FROM gapminder.gapminder WHERE continent = $1 ORDER BY country", [CONTINENT]).fetchdf()
dl_res["cold_filter_ms"] = ms(time.perf_counter() - t0)

# --- T2: Warm filter ---
times = []
for _ in range(RUNS):
    t0 = time.perf_counter()
    dl_con.execute("SELECT country, year, lifeExp, gdpPercap, pop FROM gapminder.gapminder WHERE continent = $1 ORDER BY country", [CONTINENT]).fetchdf()
    times.append(time.perf_counter() - t0)
dl_res["warm_filter_ms"] = ms(statistics.median(times))

# --- T3: Cold aggregation ---
gc.collect()
t0 = time.perf_counter()
dl_con.execute("SELECT year, AVG(lifeExp) AS avg_life FROM gapminder.gapminder WHERE continent = $1 GROUP BY year ORDER BY year", [CONTINENT]).fetchdf()
dl_res["cold_agg_ms"] = ms(time.perf_counter() - t0)

# --- T4: Warm aggregation ---
times = []
for _ in range(RUNS):
    t0 = time.perf_counter()
    dl_con.execute("SELECT year, AVG(lifeExp) AS avg_life FROM gapminder.gapminder WHERE continent = $1 GROUP BY year ORDER BY year", [CONTINENT]).fetchdf()
    times.append(time.perf_counter() - t0)
dl_res["warm_agg_ms"] = ms(statistics.median(times))

# --- M1 before append ---
dl_res["data_files_before"] = count_files("./data")
dl_res["metadata_size_before"] = dir_size("./gapminder_ducklake.sqlite")

# --- T5: Append ---
gc.collect()
t0 = time.perf_counter()
dl_con.execute("""
    INSERT INTO gapminder.gapminder (country, continent, year, lifeExp, pop, gdpPercap)
    SELECT country, continent, year, lifeExp, pop, gdpPercap FROM append_df;
""")
dl_res["append_ms"] = ms(time.perf_counter() - t0)

# --- M1/M2 after append ---
dl_res["data_files_after"] = count_files("./data")
dl_res["metadata_size_after"] = dir_size("./gapminder_ducklake.sqlite")
dl_res["total_size_after"] = dir_size("./data") + dir_size("./gapminder_ducklake.sqlite")
dl_res["row_count_after"] = dl_con.execute("SELECT count(*) FROM gapminder.gapminder").fetchone()[0]

dl_con.close()
RESULTS["DuckLake"] = dl_res
print(f"  Done: {dl_res}\n")

# ===================================================================
# 2. APACHE ICEBERG
# ===================================================================
print("=" * 60)
print("APACHE ICEBERG")
print("=" * 60)

from pyiceberg.catalog.sql import SqlCatalog

ic_res: dict[str, object] = {}

# --- Setup ---
t0 = time.perf_counter()
catalog = SqlCatalog("gapminder_iceberg", **{
    "type": "sql",
    "uri": "sqlite:///gapminder_iceberg.sqlite",
    "warehouse": "./iceberg_data",
})
catalog.create_namespace("main")
schema = pa.schema([
    pa.field("country", pa.string()),
    pa.field("continent", pa.string()),
    pa.field("year", pa.int32()),
    pa.field("lifeExp", pa.float64()),
    pa.field("pop", pa.float64()),
    pa.field("gdpPercap", pa.float64()),
])
ic_table = catalog.create_table("main.gapminder", schema=schema)
arrow_table = pa.Table.from_pandas(source_df, schema=schema)
ic_table.append(arrow_table)
ic_meta = ic_table.metadata_location

ic_con = duckdb.connect()
ic_res["setup_ms"] = ms(time.perf_counter() - t0)
ic_res["row_count"] = ic_con.execute("SELECT count(*) FROM iceberg_scan($1)", [ic_meta]).fetchone()[0]
print(f"  Setup: {ic_res['setup_ms']} ms  rows: {ic_res['row_count']}")

# --- T1: Cold filter ---
gc.collect()
t0 = time.perf_counter()
ic_con.execute("SELECT country, year, lifeExp, gdpPercap, pop FROM iceberg_scan($1) WHERE continent = $2 ORDER BY country", [ic_meta, CONTINENT]).fetchdf()
ic_res["cold_filter_ms"] = ms(time.perf_counter() - t0)

# --- T2: Warm filter ---
times = []
for _ in range(RUNS):
    t0 = time.perf_counter()
    ic_con.execute("SELECT country, year, lifeExp, gdpPercap, pop FROM iceberg_scan($1) WHERE continent = $2 ORDER BY country", [ic_meta, CONTINENT]).fetchdf()
    times.append(time.perf_counter() - t0)
ic_res["warm_filter_ms"] = ms(statistics.median(times))

# --- T3: Cold aggregation ---
gc.collect()
t0 = time.perf_counter()
ic_con.execute("SELECT year, AVG(lifeExp) AS avg_life FROM iceberg_scan($1) WHERE continent = $2 GROUP BY year ORDER BY year", [ic_meta, CONTINENT]).fetchdf()
ic_res["cold_agg_ms"] = ms(time.perf_counter() - t0)

# --- T4: Warm aggregation ---
times = []
for _ in range(RUNS):
    t0 = time.perf_counter()
    ic_con.execute("SELECT year, AVG(lifeExp) AS avg_life FROM iceberg_scan($1) WHERE continent = $2 GROUP BY year ORDER BY year", [ic_meta, CONTINENT]).fetchdf()
    times.append(time.perf_counter() - t0)
ic_res["warm_agg_ms"] = ms(statistics.median(times))

# --- M1 before append ---
ic_res["data_files_before"] = count_files("./iceberg_data")
ic_res["metadata_size_before"] = dir_size("./iceberg_data/main/gapminder/metadata") + dir_size("./gapminder_iceberg.sqlite")

# --- T5: Append ---
gc.collect()
append_arrow = pa.Table.from_pandas(append_df, schema=schema)
t0 = time.perf_counter()
ic_table.append(append_arrow)
ic_res["append_ms"] = ms(time.perf_counter() - t0)

# Reload metadata location after append
ic_table = catalog.load_table("main.gapminder")
ic_meta_new = ic_table.metadata_location

# --- M1/M2 after append ---
ic_res["data_files_after"] = count_files("./iceberg_data")
ic_res["metadata_size_after"] = dir_size("./iceberg_data/main/gapminder/metadata") + dir_size("./gapminder_iceberg.sqlite")
ic_res["total_size_after"] = dir_size("./iceberg_data") + dir_size("./gapminder_iceberg.sqlite")
ic_res["row_count_after"] = ic_con.execute("SELECT count(*) FROM iceberg_scan($1)", [ic_meta_new]).fetchone()[0]

ic_con.close()
RESULTS["Iceberg"] = ic_res
print(f"  Done: {ic_res}\n")

# ===================================================================
# 3. DELTA LAKE
# ===================================================================
print("=" * 60)
print("DELTA LAKE")
print("=" * 60)

from deltalake import DeltaTable, write_deltalake

dt_res: dict[str, object] = {}
DELTA_PATH = "./delta_data/gapminder"

# --- Setup ---
t0 = time.perf_counter()
Path(DELTA_PATH).mkdir(parents=True, exist_ok=True)
write_deltalake(DELTA_PATH, pa.Table.from_pandas(source_df, preserve_index=False), mode="overwrite")
dt_obj = DeltaTable(DELTA_PATH)

dt_con = duckdb.connect()
use_duckdb_delta = False
try:
    dt_con.execute("INSTALL delta; LOAD delta;")
    use_duckdb_delta = True
except Exception:
    pass

dt_res["setup_ms"] = ms(time.perf_counter() - t0)
dt_res["query_engine"] = "duckdb_delta_scan" if use_duckdb_delta else "delta-rs"

if use_duckdb_delta:
    dt_res["row_count"] = dt_con.execute("SELECT count(*) FROM delta_scan($1)", [DELTA_PATH]).fetchone()[0]
else:
    dt_res["row_count"] = dt_obj.to_pyarrow_table().num_rows
print(f"  Setup: {dt_res['setup_ms']} ms  rows: {dt_res['row_count']}  engine: {dt_res['query_engine']}")


def delta_filter_query():
    if use_duckdb_delta:
        return dt_con.execute(
            "SELECT country, year, lifeExp, gdpPercap, pop FROM delta_scan($1) WHERE continent = $2 ORDER BY country",
            [DELTA_PATH, CONTINENT],
        ).fetchdf()
    return (
        dt_obj.to_pyarrow_table(filters=[("continent", "=", CONTINENT)])
        .to_pandas()[["country", "year", "lifeExp", "gdpPercap", "pop"]]
        .sort_values("country")
    )


def delta_agg_query():
    if use_duckdb_delta:
        return dt_con.execute(
            "SELECT year, AVG(lifeExp) AS avg_life FROM delta_scan($1) WHERE continent = $2 GROUP BY year ORDER BY year",
            [DELTA_PATH, CONTINENT],
        ).fetchdf()
    tbl = dt_obj.to_pyarrow_table(filters=[("continent", "=", CONTINENT)]).to_pandas()
    return tbl.groupby("year")["lifeExp"].mean().reset_index().sort_values("year")


# --- T1: Cold filter ---
gc.collect()
t0 = time.perf_counter()
delta_filter_query()
dt_res["cold_filter_ms"] = ms(time.perf_counter() - t0)

# --- T2: Warm filter ---
times = []
for _ in range(RUNS):
    t0 = time.perf_counter()
    delta_filter_query()
    times.append(time.perf_counter() - t0)
dt_res["warm_filter_ms"] = ms(statistics.median(times))

# --- T3: Cold aggregation ---
gc.collect()
t0 = time.perf_counter()
delta_agg_query()
dt_res["cold_agg_ms"] = ms(time.perf_counter() - t0)

# --- T4: Warm aggregation ---
times = []
for _ in range(RUNS):
    t0 = time.perf_counter()
    delta_agg_query()
    times.append(time.perf_counter() - t0)
dt_res["warm_agg_ms"] = ms(statistics.median(times))

# --- M1 before append ---
dt_res["data_files_before"] = count_files(DELTA_PATH)
dt_res["metadata_size_before"] = dir_size(Path(DELTA_PATH) / "_delta_log")

# --- T5: Append ---
gc.collect()
append_arrow_dt = pa.Table.from_pandas(append_df, preserve_index=False)
t0 = time.perf_counter()
write_deltalake(DELTA_PATH, append_arrow_dt, mode="append")
dt_res["append_ms"] = ms(time.perf_counter() - t0)

# Reload
dt_obj = DeltaTable(DELTA_PATH)

# --- M1/M2 after append ---
dt_res["data_files_after"] = count_files(DELTA_PATH)
dt_res["metadata_size_after"] = dir_size(Path(DELTA_PATH) / "_delta_log")
dt_res["total_size_after"] = dir_size(DELTA_PATH)
if use_duckdb_delta:
    dt_res["row_count_after"] = dt_con.execute("SELECT count(*) FROM delta_scan($1)", [DELTA_PATH]).fetchone()[0]
else:
    dt_res["row_count_after"] = dt_obj.to_pyarrow_table().num_rows

dt_con.close()
RESULTS["Delta"] = dt_res
print(f"  Done: {dt_res}\n")

# ===================================================================
# RESULTS TABLE
# ===================================================================
print("=" * 60)
print("BENCHMARK RESULTS")
print("=" * 60)

header = f"{'Metric':<30} {'DuckLake':>12} {'Iceberg':>12} {'Delta':>12}"
print(header)
print("-" * len(header))

metrics = [
    ("Setup (ms)",              "setup_ms"),
    ("Row count",               "row_count"),
    ("Cold filter (ms)",        "cold_filter_ms"),
    ("Warm filter median (ms)", "warm_filter_ms"),
    ("Cold aggregation (ms)",   "cold_agg_ms"),
    ("Warm agg median (ms)",    "warm_agg_ms"),
    ("Append 100 rows (ms)",    "append_ms"),
    ("Data files before",       "data_files_before"),
    ("Data files after",        "data_files_after"),
    ("Metadata size before (B)","metadata_size_before"),
    ("Metadata size after (B)", "metadata_size_after"),
    ("Total size after (B)",    "total_size_after"),
    ("Row count after",         "row_count_after"),
]

for label, key in metrics:
    vals = []
    for fmt in ["DuckLake", "Iceberg", "Delta"]:
        v = RESULTS[fmt].get(key, "N/A")
        vals.append(str(v))
    print(f"{label:<30} {vals[0]:>12} {vals[1]:>12} {vals[2]:>12}")

# Save raw JSON
with open("benchmark_results.json", "w") as f:
    json.dump(RESULTS, f, indent=2, default=str)
print(f"\nRaw results saved to benchmark_results.json")
