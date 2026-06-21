"""
Dash in 20 Minutes Tutorial + Delta Lake
https://dash.plotly.com/tutorial

Parallel implementation to app.py (DuckLake) and app_iceberg.py (Iceberg)
for thesis comparison.
"""

from pathlib import Path

import dash
from dash import Input, Output, dcc, html
import duckdb
from deltalake import DeltaTable, write_deltalake
import pandas as pd
import plotly.express as px
import pyarrow as pa

# === DELTA LAKE SETUP ===
print("Setting up Delta Lake...")

DELTA_TABLE_PATH = Path("./delta_data/gapminder")
DELTA_LOG_PATH = DELTA_TABLE_PATH / "_delta_log"

if not DELTA_LOG_PATH.exists():
    DELTA_TABLE_PATH.mkdir(parents=True, exist_ok=True)

    # Load Gapminder data (same source as the other implementations)
    print("Loading Gapminder data...")
    gapminder_url = "https://raw.githubusercontent.com/plotly/datasets/master/gapminderDataFiveYear.csv"
    df = pd.read_csv(gapminder_url)

    # Write initial Delta table
    write_deltalake(
        str(DELTA_TABLE_PATH),
        pa.Table.from_pandas(df, preserve_index=False),
        mode="overwrite",
    )
else:
    print("Table already exists, skipping data load.")

dt = DeltaTable(str(DELTA_TABLE_PATH))

# Prefer DuckDB delta_scan for SQL parity with the other apps.
con = duckdb.connect()
use_duckdb_delta = False
try:
    con.execute("INSTALL delta; LOAD delta;")
    use_duckdb_delta = True
except Exception:
    # Fallback to Delta-RS reads if DuckDB delta extension is unavailable.
    use_duckdb_delta = False


def query_delta(continent: str) -> pd.DataFrame:
    if use_duckdb_delta:
        return con.execute(
            """
            SELECT country, year, lifeExp, gdpPercap, pop
            FROM delta_scan($1)
            WHERE continent = $2
            ORDER BY country
            """,
            [str(DELTA_TABLE_PATH), continent],
        ).fetchdf()

    filt = [("continent", "=", continent)]
    return dt.to_pyarrow_table(filters=filt).to_pandas()[
        ["country", "year", "lifeExp", "gdpPercap", "pop"]
    ].sort_values("country")


if use_duckdb_delta:
    row_count = con.execute("SELECT count(*) FROM delta_scan($1)", [str(DELTA_TABLE_PATH)]).fetchone()[0]
else:
    row_count = dt.to_pyarrow_table().num_rows

print(f"Delta Lake ready! {row_count} rows loaded")
print(f"Delta table version: {dt.version()}")

# === DASH APP ===
app = dash.Dash(__name__)

app.layout = html.Div(
    [
        html.H1(
            "Population Dashboard with Delta Lake",
            style={"textAlign": "center", "color": "#2b8cbe"},
        ),
        html.Label("Choose a continent:", style={"fontSize": 20}),
        dcc.Dropdown(
            id="dropdown",
            options=[
                {"label": "Africa", "value": "Africa"},
                {"label": "Americas", "value": "Americas"},
                {"label": "Asia", "value": "Asia"},
                {"label": "Europe", "value": "Europe"},
                {"label": "Oceania", "value": "Oceania"},
            ],
            value="Asia",
        ),
        dcc.Graph(id="graph"),
        html.Hr(),
        html.P(
            "Powered by Delta Lake (_delta_log + Parquet data)",
            style={"textAlign": "center"},
        ),
    ]
)


@app.callback(Output("graph", "figure"), Input("dropdown", "value"))
def update_graph(continent: str):
    df = query_delta(continent)

    fig = px.scatter(
        df,
        x="gdpPercap",
        y="lifeExp",
        size="pop",
        color="country",
        hover_name="country",
        log_x=True,
        size_max=60,
        opacity=0.7,
    )

    fig.update_layout(
        transition_duration=500,
        title=f"Life Expectancy vs. GDP per Capita ({continent})",
    )
    return fig


if __name__ == "__main__":
    app.run(host="0.0.0.0", debug=True, port=8052)
