"""
Dash in 20 Minutes Tutorial + Apache Iceberg
https://dash.plotly.com/tutorial

Parallel implementation to app.py (DuckLake) for thesis comparison.
Uses PyIceberg with SQLite catalog + Parquet data files.
"""

import dash
from dash import dcc, html, Input, Output
import plotly.express as px
import duckdb
import pandas as pd
import pyarrow as pa
from pyiceberg.catalog.sql import SqlCatalog

# === APACHE ICEBERG SETUP ===
print("🧊 Setting up Apache Iceberg...")

CATALOG_DB = "gapminder_iceberg.sqlite"
WAREHOUSE_PATH = "./iceberg_data"

catalog = SqlCatalog("gapminder_iceberg", **{
    "type": "sql",
    "uri": f"sqlite:///{CATALOG_DB}",
    "warehouse": WAREHOUSE_PATH,
})

# Create namespace if it doesn't exist
existing_namespaces = catalog.list_namespaces()
if ("main",) not in existing_namespaces:
    catalog.create_namespace("main")

# Create table if it doesn't exist
existing_tables = catalog.list_tables("main")
if ("main", "gapminder") not in existing_tables:
    schema = pa.schema([
        pa.field("country", pa.string()),
        pa.field("continent", pa.string()),
        pa.field("year", pa.int32()),
        pa.field("lifeExp", pa.float64()),
        pa.field("pop", pa.float64()),
        pa.field("gdpPercap", pa.float64()),
    ])
    table = catalog.create_table("main.gapminder", schema=schema)

    # Load Gapminder data (exact same as Dash tutorial)
    print("📊 Loading Gapminder data...")
    gapminder_url = 'https://raw.githubusercontent.com/plotly/datasets/master/gapminderDataFiveYear.csv'
    df = pd.read_csv(gapminder_url)

    # Convert to PyArrow and append to Iceberg table
    arrow_table = pa.Table.from_pandas(df, schema=schema)
    table.append(arrow_table)
else:
    table = catalog.load_table("main.gapminder")
    print("📊 Table already exists, skipping data load.")

# Set up DuckDB connection for querying Iceberg
con = duckdb.connect()

# Get the table's metadata location for querying
metadata_location = table.metadata_location

row_count = con.execute(
    "SELECT count(*) FROM iceberg_scan($1)",
    [metadata_location]
).fetchone()[0]
print(f"✅ Iceberg ready! {row_count} rows loaded")

# List data files from Iceberg metadata
scan = table.scan()
print(f"Snapshots: {[s.snapshot_id for s in table.metadata.snapshots]}")

# === DASH APP ===
app = dash.Dash(__name__)

app.layout = html.Div([
    html.H1("Population Dashboard with Apache Iceberg",
            style={'textAlign': 'center', 'color': '#e34a33'}),

    # Dropdown
    html.Label("Choose a continent:", style={'fontSize': 20}),
    dcc.Dropdown(
        id='dropdown',
        options=[
            {'label': 'Africa', 'value': 'Africa'},
            {'label': 'Americas', 'value': 'Americas'},
            {'label': 'Asia', 'value': 'Asia'},
            {'label': 'Europe', 'value': 'Europe'},
            {'label': 'Oceania', 'value': 'Oceania'}
        ],
        value='Asia'
    ),

    # Graph
    dcc.Graph(id='graph'),

    html.Hr(),
    html.P("🧊 Powered by Apache Iceberg (SQLite catalog + Parquet data)",
           style={'textAlign': 'center'})
])

# Callback
@app.callback(
    Output('graph', 'figure'),
    Input('dropdown', 'value')
)
def update_graph(continent):
    # Query Iceberg via DuckDB iceberg_scan
    df = con.execute("""
        SELECT country, year, lifeExp, gdpPercap, pop
        FROM iceberg_scan($1)
        WHERE continent = $2
        ORDER BY country
    """, [metadata_location, continent]).fetchdf()

    fig = px.scatter(df, x="gdpPercap", y="lifeExp",
                     size="pop", color="country",
                     hover_name="country", log_x=True,
                     size_max=60, opacity=0.7)

    fig.update_layout(transition_duration=500,
                      title=f"Life Expectancy vs. GDP per Capita ({continent})")

    return fig

if __name__ == '__main__':
    app.run(host='0.0.0.0', debug=True, port=8051)
