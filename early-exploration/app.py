"""
Dash in 20 Minutes Tutorial + DuckLake
https://dash.plotly.com/tutorial
"""

import dash
from dash import dcc, html, Input, Output
import plotly.express as px
import plotly.graph_objects as go
import duckdb
import pandas as pd

# === DUCKLAKE SETUP ===
print("🚀 Setting up DuckLake...")
con = duckdb.connect()
con.execute("INSTALL ducklake; LOAD ducklake;")
# FIXED: Add DATA_PATH
con.execute("ATTACH 'ducklake:sqlite:./gapminder_ducklake.sqlite' AS gapminder (DATA_PATH './data/');")

# Create gapminder table only if it doesn't already exist
con.execute("USE gapminder;")
table_exists = con.sql("SELECT count(*) FROM information_schema.tables WHERE table_name = 'gapminder';").fetchone()[0] > 0

if not table_exists:
    con.execute("""
        CREATE TABLE gapminder (
            country VARCHAR,
            continent VARCHAR,
            year INTEGER,
            lifeExp DOUBLE,
            pop DOUBLE,
            gdpPercap DOUBLE
        );
    """)

    # Load Gapminder data (exact same as Dash tutorial)
    print("📊 Loading Gapminder data...")
    gapminder_url = 'https://raw.githubusercontent.com/plotly/datasets/master/gapminderDataFiveYear.csv'
    df = pd.read_csv(gapminder_url)

    con.execute("""
        INSERT INTO gapminder (country, continent, year, lifeExp, pop, gdpPercap)
        SELECT country, continent, year, lifeExp, pop, gdpPercap FROM df;
    """)
else:
    print("📊 Table already exists, skipping data load.")


print(f"✅ DuckLake ready! {con.sql('SELECT count(*) FROM gapminder.gapminder;').fetchone()[0]} rows loaded")
print("Files:", con.sql("SELECT path FROM __ducklake_metadata_gapminder.ducklake_data_file;").fetchdf())

# === DASH APP ===
app = dash.Dash(__name__)

app.layout = html.Div([  # ← FIXED: added missing [
    html.H1("Poplulation Dashboard with DuckLake", 
            style={'textAlign': 'center', 'color': '#1f77b4'}),
    
    # Dropdown (Step 1-2 from tutorial)
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
    
    # Graph (Step 3-4 from tutorial)
    dcc.Graph(id='graph'),
    
    html.Hr(),
    html.P("🦆 Powered by DuckLake (SQLite catalog + Parquet data)", 
           style={'textAlign': 'center'})
])  # ← FIXED: matching ]

# Callback (Step 5-6 from tutorial)
@app.callback(
    Output('graph', 'figure'),
    Input('dropdown', 'value')
)
def update_graph(continent):
    # QUERIES DUCKLAKE! (not Pandas CSV)
    df = con.execute("""
        SELECT country, year, lifeExp, gdpPercap, pop
        FROM gapminder.gapminder
        WHERE continent = $1
        ORDER BY country
    """, [continent]).fetchdf()
    
    # Exact same Plotly chart as tutorial
    fig = px.scatter(df, x="gdpPercap", y="lifeExp",
                     size="pop", color="country",
                     hover_name="country", log_x=True,
                     size_max=60, opacity=0.7)
    
    fig.update_layout(transition_duration=500,
                      title=f"Life Expectancy vs. GDP per Capita ({continent})")
    
    return fig

if __name__ == '__main__':
    app.run(host='0.0.0.0', debug=True, port=8050)
