import sqlite3
from pathlib import Path
import pandas as pd
from datetime import datetime, timedelta

# Define the path for the database in a dedicated data directory
DB_PATH = Path(__file__).parent.parent.parent / "data" / "forecasts.db"
DB_PATH.parent.mkdir(parents=True, exist_ok=True)

def get_db_connection() -> sqlite3.Connection:
    """Establishes a connection to the SQLite database."""
    conn = sqlite3.connect(DB_PATH)
    conn.execute("PRAGMA foreign_keys = ON;")  # Enforce foreign key constraints
    conn.row_factory = sqlite3.Row
    return conn

def create_tables():
    """Creates the necessary tables in the database if they don't already exist."""
    conn = get_db_connection()
    cursor = conn.cursor()

    # Table for forecast runs to group all data from a single execution
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS forecast_runs (
            model_name TEXT NOT NULL,
            run_timestamp DATETIME NOT NULL,
            PRIMARY KEY (model_name, run_timestamp)
        );
    """)

    # Table for raw forecast data from each ensemble member (long format)
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS raw_forecast_data (
            data_id INTEGER PRIMARY KEY AUTOINCREMENT,
            model_name TEXT NOT NULL,
            run_timestamp DATETIME NOT NULL,
            member TEXT NOT NULL,
            variable TEXT NOT NULL,
            forecast_timestamp DATETIME NOT NULL,
            value REAL NOT NULL,
            FOREIGN KEY (model_name, run_timestamp) REFERENCES forecast_runs (model_name, run_timestamp) ON DELETE CASCADE,
            UNIQUE(model_name, run_timestamp, member, variable, forecast_timestamp)
        );
    """)

    # Table for pre-calculated statistical forecasts (long format)
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS statistical_forecasts (
            stat_id INTEGER PRIMARY KEY AUTOINCREMENT,
            model_name TEXT NOT NULL,
            run_timestamp DATETIME NOT NULL,
            variable TEXT NOT NULL,
            statistic TEXT NOT NULL,
            forecast_timestamp DATETIME NOT NULL,
            value REAL,
            FOREIGN KEY (model_name, run_timestamp) REFERENCES forecast_runs (model_name, run_timestamp) ON DELETE CASCADE,
            UNIQUE(model_name, run_timestamp, variable, statistic, forecast_timestamp)
        );
    """)

    conn.commit()
    conn.close()

if __name__ == '__main__':
    # This allows us to initialize the database by running the script directly
    print(f"Initializing database at {DB_PATH}...")
    create_tables()
    print("Database and tables created successfully.")

def purge_old_runs(retention_days: int):
    """Removes forecast runs and their associated data older than the specified retention period."""
    conn = get_db_connection()
    cursor = conn.cursor()

    # Calculate the cutoff date
    cutoff_date = datetime.now() - timedelta(days=retention_days)

    # Find old runs
    cursor.execute("SELECT model_name, run_timestamp FROM forecast_runs WHERE run_timestamp < ?", (cutoff_date.isoformat(),))
    old_runs = cursor.fetchall()

    if not old_runs:
        print("No old forecast runs to purge.")
        conn.close()
        return

    print(f"Found {len(old_runs)} old run(s) to purge.")

    # Delete the old forecast runs. Associated data will be deleted automatically due to ON DELETE CASCADE.
    cursor.executemany("DELETE FROM forecast_runs WHERE model_name = ? AND run_timestamp = ?", old_runs)

    conn.commit()
    conn.close()
    print(f"Successfully purged {len(old_runs)} old forecast run(s).")


def get_last_run_timestamp(model_name: str) -> datetime | None:
    """
    Retrieves the most recent run timestamp for a given model from the database.

    Args:
        model_name: The name of the model.

    Returns:
        A datetime object of the last run, or None if no run is found.
    """
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute(
        "SELECT MAX(run_timestamp) FROM forecast_runs WHERE model_name = ?",
        (model_name,)
    )
    result = cursor.fetchone()
    conn.close()

    if result and result[0]:
        return datetime.fromisoformat(result[0])
    return None

def load_raw_data(model_name: str, run_timestamp: datetime) -> dict[str, pd.DataFrame]:
    """
    Loads raw forecast data for a specific model and run from the database.
    """
    conn = get_db_connection()
    raw_data_query = """
        SELECT forecast_timestamp, member, variable, value
        FROM raw_forecast_data
        WHERE model_name = ? AND run_timestamp = ?
    """
    raw_df_long = pd.read_sql_query(raw_data_query, conn, params=(model_name, run_timestamp.isoformat()), parse_dates=['forecast_timestamp'])
    conn.close()

    data = {}
    if not raw_df_long.empty:
        for variable in raw_df_long['variable'].unique():
            variable_df = raw_df_long[raw_df_long['variable'] == variable]
            pivot_df = variable_df.pivot(
                index='forecast_timestamp',
                columns='member',
                values='value'
            ).add_prefix('member')
            pivot_df.index.name = 'date'
            pivot_df.columns.name = None
            data[variable] = pivot_df
    return data

def load_statistics(model_name: str, run_timestamp: datetime) -> dict[str, pd.DataFrame]:
    """
    Loads statistical forecast data for a specific model and run from the database.
    """
    conn = get_db_connection()
    stats_query = """
        SELECT forecast_timestamp, variable, statistic, value
        FROM statistical_forecasts
        WHERE model_name = ? AND run_timestamp = ?
    """
    stats_df_long = pd.read_sql_query(stats_query, conn, params=(model_name, run_timestamp.isoformat()), parse_dates=['forecast_timestamp'])
    conn.close()

    statistics = {}
    if not stats_df_long.empty:
        for variable in stats_df_long['variable'].unique():
            variable_stats_df = stats_df_long[stats_df_long['variable'] == variable]
            pivot_stats_df = variable_stats_df.pivot(
                index='forecast_timestamp',
                columns='statistic',
                values='value'
            )
            pivot_stats_df.index.name = 'date'
            pivot_stats_df.columns.name = None
            statistics[variable] = pivot_stats_df
    return statistics

def save_forecast_run(conn: sqlite3.Connection, model_name: str, run_timestamp: datetime):
    """
    Saves a forecast run entry to the database.
    """
    cursor = conn.cursor()
    cursor.execute("INSERT INTO forecast_runs (model_name, run_timestamp) VALUES (?, ?)", (model_name, run_timestamp.isoformat()))
    print(f"Forecast run for {model_name} at {run_timestamp} recorded.")

def save_raw_data(conn: sqlite3.Connection, model_name: str, run_timestamp: datetime, data: dict[str, pd.DataFrame]):
    """
    Saves raw forecast data to the database.
    """
    cursor = conn.cursor()
    for variable, data_df in data.items():
        if data_df is None:
            continue

        melted_df = data_df.reset_index().melt(
            id_vars=['date'],
            var_name='member',
            value_name='value'
        )
        melted_df.dropna(subset=['value'], inplace=True)
        melted_df['member'] = melted_df['member'].str.extract(r'member(\d+)').fillna('0')

        records = [
            (model_name, run_timestamp.isoformat(), row['member'], variable, row['date'].isoformat(), row['value'])
            for _, row in melted_df.iterrows()
        ]
        cursor.executemany("""
            INSERT INTO raw_forecast_data (model_name, run_timestamp, member, variable, forecast_timestamp, value)
            VALUES (?, ?, ?, ?, ?, ?)
        """, records)
    print(f"Successfully saved raw data to the database for model {model_name}.")

def save_statistics(conn: sqlite3.Connection, model_name: str, run_timestamp: datetime, statistics: dict[str, pd.DataFrame]):
    """
    Saves calculated statistics to the database.
    """
    cursor = conn.cursor()
    for variable, stats_df in statistics.items():
        if stats_df is None or stats_df.empty:
            continue

        melted_df = stats_df.reset_index().melt(
            id_vars=['date'],
            var_name='statistic',
            value_name='value'
        )
        melted_df.dropna(subset=['value'], inplace=True)

        records = [
            (model_name, run_timestamp.isoformat(), variable, row['statistic'], row['date'].isoformat(), row['value'])
            for _, row in melted_df.iterrows()
        ]
        if not records:
            continue

        cursor.executemany("""
            INSERT INTO statistical_forecasts (model_name, run_timestamp, variable, statistic, forecast_timestamp, value)
            VALUES (?, ?, ?, ?, ?, ?)
        """, records)
    print(f"Successfully saved statistics to the database for model {model_name}.")
