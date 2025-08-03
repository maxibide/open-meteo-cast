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
