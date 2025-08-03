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
    conn.row_factory = sqlite3.Row
    return conn

def create_tables():
    """Creates the necessary tables in the database if they don't already exist."""
    conn = get_db_connection()
    cursor = conn.cursor()

    # Table for forecast runs to group all data from a single execution
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS forecast_runs (
            run_id INTEGER PRIMARY KEY AUTOINCREMENT,
            model_name TEXT NOT NULL,
            run_timestamp DATETIME NOT NULL
        );
    """)

    # Table for raw forecast data from each ensemble member (long format)
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS raw_forecast_data (
            data_id INTEGER PRIMARY KEY AUTOINCREMENT,
            run_id INTEGER NOT NULL,
            member TEXT NOT NULL,
            variable TEXT NOT NULL,
            forecast_timestamp DATETIME NOT NULL,
            value REAL NOT NULL,
            FOREIGN KEY (run_id) REFERENCES forecast_runs (run_id),
            UNIQUE(run_id, member, variable, forecast_timestamp)
        );
    """)

    # Table for pre-calculated statistical forecasts (long format)
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS statistical_forecasts (
            stat_id INTEGER PRIMARY KEY AUTOINCREMENT,
            run_id INTEGER NOT NULL,
            variable TEXT NOT NULL,
            statistic TEXT NOT NULL,
            forecast_timestamp DATETIME NOT NULL,
            value REAL,
            FOREIGN KEY (run_id) REFERENCES forecast_runs (run_id),
            UNIQUE(run_id, variable, statistic, forecast_timestamp)
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

    # Find old run_ids
    cursor.execute("SELECT run_id FROM forecast_runs WHERE run_timestamp < ?", (cutoff_date.isoformat(),))
    old_run_ids = [row[0] for row in cursor.fetchall()]

    if not old_run_ids:
        print("No old forecast runs to purge.")
        conn.close()
        return

    print(f"Found {len(old_run_ids)} old run(s) to purge.")

    # Use a tuple for the IN clause
    run_ids_tuple = tuple(old_run_ids)

    # Delete associated data first to maintain foreign key integrity
    cursor.execute(f"DELETE FROM raw_forecast_data WHERE run_id IN ({','.join(['?']*len(old_run_ids))})", run_ids_tuple)
    cursor.execute(f"DELETE FROM statistical_forecasts WHERE run_id IN ({','.join(['?']*len(old_run_ids))})", run_ids_tuple)

    # Finally, delete the old forecast runs
    cursor.execute(f"DELETE FROM forecast_runs WHERE run_id IN ({','.join(['?']*len(old_run_ids))})", run_ids_tuple)

    conn.commit()
    conn.close()
    print(f"Successfully purged {len(old_run_ids)} old forecast run(s).")
