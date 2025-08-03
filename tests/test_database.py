import sqlite3
import pytest
from datetime import datetime, timedelta
from unittest.mock import patch
import os

from src.open_meteo_cast import database
from src.open_meteo_cast.database import (
    create_tables,
    purge_old_runs,
)

DB_FILE = "test_forecasts.db"

@pytest.fixture
def test_db():
    """Fixture to set up a test database file and patch the path."""
    if os.path.exists(DB_FILE):
        os.remove(DB_FILE)
    
    with patch.object(database, 'DB_PATH', DB_FILE):
        yield DB_FILE
    
    if os.path.exists(DB_FILE):
        os.remove(DB_FILE)

def test_create_tables(test_db):
    create_tables()

    conn = sqlite3.connect(test_db)
    cursor = conn.cursor()
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='forecast_runs'")
    assert cursor.fetchone() is not None
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='raw_forecast_data'")
    assert cursor.fetchone() is not None
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='statistical_forecasts'")
    assert cursor.fetchone() is not None
    conn.close()

def test_purge_old_runs(test_db):
    create_tables()
    conn = sqlite3.connect(test_db)
    cursor = conn.cursor()
    
    now = datetime.now()
    old_run_time = now - timedelta(days=40)
    new_run_time = now - timedelta(days=10)

    # Insert an old run and a new run
    cursor.execute("INSERT INTO forecast_runs (model_name, run_timestamp) VALUES (?, ?)", ("gfs", old_run_time.isoformat()))
    cursor.execute("INSERT INTO forecast_runs (model_name, run_timestamp) VALUES (?, ?)", ("gfs", new_run_time.isoformat()))
    
    conn.commit()
    conn.close()

    # Purge runs older than 30 days
    purge_old_runs(30)

    # Check that the old run is purged and the new one remains
    conn = sqlite3.connect(test_db)
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM forecast_runs WHERE run_timestamp = ?", (old_run_time.isoformat(),))
    assert cursor.fetchone() is None, "Old run should be purged"
    cursor.execute("SELECT * FROM forecast_runs WHERE run_timestamp = ?", (new_run_time.isoformat(),))
    assert cursor.fetchone() is not None, "New run should not be purged"
    conn.close()