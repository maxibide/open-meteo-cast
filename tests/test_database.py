import sqlite3
import pytest
from datetime import datetime, timedelta
from unittest.mock import patch
import os
import pandas as pd

from src.open_meteo_cast import database
from src.open_meteo_cast.database import (
    create_tables,
    purge_old_runs,
    load_raw_data, load_statistics, save_forecast_run, save_raw_data, save_statistics
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

def test_get_last_run_timestamp(test_db):
    create_tables()
    conn = sqlite3.connect(test_db)
    cursor = conn.cursor()

    # Timestamps for two different models
    gfs_time1 = datetime.now() - timedelta(days=2)
    gfs_time2 = datetime.now() - timedelta(days=1)
    ecmwf_time = datetime.now() - timedelta(hours=6)

    # Insert multiple runs for different models
    cursor.execute("INSERT INTO forecast_runs (model_name, run_timestamp) VALUES (?, ?)", ("gfs", gfs_time1.isoformat()))
    cursor.execute("INSERT INTO forecast_runs (model_name, run_timestamp) VALUES (?, ?)", ("gfs", gfs_time2.isoformat()))
    cursor.execute("INSERT INTO forecast_runs (model_name, run_timestamp) VALUES (?, ?)", ("ecmwf", ecmwf_time.isoformat()))
    conn.commit()
    conn.close()

    # Test that the latest timestamp is returned for 'gfs'
    latest_gfs_run = database.get_last_run_timestamp("gfs")
    assert latest_gfs_run is not None
    assert latest_gfs_run.isoformat(timespec='seconds') == gfs_time2.isoformat(timespec='seconds')

    # Test that the correct timestamp is returned for 'ecmwf'
    latest_ecmwf_run = database.get_last_run_timestamp("ecmwf")
    assert latest_ecmwf_run is not None
    assert latest_ecmwf_run.isoformat(timespec='seconds') == ecmwf_time.isoformat(timespec='seconds')

    # Test that None is returned for a model with no runs
    assert database.get_last_run_timestamp("icon") is None

def test_save_and_load_data(test_db):
    create_tables()
    model_name = "test_model"
    run_timestamp = datetime.now()

    # Sample Data
    raw_data = {
        'temperature': pd.DataFrame({
            'member1': [10, 20],
            'member2': [12, 22]
        }, index=pd.to_datetime(['2023-01-01', '2023-01-02']))
    }
    raw_data['temperature'].index.name = 'date'
    statistics_data = {
        'temperature': pd.DataFrame({
            'p10': [10.2, 20.2],
            'median': [11, 21]
        }, index=pd.to_datetime(['2023-01-01', '2023-01-02']))
    }
    statistics_data['temperature'].index.name = 'date'

    # Save Data
    conn = sqlite3.connect(test_db)
    save_forecast_run(conn, model_name, run_timestamp)
    save_raw_data(conn, model_name, run_timestamp, raw_data)
    save_statistics(conn, model_name, run_timestamp, statistics_data)
    conn.commit()
    conn.close()

    # Load Data
    loaded_raw = load_raw_data(model_name, run_timestamp)
    loaded_stats = load_statistics(model_name, run_timestamp)

    # Assertions
    assert 'temperature' in loaded_raw
    pd.testing.assert_frame_equal(raw_data['temperature'].astype('float64').sort_index(axis=1), loaded_raw['temperature'].sort_index(axis=1))

    assert 'temperature' in loaded_stats
    pd.testing.assert_frame_equal(statistics_data['temperature'].astype('float64').sort_index(axis=1), loaded_stats['temperature'].sort_index(axis=1))
