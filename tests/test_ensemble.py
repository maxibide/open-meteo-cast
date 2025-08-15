import pandas as pd
from unittest.mock import MagicMock, patch
from src.open_meteo_cast.ensemble import Ensemble
from src.open_meteo_cast import database
from datetime import datetime

@patch('src.open_meteo_cast.ensemble.logging')
def test_to_html_table_with_data(mock_logging):
    # Create a mock WeatherModel
    mock_model = MagicMock()
    mock_model.name = "test_model"
    mock_model.metadata = {'last_run_availability_time': '2025-08-15T12:00:00'}

    # Create a sample statistics DataFrame
    data = {
        'temperature_2m_median': [10, 12],
        'precipitation_probability': [0.2, 0.5],
        'cloud_cover_octa_1_prob': [0.1, 0.3],
        'cloud_cover_octa_2_prob': [0.9, 0.7],
        'wind_direction_10m_N_prob': [0.8, 0.2],
        'wind_direction_10m_S_prob': [0.2, 0.8],
    }
    index = pd.to_datetime(['2025-08-14 12:00:00', '2025-08-14 13:00:00'], utc=True)
    stats_df = pd.DataFrame(data, index=index)

    # Create a mock Ensemble object
    ensemble = Ensemble(models=[mock_model], config={'location': {'timezone': 'UTC'}, 'forecast_hours': 72})
    ensemble.stats_df = stats_df

    # Generate the HTML table
    html = ensemble.to_html_table(config={'location': {'timezone': 'UTC'}, 'forecast_hours': 72})

    # Assert that the HTML contains expected elements
    assert '<h2>Ensemble Weather Forecast</h2>' in html
    assert '<table' in html
    assert 'Temperature (°C)' in html
    assert '<td>10</td>' in html
    assert '50%' in html
    assert '2/8 (90%)' in html
    assert 'N (80%)' in html

@patch('src.open_meteo_cast.ensemble.database.get_db_connection')
@patch('src.open_meteo_cast.ensemble.database.save_ensemble_run')
@patch('src.open_meteo_cast.ensemble.database.save_ensemble_statistics')
@patch('importlib.metadata.version', return_value="0.1.0")
def test_save_to_db(mock_version, mock_save_stats, mock_save_run, mock_get_conn):
    # Create a mock WeatherModel
    mock_model = MagicMock()
    mock_model.name = "test_model"
    mock_model.metadata = {'last_run_availability_time': '2025-08-15T12:00:00'}

    # Create a sample statistics DataFrame
    data = {
        'temperature_2m_median': [10, 12],
    }
    index = pd.to_datetime(['2025-08-14 12:00:00', '2025-08-14 13:00:00'], utc=True)
    stats_df = pd.DataFrame(data, index=index)

    # Create a mock Ensemble object
    ensemble = Ensemble(models=[mock_model], config={'location': {'timezone': 'UTC'}, 'forecast_hours': 72})
    ensemble.stats_df = stats_df

    # Call save_to_db
    ensemble.save_to_db()

    # Assertions
    mock_get_conn.assert_called_once()
    mock_save_run.assert_called_once()
    mock_save_stats.assert_called_once()

    # Check the arguments passed to save_ensemble_run
    args, kwargs = mock_save_run.call_args
    assert args[3] == "0.1.0"
