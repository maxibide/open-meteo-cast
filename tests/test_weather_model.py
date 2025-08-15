import pytest
from unittest.mock import patch
import pandas as pd
from datetime import datetime

from src.open_meteo_cast.weather_model import WeatherModel

@pytest.fixture
def mock_config():
    return {
        "api": {
            "open-meteo": {
                "ensemble_metadata": {
                    "gfs025": "http://dummy-url.com"
                },
                "ensemble_url": "http://dummy-ensemble-url.com"
            }
        },
        "location": {
            "latitude": 0,
            "longitude": 0
        },
        "logging": {
            "console": False
        }
    }

@pytest.fixture
def mock_metadata():
    return {
        "model": "gfs025",
        "last_run_initialisation_time": datetime(2023, 3, 15, 12, 0, 0),
        "last_run_availability_time": datetime(2023, 3, 15, 12, 5, 0), # 5 minutes after init
    }

@patch('src.open_meteo_cast.weather_model.logging')
@patch('src.open_meteo_cast.weather_model.retrieve_model_metadata')
@patch('src.open_meteo_cast.weather_model.get_last_run_timestamp')
@patch('src.open_meteo_cast.weather_model.WeatherModel.load_from_db')
def test_init_not_new(mock_load_from_db, mock_get_last_run_timestamp, mock_retrieve_metadata, mock_logging, mock_config, mock_metadata):
    mock_retrieve_metadata.return_value = mock_metadata
    mock_get_last_run_timestamp.return_value = datetime(2023, 3, 16, 12, 0, 0) # Not new
    model = WeatherModel("gfs025", mock_config)
    assert not model.is_new
    mock_load_from_db.assert_called_once()

@patch('src.open_meteo_cast.weather_model.logging')
@patch('src.open_meteo_cast.weather_model.datetime')
@patch('src.open_meteo_cast.weather_model.retrieve_model_metadata')
@patch('src.open_meteo_cast.weather_model.get_last_run_timestamp')
@patch('src.open_meteo_cast.weather_model.retrieve_model_variable')
@patch('src.open_meteo_cast.weather_model.WeatherModel.calculate_statistics')
@patch('src.open_meteo_cast.weather_model.WeatherModel.save_to_db')
def test_init_new_run(mock_save_to_db, mock_calculate_statistics, mock_retrieve_model_variable, mock_get_last_run_timestamp, mock_retrieve_metadata, mock_datetime, mock_logging, mock_config, mock_metadata):
    mock_retrieve_metadata.return_value = mock_metadata
    mock_get_last_run_timestamp.return_value = datetime(2023, 3, 14, 12, 0, 0) # New
    mock_datetime.now.return_value = datetime(2023, 3, 15, 12, 15, 0) # 15 minutes after init
    model = WeatherModel("gfs025", mock_config)
    assert model.is_new
    mock_save_to_db.assert_called_once()

@patch('src.open_meteo_cast.weather_model.logging')
@patch('src.open_meteo_cast.weather_model.retrieve_model_metadata')
@patch('src.open_meteo_cast.weather_model.get_last_run_timestamp')
@patch('src.open_meteo_cast.weather_model.load_raw_data')
@patch('src.open_meteo_cast.weather_model.load_statistics')
@patch('src.open_meteo_cast.weather_model.WeatherModel.check_if_new', return_value=False)
def test_load_from_db(mock_check_if_new, mock_load_statistics, mock_load_raw_data, mock_get_last_run_timestamp, mock_retrieve_metadata, mock_logging, mock_config, mock_metadata):
    # We need to mock the __init__ to avoid calling get_db_connection there
    with patch.object(WeatherModel, '__init__', lambda x, y, z: None):
        model = WeatherModel("gfs025", mock_config)
        model.name = "gfs025"
        last_run = datetime(2023, 3, 15, 12, 0, 0)
        mock_get_last_run_timestamp.return_value = last_run
        raw_data = {'temp': pd.DataFrame()}
        stats_data = {'temp_stats': pd.DataFrame()}
        mock_load_raw_data.return_value = raw_data
        mock_load_statistics.return_value = stats_data

        model.load_from_db()

        mock_load_raw_data.assert_called_with("gfs025", last_run)
        mock_load_statistics.assert_called_with("gfs025", last_run)
        assert model.data == raw_data
        assert model.statistics == stats_data

@patch('src.open_meteo_cast.weather_model.logging')
@patch('src.open_meteo_cast.weather_model.datetime')
@patch('src.open_meteo_cast.weather_model.retrieve_model_metadata')
@patch('src.open_meteo_cast.weather_model.get_db_connection')
@patch('src.open_meteo_cast.weather_model.save_forecast_run')
@patch('src.open_meteo_cast.weather_model.save_raw_data')
@patch('src.open_meteo_cast.weather_model.save_statistics')
@patch('src.open_meteo_cast.weather_model.WeatherModel.check_if_new', return_value=True)
@patch('src.open_meteo_cast.weather_model.retrieve_model_variable')
@patch('src.open_meteo_cast.weather_model.WeatherModel.calculate_statistics')
def test_save_to_db(mock_calculate_statistics, mock_retrieve_model_variable, mock_check_if_new, mock_save_statistics, mock_save_raw_data, mock_save_forecast_run, mock_get_db_connection, mock_retrieve_metadata, mock_datetime, mock_logging, mock_config, mock_metadata):
    mock_retrieve_metadata.return_value = mock_metadata
    mock_datetime.now.return_value = datetime(2023, 3, 15, 12, 15, 0) # 15 minutes after init
    
    # We need to mock the __init__ to avoid calling get_db_connection there
    with patch.object(WeatherModel, '__init__', lambda x, y, z: None):
        model = WeatherModel("gfs025", mock_config)
        model.name = "gfs025"
        model.metadata = mock_metadata
        model.data = {'temp': pd.DataFrame()}
        model.statistics = {'temp_stats': pd.DataFrame()}
        last_run = mock_metadata['last_run_initialisation_time']

        model.save_to_db()

        mock_get_db_connection.assert_called_once()
        mock_save_forecast_run.assert_called_with(mock_get_db_connection.return_value, "gfs025", last_run)
        mock_save_raw_data.assert_called_with(mock_get_db_connection.return_value, "gfs025", last_run, model.data)
        mock_save_statistics.assert_called_with(mock_get_db_connection.return_value, "gfs025", last_run, model.statistics)