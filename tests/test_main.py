from unittest.mock import patch, mock_open, MagicMock
import json
from datetime import datetime, timedelta
import requests

from src.open_meteo_cast.main import (
    load_config,
    main,
)
from src.open_meteo_cast.weather_model import (
    retrieve_model_metadata,
)

# Test for load_config
def test_load_config_success():
    mock_yaml_content = """
api:
  open-meteo:
    ensemble_metadata:
      gfs: "https://api.open-meteo.com/v1/gfs?latitude=0&longitude=0&hourly=temperature_2m"
"""
    with patch("builtins.open", mock_open(read_data=mock_yaml_content)):
        config = load_config("dummy_config.yaml")
        assert config == {
            "api": {
                "open-meteo": {
                    "ensemble_metadata": {
                        "gfs": "https://api.open-meteo.com/v1/gfs?latitude=0&longitude=0&hourly=temperature_2m"
                    }
                }
            }
        }

def test_load_config_file_not_found():
    with patch("builtins.open", side_effect=FileNotFoundError):
        with patch("builtins.print") as mock_print:
            config = load_config("non_existent_config.yaml")
            assert config == {}
            mock_print.assert_called_with("Error: File non_existent_config.yaml not found")

def test_load_config_yaml_error():
    with patch("builtins.open", mock_open(read_data="invalid: - yaml")):
        with patch("builtins.print") as mock_print:
            config = load_config("invalid_config.yaml")
            assert config == {}
            mock_print.assert_called_once()
            printed_message = mock_print.call_args[0][0]
            assert "Error reading YAML file" in printed_message

# Test for retrieve_model_metadata
def test_retrieve_model_metadata_success():
    mock_response_json = {"model": "gfs", "data": "some_data", "last_run_initialisation_time": 1678886400}
    with patch("requests.get") as mock_get:
        mock_get.return_value.status_code = 200
        mock_get.return_value.json.return_value = mock_response_json
        mock_get.return_value.raise_for_status.return_value = None
        
        metadata = retrieve_model_metadata("http://dummy-url.com")
        assert metadata["last_run_initialisation_time"] == datetime.fromtimestamp(1678886400)

def test_retrieve_model_metadata_request_exception():
    with patch("requests.get", side_effect=requests.exceptions.RequestException("Connection error")):
        with patch("builtins.print") as mock_print:
            metadata = retrieve_model_metadata("http://dummy-url.com")
            assert metadata is None
            mock_print.assert_called_with("Error retrieving data from http://dummy-url.com: Connection error")

def test_retrieve_model_metadata_json_decode_error():
    with patch("requests.get") as mock_get:
        mock_get.return_value.status_code = 200
        mock_get.return_value.json.side_effect = json.JSONDecodeError("Invalid JSON", "doc", 0)
        mock_get.return_value.raise_for_status.return_value = None

        with patch("builtins.print") as mock_print:
            metadata = retrieve_model_metadata("http://dummy-url.com")
            assert metadata is None
            mock_print.assert_called_with("Error decoding JSON from http://dummy-url.com: Invalid JSON: line 1 column 1 (char 0)")

# Test for main function
@patch('src.open_meteo_cast.main.database.purge_old_runs')
@patch('src.open_meteo_cast.main.database.create_tables')
@patch('src.open_meteo_cast.main.WeatherModel')
@patch('src.open_meteo_cast.main.load_config')
def test_main_no_new_runs(mock_load_config, mock_weather_model, mock_create_tables, mock_purge_old_runs, capsys):
    # Setup: One model, no new run
    mock_load_config.return_value = {'models_used': ['gfs025'], 'database': {'retention_days': 30}}
    mock_model_instance = MagicMock()
    mock_model_instance.check_if_new.return_value = False
    mock_weather_model.return_value = mock_model_instance

    main()

    mock_create_tables.assert_called_once()
    mock_purge_old_runs.assert_called_once_with(30)
    mock_model_instance.check_if_new.assert_called_once()
    mock_model_instance.retrieve_data.assert_not_called()
    captured = capsys.readouterr()
    assert "No new model runs found for any model. Exiting." in captured.out

@patch('src.open_meteo_cast.main.database.get_db_connection')
@patch('src.open_meteo_cast.main.database.purge_old_runs')
@patch('src.open_meteo_cast.main.database.create_tables')
@patch('src.open_meteo_cast.main.WeatherModel')
@patch('src.open_meteo_cast.main.load_config')
def test_main_one_new_run_proceeds(mock_load_config, mock_weather_model, mock_create_tables, mock_purge_old_runs, mock_get_db_connection, capsys):
    # Setup: One model with a new run, available long enough ago
    mock_load_config.return_value = {'models_used': ['gfs025'], 'database': {'retention_days': 30}}
    mock_model_instance = MagicMock()
    mock_model_instance.name = 'gfs025'
    mock_model_instance.check_if_new.return_value = True
    mock_model_instance.metadata = {'last_run_availability_time': datetime.now() - timedelta(minutes=15)}
    mock_model_instance.last_run_time = datetime(2023, 1, 1, 12, 0, 0)
    mock_weather_model.return_value = mock_model_instance

    # Mock database connection
    mock_conn = MagicMock()
    mock_cursor = MagicMock()
    mock_get_db_connection.return_value = mock_conn
    mock_conn.cursor.return_value = mock_cursor
    mock_cursor.lastrowid = 1

    main()

    mock_create_tables.assert_called_once()
    mock_purge_old_runs.assert_called_once_with(30)
    mock_model_instance.check_if_new.assert_called_once()
    mock_model_instance.retrieve_data.assert_called_once()
    mock_model_instance.calculate_statistics.assert_called_once()
    mock_model_instance.save_to_db.assert_called_once()
    captured = capsys.readouterr()
    assert "Found new runs for the following models: ['gfs025']" in captured.out

@patch('src.open_meteo_cast.main.database.purge_old_runs')
@patch('src.open_meteo_cast.main.database.create_tables')
@patch('src.open_meteo_cast.main.WeatherModel')
@patch('src.open_meteo_cast.main.load_config')
def test_main_one_new_run_waits(mock_load_config, mock_weather_model, mock_create_tables, mock_purge_old_runs, capsys):
    # Setup: One model with a new run, but too recent
    mock_load_config.return_value = {'models_used': ['gfs025'], 'database': {'retention_days': 30}}
    mock_model_instance = MagicMock()
    mock_model_instance.name = 'gfs025'
    mock_model_instance.check_if_new.return_value = True
    mock_model_instance.metadata = {'last_run_availability_time': datetime.now() - timedelta(minutes=5)}
    mock_weather_model.return_value = mock_model_instance

    main()

    mock_create_tables.assert_called_once()
    mock_purge_old_runs.assert_called_once_with(30)
    mock_model_instance.check_if_new.assert_called_once()
    mock_model_instance.retrieve_data.assert_not_called()
    captured = capsys.readouterr()
    assert "Last run for gfs025 was available less than 10 minutes ago." in captured.out

@patch('src.open_meteo_cast.main.database.get_db_connection')
@patch('src.open_meteo_cast.main.database.purge_old_runs')
@patch('src.open_meteo_cast.main.database.create_tables')
@patch('src.open_meteo_cast.main.WeatherModel')
@patch('src.open_meteo_cast.main.load_config')
def test_main_mixed_runs_processes_only_new(mock_load_config, mock_weather_model, mock_create_tables, mock_purge_old_runs, mock_get_db_connection, capsys):
    # Setup: Two models, one new, one old
    mock_load_config.return_value = {'models_used': ['gfs025', 'ecmwf'], 'database': {'retention_days': 30}}
    
    model_gfs = MagicMock()
    model_gfs.name = 'gfs025'
    model_gfs.check_if_new.return_value = True
    model_gfs.metadata = {'last_run_availability_time': datetime.now() - timedelta(minutes=15)}
    model_gfs.last_run_time = datetime(2023, 1, 1, 12, 0, 0)

    model_ecmwf = MagicMock()
    model_ecmwf.name = 'ecmwf'
    model_ecmwf.check_if_new.return_value = False

    # The WeatherModel constructor will be called twice, returning these mocks in order
    mock_weather_model.side_effect = [model_gfs, model_ecmwf]

    # Mock database connection
    mock_conn = MagicMock()
    mock_cursor = MagicMock()
    mock_get_db_connection.return_value = mock_conn
    mock_conn.cursor.return_value = mock_cursor
    mock_cursor.lastrowid = 1

    main()

    mock_create_tables.assert_called_once()
    mock_purge_old_runs.assert_called_once_with(30)
    assert model_gfs.check_if_new.called
    assert model_ecmwf.check_if_new.called

    model_gfs.retrieve_data.assert_called_once()
    model_gfs.calculate_statistics.assert_called_once()
    model_gfs.save_to_db.assert_called_once()
    model_ecmwf.retrieve_data.assert_not_called()

    captured = capsys.readouterr()
    assert "Found new runs for the following models: ['gfs025']" in captured.out
    assert "--- Processing model: gfs025 ---" in captured.out
    assert "--- Processing model: ecmwf ---" not in captured.out