from unittest.mock import patch, mock_open, MagicMock
import json
from datetime import datetime
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
        assert metadata["last_run_initialisation_time"] == datetime.fromtimestamp(1678886400).strftime('%Y-%m-%d %H:%M:%S')

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
@patch('src.open_meteo_cast.main.WeatherModel')
@patch('src.open_meteo_cast.main.load_config')
def test_main_function_new_run(mock_load_config, mock_weather_model, capsys):
    mock_load_config.return_value = {"api": {"open-meteo": {"ensemble_metadata": {"gfs025": "some_url"}, "ensemble_url": "http://dummy-ensemble-url.com"}}, "location": {"latitude": 0, "longitude": 0}}
    
    mock_model_instance = MagicMock()
    mock_model_instance.check_if_new.return_value = True
    mock_weather_model.return_value = mock_model_instance

    main()

    mock_load_config.assert_called_once_with('resources/default_config.yaml')
    mock_weather_model.assert_called_once_with("gfs025", mock_load_config.return_value)
    mock_model_instance.check_if_new.assert_called_once()
    mock_model_instance.print_metadata.assert_called_once()
    mock_model_instance.retrieve_data.assert_called_once_with(mock_load_config.return_value)
    mock_model_instance.print_data.assert_called_once()
    
    captured = capsys.readouterr()
    assert "New model runs found" in captured.out

@patch('src.open_meteo_cast.main.WeatherModel')
@patch('src.open_meteo_cast.main.load_config')
def test_main_function_no_new_run(mock_load_config, mock_weather_model, capsys):
    mock_load_config.return_value = {"api": {"open-meteo": {"ensemble_metadata": {"gfs025": "some_url"}, "ensemble_url": "http://dummy-ensemble-url.com"}}, "location": {"latitude": 0, "longitude": 0}}
    
    mock_model_instance = MagicMock()
    mock_model_instance.check_if_new.return_value = False
    mock_weather_model.return_value = mock_model_instance

    main()

    mock_load_config.assert_called_once_with('resources/default_config.yaml')
    mock_weather_model.assert_called_once_with("gfs025", mock_load_config.return_value)
    mock_model_instance.check_if_new.assert_called_once()
    
    captured = capsys.readouterr()
    assert "No new model runs found for any model. Exiting." in captured.out