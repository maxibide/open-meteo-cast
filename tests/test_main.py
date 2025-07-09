import pytest
from unittest.mock import patch, mock_open, MagicMock
import json
from datetime import datetime
import os
import requests

from src.open_meteo_cast.main import (
    load_config,
    retrieve_model_metadata,
    print_model_metadata,
    check_if_model_run_is_new,
    main
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
            assert printed_message.startswith("Error reading YAML file: sequence entries are not allowed here")
            assert "line 1, column 10" in printed_message

# Test for retrieve_model_metadata
def test_retrieve_model_metadata_success():
    mock_response_json = {"model": "gfs", "data": "some_data"}
    with patch("requests.get") as mock_get:
        mock_get.return_value.status_code = 200
        mock_get.return_value.json.return_value = mock_response_json
        mock_get.return_value.raise_for_status.return_value = None
        
        metadata = retrieve_model_metadata("http://dummy-url.com")
        assert metadata == mock_response_json
        mock_get.assert_called_once_with("http://dummy-url.com", timeout=30)

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

# Test for print_model_metadata
def test_print_model_metadata_success(capsys):
    model_metadata = {
        "model": "gfs",
        "data_end_time": 1678886400,
        "last_run_availability_time": 1678886400,
        "last_run_initialisation_time": 1678886400,
        "last_run_modification_time": 1678886400,
        "other_key": "other_value"
    }
    expected_output = (
        f"model: gfs\n"
        f"data_end_time: {datetime.fromtimestamp(1678886400).strftime('%Y-%m-%d %H:%M:%S')}\n"
        f"last_run_availability_time: {datetime.fromtimestamp(1678886400).strftime('%Y-%m-%d %H:%M:%S')}\n"
        f"last_run_initialisation_time: {datetime.fromtimestamp(1678886400).strftime('%Y-%m-%d %H:%M:%S')}\n"
        f"last_run_modification_time: {datetime.fromtimestamp(1678886400).strftime('%Y-%m-%d %H:%M:%S')}\n"
        f"other_key: other_value\n"
    )
    
    result = print_model_metadata(model_metadata)
    captured = capsys.readouterr()
    assert result == 0
    assert captured.out == expected_output

def test_print_model_metadata_empty_dict(capsys):
    model_metadata = {}
    result = print_model_metadata(model_metadata)
    captured = capsys.readouterr()
    assert result == 0
    assert captured.out == ""

def test_print_model_metadata_invalid_timestamp_conversion(capsys):
    model_metadata = {
        "model": "gfs",
        "data_end_time": "invalid_timestamp",
        "last_run_initialisation_time": 1678886400,
    }
    expected_output = (
        f"model: gfs\n"
        f"data_end_time: invalid_timestamp\n"
        f"last_run_initialisation_time: {datetime.fromtimestamp(1678886400).strftime('%Y-%m-%d %H:%M:%S')}\n"
    )
    result = print_model_metadata(model_metadata)
    captured = capsys.readouterr()
    assert result == 0
    assert captured.out == expected_output

# Test for check_if_model_run_is_new
@pytest.fixture
def setup_last_run_file():
    file_path = 'last_run.json'
    if os.path.exists(file_path):
        os.remove(file_path)
    yield
    if os.path.exists(file_path):
        os.remove(file_path)

def test_check_if_model_run_is_new_first_run(setup_last_run_file, capsys):
    model_metadata = {"model": "gfs", "last_run_initialisation_time": 100}
    with patch("builtins.open", mock_open()) as mocked_file:
        with patch("json.dump") as mocked_json_dump:
            result = check_if_model_run_is_new(model_metadata)
            assert result is True
            captured = capsys.readouterr()
            assert "New model run detected for gfs." in captured.out
            mocked_json_dump.assert_called_once_with({"gfs": 100}, mocked_file(), indent=4)

def test_check_if_model_run_is_new_newer_run(setup_last_run_file, capsys):
    initial_content = {"gfs": 50}
    model_metadata = {"model": "gfs", "last_run_initialisation_time": 100}
    
    with patch("builtins.open", mock_open(read_data=json.dumps(initial_content))) as mocked_file:
        with patch("json.dump") as mocked_json_dump:
            result = check_if_model_run_is_new(model_metadata)
            assert result is True
            captured = capsys.readouterr()
            assert "New model run detected for gfs." in captured.out
            mocked_json_dump.assert_called_once_with({"gfs": 100}, mocked_file(), indent=4)

def test_check_if_model_run_is_new_older_run(setup_last_run_file, capsys):
    initial_content = {"gfs": 100}
    model_metadata = {"model": "gfs", "last_run_initialisation_time": 50}
    
    with patch("builtins.open", mock_open(read_data=json.dumps(initial_content))):
        with patch("json.dump") as mocked_json_dump:
            result = check_if_model_run_is_new(model_metadata)
            assert result is False
            captured = capsys.readouterr()
            assert "No new model run for gfs." in captured.out
            mocked_json_dump.assert_not_called()

def test_check_if_model_run_is_new_file_not_found(setup_last_run_file, capsys):
    model_metadata = {"model": "gfs", "last_run_initialisation_time": 100}

    mock_write_file_handle = MagicMock()

    mock_open_func = mock_open()
    mock_open_func.side_effect = [FileNotFoundError, mock_open_func.return_value]
    mock_open_func.return_value.__enter__.return_value = mock_write_file_handle

    with patch("builtins.open", mock_open_func):
        with patch("json.dump") as mocked_json_dump:
            result = check_if_model_run_is_new(model_metadata)
            assert result is True
            captured = capsys.readouterr()
            assert "New model run detected for gfs." in captured.out
            mocked_json_dump.assert_called_once_with({"gfs": 100}, mock_write_file_handle, indent=4)

def test_check_if_model_run_is_new_json_decode_error(setup_last_run_file, capsys):
    model_metadata = {"model": "gfs", "last_run_initialisation_time": 100}

    mock_write_file_handle = MagicMock()

    mock_open_func = mock_open(read_data="invalid json")
    mock_open_func.side_effect = [json.JSONDecodeError("Invalid JSON", "doc", 0), mock_open_func.return_value]
    mock_open_func.return_value.__enter__.return_value = mock_write_file_handle

    with patch("builtins.open", mock_open_func):
        with patch("json.dump") as mocked_json_dump:
            result = check_if_model_run_is_new(model_metadata)
            assert result is True
            captured = capsys.readouterr()
            assert "Warning: Could not decode JSON from last_run.json. Treating as empty." in captured.out
            assert "New model run detected for gfs." in captured.out
            mocked_json_dump.assert_called_once_with({"gfs": 100}, mock_write_file_handle, indent=4)

def test_check_if_model_run_is_new_incomplete_metadata(capsys):
    model_metadata = {"model": "gfs"} # Missing last_run_initialisation_time
    result = check_if_model_run_is_new(model_metadata)
    assert result is False
    captured = capsys.readouterr()
    assert "Error: Incomplete model metadata provided." in captured.out

# Test for main function
def test_main_function(capsys):
    mock_config = {
        "api": {
            "open-meteo": {
                "ensemble_metadata": {
                    "gfs": "http://dummy-open-meteo-url.com"
                }
            }
        }
    }
    mock_metadata = {
        "model": "gfs",
        "data_end_time": 1678886400,
        "last_run_initialisation_time": 1678886400,
        "last_run_availability_time": 1678886400,
        "last_run_modification_time": 1678886400,
    }

    with patch("src.open_meteo_cast.main.load_config", return_value=mock_config), \
         patch("src.open_meteo_cast.main.retrieve_model_metadata", return_value=mock_metadata), \
         patch("src.open_meteo_cast.main.print_model_metadata", return_value=0), \
         patch("src.open_meteo_cast.main.check_if_model_run_is_new", return_value=True):
        
        main()
        captured = capsys.readouterr()
        assert "\ngfs\n" in captured.out
        # Further assertions can be made to check if print_model_metadata and check_if_model_run_is_new were called with correct arguments
