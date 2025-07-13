import pytest
from unittest.mock import patch, mock_open
import json
import pandas as pd
from datetime import datetime

from src.open_meteo_cast.weather_model import WeatherModel

class TestWeatherModel:
    @pytest.fixture
    def mock_config(self):
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
            }
        }

    @pytest.fixture
    def mock_metadata(self):
        return {
            "model": "gfs025",
            "last_run_initialisation_time": datetime(2023, 3, 15, 12, 0, 0),
        }

    @pytest.fixture
    def mock_weather_model_instance(self, mock_config, mock_metadata):
        with patch('src.open_meteo_cast.weather_model.retrieve_model_metadata') as mock_retrieve_metadata:
            mock_retrieve_metadata.return_value = mock_metadata
            model = WeatherModel("gfs025", mock_config)
            return model

    def test_init(self, mock_weather_model_instance, mock_config, mock_metadata):
        model = mock_weather_model_instance
        assert model.name == "gfs025"
        assert model.metadata_url == "http://dummy-url.com"
        assert model.metadata == mock_metadata

    def test_print_metadata(self, mock_weather_model_instance, capsys):
        model = mock_weather_model_instance
        model.print_metadata()
        captured = capsys.readouterr()
        expected_output = (
            "Name: gfs025\n"
            "model: gfs025\n"
            "last_run_initialisation_time: 2023-03-15 12:00:00\n"
        )
        assert captured.out == expected_output

    def test_check_if_new_first_run(self, mock_weather_model_instance, capsys):
        model = mock_weather_model_instance
        with patch("builtins.open", mock_open()) as mocked_file:
            with patch("json.dump") as mocked_json_dump:
                result = model.check_if_new()
                assert result is True
                captured = capsys.readouterr()
                assert "New model run detected for gfs025." in captured.out
                mocked_json_dump.assert_called_once_with({'gfs025': '2023-03-15T12:00:00'}, mocked_file(), indent=4)

    def test_check_if_new_newer_run(self, mock_weather_model_instance, capsys):
        model = mock_weather_model_instance
        initial_content = {"gfs025": "2023-03-14T12:00:00"}
        with patch("builtins.open", mock_open(read_data=json.dumps(initial_content))) as mocked_file:
            with patch("json.dump") as mocked_json_dump:
                result = model.check_if_new()
                assert result is True
                captured = capsys.readouterr()
                assert "New model run detected for gfs025." in captured.out
                mocked_json_dump.assert_called_once_with({'gfs025': '2023-03-15T12:00:00'}, mocked_file(), indent=4)

    def test_check_if_new_older_run(self, mock_weather_model_instance, capsys):
        model = mock_weather_model_instance
        initial_content = {"gfs025": "2023-03-16T12:00:00"}
        with patch("builtins.open", mock_open(read_data=json.dumps(initial_content))):
            with patch("json.dump") as mocked_json_dump:
                result = model.check_if_new()
                assert result is False
                captured = capsys.readouterr()
                assert "No new model run for gfs025." in captured.out
                mocked_json_dump.assert_not_called()

    def test_init_no_metadata(self, mock_config):
        with patch('src.open_meteo_cast.weather_model.retrieve_model_metadata') as mock_retrieve_metadata:
            mock_retrieve_metadata.return_value = None
            model = WeatherModel("gfs025", mock_config)
            assert model.name == "gfs025"
            assert model.metadata is None

    def test_check_if_new_missing_timestamp(self, mock_config, capsys):
        with patch('src.open_meteo_cast.weather_model.retrieve_model_metadata') as mock_retrieve_metadata:
            mock_retrieve_metadata.return_value = {"model": "gfs025"}  # Missing timestamp
            model = WeatherModel("gfs025", mock_config)
            result = model.check_if_new()
            assert result is False
            captured = capsys.readouterr()
            assert "Error: Could not determine current run time for gfs025." in captured.out

    def test_check_if_new_write_error(self, mock_weather_model_instance, capsys):
        model = mock_weather_model_instance
        m = mock_open()
        m.side_effect = [
            mock_open(read_data='{}').return_value,  # for the read
            IOError("Disk full")  # for the write
        ]
        with patch('builtins.open', m):
            result = model.check_if_new()
            assert result is True
            captured = capsys.readouterr()
            assert "New model run detected for gfs025." in captured.out
            assert "Error writing updated run time to last_run.json: Disk full" in captured.out

    @patch('src.open_meteo_cast.weather_model.retrieve_model_run')
    def test_retrieve_data(self, mock_retrieve_model_run, mock_weather_model_instance, mock_config):
        model = mock_weather_model_instance
        mock_retrieve_model_run.return_value = pd.DataFrame({'date': [], 'temperature_2m_member0': []})
        model.retrieve_data(mock_config)
        mock_retrieve_model_run.assert_called_once_with(mock_config, "gfs025")
        assert isinstance(model.data, pd.DataFrame)

    def test_calculate_statistics(self, mock_weather_model_instance):
        model = mock_weather_model_instance
        # Mock some data for calculation
        df = pd.DataFrame({
            'date': pd.to_datetime(['2023-01-01', '2023-01-02']),
            'member1': [10, 20],
            'member2': [12, 22],
            'member3': [11, 21],
            'member4': [13, 23],
            'member5': [14, 24]
        })
        df.set_index('date', inplace=True)
        model.data = df
        model.calculate_statistics()
        assert isinstance(model.statistics, pd.DataFrame)
        assert isinstance(model.statistics.index, pd.DatetimeIndex)
        assert 'p10' in model.statistics.columns
        assert 'median' in model.statistics.columns
        assert 'p90' in model.statistics.columns
        assert model.statistics['p10'].iloc[0] == pytest.approx(10.4)
        assert model.statistics['median'].iloc[0] == pytest.approx(12.0)
        assert model.statistics['p90'].iloc[0] == pytest.approx(13.6)

    def test_calculate_statistics_no_data(self, mock_weather_model_instance, capsys):
        model = mock_weather_model_instance
        model.data = None
        model.calculate_statistics()
        assert model.statistics is None
        captured = capsys.readouterr()
        assert "Error: No data available to calculate statistics for gfs025." in captured.out

    def test_print_statistics(self, mock_weather_model_instance, capsys):
        model = mock_weather_model_instance
        index = pd.to_datetime(['2023-01-01'])
        model.statistics = pd.DataFrame({
            'p10': [10.4],
            'median': [12.0],
            'p90': [13.6]
        }, index=index)
        model.print_statistics()
        captured = capsys.readouterr()
        assert "Statistics for gfs025:" in captured.out
        assert "p10" in captured.out
        assert "median" in captured.out
        assert "p90" in captured.out

    def test_print_statistics_no_statistics(self, mock_weather_model_instance, capsys):
        model = mock_weather_model_instance
        model.statistics = None
        model.print_statistics()
        captured = capsys.readouterr()
        assert "No statistics available for gfs025." in captured.out

    def test_get_name(self, mock_weather_model_instance):
        model = mock_weather_model_instance
        assert model.name == "gfs025"

    def test_get_metadata(self, mock_weather_model_instance, mock_metadata):
        model = mock_weather_model_instance
        assert model.metadata == mock_metadata

    def test_get_last_run_time(self, mock_weather_model_instance):
        model = mock_weather_model_instance
        assert model.last_run_time == datetime(2023, 3, 15, 12, 0, 0)

    @patch('src.open_meteo_cast.weather_model.retrieve_model_run')
    @patch('pandas.DataFrame.to_csv')
    def test_print_data(self, mock_to_csv, mock_retrieve_model_run, mock_weather_model_instance, mock_config, capsys):
        model = mock_weather_model_instance
        mock_df = pd.DataFrame({'date': [pd.Timestamp('2023-03-15 00:00:00')], 'temperature_2m_member0': [10.0]})
        mock_retrieve_model_run.return_value = mock_df
        model.retrieve_data(mock_config) # Populate model.data
        model.print_data()
        captured = capsys.readouterr()
        assert str(mock_df) in captured.out
        mock_to_csv.assert_called_once_with('datos.csv')

    @patch('src.open_meteo_cast.weather_model.retrieve_model_run')
    def test_get_data_none(self, mock_retrieve_model_run, mock_weather_model_instance):
        model = mock_weather_model_instance
        assert model.data is None
