import pytest
from unittest.mock import patch, mock_open
import json
import pandas as pd
from datetime import datetime
import os

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

    @patch('src.open_meteo_cast.weather_model.retrieve_model_variable')
    def test_retrieve_data(self, mock_retrieve_model_variable, mock_weather_model_instance, mock_config):
        model = mock_weather_model_instance
        # Mock the return value for each variable
        mock_retrieve_model_variable.side_effect = [
            pd.DataFrame({'date': [], 'temperature_2m_member0': []}),
            pd.DataFrame({'date': [], 'dew_point_2m_member0': []}),
            pd.DataFrame({'date': [], 'pressure_msl_member0': []}),
            pd.DataFrame({'date': [], 'temperature_850hPa_member0': []}),
            pd.DataFrame({'date': [], 'precipitation_member0': []}),
            pd.DataFrame({'date': [], 'snowfall_member0': []}),
            pd.DataFrame({'date': [], 'cloud_cover_member0': []}),
            pd.DataFrame({'date': [], 'wind_speed_10m_member0': []}),
            pd.DataFrame({'date': [], 'wind_gusts_10m_member0': []})
        ]
        model.retrieve_data(mock_config)
        # Check that retrieve_model_variable was called for each variable
        assert mock_retrieve_model_variable.call_count == 9
        mock_retrieve_model_variable.assert_any_call(mock_config, "gfs025", "temperature_2m")
        mock_retrieve_model_variable.assert_any_call(mock_config, "gfs025", "dew_point_2m")
        mock_retrieve_model_variable.assert_any_call(mock_config, "gfs025", "pressure_msl")
        mock_retrieve_model_variable.assert_any_call(mock_config, "gfs025", "temperature_850hPa")
        mock_retrieve_model_variable.assert_any_call(mock_config, "gfs025", "precipitation")
        mock_retrieve_model_variable.assert_any_call(mock_config, "gfs025", "snowfall")
        mock_retrieve_model_variable.assert_any_call(mock_config, "gfs025", "cloud_cover")
        mock_retrieve_model_variable.assert_any_call(mock_config, "gfs025", "wind_speed_10m")
        mock_retrieve_model_variable.assert_any_call(mock_config, "gfs025", "wind_gusts_10m")
        assert isinstance(model.data, dict)
        assert "temperature_2m" in model.data
        assert "dew_point_2m" in model.data
        assert "pressure_msl" in model.data
        assert "temperature_850hPa" in model.data
        assert "precipitation" in model.data
        assert "snowfall" in model.data
        assert "cloud_cover" in model.data
        assert "wind_speed_10m" in model.data
        assert "wind_gusts_10m" in model.data

    def test_calculate_statistics(self, mock_weather_model_instance):
        model = mock_weather_model_instance
        # Mock some data for calculation
        df_temp = pd.DataFrame({
            'date': pd.to_datetime(['2023-01-01', '2023-01-02']),
            'member1': [10, 20],
            'member2': [12, 22]
        }).set_index('date')
        df_dew = pd.DataFrame({
            'date': pd.to_datetime(['2023-01-01', '2023-01-02']),
            'member1': [5, 15],
            'member2': [7, 17]
        }).set_index('date')
        df_cloud = pd.DataFrame({
            'date': pd.to_datetime(['2023-01-01', '2023-01-02']),
            'member1': [50, 100],
            'member2': [60, 0]
        }).set_index('date')
        model.data = {"temperature_2m": df_temp, "dew_point_2m": df_dew, "cloud_cover": df_cloud}
        model.calculate_statistics()
        assert isinstance(model.statistics, dict)
        assert "temperature_2m" in model.statistics
        assert "dew_point_2m" in model.statistics
        assert "cloud_cover" in model.statistics
        assert isinstance(model.statistics["temperature_2m"], pd.DataFrame)
        assert isinstance(model.statistics["dew_point_2m"], pd.DataFrame)
        assert isinstance(model.statistics["cloud_cover"], pd.DataFrame)
        assert 'p10' in model.statistics["temperature_2m"].columns
        assert model.statistics["temperature_2m"]['p10'].iloc[0] == pytest.approx(10.2)
        assert 'octa_6_prob' in model.statistics["cloud_cover"].columns
        assert model.statistics["cloud_cover"]['octa_6_prob'].iloc[0] == pytest.approx(0.0) # 50% and 60% -> 4 and 5 octas. 0.0 for 6
        assert model.statistics["cloud_cover"]['octa_0_prob'].iloc[1] == pytest.approx(0.5) # 100% and 0% -> 8 and 0 octas. 0.5 for 8 and 0.5 for 0

    def test_calculate_statistics_no_data(self, mock_weather_model_instance, capsys):
        model = mock_weather_model_instance
        model.data = {}
        model.calculate_statistics()
        assert model.statistics == {}
        captured = capsys.readouterr()
        assert "Error: No data available to calculate statistics for gfs025." in captured.out

    def test_print_statistics(self, mock_weather_model_instance, capsys):
        model = mock_weather_model_instance
        index = pd.to_datetime(['2023-01-01'])
        model.statistics = {
            "temperature_2m": pd.DataFrame({'p10': [10.4], 'median': [12.0], 'p90': [13.6]}, index=index),
            "dew_point_2m": pd.DataFrame({'p10': [5.2], 'median': [6.0], 'p90': [6.8]}, index=index)
        }
        model.print_statistics()
        captured = capsys.readouterr()
        assert "Statistics for gfs025 temperature_2m:" in captured.out
        assert "p10" in captured.out
        assert "Statistics for gfs025 dew_point_2m:" in captured.out

    def test_print_statistics_no_statistics(self, mock_weather_model_instance, capsys):
        model = mock_weather_model_instance
        model.statistics = None
        model.print_statistics()
        captured = capsys.readouterr()
        assert "No statistics available for gfs025." in captured.out

    def test_export_statistics_to_csv(self, mock_weather_model_instance, mock_config, tmpdir):
        model = mock_weather_model_instance
        index = pd.to_datetime(['2023-01-01'])
        model.statistics = {
            "temperature_2m": pd.DataFrame({'p10': [10.4]}, index=index),
            "dew_point_2m": pd.DataFrame({'p10': [5.2]}, index=index)
        }
        output_dir = str(tmpdir)
        model.export_statistics_to_csv(output_dir=output_dir, config=mock_config)
        
        expected_filename = os.path.join(output_dir, 'gfs025_20230315T120000.csv')
        assert os.path.exists(expected_filename)
        
        df = pd.read_csv(expected_filename)
        assert 'temperature_2m_p10' in df.columns
        assert 'dew_point_2m_p10' in df.columns
        assert df['temperature_2m_p10'][0] == 10.4
        assert df['dew_point_2m_p10'][0] == 5.2

    def test_get_name(self, mock_weather_model_instance):
        model = mock_weather_model_instance
        assert model.name == "gfs025"

    def test_get_metadata(self, mock_weather_model_instance, mock_metadata):
        model = mock_weather_model_instance
        assert model.metadata == mock_metadata

    def test_get_last_run_time(self, mock_weather_model_instance):
        model = mock_weather_model_instance
        assert model.last_run_time == datetime(2023, 3, 15, 12, 0, 0)

    @patch('src.open_meteo_cast.weather_model.retrieve_model_variable')
    def test_print_data(self, mock_retrieve_model_variable, mock_weather_model_instance, mock_config, capsys):
        model = mock_weather_model_instance
        mock_df_temp = pd.DataFrame({'date': [pd.Timestamp('2023-03-15 00:00:00')], 'temperature_2m_member0': [10.0]})
        mock_df_dew = pd.DataFrame({'date': [pd.Timestamp('2023-03-15 00:00:00')], 'dew_point_2m_member0': [5.0]})
        mock_df_precip = pd.DataFrame({'date': [pd.Timestamp('2023-03-15 00:00:00')], 'precipitation_member0': [1.0]})
        mock_df_snowfall = pd.DataFrame({'date': [pd.Timestamp('2023-03-15 00:00:00')], 'snowfall_member0': [2.0]})
        mock_df_cloud_cover = pd.DataFrame({'date': [pd.Timestamp('2023-03-15 00:00:00')], 'cloud_cover_member0': [50.0]})
        mock_df_wind_speed = pd.DataFrame({'date': [pd.Timestamp('2023-03-15 00:00:00')], 'wind_speed_10m_member0': [15.0]})
        mock_df_wind_gusts = pd.DataFrame({'date': [pd.Timestamp('2023-03-15 00:00:00')], 'wind_gusts_10m_member0': [25.0]})
        mock_retrieve_model_variable.side_effect = [mock_df_temp, mock_df_dew, None, None, mock_df_precip, mock_df_snowfall, mock_df_cloud_cover, mock_df_wind_speed, mock_df_wind_gusts] # Simulate one variable having no data
        model.retrieve_data(mock_config)
        model.print_data()
        captured = capsys.readouterr()
        assert "Data for temperature_2m:" in captured.out
        assert str(mock_df_temp) in captured.out
        assert "Data for dew_point_2m:" in captured.out
        assert str(mock_df_dew) in captured.out
        assert "No data available for pressure_msl." in captured.out
        assert "No data available for temperature_850hPa." in captured.out
        assert "Data for precipitation:" in captured.out
        assert str(mock_df_precip) in captured.out
        assert "Data for snowfall:" in captured.out
        assert str(mock_df_snowfall) in captured.out
        assert "Data for cloud_cover:" in captured.out
        assert str(mock_df_cloud_cover) in captured.out
        assert "Data for wind_speed_10m:" in captured.out
        assert str(mock_df_wind_speed) in captured.out
        assert "Data for wind_gusts_10m:" in captured.out
        assert str(mock_df_wind_gusts) in captured.out

    
