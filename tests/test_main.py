from unittest.mock import patch, MagicMock, mock_open
from src.open_meteo_cast.main import main

@patch('src.open_meteo_cast.main.database.purge_old_runs')
@patch('src.open_meteo_cast.main.database.create_tables')
@patch('src.open_meteo_cast.main.WeatherModel')
@patch('src.open_meteo_cast.main.load_config')
@patch('src.open_meteo_cast.main.Ensemble')
def test_main_no_new_runs(mock_ensemble, mock_load_config, mock_weather_model, mock_create_tables, mock_purge_old_runs, capsys):
    # Setup: One model, no new run
    mock_load_config.return_value = {'models_used': ['gfs025'], 'database': {'retention_days': 30}}
    mock_model_instance = MagicMock()
    mock_model_instance.is_new = False
    mock_model_instance.is_valid = True
    mock_model_instance.data = {'some_data': True}
    mock_weather_model.return_value = mock_model_instance

    main()

    mock_create_tables.assert_called_once()
    mock_purge_old_runs.assert_called_once_with(30)
    mock_weather_model.assert_called_once_with('gfs025', mock_load_config.return_value)
    mock_ensemble.assert_not_called()
    captured = capsys.readouterr()
    assert "No new model runs" in captured.out

@patch('src.open_meteo_cast.main.os.makedirs')
@patch('src.open_meteo_cast.main.database.purge_old_runs')
@patch('src.open_meteo_cast.main.database.create_tables')
@patch('src.open_meteo_cast.main.WeatherModel')
@patch('src.open_meteo_cast.main.load_config')
@patch('src.open_meteo_cast.main.Ensemble')
def test_main_one_new_run_proceeds(mock_ensemble, mock_load_config, mock_weather_model, mock_create_tables, mock_purge_old_runs, mock_makedirs, capsys):
    # Setup: One model with a new run
    mock_load_config.return_value = {'models_used': ['gfs025'], 'database': {'retention_days': 30}}
    mock_model_instance = MagicMock()
    mock_model_instance.is_new = True
    mock_model_instance.is_valid = True
    mock_model_instance.data = {'some_data': True}
    mock_weather_model.return_value = mock_model_instance
    mock_ensemble.return_value.to_html_table.return_value = "<html>test table</html>"

    main()

    mock_create_tables.assert_called_once()
    mock_purge_old_runs.assert_called_once_with(30)
    mock_weather_model.assert_called_once_with('gfs025', mock_load_config.return_value)
    mock_model_instance.export_statistics_to_csv.assert_called_once()
    mock_ensemble.assert_called_once_with([mock_model_instance], mock_load_config.return_value)
    mock_ensemble.return_value.to_csv.assert_called_once()
    captured = capsys.readouterr()
    assert "New models downloaded" in captured.out

@patch('src.open_meteo_cast.main.open', new_callable=mock_open)
@patch('src.open_meteo_cast.main.os.path.join', return_value='output/ensemble_forecast.html')
@patch('src.open_meteo_cast.main.Ensemble')
@patch('src.open_meteo_cast.main.load_config')
@patch('src.open_meteo_cast.main.WeatherModel')
@patch('src.open_meteo_cast.main.database.create_tables')
@patch('src.open_meteo_cast.main.database.purge_old_runs')
def test_main_creates_html_table(mock_purge, mock_create_tables, mock_weather_model, mock_load_config, mock_ensemble, mock_path_join, mock_open_file):
    # Setup
    mock_load_config.return_value = {'models_used': ['gfs025'], 'database': {'retention_days': 30}}
    mock_model_instance = MagicMock()
    mock_model_instance.is_new = True
    mock_model_instance.is_valid = True
    mock_model_instance.data = {'some_data': True}
    mock_weather_model.return_value = mock_model_instance
    mock_ensemble.return_value.to_html_table.return_value = "<html>test table</html>"

    main()

    mock_ensemble.return_value.to_html_table.assert_called_once()
    mock_open_file.assert_called_once_with('output/ensemble_forecast.html', 'w', encoding='utf-8')
    mock_open_file().write.assert_called_once_with("<html>test table</html>")
