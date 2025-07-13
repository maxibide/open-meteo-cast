from unittest.mock import patch, MagicMock
import pandas as pd
import numpy as np

from src.open_meteo_cast.open_meteo_api import retrieve_model_variable

@patch('openmeteo_requests.Client')
@patch('requests_cache.CachedSession')
@patch('retry_requests.retry')
def test_retrieve_model_variable_success(mock_retry, mock_cached_session, mock_openmeteo_client):
    # Mock the configuration
    config = {
        "api": {
            "open-meteo": {
                "ensemble_url": "http://test-ensemble-api.com/v1/ensemble"
            }
        },
        "location": {
            "latitude": 40.7128,
            "longitude": -74.0060
        }
    }
    model_name = "gfs025"
    variable = "temperature_2m"

    # Mock the Open-Meteo API response
    mock_response = MagicMock()
    mock_response.Latitude.return_value = 40.7128
    mock_response.Longitude.return_value = -74.0060
    mock_response.Elevation.return_value = 10
    mock_response.Timezone.return_value = "America/New_York"
    mock_response.TimezoneAbbreviation.return_value = "EST"
    mock_response.UtcOffsetSeconds.return_value = -18000

    mock_hourly = MagicMock()
    mock_hourly.Time.return_value = 1678886400  # Example timestamp
    mock_hourly.TimeEnd.return_value = 1678886400 + 72 * 3600 # 72 hours later
    mock_hourly.Interval.return_value = 3600 # 1 hour interval

    # Mock hourly variables
    mock_variable_temp_member0 = MagicMock()
    mock_variable_temp_member0.Variable.return_value = MagicMock()
    mock_variable_temp_member0.Variable.return_value.__eq__.return_value = True # To make it equal to Variable.temperature
    mock_variable_temp_member0.Altitude.return_value = 2
    mock_variable_temp_member0.EnsembleMember.return_value = 0
    mock_variable_temp_member0.ValuesAsNumpy.return_value = np.array([10.0] * 72)

    mock_variable_temp_member1 = MagicMock()
    mock_variable_temp_member1.Variable.return_value = MagicMock()
    mock_variable_temp_member1.Variable.return_value.__eq__.return_value = True # To make it equal to Variable.temperature
    mock_variable_temp_member1.Altitude.return_value = 2
    mock_variable_temp_member1.EnsembleMember.return_value = 1
    mock_variable_temp_member1.ValuesAsNumpy.return_value = np.array([10.5] * 72)

    mock_hourly.Variables.side_effect = lambda i: [mock_variable_temp_member0, mock_variable_temp_member1][i]
    mock_hourly.VariablesLength.return_value = 2 # Number of mocked variables

    mock_response.Hourly.return_value = mock_hourly
    mock_openmeteo_client.return_value.weather_api.return_value = [mock_response]

    # Call the function
    df = retrieve_model_variable(config, model_name, variable)

    # Assertions
    
    mock_openmeteo_client.return_value.weather_api.assert_called_once_with(
        config["api"]["open-meteo"]["ensemble_url"],
        params={
            "latitude": config["location"]["latitude"],
            "longitude": config["location"]["longitude"],
            "hourly": variable,
            "models": [model_name],
            "timezone": "auto",
            "forecast_hours": 72
        }
    )

    assert isinstance(df, pd.DataFrame)
    assert "date" in df.columns
    assert "temperature_2m_member0" in df.columns
    assert "temperature_2m_member1" in df.columns
    assert len(df) == 72 # Based on mocked data length
    np.testing.assert_array_equal(df["temperature_2m_member0"].values, np.array([10.0] * 72))
    np.testing.assert_array_equal(df["temperature_2m_member1"].values, np.array([10.5] * 72))

@patch('openmeteo_requests.Client')
@patch('requests_cache.CachedSession')
@patch('retry_requests.retry')
def test_retrieve_model_variable_empty_response(mock_retry, mock_cached_session, mock_openmeteo_client):
    config = {
        "api": {
            "open-meteo": {
                "ensemble_url": "http://test-ensemble-api.com/v1/ensemble"
            }
        },
        "location": {
            "latitude": 40.7128,
            "longitude": -74.0060
        }
    }
    model_name = "gfs025"
    variable = "temperature_2m"

    mock_openmeteo_client.return_value.weather_api.return_value = [] # Empty response

    df = retrieve_model_variable(config, model_name, variable)

    assert df is None # Or handle as appropriate for empty response, e.g., empty DataFrame
