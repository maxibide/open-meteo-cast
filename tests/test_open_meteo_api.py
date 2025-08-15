import pytest
from unittest.mock import patch, MagicMock
import pandas as pd
import numpy as np

from src.open_meteo_cast.open_meteo_api import retrieve_model_variable
from openmeteo_sdk.Variable import Variable

@pytest.mark.parametrize("variable,var_enum,altitude,pressure,expected_col_name", [
    ("temperature_2m", Variable.temperature, 2, None, "temperature_2m_member0"),
    ("dew_point_2m", Variable.dew_point, 2, None, "dew_point_2m_member0"),
    ("pressure_msl", Variable.pressure_msl, None, None, "pressure_msl_member0"),
    ("temperature_850hPa", Variable.temperature, None, 850, "temperature_850hPa_member0"),
])
@patch('openmeteo_requests.Client')
@patch('requests_cache.CachedSession')
@patch('retry_requests.retry')
def test_retrieve_model_variable_success(mock_retry, mock_cached_session, mock_openmeteo_client, variable, var_enum, altitude, pressure, expected_col_name):
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
        },
        "forecast_hours": 72
    }
    model_name = "gfs025"

    # Mock the Open-Meteo API response
    mock_response = MagicMock()
    mock_response.Latitude.return_value = 40.7128
    mock_response.Longitude.return_value = -74.0060
    mock_response.Elevation.return_value = 10
    mock_response.Timezone.return_value = "America/New_York"
    mock_response.TimezoneAbbreviation.return_value = "EST"
    mock_response.UtcOffsetSeconds.return_value = -18000

    mock_hourly = MagicMock()
    mock_hourly.Time.return_value = 1678886400
    mock_hourly.TimeEnd.return_value = 1678886400 + 72 * 3600
    mock_hourly.Interval.return_value = 3600

    # Mock hourly variables
    mock_variable = MagicMock()
    mock_variable.Variable.return_value = var_enum
    mock_variable.Altitude.return_value = altitude
    mock_variable.PressureLevel.return_value = pressure
    mock_variable.EnsembleMember.return_value = 0
    mock_variable.ValuesAsNumpy.return_value = np.array([10.0] * 72)

    mock_hourly.Variables.side_effect = lambda i: [mock_variable][i]
    mock_hourly.VariablesLength.return_value = 1

    mock_response.Hourly.return_value = mock_hourly
    mock_openmeteo_client.return_value.weather_api.return_value = [mock_response]

    # Call the function
    df = retrieve_model_variable(config, model_name, variable)

    # Assertions
    expected_model = model_name
    if model_name == "gfs025" and variable == "temperature_850hPa":
        expected_model = "gfs05"
    mock_openmeteo_client.return_value.weather_api.assert_called_once_with(
        config["api"]["open-meteo"]["ensemble_url"],
        params={
            "latitude": config["location"]["latitude"],
            "longitude": config["location"]["longitude"],
            "hourly": variable,
            "models": [expected_model],
            "timezone": "auto",
            "forecast_hours": 84
        }
    )

    assert isinstance(df, pd.DataFrame)
    assert "date" in df.columns
    assert expected_col_name in df.columns
    assert len(df) == 72
    np.testing.assert_array_equal(df[expected_col_name].values, np.array([10.0] * 72))

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
        },
        "forecast_hours": 72
    }
    model_name = "gfs025"
    variable = "temperature_2m"

    mock_openmeteo_client.return_value.weather_api.return_value = [] # Empty response

    df = retrieve_model_variable(config, model_name, variable)

    assert df is None # Or handle as appropriate for empty response, e.g., empty DataFrame
