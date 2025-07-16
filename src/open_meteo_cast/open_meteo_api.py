from typing import Dict, Optional, Any
import requests
import json
from datetime import datetime

import openmeteo_requests

from openmeteo_sdk.Variable import Variable

import pandas as pd
import requests_cache
from retry_requests import retry

def retrieve_model_metadata(url: str, timeout: int = 30) -> Optional[Dict[str, Any]]:
    """Retrieves model metadata from a specified Open-Meteo API URL.

    This function sends a GET request to the given URL, expecting a JSON response
    containing the metadata for a specific weather model.

    Args:
        url: The URL of the Open-Meteo model metadata API endpoint.
        timeout: The request timeout in seconds. Defaults to 30.

    Returns:
        A dictionary containing the model metadata if the request is successful,
        otherwise None.
    """
    timestamp_keys = [
        "data_end_time",
        "last_run_availability_time",
        "last_run_initialisation_time",
        "last_run_modification_time"
    ]

    try:
        response = requests.get(url, timeout=timeout)
        response.raise_for_status()  # Raise an exception for bad status codes
        json_metadata: Dict[str, Any] = response.json()

        for key in timestamp_keys:
            if key in json_metadata and isinstance(json_metadata[key], (int, float)):
                try:
                    json_metadata[key] = datetime.fromtimestamp(json_metadata[key])
                except (ValueError, OSError):
                    pass
        return json_metadata
    except requests.exceptions.RequestException as e:
        print(f"Error retrieving data from {url}: {e}")
        return None
    except json.JSONDecodeError as e:
        print(f"Error decoding JSON from {url}: {e}")
        return None

def retrieve_model_variable(config: Dict[str, Any], model_name: str, var_to_retrieve: str) -> pd.DataFrame:
    """Retrieves hourly temperature data for a specific weather model from the Open-Meteo API.

    This function sets up an Open-Meteo API client with caching and retry mechanisms,
    then fetches the hourly data for a given model, variable and location based on the
    provided configuration.

    Args:
        config: A dictionary containing the application configuration, including API
                endpoints and location details.
        model_name: The name of the weather model to retrieve data for (e.g., 'gfs').
        variable: The name of the parameter to be retrieved (e.g., 'temperature_2m')

    Returns:
        A pandas DataFrame containing the hourly temperature data for the specified model,
        with a 'date' column and columns for each ensemble member's temperature.
    """

    # Setup the Open-Meteo API client with cache and retry on error
    cache_session = requests_cache.CachedSession('.cache', expire_after = 3600)
    retry_session = retry(cache_session, retries = 5, backoff_factor = 0.2)
    openmeteo = openmeteo_requests.Client(session = retry_session)

    if model_name == "gfs025" and var_to_retrieve == "temperature_850hPa":
        model_name = "gfs05"

    url = config['api']['open-meteo']['ensemble_url']
    params = {
        "latitude": config['location']['latitude'],
        "longitude": config['location']['longitude'],
        "hourly": var_to_retrieve,
        "models": [model_name],
        "timezone": "auto",
        "forecast_hours": 72
    }

    responses = openmeteo.weather_api(url, params=params)

    if not responses:
        print("No data received from Open-Meteo API.")
        return None

    # Process first location. Add a for-loop for multiple locations or weather models
    response = responses[0]
    print(f"Coordinates {response.Latitude()}°N {response.Longitude()}°E")
    print(f"Elevation {response.Elevation()} m asl")
    print(f"Timezone {response.Timezone()}{response.TimezoneAbbreviation()}")
    print(f"Timezone difference to GMT+0 {response.UtcOffsetSeconds()} s")

    # Process hourly data
    hourly = response.Hourly()
    hourly_variables = list(map(lambda i: hourly.Variables(i), range(0, hourly.VariablesLength())))
    variable_filters = {
        "temperature_2m": lambda x: x.Variable() == Variable.temperature and x.Altitude() == 2,
        "dew_point_2m": lambda x: x.Variable() == Variable.dew_point and x.Altitude() == 2,
        "pressure_msl": lambda x: x.Variable() == Variable.pressure_msl,
        "temperature_850hPa": lambda x: x.Variable() == Variable.temperature and x.PressureLevel() == 850,
        "precipitation": lambda x: x.Variable() == Variable.precipitation,
        "snowfall": lambda x: x.Variable() == Variable.snowfall,
        "cloud_cover": lambda x: x.Variable() == Variable.cloud_cover,
        "wind_speed_10m": lambda x: x.Variable() == Variable.wind_speed and x.Altitude() == 10,
        "wind_gusts_10m": lambda x: x.Variable() == Variable.wind_gusts and x.Altitude() == 10,
        "wind_direction_10m": lambda x: x.Variable() == Variable.wind_direction and x.Altitude() == 10,
        "cape": lambda x: x.Variable() == Variable.cape
    }

    if var_to_retrieve not in variable_filters:
        print(f"Variable {var_to_retrieve} not supported")
        return

    hourly_variable = filter(variable_filters[var_to_retrieve], hourly_variables)

    hourly_data = {"date": pd.date_range(
        start = pd.to_datetime(hourly.Time(), unit = "s", utc = True),
        end = pd.to_datetime(hourly.TimeEnd(), unit = "s", utc = True),
        freq = pd.Timedelta(seconds = hourly.Interval()),
        inclusive = "left"
    )}

    # Process all members
    for variable in hourly_variable:
        member = variable.EnsembleMember()
        hourly_data[f"{var_to_retrieve}_member{member}"] = variable.ValuesAsNumpy()

    hourly_dataframe = pd.DataFrame(data = hourly_data)
    print(hourly_dataframe)
    return hourly_dataframe