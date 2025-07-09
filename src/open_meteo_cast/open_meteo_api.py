from typing import Dict, Optional, Any
import requests
import json
from datetime import datetime

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
                    json_metadata[key] = datetime.fromtimestamp(json_metadata[key]).strftime('%Y-%m-%d %H:%M:%S')
                except (ValueError, OSError):
                    pass
        return json_metadata
    except requests.exceptions.RequestException as e:
        print(f"Error retrieving data from {url}: {e}")
        return None
    except json.JSONDecodeError as e:
        print(f"Error decoding JSON from {url}: {e}")
        return None
