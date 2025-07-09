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

class WeatherModel:
    """
    Represents a weather model, handling metadata checks, data loading, and processing.
    """
    def __init__(self, model_name: str, config: Dict):
        """
        Initializes the WeatherModel instance.

        Args:
            model_name: The name of the model (e.g., 'gfs').
            config: The application configuration dictionary.
        """
        self.name = model_name
        self.metadata_url = config.get('api', {}).get('open-meteo', {}).get('ensemble_metadata', {}).get(model_name)
        self.metadata = retrieve_model_metadata(self.metadata_url)
        self.data = None

    def check_if_new(self, last_run_file: str = 'last_run.json') -> bool:
        """
        Checks if the model run is newer than the last recorded run.
        It fetches metadata and compares timestamps.
        """
        if self.metadata is None:
            print(f"Error: Metadata not available for {self.name}. Cannot check for new run.")
            return False

        try:
            with open(last_run_file, 'r', encoding='utf-8') as file:
                last_runs = json.load(file)
        except FileNotFoundError:
            last_runs = {}
        except json.JSONDecodeError:
            print(f"Warning: Could not decode JSON from {last_run_file}. Treating as empty.")
            last_runs = {}

        current_run_time = self.metadata.get('last_run_initialisation_time')

        if current_run_time is None:
            print(f"Error: Could not determine current run time for {self.name}.")
            return False

        last_run_time = last_runs.get(self.name)

        if last_run_time is None or current_run_time > last_run_time:
            print(f"New model run detected for {self.name}.")
            last_runs[self.name] = current_run_time
            try:
                with open(last_run_file, 'w', encoding='utf-8') as file:
                    json.dump(last_runs, file, indent=4)
            except IOError as e:
                print(f"Error writing updated run time to {last_run_file}: {e}")
            return True
        
        print(f"No new model run for {self.name}.")
        return False

    def print_metadata(self) -> None:
        """Formats and prints dictionary with model metadata"""
        print(f"Name: {self.name}")
        if self.metadata is None:
            print(f"Error: Metadata not available for {self.name}.")
            return
        for key, value in self.metadata.items():
            print(f"{key}: {value}")