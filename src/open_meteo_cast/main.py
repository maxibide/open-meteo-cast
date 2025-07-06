from typing import Dict, Optional
import yaml
import requests
import json
from datetime import datetime

def load_config(config_path: str) -> Dict:
    """Load configuration from a YAML archive"""
    try:
        with open(config_path, 'r', encoding='utf-8') as file:
            return yaml.safe_load(file)
    except FileNotFoundError:
        print(f"Error: File {config_path} not found")
        return {}
    except yaml.YAMLError as e:
        print(f"Error reading YAML file: {e}")
        return {} 
    
def retrieve_model_metadata(url: str, timeout: int = 30) -> Optional[Dict]:
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
    try:
        response = requests.get(url, timeout=timeout)
        response.raise_for_status()  # Raise an exception for bad status codes
        return response.json()
    except requests.exceptions.RequestException as e:
        print(f"Error retrieving data from {url}: {e}")
        return None
    except json.JSONDecodeError as e:
        print(f"Error decoding JSON from {url}: {e}")
        return None

def main():
    """Main function to load config, retrieve data, and print it."""
    config = load_config('resources/default_config.yaml')
    if not config:
        return

    try:
        url = config['api']['open-meteo']['ensemble_metadata']['gfs']
    except KeyError:
        print("Error: Could not find the required URL in the configuration file.")
        return

    model_metadata = retrieve_model_metadata(url)

    if model_metadata:
        timestamp_keys = [
            "data_end_time",
            "last_run_availability_time",
            "last_run_initialisation_time",
            "last_run_modification_time"
        ]
        for key in timestamp_keys:
            if key in model_metadata and isinstance(model_metadata[key], (int, float)):
                try:
                    model_metadata[key] = datetime.fromtimestamp(model_metadata[key]).strftime('%Y-%m-%d %H:%M:%S')
                except (ValueError, OSError):
                    pass # Keep original value if conversion fails
        for key, value in model_metadata.items():
            print(f"{key}: {value}")

if __name__ == "__main__":
    main()
