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

def print_model_metadata(model_metadata: Dict) -> int:
    """Formats and prints dictionary with model metadata"""
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
    return 0

def check_if_model_run_is_new(model_metadata: Dict) -> bool:
    """Checks if the model run is newer than the last recorded run."""
    last_run_file = 'last_run.json'
    
    try:
        with open(last_run_file, 'r', encoding='utf-8') as file:
            last_runs = json.load(file)
    except FileNotFoundError:
        last_runs = {}
    except json.JSONDecodeError:
        print(f"Warning: Could not decode JSON from {last_run_file}. Treating as empty.")
        last_runs = {}

    model_name = model_metadata.get('model')
    current_run_time = model_metadata.get('last_run_initialisation_time')

    if not model_name or current_run_time is None:
        print("Error: Incomplete model metadata provided.")
        return False

    last_run_time = last_runs.get(model_name)

    if last_run_time is None or current_run_time > last_run_time:
        print(f"New model run detected for {model_name}.")
        last_runs[model_name] = current_run_time
        try:
            with open(last_run_file, 'w', encoding='utf-8') as file:
                json.dump(last_runs, file, indent=4)
        except IOError as e:
            print(f"Error writing updated run time to {last_run_file}: {e}")
        return True
    
    print(f"No new model run for {model_name}.")
    return False

def main():
    """Main function to load config, retrieve data, and print it."""
    config = load_config('resources/default_config.yaml')
    if not config:
        return

    models = ['gfs']
    
    for model in models:
        url = config['api']['open-meteo']['ensemble_metadata'][model]
        print(f"\n{model}\n")
        model_metadata = retrieve_model_metadata(url)
        model_metadata['model'] = model
        print_model_metadata(model_metadata)
        check_if_model_run_is_new(model_metadata)

if __name__ == "__main__":
    main()
