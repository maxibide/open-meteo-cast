from typing import Dict
import json
from .open_meteo_api import retrieve_model_metadata

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