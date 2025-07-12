from typing import Any, Dict, Optional
import json
import pandas as pd
from datetime import datetime
from .open_meteo_api import retrieve_model_metadata, retrieve_model_run
from .statistics import calculate_percentiles

class WeatherModel:
    """
    Represents a weather model, handling metadata checks, data loading, and processing.
    """
    def __init__(self, model_name: str, config: Dict):
        """
        Initializes the WeatherModel instance.

        Args:
            model_name: The name of the model (e.g., 'gfs025').
            config: The application configuration dictionary.
        """
        self.name = model_name
        self.metadata_url = config.get('api', {}).get('open-meteo', {}).get('ensemble_metadata', {}).get(model_name)
        self.metadata = retrieve_model_metadata(self.metadata_url)
        self.data = None
        self.statistics = None

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

        last_run_time_str = last_runs.get(self.name)
        last_run_time = None
        if last_run_time_str:
            try:
                last_run_time = datetime.fromisoformat(last_run_time_str)
            except ValueError:
                print(f"Warning: Could not parse last run time '{last_run_time_str}' for {self.name}.")


        if last_run_time is None or current_run_time > last_run_time:
            print(f"New model run detected for {self.name}.")
            last_runs[self.name] = current_run_time.isoformat()
            try:
                with open(last_run_file, 'w', encoding='utf-8') as file:
                    json.dump(last_runs, file, indent=4)
            except IOError as e:
                print(f"Error writing updated run time to {last_run_file}: {e}")
            return True
        
        print(f"No new model run for {self.name}.")
        return False

    def print_metadata(self) -> None:
        """Formats and prints dictionary with model metadata."""
        print(f"Name: {self.name}")
        if self.metadata is None:
            print(f"Error: Metadata not available for {self.name}.")
            return
        for key, value in self.metadata.items():
            print(f"{key}: {value}")

    def retrieve_data(self, config: Dict[str, Any]) -> None:
        """Retrieves the weather model data using the provided configuration.

        Args:

            config: The application configuration dictionary.

        """
        self.data = retrieve_model_run(config, self.name)

    def print_data(self) -> None:
        """Prints the retrieved weather data and saves it to 'datos.csv'."""
        print(self.data)
        if self.data is not None:
            self.data.to_csv('datos.csv')

    def calculate_statistics(self) -> None:
        """
        Calculates row-wise statistics (p10, median, p90) from the retrieved data
        and stores them in self.statistics.
        """
        if self.data is not None:
            self.statistics = calculate_percentiles(self.data)
        else:
            print(f"Error: No data available to calculate statistics for {self.name}.")
            self.statistics = None

    def print_statistics(self) -> None:
        """
        Prints the calculated statistics.
        """
        if self.statistics is not None:
            print(f"\nStatistics for {self.name}:")
            print(self.statistics)
        else:
            print(f"No statistics available for {self.name}.")

    @property
    def last_run_time(self) -> Optional[datetime]:
        """Returns the last run initialization time from the model metadata.

        Returns:

            A datetime object representing the last run initialization time, or None if not available.

        """
        if self.metadata:
            return self.metadata.get('last_run_initialisation_time')
        return None