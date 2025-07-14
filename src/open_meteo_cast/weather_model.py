from typing import Any, Dict, Optional
import json
import os
from datetime import datetime
import pandas as pd
from .open_meteo_api import retrieve_model_metadata, retrieve_model_variable
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
        self.data: Dict[str, Optional[pd.DataFrame]] = {}
        self.statistics: Dict[str, Optional[pd.DataFrame]] = {}

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
        variables = ["temperature_2m", "dew_point_2m", "pressure_msl", "temperature_850hPa"]
        for variable in variables:
            df = retrieve_model_variable(config, self.name, variable)
            if df is not None and 'date' in df.columns:
                df.set_index('date', inplace=True)
            self.data[variable] = df

    def print_data(self) -> None:
        """Prints the retrieved weather data."""
        for variable, data_df in self.data.items():
            print(f"\nData for {variable}:")
            if data_df is not None:
                print(data_df)
            else:
                print(f"No data available for {variable}.")

    def calculate_statistics(self) -> None:
        """
        Calculates row-wise statistics (p10, median, p90) from the retrieved data
        and stores them in self.statistics.
        """
        if not self.data:
            print(f"Error: No data available to calculate statistics for {self.name}.")
            return

        for variable, data_df in self.data.items():
            if data_df is not None:
                self.statistics[variable] = calculate_percentiles(data_df)
            else:
                print(f"Warning: No data for variable '{variable}' to calculate statistics.")
            
    def print_statistics(self) -> None:
        """
        Prints the calculated statistics.
        """
        if self.statistics is not None:
            for variable in self.statistics.keys():
                print(f"\nStatistics for {self.name} {variable}:")
                print(self.statistics[variable])
        else:
            print(f"No statistics available for {self.name}.")

    def export_statistics_to_csv(self, output_dir: str = 'output', config: Dict = {}) -> None:
        """
        Exports the calculated statistics to a CSV file in the specified directory.

        The filename is generated based on the model name and the last run initialization time.

        Args:
            output_dir: The directory where the CSV file will be saved. Defaults to 'output'.
            config: The application configuration dictionary.
        """
        if not self.statistics:
            print(f"No statistics available to export for {self.name}.")
            return

        last_run = self.last_run_time
        if last_run is None:
            print(f"Error: Cannot determine last run time for {self.name}. Cannot export statistics.")
            return

        timestamp_str = last_run.strftime('%Y%m%dT%H%M%S')
        timezone = config.get('location', {}).get('timezone')

        for variable, stats_df in self.statistics.items():
            if stats_df is None:
                print(f"No statistics to export for variable '{variable}'.")
                continue

            filename = f"{self.name}_{timestamp_str}_{variable}.csv"
            filepath = os.path.join(output_dir, filename)

            export_df = stats_df.copy()

            if isinstance(export_df.index, pd.DatetimeIndex) and timezone:
                export_df.index = export_df.index.tz_convert(timezone)

            for col in export_df.columns:
                if pd.api.types.is_numeric_dtype(export_df[col]):
                    export_df[col] = export_df[col].round(1)
            
            try:
                export_df.to_csv(filepath, index=True)
                print(f"Successfully exported statistics to {filepath}")
            except IOError as e:
                print(f"Error exporting statistics to {filepath}: {e}")

    @property
    def last_run_time(self) -> Optional[datetime]:
        """Returns the last run initialization time from the model metadata.

        Returns:

            A datetime object representing the last run initialization time, or None if not available.

        """
        if self.metadata:
            return self.metadata.get('last_run_initialisation_time')
        return None
