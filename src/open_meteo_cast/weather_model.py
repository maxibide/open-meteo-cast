from typing import Any, Dict, Optional
import json
import os
from datetime import datetime
import pandas as pd
import numpy as np
from .database import get_db_connection, get_last_run_timestamp
from .open_meteo_api import retrieve_model_metadata, retrieve_model_variable
from .statistics import calculate_percentiles, calculate_precipitation_statistics, calculate_octa_probabilities, calculate_wind_direction_probabilities, calculate_weather_code_probabilities

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

    def check_if_new(self) -> bool:
        """
        Checks if the model run is newer than the last recorded run in the database.
        """
        if self.metadata is None:
            print(f"Error: Metadata not available for {self.name}. Cannot check for new run.")
            return False

        current_run_time = self.metadata.get('last_run_initialisation_time')
        if current_run_time is None:
            print(f"Error: Could not determine current run time for {self.name}.")
            return False

        last_run_from_db = get_last_run_timestamp(self.name)

        if last_run_from_db is None or current_run_time > last_run_from_db:
            print(f"New model run detected for {self.name}.")
            return True
        
        print(f"No new model run for {self.name}. Loading from database.")
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
        variables = ["temperature_2m", "dew_point_2m", "pressure_msl", "temperature_850hPa", "precipitation",
                     "snowfall", "cloud_cover", "wind_speed_10m", "wind_gusts_10m", "wind_direction_10m",
                     "cape", "weather_code"]
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
        Calculates row-wise statistics from the retrieved data
        and stores them in self.statistics.
        For the 'precipitation' variable, it calculates the probability of precipitation
        and the conditional average. For all other variables, it calculates
        p10, median, and p90 percentiles.
        """
        if not self.data:
            print(f"Error: No data available to calculate statistics for {self.name}.")
            return

        for variable, data_df in self.data.items():
            if data_df is not None:
                if variable == 'precipitation' or variable == 'snowfall':
                    self.statistics[variable] = calculate_precipitation_statistics(data_df)
                elif variable == 'cloud_cover':
                    # Convert cloud cover from percentage to octas
                    octas_df = (data_df / 100 * 8).round().astype('Int64')
                    self.statistics[variable] = calculate_octa_probabilities(octas_df)
                elif variable == 'wind_direction_10m':
                    self.statistics[variable] = calculate_wind_direction_probabilities(data_df)
                elif variable == 'weather_code':
                    self.statistics[variable] = calculate_weather_code_probabilities(data_df)
                else:
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
        Exports the calculated statistics to a single CSV file for the model.

        The filename is generated based on the model name and the last run initialization time.
        Each variable's statistics are prefixed with the variable name.

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

        all_stats_df = pd.DataFrame()

        for variable, stats_df in self.statistics.items():
            if stats_df is None:
                print(f"No statistics to export for variable '{variable}'.")
                continue

            # Add prefix to columns to identify the variable
            prefixed_stats_df = stats_df.add_prefix(f"{variable}_")
            
            if all_stats_df.empty:
                all_stats_df = prefixed_stats_df
            else:
                all_stats_df = all_stats_df.join(prefixed_stats_df, how='outer')

        if all_stats_df.empty:
            print(f"No statistics to export for model {self.name}.")
            return

        filename = f"{self.name}_{timestamp_str}.csv"
        filepath = os.path.join(output_dir, filename)

        export_df = all_stats_df.copy()

        if isinstance(export_df.index, pd.DatetimeIndex) and timezone:
            export_df.index = export_df.index.tz_convert(timezone)

        for col in export_df.columns:
            if pd.api.types.is_numeric_dtype(export_df[col]):
                if col.startswith('cloud_cover'):
                    if 'prob' in col:
                        export_df[col] = export_df[col].round(2)
                    else:
                        export_df[col] = export_df[col].round(0).astype('Int64')
                elif 'prob' in col:
                    export_df[col] = export_df[col].round(2)
                elif col.startswith('precipitation') and col.endswith('_probability'):
                    # Round up to the nearest 0.05 for probability
                    export_df[col] = np.ceil(export_df[col] * 20) / 20
                else:
                    export_df[col] = export_df[col].round(1)
        
        try:
            export_df.to_csv(filepath, index=True)
            print(f"Successfully exported statistics to {filepath}")
        except IOError as e:
            print(f"Error exporting statistics to {filepath}: {e}")

    def _save_raw_data_to_db(self, model_name: str, run_timestamp: datetime) -> None:
        """Saves raw forecast data to the database."""
        if not self.data:
            print(f"No raw data to save for model {self.name}.")
            return

        conn = get_db_connection()
        cursor = conn.cursor()

        for variable, data_df in self.data.items():
            if data_df is None:
                continue

            # Melt the DataFrame to long format
            melted_df = data_df.reset_index().melt(
                id_vars=['date'],
                var_name='member',
                value_name='value'
            )
            
            # Remove rows with missing values that would violate the NOT NULL constraint
            melted_df.dropna(subset=['value'], inplace=True)
            
            # Extract member number from the 'member' column
            melted_df['member'] = melted_df['member'].str.extract(r'member(\d+)').fillna('0')

            records = [
                (model_name, run_timestamp.isoformat(), row['member'], variable, row['date'].isoformat(), row['value'])
                for _, row in melted_df.iterrows()
            ]

            cursor.executemany("""
                INSERT INTO raw_forecast_data (model_name, run_timestamp, member, variable, forecast_timestamp, value)
                VALUES (?, ?, ?, ?, ?, ?)
            """, records)

        conn.commit()
        conn.close()
        print(f"Successfully saved raw data to the database for model {self.name}.")

    def _save_statistics_to_db(self, model_name: str, run_timestamp: datetime) -> None:
        """Saves calculated statistics to the database."""
        if not self.statistics:
            print(f"No statistics to save for model {self.name}.")
            return

        conn = get_db_connection()
        cursor = conn.cursor()

        for variable, stats_df in self.statistics.items():
            if stats_df is None or stats_df.empty:
                continue

            # Melt the DataFrame to long format.
            # The index is expected to be a DatetimeIndex named 'date'.
            melted_df = stats_df.reset_index().melt(
                id_vars=['date'],
                var_name='statistic',
                value_name='value'
            )

            # Remove rows with missing values.
            melted_df.dropna(subset=['value'], inplace=True)

            records = [
                (model_name, run_timestamp.isoformat(), variable, row['statistic'], row['date'].isoformat(), row['value'])
                for _, row in melted_df.iterrows()
            ]

            if not records:
                continue

            cursor.executemany("""
                INSERT INTO statistical_forecasts (model_name, run_timestamp, variable, statistic, forecast_timestamp, value)
                VALUES (?, ?, ?, ?, ?, ?)
            """, records)

        conn.commit()
        conn.close()
        print(f"Successfully saved statistics to the database for model {self.name}.")

    @property
    def last_run_time(self) -> Optional[datetime]:
        """Returns the last run initialization time from the model metadata.

        Returns:

            A datetime object representing the last run initialization time, or None if not available.

        """
        if self.metadata:
            return self.metadata.get('last_run_initialisation_time')
        return None
