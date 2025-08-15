from typing import Any, Dict, Optional
import os
import logging
from datetime import datetime, timedelta
import pandas as pd
import sqlite3
import importlib.metadata
from .database import get_db_connection, get_last_run_timestamp, load_raw_data, load_statistics, save_forecast_run, save_raw_data, save_statistics
from .open_meteo_api import retrieve_model_metadata, retrieve_model_variable
from .statistics import calculate_percentiles, calculate_precipitation_statistics, calculate_octa_probabilities, calculate_wind_direction_probabilities, calculate_weather_code_probabilities

from .formatting import format_statistics_dataframe

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
        logging.info(f"--- Processing model: {model_name} ---")
        self.name = model_name
        self.is_valid = False
        self.is_new = False
        self.metadata_url = config.get('api', {}).get('open-meteo', {}).get('ensemble_metadata', {}).get(model_name)
        self.metadata = retrieve_model_metadata(self.metadata_url)
        self.print_metadata()
        self.data: Dict[str, Optional[pd.DataFrame]] = {}
        self.statistics: Dict[str, Optional[pd.DataFrame]] = {}

        if not self.check_if_new():
            self.load_from_db()
            if self.data: # Verify successful model load
                self.is_valid = True

        else:
            if self.metadata is None:
                logging.error(f"Error: Metadata not available for {self.name}. Cannot process new run.")
                return

            last_run_availability_time = self.metadata.get('last_run_availability_time')
            if last_run_availability_time is None:
                logging.error(f"Error: last_run_availability_time not available for {self.name}. Cannot process new run.")
                return

            if datetime.now() - last_run_availability_time < timedelta(minutes=10):
                logging.warning(f"Last run for {self.name} was available less than 10 minutes ago.")
                logging.warning("To ensure data integrity, please wait a few more minutes before downloading.")
                self.load_from_db()
                if self.data: # Verify successful model load
                    self.is_valid = True
            else:
                last_run_initialisation_time = self.metadata.get('last_run_initialisation_time')
                if last_run_initialisation_time is None:
                    logging.error(f"Error: last_run_initialisation_time not available for {self.name}. Skipping.")
                    return

                if datetime.now() - last_run_initialisation_time > timedelta(days=1):
                    logging.warning(f"Last run for {self.name} too old. Skipping.")
                else:
                    self.retrieve_data(config)
                    self.calculate_statistics()
                    if self.data: # Verify successful model load
                        self.is_valid = True
                        self.is_new = True # Indicate this is a recent downloaded run
                    self.save_to_db()

    def check_if_new(self) -> bool:
        """
        Checks if the model run is newer than the last recorded run in the database.
        """
        if self.metadata is None:
            logging.error(f"Error: Metadata not available for {self.name}. Cannot check for new run.")
            return False

        current_run_time = self.metadata.get('last_run_initialisation_time')
        if current_run_time is None:
            logging.error(f"Error: Could not determine current run time for {self.name}.")
            return False

        last_run_from_db = get_last_run_timestamp(self.name)

        if last_run_from_db is None or current_run_time > last_run_from_db:
            logging.info(f"New model run detected for {self.name}.")
            return True
        
        logging.info(f"No new model run for {self.name}. Loading from database.")
        return False

    def load_from_db(self) -> None:
        """
        Loads the latest available model data and statistics from the database.
        """
        logging.info(f"Loading data from database for model {self.name}...")
        last_run_timestamp = get_last_run_timestamp(self.name)
        if last_run_timestamp is None:
            logging.warning(f"No data found in database for model {self.name}.")
            return

        self.data = load_raw_data(self.name, last_run_timestamp)
        self.statistics = load_statistics(self.name, last_run_timestamp)

        logging.info(f"Data for model {self.name} (run: {last_run_timestamp}) loaded successfully from database.")

    def print_metadata(self) -> None:
        """Formats and prints dictionary with model metadata."""
        logging.info(f"Name: {self.name}")
        if self.metadata is None:
            logging.error(f"Error: Metadata not available for {self.name}.")
            return
        for key, value in self.metadata.items():
            logging.info(f"{key}: {value}")

    def retrieve_data(self, config: Dict[str, Any]) -> None:
        """
        Retrieves the weather model data using the provided configuration.

        Args:

            config: The application configuration dictionary.

        """
        logging.info(f"Retrieving data for {self.name}")
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
            logging.info(f"Data for {variable}:")
            if data_df is not None:
                logging.info(data_df)
            else:
                logging.warning(f"No data available for {variable}.")

    def calculate_statistics(self) -> None:
        """
        Calculates row-wise statistics from the retrieved data
        and stores them in self.statistics.
        For the 'precipitation' variable, it calculates the probability of precipitation
        and the conditional average. For all other variables, it calculates
        p10, median, and p90 percentiles.
        """
        if not self.data:
            logging.error(f"Error: No data available to calculate statistics for {self.name}.")
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
                logging.warning(f"Warning: No data for variable '{variable}' to calculate statistics.")
            
    def print_statistics(self) -> None:
        """
        Prints the calculated statistics.
        """
        if self.statistics is not None:
            for variable in self.statistics.keys():
                logging.info(f"Statistics for {self.name} {variable}:")
                logging.info(self.statistics[variable])
        else:
            logging.warning(f"No statistics available for {self.name}.")

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
            logging.warning(f"No statistics available to export for {self.name}.")
            return

        if self.metadata is None:
            logging.error(f"Error: Metadata not available for {self.name}. Cannot export statistics.")
            return

        last_run = self.metadata.get('last_run_initialisation_time')
        if last_run is None:
            logging.error(f"Error: Cannot determine last run time for {self.name}. Cannot export statistics.")
            return

        timestamp_str = last_run.strftime('%Y%m%dT%H%M%S')
        timezone = config.get('location', {}).get('timezone')

        all_stats_df = pd.DataFrame()

        for variable, stats_df in self.statistics.items():
            if stats_df is None:
                logging.warning(f"No statistics to export for variable '{variable}'.")
                continue

            # Add prefix to columns to identify the variable
            prefixed_stats_df = stats_df.add_prefix(f"{variable}_")
            
            if all_stats_df.empty:
                all_stats_df = prefixed_stats_df
            else:
                all_stats_df = all_stats_df.join(prefixed_stats_df, how='outer')

        if all_stats_df.empty:
            logging.warning(f"No statistics to export for model {self.name}.")
            return

        filename = f"{self.name}_{timestamp_str}.csv"
        filepath = os.path.join(output_dir, filename)

        export_df = format_statistics_dataframe(all_stats_df)

        if isinstance(export_df.index, pd.DatetimeIndex) and timezone:
            export_df.index = export_df.index.tz_convert(timezone)
        
        try:
            export_df.to_csv(filepath, index=True)
            logging.info(f"Successfully exported statistics to {filepath}")
        except IOError as e:
            logging.error(f"Error exporting statistics to {filepath}: {e}")

    def save_to_db(self) -> None:
        """Saves the model's raw data and statistics to the database."""
        if self.metadata is None:
            logging.error(f"Error: Metadata not available for {self.name}. Cannot save to database.")
            return

        last_run = self.metadata.get('last_run_initialisation_time')
        if last_run is None:
            logging.error(f"Error: Cannot determine last run time for {self.name}. Cannot save to database.")
            return

        conn = get_db_connection()
        try:
            version = importlib.metadata.version("open-meteo-cast")
            conn.execute("BEGIN")
            save_forecast_run(conn, self.name, last_run, version)
            save_raw_data(conn, self.name, last_run, self.data)
            save_statistics(conn, self.name, last_run, self.statistics)
            conn.commit()
        except sqlite3.IntegrityError:
            conn.rollback()
            logging.warning(f"Data for model {self.name} and run {last_run} already exists in the database. Skipping.")
        except Exception as e:
            conn.rollback()
            logging.error(f"An error occurred while saving data to the database for {self.name}: {e}")
        finally:
            conn.close()