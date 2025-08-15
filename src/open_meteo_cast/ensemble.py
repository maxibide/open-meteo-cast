from typing import List
import pandas as pd
import os
import logging
from datetime import datetime
from .weather_model import WeatherModel


from .formatting import format_statistics_dataframe
from . import database
import json
import importlib.metadata
from .plotting import generate_plots

class Ensemble:
    """
    A class to combine statistics from multiple WeatherModels into a single ensemble.
    """

    def __init__(self, models: List[WeatherModel], config: dict):
        """
        Initializes the Ensemble object with a list of WeatherModel objects.

        Args:
            models: A list of WeatherModel objects.
            config: The application configuration dictionary.
        """
        self.models = models
        self.runs = [f"{model.name}_{model.metadata.get('last_run_availability_time', 'unknown') if model.metadata else 'unknown'}" for model in self.models]
        logging.info(f"Calculating ensemble from {[model.name for model in self.models]}")
        self.stats_df = self._calculate_ensemble_stats()

        # Trim the dataframe to start from the current hour
        now = pd.Timestamp.now(tz=config['location']['timezone']).floor('h')
        forecast_end = now + pd.Timedelta(hours=config['forecast_hours'])
        self.stats_df = self.stats_df.loc[now:forecast_end]

    def _calculate_ensemble_stats(self) -> pd.DataFrame:
        """
        Calculates the ensemble statistics by averaging the statistics of all models.

        Returns:
            A pandas DataFrame with the ensemble statistics.
        """
        all_models_stats_dfs = []
        for model in self.models:
            if model.statistics:
                # For each model, create a single DataFrame with all its statistics
                single_model_all_stats = pd.DataFrame()
                for variable, stats_df in model.statistics.items():
                    if stats_df is not None:
                        # Ensure the index is a DatetimeIndex
                        if isinstance(stats_df.index, pd.DatetimeIndex):
                        # Ensure all timestamps are timezone-aware and in UTC
                            if stats_df.index.tz is None:
                                # If timezone-naive, localize to UTC. Assuming data is implicitly UTC if no tz
                                stats_df.index = stats_df.index.tz_localize('UTC')
                            else:
                                # If timezone-aware, convert to UTC
                                stats_df.index = stats_df.index.tz_convert('UTC')

                        prefixed_stats_df = stats_df.add_prefix(f"{variable}_")
                        if single_model_all_stats.empty:
                            single_model_all_stats = prefixed_stats_df
                        else:
                            single_model_all_stats = single_model_all_stats.join(prefixed_stats_df, how='outer')
                
                if not single_model_all_stats.empty:
                    all_models_stats_dfs.append(single_model_all_stats)

        if not all_models_stats_dfs:
            return pd.DataFrame()

        # Concatenate all models' statistics and calculate the mean
        combined_stats = pd.concat(all_models_stats_dfs)
        ensemble_stats = combined_stats.groupby(combined_stats.index).mean()
        return ensemble_stats

    def to_csv(self, output_dir: str, config: dict):
        """
        Exports the ensemble statistics to a CSV file.

        Args:
            output_dir: The directory where the CSV file will be saved.
            config: The application configuration dictionary.
        """
        if self.stats_df.empty:
            logging.warning("No ensemble statistics to export.")
            return

        # Use the current time for the timestamp
        timestamp_str = datetime.now().strftime('%Y%m%dT%H%M%S')
        filename = f"ensemble_{timestamp_str}.csv"
        filepath = os.path.join(output_dir, filename)

        export_df = format_statistics_dataframe(self.stats_df)

        timezone = config.get('location', {}).get('timezone')
        if isinstance(export_df.index, pd.DatetimeIndex) and timezone:
            export_df.index = export_df.index.tz_convert(timezone)

        try:
            export_df.to_csv(filepath, index=True)
            logging.info(f"Successfully exported ensemble statistics to {filepath}")
        except IOError as e:
            logging.error(f"Error exporting ensemble statistics to {filepath}: {e}")

    def get_ensemble_stats(self) -> pd.DataFrame:
        """
        Returns the ensemble statistics DataFrame.

        Returns:
            A pandas DataFrame with the ensemble statistics.
        """
        return self.stats_df

    def save_to_db(self):
        """
        Saves the ensemble statistics to the database.
        """
        if self.stats_df.empty:
            logging.warning("No ensemble statistics to save to the database.")
            return

        conn = database.get_db_connection()
        try:
            # 1. Gather model runs info
            model_runs_info_json = json.dumps(self.runs)

            # 2. Save ensemble run and get the ID
            creation_timestamp = datetime.now()
            version = importlib.metadata.version("open-meteo-cast")
            ensemble_run_id = database.save_ensemble_run(conn, creation_timestamp, model_runs_info_json, version)

            # 3. Save ensemble statistics
            if ensemble_run_id:
                database.save_ensemble_statistics(conn, ensemble_run_id, self.stats_df)
                logging.info(f"Ensemble statistics saved to database with run ID: {ensemble_run_id}")

        except Exception as e:
            conn.rollback()
            logging.error(f"Error saving ensemble statistics to the database: {e}")
        finally:
            conn.close()

    def plot_statistics(self, output_dir: str = 'output', config: dict = {}) -> None:
        """
        Generates and saves a combined plot of the ensemble statistics.
        """
        if self.stats_df.empty:
            logging.warning("No ensemble statistics to plot.")
            return

        # Use the current time for the timestamp
        timestamp_str = datetime.now().strftime('%Y%m%dT%H%M%S')

        # Ensure the index is a DatetimeIndex for plotting
        if not isinstance(self.stats_df.index, pd.DatetimeIndex):
            self.stats_df.index = pd.to_datetime(self.stats_df.index)

        # Convert index to local timezone if specified in config
        timezone = config.get('location', {}).get('timezone')
        if timezone:
            self.stats_df.index = self.stats_df.index.tz_convert(timezone)

        generate_plots(self.stats_df, "ensemble", output_dir, config, timestamp_str)

    def _format_cloud_cover(self, row):
        # Extract cloud cover probabilities
        cloud_cover_probs = {col: row[col] for col in row.index if 'cloud_cover_octa_' in col}
        if not cloud_cover_probs:
            return ""
        
        # Sort by probability
        sorted_probs = sorted(cloud_cover_probs.items(), key=lambda item: item[1], reverse=True)
        
        # Check if the highest probability is > 70%
        if sorted_probs[0][1] > 0.7:
            octa = sorted_probs[0][0].split('_')[-2]
            prob = sorted_probs[0][1]
            return f"{octa}/8 ({prob:.0%})"
        else:
            # Return the two most probable
            return_str = []
            for i in range(min(2, len(sorted_probs))):
                octa = sorted_probs[i][0].split('_')[-2]
                prob = sorted_probs[i][1]
                return_str.append(f"{octa}/8 ({prob:.0%})")
            return " ".join(return_str)

    def _format_wind_direction(self, row):
        # Extract wind direction probabilities
        wind_dir_probs = {col: row[col] for col in row.index if 'wind_direction_10m_' in col and col.endswith('_prob')}
        if not wind_dir_probs:
            return ""

        # Sort by probability
        sorted_probs = sorted(wind_dir_probs.items(), key=lambda item: item[1], reverse=True)

        # Check if the highest probability is > 70%
        if sorted_probs[0][1] > 0.7:
            direction = sorted_probs[0][0].split('_')[-2]
            prob = sorted_probs[0][1]
            return f"{direction} ({prob:.0%})"
        else:
            # Return the two most probable
            return_str = []
            for i in range(min(2, len(sorted_probs))):
                direction = sorted_probs[i][0].split('_')[-2]
                prob = sorted_probs[i][1]
                return_str.append(f"{direction} ({prob:.0%})")
            return " ".join(return_str)

    def to_html_table(self, config: dict) -> str:
        """
        Generates a simple HTML table from the ensemble statistics.
        """
        if self.stats_df.empty:
            return "<p>No ensemble statistics to display.</p>"

        df = self.stats_df.copy()

        # 1. Select and rename columns
        columns_to_display = {
            'temperature_2m_median': 'Temperature (°C)',
            'dew_point_2m_median': 'Dew Point (°C)',
            'pressure_msl_median': 'Pressure (hPa)',
            'temperature_850hPa_median': 'Temp 850hPa (°C)',
            'precipitation_probability': 'Precip. Prob.',
            'precipitation_conditional_average': 'Precip. Avg (mm)',
            'wind_speed_10m_median': 'Wind Speed (km/h)',
            'wind_gusts_10m_median': 'Wind Gusts (km/h)',
            'cape_median': 'CAPE (J/kg)',
            'weather_code_Fog_prob': 'Fog Prob.',
            'weather_code_Storm_prob': 'Storm Prob.',
            'weather_code_Severe_Storm_prob': 'Severe Storm Prob.'
        }
        
        # Filter for existing columns
        existing_columns = {k: v for k, v in columns_to_display.items() if k in df.columns}
        display_df = df[list(existing_columns.keys())].rename(columns=existing_columns)

        # 2. Process cloud cover and wind direction
        display_df['Cloud Cover'] = df.apply(self._format_cloud_cover, axis=1)
        display_df['Wind Direction'] = df.apply(self._format_wind_direction, axis=1)
        
        # Reorder columns to match user request
        final_columns = [
            'Temperature (°C)', 'Dew Point (°C)', 'Pressure (hPa)', 'Temp 850hPa (°C)',
            'Precip. Prob.', 'Precip. Avg (mm)', 'Cloud Cover', 'Wind Speed (km/h)',
            'Wind Gusts (km/h)', 'Wind Direction', 'CAPE (J/kg)', 'Fog Prob.', 'Storm Prob.', 'Severe Storm Prob.'
        ]
        
        # Filter for columns that were actually created
        final_columns_existing = [col for col in final_columns if col in display_df.columns]
        display_df = display_df[final_columns_existing]

        # 3. Format probabilities as percentages
        for col in ['Precip. Prob.', 'Fog Prob.', 'Storm Prob.', 'Severe Storm Prob.']:
            if col in display_df.columns:
                display_df[col] = display_df[col].map('{:.0%}'.format)
        
        # 4. Convert index to local timezone
        timezone = config.get('location', {}).get('timezone')
        if isinstance(display_df.index, pd.DatetimeIndex) and timezone:
            display_df.index = display_df.index.tz_convert(timezone)

        # 5. Generate HTML
        html_table = display_df.to_html(
            classes='table table-striped table-hover',
            border=0,
            float_format='{:.1f}'.format
        )
        
        # Add some basic styling
        html_string = f"""
        <html>
        <head>
        <title>Ensemble Weather Forecast</title>
        <style>
            body {{ font-family: sans-serif; }}
            table {{ border-collapse: collapse; width: 100%; }}
            th, td {{ padding: 8px; text-align: left; border-bottom: 1px solid #ddd; }}
            th {{ background-color: #f2f2f2; }}
        </style>
        </head>
        <body>
        <h2>Ensemble Weather Forecast</h2>
        {html_table}
        </body>
        </html>
        """
        return html_string