from typing import List
import pandas as pd
import numpy as np
import os
from datetime import datetime
from .weather_model import WeatherModel


from .formatting import format_statistics_dataframe

class Ensemble:
    """
    A class to combine statistics from multiple WeatherModels into a single ensemble.
    """

    def __init__(self, models: List[WeatherModel]):
        """
        Initializes the Ensemble object with a list of WeatherModel objects.

        Args:
            models: A list of WeatherModel objects.
        """
        self.models = models
        self.stats_df = self._calculate_ensemble_stats()

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
            print("No ensemble statistics to export.")
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
            print(f"Successfully exported ensemble statistics to {filepath}")
        except IOError as e:
            print(f"Error exporting ensemble statistics to {filepath}: {e}")

    def get_ensemble_stats(self) -> pd.DataFrame:
        """
        Returns the ensemble statistics DataFrame.

        Returns:
            A pandas DataFrame with the ensemble statistics.
        """
        return self.stats_df
