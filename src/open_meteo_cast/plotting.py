import matplotlib.pyplot as plt
import pandas as pd
import os
import logging
from typing import Dict, Any, Optional

def plot_percentiles(ax: plt.Axes, df: pd.DataFrame, variable_name: str):
    """Plots median, 10th, and 90th percentiles for a given variable."""
    if df.empty:
        logging.warning(f"No data to plot for {variable_name} percentiles.")
        return

    median_col = f"{variable_name}_median"
    p10_col = f"{variable_name}_p10"
    p90_col = f"{variable_name}_p90"

    if median_col in df.columns and p10_col in df.columns and p90_col in df.columns:
        ax.plot(df.index, df[median_col], color='black', label='Median')
        ax.fill_between(df.index, df[p10_col], df[p90_col], color='gray', alpha=0.3, label='P10-P90 Range')
        ax.set_title(f'{variable_name.replace("_", " ").title()} Forecast')
        ax.set_ylabel(f'{variable_name.replace("_", " ").title()}')
        ax.legend()
        ax.grid(True, linestyle='--', alpha=0.6)
        if variable_name == 'temperature_2m':
            # ax.set_ylim(-10, 45)
            pass
        elif variable_name == 'dew_point_2m':
            # ax.set_ylim(-10, 30)
            pass
        elif variable_name == 'temperature_850hPa':
            # ax.set_ylim(-10, 30)
            ymin, ymax = ax.get_ylim()
            ax.fill_between(df.index, 20, 50, color='red', alpha=0.3, zorder=0)
            ax.fill_between(df.index, 15, 20, color='red', alpha=0.1, zorder=0)
            ax.fill_between(df.index, -10, 0, color='blue', alpha=0.3, zorder=0)
            ax.fill_between(df.index, 0, 5, color='blue', alpha=0.1, zorder=0)
            ax.set_ylim(ymin, ymax)
        elif variable_name == 'cape':
            # ax.set_ylim(0, 4000)
            ymin, ymax = ax.get_ylim()
            ax.fill_between(df.index, 800, 1200, color='yellow', alpha=0.3, zorder=0)
            ax.fill_between(df.index, 1200, 2000, color='orange', alpha=0.3, zorder=0)
            ax.fill_between(df.index, 2000, 10000, color='red', alpha=0.3, zorder=0)
            ax.set_ylim(ymin, ymax)
        elif variable_name == 'wind_speed_10m' or variable_name == 'wind_gusts_10m':
            # ax.set_ylim(0,150)
            ymin, ymax = ax.get_ylim()
            ax.fill_between(df.index, 65, 90, color='yellow', alpha=0.3, zorder=0)
            ax.fill_between(df.index, 90, 110, color='orange', alpha=0.3, zorder=0)
            ax.fill_between(df.index, 110, 350, color='red', alpha=0.3, zorder=0)
            ax.set_ylim(ymin, ymax)
    else:
        logging.warning(f"Missing percentile columns for {variable_name}. Expected: {median_col}, {p10_col}, {p90_col}")

def plot_precipitation_probabilities(ax: plt.Axes, df: pd.DataFrame):
    """Plots precipitation probabilities."""
    if df.empty:
        logging.warning("No data to plot for precipitation probabilities.")
        return

    prob_col = "precipitation_probability"
    if prob_col in df.columns:
        ax.bar(df.index, df[prob_col], width=0.02, color='skyblue', label='Probability')
        ax.set_title('Precipitation Probability')
        ax.set_ylabel('Probability')
        ax.set_ylim(0, 1)
        ax.legend()
        ax.grid(True, linestyle='--', alpha=0.6)
    else:
        logging.warning(f"Missing column for precipitation probability. Expected: {prob_col}")

def plot_precipitation_conditional_average(ax: plt.Axes, df: pd.DataFrame):
    """Plots precipitation conditional average."""
    if df.empty:
        logging.warning("No data to plot for precipitation conditional average.")
        return

    avg_col = "precipitation_conditional_average"
    if avg_col in df.columns:
        ax.bar(df.index, df[avg_col], width=0.02, color='lightcoral', label='Conditional Average')
        ax.set_title('Precipitation Conditional Average')
        ax.set_ylabel('Average (mm)')
        ax.legend()
        ax.grid(True, linestyle='--', alpha=0.6)
    else:
        logging.warning(f"Missing column for precipitation conditional average. Expected: {avg_col}")

def plot_wind_direction_probabilities(ax: plt.Axes, df: pd.DataFrame):
    """Plots wind direction probabilities."""
    if df.empty:
        logging.warning("No data to plot for wind direction probabilities.")
        return

    wind_cols = [col for col in df.columns if col.startswith('wind_direction_10m_') and col.endswith('_prob')]
    if not wind_cols:
        logging.warning("No wind direction probability columns found.")
        return

    # Extract directions from column names (e.g., 'N', 'NE', 'E')
    directions = [col.replace('wind_direction_10m_', '').replace('_prob', '') for col in wind_cols]
    
    # Plot each direction's probability
    for i, col in enumerate(wind_cols):
        ax.plot(df.index, df[col], label=directions[i])
    
    ax.set_title('Wind Direction Probabilities')
    ax.set_ylabel('Probability')
    ax.set_ylim(0, 1)
    ax.legend(loc='center left', bbox_to_anchor=(1, 0.5))
    ax.grid(True, linestyle='--', alpha=0.6)

def plot_cloud_cover_probabilities(ax: plt.Axes, df: pd.DataFrame):
    """Plots cloud cover (octa) probabilities."""
    if df.empty:
        logging.warning("No data to plot for cloud cover probabilities.")
        return

    cloud_cols = [col for col in df.columns if col.startswith('cloud_cover_octa_') and col.endswith('_prob')]
    if not cloud_cols:
        logging.warning("No cloud cover probability columns found.")
        return

    # Extract octas from column names (e.g., '0', '1', ..., '8')
    octas = [col.replace('cloud_cover_octa_', '').replace('_prob', '') for col in cloud_cols]
    
    # Plot each octa's probability
    for i, col in enumerate(cloud_cols):
        ax.plot(df.index, df[col], label=f'{octas[i]}/8')
    
    ax.set_title('Cloud Cover Probabilities')
    ax.set_ylabel('Probability')
    ax.set_ylim(0, 1)
    ax.legend(loc='center left', bbox_to_anchor=(1, 0.5))
    ax.grid(True, linestyle='--', alpha=0.6)

def plot_weather_code_probabilities(ax: plt.Axes, df: pd.DataFrame):
    """Plots weather code probabilities (Fog, Storm, Severe Storm)."""
    if df.empty:
        logging.warning("No data to plot for weather code probabilities.")
        return

    weather_cols = [col for col in df.columns if col.startswith('weather_code_') and col.endswith('_prob')]
    if not weather_cols:
        logging.warning("No weather code probability columns found.")
        return

    # Extract weather types from column names (e.g., 'Fog', 'Storm', 'Severe_Storm')
    weather_types = [col.replace('weather_code_', '').replace('_prob', '') for col in weather_cols]
    
    # Plot each weather type's probability
    for i, col in enumerate(weather_cols):
        ax.plot(df.index, df[col], label=weather_types[i].replace('_', ' '))
    
    ax.set_title('Weather Code Probabilities')
    ax.set_ylabel('Probability')
    ax.set_ylim(0, 1)
    ax.legend()
    ax.grid(True, linestyle='--', alpha=0.6)

def generate_plots(stats_df: pd.DataFrame, name: str, output_dir: str, config: Dict[str, Any], timestamp_str: str):
    """
    Generates a combined plot for all relevant statistics and saves it to a file.
    """
    if stats_df.empty:
        logging.warning(f"No statistics data to generate plots for {name}.")
        return

    # Define variables that have percentiles
    percentile_variables = [
        "temperature_2m", "dew_point_2m", "pressure_msl", "temperature_850hPa",
        "wind_speed_10m", "wind_gusts_10m", "cape"
    ]

    # Determine how many subplots are needed
    num_percentile_plots = sum(1 for var in percentile_variables if f"{var}_median" in stats_df.columns)
    
    # Always include precipitation, wind, cloud, and weather code plots if data exists
    has_precipitation_prob = "precipitation_probability" in stats_df.columns
    has_precipitation_avg = "precipitation_conditional_average" in stats_df.columns
    has_wind_dir_prob = any(col.startswith('wind_direction_10m_') and col.endswith('_prob') for col in stats_df.columns)
    has_cloud_cover_prob = any(col.startswith('cloud_cover_octa_') and col.endswith('_prob') for col in stats_df.columns)
    has_weather_code_prob = any(col.startswith('weather_code_') and col.endswith('_prob') for col in stats_df.columns)

    num_other_plots = (
        (1 if has_precipitation_prob else 0) +
        (1 if has_precipitation_avg else 0) +
        (1 if has_wind_dir_prob else 0) +
        (1 if has_cloud_cover_prob else 0) +
        (1 if has_weather_code_prob else 0)
    )
    
    total_plots = num_percentile_plots + num_other_plots

    if total_plots == 0:
        logging.warning(f"No relevant data columns found to generate any plots for {name}.")
        return

    fig, axes = plt.subplots(total_plots, 1, figsize=(12, 4 * total_plots), sharex=True)
    
    # Ensure axes is always an array, even for a single subplot
    if total_plots == 1:
        axes = [axes]

    plot_idx = 0

    # Plot percentile variables
    for var in percentile_variables:
        if f"{var}_median" in stats_df.columns:
            plot_percentiles(axes[plot_idx], stats_df, var)
            plot_idx += 1

    # Plot precipitation statistics
    if has_precipitation_prob:
        plot_precipitation_probabilities(axes[plot_idx], stats_df)
        plot_idx += 1
    if has_precipitation_avg:
        plot_precipitation_conditional_average(axes[plot_idx], stats_df)
        plot_idx += 1

    # Plot wind direction probabilities
    if has_wind_dir_prob:
        plot_wind_direction_probabilities(axes[plot_idx], stats_df)
        plot_idx += 1

    # Plot cloud cover probabilities
    if has_cloud_cover_prob:
        plot_cloud_cover_probabilities(axes[plot_idx], stats_df)
        plot_idx += 1

    # Plot weather code probabilities
    if has_weather_code_prob:
        plot_weather_code_probabilities(axes[plot_idx], stats_df)
        plot_idx += 1

    plt.tight_layout()
    plt.xticks(rotation=45, ha='right')
    plt.subplots_adjust(right=0.85) # Adjust to make space for legend

    filename = f"{name}_{timestamp_str}.png"
    filepath = os.path.join(output_dir, filename)

    try:
        plt.savefig(filepath, bbox_inches='tight')
        logging.info(f"Successfully exported plot to {filepath}")
    except IOError as e:
        logging.error(f"Error exporting plot to {filepath}: {e}")
    finally:
        plt.close(fig) # Close the figure to free up memory
