# Open-Meteo-Cast

Open-Meteo-Cast is a powerful and flexible tool that downloads data from multiple global weather models and combines them to create a single, unified **ensemble forecast** for any location on Earth. By averaging the statistics from different models (like GFS and ECMWF), it produces a more robust and reliable probabilistic forecast, offering deep insights into forecast uncertainty.

This tool is ideal for weather enthusiasts, data analysts, and developers who need reliable, automated weather forecasts.

## Key Features

*   **Unified Ensemble Forecast**: Creates a single, unified forecast by combining and averaging data from all available global models (e.g., GFS, ECMWF) for greater accuracy.
*   **Comprehensive Weather Variables**: Supports a wide range of variables, including temperature, precipitation, cloud cover, wind dynamics, and more.
*   **Advanced Statistical Analysis**:
    *   Calculates **percentiles (p10, median, p90)** for continuous variables to represent the range of possible outcomes.
    *   Provides **probability of precipitation** and conditional averages.
    *   Computes detailed **cloud cover probabilities** in octas.
    *   Analyzes wind direction probabilities across 8 octants.
    *   Groups complex **weather codes** into clear, probabilistic categories: **Fog**, **Storm**, and **Severe Storm**.
*   **Automated Updates**: Checks for new model runs to ensure forecasts are always based on the latest data.
*   **Data Persistence**: Stores all downloaded model data and calculated statistics in a local SQLite database, ensuring data is not lost and can be re-used without re-downloading.
*   **User-Friendly Output**: Consolidates the full statistical forecast into a single, clean CSV file, with timestamps automatically converted to the user's local timezone.
*   **HTML Report**: Generates a clean, easy-to-read HTML table of the forecast, perfect for quick viewing in a browser.

## Usage

1.  **Installation**:
    ```bash
    poetry install
    ```

2.  **Configuration**:
    To change the location or the models, edit the `resources/default_config.yaml` file directly.
    ```yaml
    location:
      latitude: 52.52
      longitude: 13.41
      timezone: "Europe/Berlin"

    models:
      - "gfs025"
      - "ecmwf_ifs025"
    ```

3.  **Run the tool**:
    ```bash
    poetry run open-meteo-cast
    ```

## Output

The primary output is the unified ensemble forecast, saved in the `output/` directory in two formats:

*   `ensemble_{timestamp}.csv`: A detailed CSV file containing the full statistical ensemble forecast.
*   `ensemble_forecast.html`: A user-friendly HTML table summarizing the key forecast variables.

The columns in the CSV are prefixed with the variable name (e.g., `temperature_2m_p10`, `precipitation_probability`, `cloud_cover_octa_3_prob`). This format provides a comprehensive view of the forecast, ready for analysis or visualization.

### Example CSV Output

```
date,temperature_2m_p10,temperature_2m_median,temperature_2m_p90,precipitation_probability,precipitation_conditional_average
2025-08-09 12:00:00,10.5,12.1,13.5,0.1,0.5
2025-08-09 13:00:00,10.8,12.5,13.9,0.15,0.6
2025-08-09 14:00:00,11.2,12.9,14.3,0.2,0.7
```
