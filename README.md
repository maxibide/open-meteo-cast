# Open-Meteo-Cast

Open-Meteo-Cast is a powerful and flexible tool for generating probabilistic weather forecasts for any location on Earth. It leverages ensemble weather models from the Open-Meteo API to produce detailed statistical forecasts, providing insights into forecast uncertainty.

This tool is ideal for weather enthusiasts, data analysts, and developers who need reliable, automated weather forecasts.

## Key Features

*   **Ensemble-Based Forecasts**: Fetches data from multiple global ensemble models (e.g., GFS, ECMWF).
*   **Comprehensive Weather Variables**: Supports a wide range of variables, including temperature, precipitation, cloud cover, wind dynamics, and more.
*   **Advanced Statistical Analysis**:
    *   Calculates **percentiles (p10, median, p90)** for continuous variables to represent the range of possible outcomes.
    *   Provides **probability of precipitation** and conditional averages.
    *   Computes detailed **cloud cover probabilities** in octas.
    *   Analyzes wind direction probabilities across 8 octants.
    *   Groups complex **weather codes** into clear, probabilistic categories: **Fog**, **Storm**, and **Severe Storm**.
*   **Automated Updates**: Checks for new model runs to ensure forecasts are always based on the latest data.
*   **User-Friendly Output**: Consolidates the full statistical forecast into a single, clean CSV file, with timestamps automatically converted to the user's local timezone.

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

The tool will generate a CSV file in the `output/` directory for each model run (e.g., `gfs025_20250719T120000.csv`).

The columns are prefixed with the variable name (e.g., `temperature_2m_p10`, `precipitation_probability`, `cloud_cover_octa_3_prob`). This format provides a comprehensive view of the forecast, ready for analysis or visualization.
