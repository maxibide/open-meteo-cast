# open-meteo-cast
This project, `open-meteo-cast`, is a tool designed to generate probabilistic weather forecasts for specific locations defined by latitude and longitude. It fetches data from ensemble weather models via the Open-Meteo API, calculates key statistical metrics, and exports the results in a user-friendly format.

The key features include:
*   Fetching data for multiple ensemble models (e.g., GFS, ECMWF).
*   Support for multiple weather variables, including temperature, dew point, pressure, precipitation, snowfall, cloud cover, wind speed, wind gusts and CAPE.
*   Calculating percentiles (p10, median, p90) and other key metrics to represent forecast uncertainty.
*   Checking for new model runs to ensure the data is always up-to-date.
*   Consolidating and exporting the statistical forecast into a single CSV file, with timestamps converted to the user's local timezone for convenience.