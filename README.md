# open-meteo-cast
This project, `open-meteo-cast`, is a tool designed to generate probabilistic weather forecasts for specific locations defined by latitude and longitude. It fetches data from ensemble weather models via the Open-Meteo API, calculates key statistical metrics, and exports the results in a user-friendly format.

The key features include:
*   Fetching data for multiple ensemble models (e.g., GFS, ECMWF).
*   Calculating percentiles (p10, median, p90) to represent the forecast uncertainty.
*   Checking for new model runs to ensure the data is always up-to-date.
*   Exporting the statistical forecast to a CSV file, with timestamps converted to the user's local timezone for convenience.
