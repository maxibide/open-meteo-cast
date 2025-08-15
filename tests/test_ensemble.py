import pandas as pd
import numpy as np
from unittest.mock import MagicMock
from src.open_meteo_cast.ensemble import Ensemble

def test_to_html_table_with_data():
    # Create a mock WeatherModel
    mock_model = MagicMock()

    # Create a sample statistics DataFrame
    data = {
        'temperature_2m_median': [10, 12],
        'precipitation_probability': [0.2, 0.5],
        'cloud_cover_octa_1_prob': [0.1, 0.3],
        'cloud_cover_octa_2_prob': [0.9, 0.7],
        'wind_direction_10m_N_prob': [0.8, 0.2],
        'wind_direction_10m_S_prob': [0.2, 0.8],
    }
    index = pd.to_datetime(['2025-08-14 12:00:00', '2025-08-14 13:00:00'], utc=True)
    stats_df = pd.DataFrame(data, index=index)

    # Create a mock Ensemble object
    ensemble = Ensemble(models=[mock_model], config={'location': {'timezone': 'UTC'}, 'forecast_hours': 72})
    ensemble.stats_df = stats_df

    # Generate the HTML table
    html = ensemble.to_html_table(config={'location': {'timezone': 'UTC'}, 'forecast_hours': 72})

    # Assert that the HTML contains expected elements
    assert '<h2>Ensemble Weather Forecast</h2>' in html
    assert '<table' in html
    assert 'Temperature (°C)' in html
    assert '<td>10</td>' in html
    assert '50%' in html
    assert '2/8 (90%)' in html
    assert 'N (80%)' in html
